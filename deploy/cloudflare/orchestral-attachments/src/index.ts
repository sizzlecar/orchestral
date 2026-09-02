import { createRemoteJWKSet, jwtVerify } from "jose";

const API_PREFIX = "/api/v1/attachments";
const PUBLIC_PREFIX = "/v1/blobs/";
const INTERNAL_PREFIX = "/v1/internal/blobs";
const OBJECT_PREFIX = "orchestral/blobs/";
const MAX_FILE_BYTES = 64 * 1024 * 1024;

export interface Env {
  ATTACHMENT_BUCKET: R2Bucket;
  ATTACHMENT_SIGNING_SECRET?: string;
  INTERNAL_API_TOKEN?: string;
  APP_HOST: string;
  PUBLIC_HOST: string;
  TEAM_DOMAIN: string;
  POLICY_AUD: string;
  ALLOWED_EMAILS: string;
}

const accessJwks = createRemoteJWKSet(
  new URL("https://tiny-resonance-d6a6.cloudflareaccess.com/cdn-cgi/access/certs"),
);

class HttpError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const requestId = request.headers.get("cf-ray")?.split("-")[0] ?? crypto.randomUUID();
    try {
      const url = new URL(request.url);
      const host = url.hostname.toLowerCase();
      let response: Response;
      if (host === env.APP_HOST.toLowerCase()) {
        await authenticate(request, env);
        response = await routeAuthenticated(request, url, env);
      } else if (host === env.PUBLIC_HOST.toLowerCase()) {
        response = url.pathname.startsWith(INTERNAL_PREFIX)
          ? await routeInternal(request, url, env)
          : await routeSignedDownload(request, url, env);
      } else {
        throw new HttpError(421, "wrong_host", "Unexpected request host");
      }
      return secureResponse(response, requestId);
    } catch (error) {
      return secureResponse(errorResponse(error), requestId);
    }
  },
} satisfies ExportedHandler<Env>;

async function authenticate(request: Request, env: Env): Promise<void> {
  const assertion = request.headers.get("cf-access-jwt-assertion");
  if (!assertion) {
    throw new HttpError(401, "access_required", "Cloudflare Access login required");
  }
  try {
    const { payload } = await jwtVerify(assertion, accessJwks, {
      issuer: env.TEAM_DOMAIN,
      audience: env.POLICY_AUD,
    });
    if (payload.type !== "app" || typeof payload.email !== "string") {
      throw new Error("missing user identity");
    }
    const allowed = new Set(
      env.ALLOWED_EMAILS.split(",").map((value) => value.trim().toLowerCase()).filter(Boolean),
    );
    if (!allowed.has(payload.email.toLowerCase())) {
      throw new HttpError(403, "email_not_allowed", "Account is not allowed");
    }
  } catch (error) {
    if (error instanceof HttpError) throw error;
    throw new HttpError(401, "invalid_access_token", "Cloudflare Access token is invalid");
  }
}

export async function routeAuthenticated(
  request: Request,
  url: URL,
  env: Env,
): Promise<Response> {
  if (url.pathname === API_PREFIX) {
    requireMethod(request, "POST");
    return uploadAttachment(request, url, env);
  }
  if (!url.pathname.startsWith(`${API_PREFIX}/`)) {
    throw new HttpError(404, "not_found", "Attachment route not found");
  }
  const relative = url.pathname.slice(API_PREFIX.length + 1);
  const segments = relative.split("/").map(decodeSegment);
  const id = requireBlobId(segments[0] ?? "");
  if (segments.length === 1) {
    if (request.method !== "GET" && request.method !== "HEAD") {
      throw new HttpError(405, "method_not_allowed", "Method not allowed");
    }
    // Keep permanent transcript links on the application origin, but move the
    // actual transfer to the public, capability-protected R2 gateway. Mobile
    // download managers commonly detach a download from the authenticated tab
    // and therefore cannot replay Cloudflare Access identity at this route.
    const signed = await signedAttachment(id, env);
    const location = new URL(signed.agent_url);
    if (url.searchParams.get("preview") === "1") {
      location.searchParams.set("preview", "1");
    }
    return new Response(null, {
      status: 307,
      headers: {
        location: location.toString(),
        "cache-control": "private, no-store",
      },
    });
  }
  if (segments.length === 2 && segments[1] === "sign") {
    requireMethod(request, "POST");
    const object = await env.ATTACHMENT_BUCKET.head(objectKey(id));
    if (!object) throw new HttpError(404, "attachment_not_found", "Attachment not found");
    return jsonResponse(await signedAttachment(id, env));
  }
  throw new HttpError(404, "not_found", "Attachment route not found");
}

async function uploadAttachment(request: Request, url: URL, env: Env): Promise<Response> {
  const fileName = requireFileName(url.searchParams.get("file_name"));
  const mediaType = requireMediaType(url.searchParams.get("media_type"));
  const byteSize = requireIntegerHeader(request, "x-file-size", 1, MAX_FILE_BYTES);
  const sha256 = request.headers.get("x-file-sha256")?.toLowerCase() ?? "";
  if (!/^[0-9a-f]{64}$/.test(sha256)) {
    throw new HttpError(400, "invalid_sha256", "x-file-sha256 must be 64 hexadecimal characters");
  }
  if (!request.body) throw new HttpError(400, "missing_body", "File body is required");
  const bytes = await request.arrayBuffer();
  if (bytes.byteLength !== byteSize) {
    throw new HttpError(422, "size_mismatch", "Uploaded file size does not match x-file-size");
  }
  const actualSha256 = await sha256Hex(bytes);
  if (actualSha256 !== sha256) {
    throw new HttpError(422, "digest_mismatch", "Uploaded file does not match x-file-sha256");
  }
  const id = sha256;
  const object = await env.ATTACHMENT_BUCKET.put(objectKey(id), bytes, {
    httpMetadata: {
      contentType: mediaType,
      contentDisposition: contentDisposition(fileName),
      cacheControl: "private, no-store",
    },
    customMetadata: {
      fileName,
      sha256,
      source: request.headers.get("x-orchestral-source") || "orchestral-pwa",
    },
    storageClass: "Standard",
  });
  const signed = await signedAttachment(id, env);
  return jsonResponse(
    {
      artifact_ref: id,
      file_name: fileName,
      media_type: mediaType,
      byte_size: object.size,
      sha256,
      download_url: `${API_PREFIX}/${id}`,
      agent_url: signed.agent_url,
      expires_at: signed.expires_at,
    },
    201,
  );
}

async function routeInternal(request: Request, url: URL, env: Env): Promise<Response> {
  requireInternalToken(request, env);
  if (url.pathname === INTERNAL_PREFIX) {
    requireMethod(request, "POST");
    const headers = new Headers(request.headers);
    headers.set("x-orchestral-source", "orchestral-host");
    return uploadAttachment(new Request(request, { headers }), url, env);
  }
  if (!url.pathname.startsWith(`${INTERNAL_PREFIX}/`)) {
    throw new HttpError(404, "not_found", "Internal blob route not found");
  }
  const relative = url.pathname.slice(INTERNAL_PREFIX.length + 1);
  const segments = relative.split("/").map(decodeSegment);
  const id = requireBlobId(segments[0] ?? "");
  if (segments.length === 1 && request.method === "DELETE") {
    const existed = (await env.ATTACHMENT_BUCKET.head(objectKey(id))) !== null;
    if (existed) await env.ATTACHMENT_BUCKET.delete(objectKey(id));
    return jsonResponse({ deleted: existed });
  }
  if (segments.length === 1 && request.method === "HEAD") {
    const object = await env.ATTACHMENT_BUCKET.head(objectKey(id));
    if (!object) throw new HttpError(404, "attachment_not_found", "Attachment not found");
    return blobMetadataResponse(id, object, 200, true);
  }
  if (segments.length === 2 && segments[1] === "resolve") {
    requireMethod(request, "POST");
    const object = await env.ATTACHMENT_BUCKET.head(objectKey(id));
    if (!object) throw new HttpError(404, "attachment_not_found", "Attachment not found");
    const signed = await signedAttachment(id, env);
    return jsonResponse({
      artifact_ref: id,
      digest: object.customMetadata?.sha256 ?? id,
      file_name: object.customMetadata?.fileName ?? null,
      media_type: object.httpMetadata?.contentType ?? "application/octet-stream",
      byte_size: object.size,
      uri: signed.agent_url,
      expires_at: signed.expires_at,
    });
  }
  throw new HttpError(404, "not_found", "Internal blob route not found");
}

async function routeSignedDownload(request: Request, url: URL, env: Env): Promise<Response> {
  if (request.method !== "GET" && request.method !== "HEAD") {
    throw new HttpError(405, "method_not_allowed", "Method not allowed");
  }
  if (!url.pathname.startsWith(PUBLIC_PREFIX)) {
    throw new HttpError(404, "not_found", "Attachment route not found");
  }
  const id = requireBlobId(decodeSegment(url.pathname.slice(PUBLIC_PREFIX.length)));
  const capability = url.searchParams.get("capability") ?? "";
  if (!(await verifyCapability(requireSecret(env), id, capability))) {
    throw new HttpError(403, "invalid_capability", "Attachment capability is invalid");
  }
  return streamObject(request, id, env, true);
}

export async function streamObject(
  request: Request,
  id: string,
  env: Env,
  signed: boolean,
): Promise<Response> {
  const rangeRequested = request.headers.has("range");
  const options = rangeRequested ? { range: request.headers } : undefined;
  const object = await env.ATTACHMENT_BUCKET.get(objectKey(id), options);
  if (!object) throw new HttpError(404, "attachment_not_found", "Attachment not found");
  const headers = new Headers();
  object.writeHttpMetadata(headers);
  if (shouldInlinePreview(request.url, object.httpMetadata?.contentType)) {
    headers.set("content-disposition", "inline");
  }
  headers.set("etag", object.httpEtag);
  headers.set("accept-ranges", "bytes");
  headers.set("cache-control", signed ? "private, max-age=3600" : "private, no-store");
  if (rangeRequested) {
    if (!object.range) {
      throw new HttpError(502, "invalid_r2_range", "R2 omitted the requested byte range");
    }
    let offset: number;
    let length: number;
    if ("suffix" in object.range && typeof object.range.suffix === "number") {
      length = Math.min(object.size, object.range.suffix);
      offset = object.size - length;
    } else if ("offset" in object.range) {
      offset = object.range.offset ?? 0;
      length = object.range.length ?? object.size - offset;
    } else {
      throw new HttpError(502, "invalid_r2_range", "R2 returned an invalid byte range");
    }
    if (
      !Number.isSafeInteger(offset) ||
      !Number.isSafeInteger(length) ||
      offset < 0 ||
      length <= 0 ||
      offset + length > object.size
    ) {
      throw new HttpError(502, "invalid_r2_range", "R2 returned an invalid byte range");
    }
    headers.set("content-range", `bytes ${offset}-${offset + length - 1}/${object.size}`);
    headers.set("content-length", String(length));
  } else {
    headers.set("content-length", String(object.size));
  }
  return new Response(request.method === "HEAD" ? null : object.body, {
    status: rangeRequested ? 206 : 200,
    headers,
  });
}

export function shouldInlinePreview(requestUrl: string, mediaType?: string): boolean {
  return (
    new URL(requestUrl).searchParams.get("preview") === "1" &&
    typeof mediaType === "string" &&
    mediaType.toLowerCase().startsWith("image/")
  );
}

async function signedAttachment(id: string, env: Env) {
  const capability = await createCapability(requireSecret(env), id);
  const url = new URL(`https://${env.PUBLIC_HOST}${PUBLIC_PREFIX}${encodeURIComponent(id)}`);
  url.searchParams.set("capability", capability);
  return {
    agent_url: url.toString(),
    expires_at: null,
  };
}

export async function createCapability(secret: string, id: string): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signature = await crypto.subtle.sign(
    "HMAC",
    key,
    new TextEncoder().encode(`orchestral-artifact-v1\n${id}`),
  );
  return base64Url(new Uint8Array(signature));
}

async function verifyCapability(
  secret: string,
  id: string,
  capability: string,
): Promise<boolean> {
  if (!/^[A-Za-z0-9_-]{43}$/.test(capability)) return false;
  const expected = await createCapability(secret, id);
  const left = new TextEncoder().encode(expected);
  const right = new TextEncoder().encode(capability);
  let difference = left.length ^ right.length;
  for (let index = 0; index < left.length; index += 1) {
    difference |= left[index] ^ (right[index] ?? 0);
  }
  return difference === 0;
}

function requireSecret(env: Env): string {
  if (!env.ATTACHMENT_SIGNING_SECRET) {
    throw new HttpError(503, "signing_unavailable", "Attachment signing is not configured");
  }
  return env.ATTACHMENT_SIGNING_SECRET;
}

function objectKey(id: string): string {
  return `${OBJECT_PREFIX}${id}`;
}

function requireBlobId(value: string): string {
  const id = value.toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(id)) {
    throw new HttpError(400, "invalid_attachment_id", "Blob id is invalid");
  }
  return id;
}

function requireInternalToken(request: Request, env: Env): void {
  const configured = env.INTERNAL_API_TOKEN;
  const supplied = request.headers.get("authorization")?.replace(/^Bearer\s+/i, "") ?? "";
  if (!configured || !supplied || !constantTimeEqual(configured, supplied)) {
    throw new HttpError(401, "internal_auth_required", "Internal API authentication required");
  }
}

function constantTimeEqual(left: string, right: string): boolean {
  const leftBytes = new TextEncoder().encode(left);
  const rightBytes = new TextEncoder().encode(right);
  let difference = leftBytes.length ^ rightBytes.length;
  for (let index = 0; index < leftBytes.length; index += 1) {
    difference |= leftBytes[index] ^ (rightBytes[index] ?? 0);
  }
  return difference === 0;
}

async function sha256Hex(bytes: ArrayBuffer): Promise<string> {
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
  return Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function blobMetadataResponse(id: string, object: R2Object, status: number, head: boolean): Response {
  const body = {
    artifact_ref: id,
    digest: object.customMetadata?.sha256 ?? id,
    file_name: object.customMetadata?.fileName ?? null,
    media_type: object.httpMetadata?.contentType ?? "application/octet-stream",
    byte_size: object.size,
  };
  const response = jsonResponse(body, status);
  return head ? new Response(null, { status, headers: response.headers }) : response;
}

function requireFileName(value: string | null): string {
  const name = (value ?? "").normalize("NFKC").trim();
  if (!name || name.length > 255 || /[\u0000-\u001f\u007f]/.test(name)) {
    throw new HttpError(400, "invalid_file_name", "File name is invalid");
  }
  return name;
}

function requireMediaType(value: string | null): string {
  const mediaType = (value || "application/octet-stream").trim().toLowerCase();
  if (mediaType.length > 160 || !/^[a-z0-9!#$&^_.+-]+\/[a-z0-9!#$&^_.+-]+$/.test(mediaType)) {
    throw new HttpError(400, "invalid_media_type", "Media type is invalid");
  }
  return mediaType;
}

function requireIntegerHeader(
  request: Request,
  name: string,
  minimum: number,
  maximum: number,
): number {
  const raw = request.headers.get(name) ?? "";
  if (!/^[0-9]+$/.test(raw)) throw new HttpError(400, "invalid_size", `${name} is invalid`);
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new HttpError(413, "file_too_large", `File must be at most ${maximum} bytes`);
  }
  return value;
}

function contentDisposition(fileName: string): string {
  const fallback = fileName.replace(/[^A-Za-z0-9._-]+/g, "_").slice(0, 120) || "attachment";
  return `attachment; filename="${fallback}"; filename*=UTF-8''${encodeURIComponent(fileName)}`;
}

function requireMethod(request: Request, method: string): void {
  if (request.method !== method) throw new HttpError(405, "method_not_allowed", "Method not allowed");
}

function decodeSegment(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    throw new HttpError(400, "invalid_path", "Attachment path is invalid");
  }
}

function base64Url(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

function errorResponse(error: unknown): Response {
  if (error instanceof HttpError) {
    return jsonResponse({ code: error.code, message: error.message }, error.status);
  }
  console.error("attachment worker failed", error);
  return jsonResponse({ code: "internal_error", message: "Attachment service failed" }, 500);
}

function secureResponse(response: Response, requestId: string): Response {
  const headers = new Headers(response.headers);
  headers.set("x-content-type-options", "nosniff");
  headers.set("referrer-policy", "no-referrer");
  headers.set("x-request-id", requestId);
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}
