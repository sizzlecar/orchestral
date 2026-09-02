import { describe, expect, it } from "vitest";
import {
  createCapability,
  routeAuthenticated,
  shouldInlinePreview,
  streamObject,
  type Env,
} from "../src/index";

const ARTIFACT_ID = "a".repeat(64);

function env(bucket: R2Bucket = {} as R2Bucket): Env {
  return {
    ATTACHMENT_BUCKET: bucket,
    ATTACHMENT_SIGNING_SECRET: "test-secret",
    INTERNAL_API_TOKEN: "internal-token",
    APP_HOST: "orchestral.example",
    PUBLIC_HOST: "files.example",
    TEAM_DOMAIN: "https://access.example",
    POLICY_AUD: "audience",
    ALLOWED_EMAILS: "user@example.com",
  };
}

describe("attachment URL capabilities", () => {
  it("are deterministic and bind the immutable artifact id", async () => {
    const first = await createCapability("test-secret", "artifact-a");
    const replay = await createCapability("test-secret", "artifact-a");
    const changed = await createCapability("test-secret", "artifact-b");
    expect(first).toBe(replay);
    expect(first).not.toBe(changed);
    expect(first).toMatch(/^[A-Za-z0-9_-]{43}$/);
  });
});

describe("authenticated image preview", () => {
  it("uses inline disposition only for an explicit image preview", () => {
    expect(
      shouldInlinePreview(
        "https://orchestral.example/api/v1/attachments/abc?preview=1",
        "image/png",
      ),
    ).toBe(true);
    expect(
      shouldInlinePreview(
        "https://orchestral.example/api/v1/attachments/abc",
        "image/png",
      ),
    ).toBe(false);
    expect(
      shouldInlinePreview(
        "https://orchestral.example/api/v1/attachments/abc?preview=1",
        "application/pdf",
      ),
    ).toBe(false);
  });

  it("redirects stable transcript URLs to the capability-protected R2 gateway", async () => {
    const preview = await routeAuthenticated(
      new Request(`https://orchestral.example/api/v1/attachments/${ARTIFACT_ID}?preview=1`),
      new URL(`https://orchestral.example/api/v1/attachments/${ARTIFACT_ID}?preview=1`),
      env(),
    );
    expect(preview.status).toBe(307);
    const previewLocation = new URL(preview.headers.get("location") ?? "");
    expect(previewLocation.host).toBe("files.example");
    expect(previewLocation.pathname).toBe(`/v1/blobs/${ARTIFACT_ID}`);
    expect(previewLocation.searchParams.get("preview")).toBe("1");
    expect(previewLocation.searchParams.get("capability")).toMatch(/^[A-Za-z0-9_-]{43}$/);

    const download = await routeAuthenticated(
      new Request(`https://orchestral.example/api/v1/attachments/${ARTIFACT_ID}`),
      new URL(`https://orchestral.example/api/v1/attachments/${ARTIFACT_ID}`),
      env(),
    );
    const downloadLocation = new URL(download.headers.get("location") ?? "");
    expect(downloadLocation.host).toBe("files.example");
    expect(downloadLocation.searchParams.has("preview")).toBe(false);
  });
});

describe("R2 byte range responses", () => {
  function bucketWithRange(range: R2Range | undefined): R2Bucket {
    const bytes = new Uint8Array([1, 2, 3, 4]);
    return {
      get: async () => ({
        body: new Blob([bytes]).stream(),
        size: bytes.byteLength,
        range,
        httpEtag: '"etag"',
        httpMetadata: {
          contentType: "image/png",
          contentDisposition: 'attachment; filename="test.png"',
        },
        writeHttpMetadata(headers: Headers) {
          headers.set("content-type", "image/png");
          headers.set("content-disposition", 'attachment; filename="test.png"');
        },
      }),
    } as unknown as R2Bucket;
  }

  it("returns 200 without Content-Range when the client did not request a range", async () => {
    // R2 may expose an empty range-shaped value even for a full object. The
    // HTTP response must follow the request, not the incidental object shape.
    const response = await streamObject(
      new Request(`https://files.example/v1/blobs/${ARTIFACT_ID}`, { method: "HEAD" }),
      ARTIFACT_ID,
      env(bucketWithRange({ suffix: undefined } as unknown as R2Range)),
      true,
    );
    expect(response.status).toBe(200);
    expect(response.headers.get("content-range")).toBeNull();
    expect(response.headers.get("content-length")).toBe("4");
  });

  it("returns a valid 206 only for an explicit client range", async () => {
    const response = await streamObject(
      new Request(`https://files.example/v1/blobs/${ARTIFACT_ID}`, {
        headers: { range: "bytes=1-2" },
      }),
      ARTIFACT_ID,
      env(bucketWithRange({ offset: 1, length: 2 })),
      true,
    );
    expect(response.status).toBe(206);
    expect(response.headers.get("content-range")).toBe("bytes 1-2/4");
    expect(response.headers.get("content-length")).toBe("2");
  });
});
