// Run from the repository root with Playwright on NODE_PATH (see the PWA README).
const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");
const assert = require("node:assert/strict");
const { createHash } = require("node:crypto");
const { chromium } = require("playwright");
const dist = path.join(process.cwd(), "web/orchestral-web/dist");
const width = Number(process.env.PWA_SMOKE_WIDTH || 390);
const now = Date.now();
const png = Buffer.from("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=", "base64");
const digest = createHash("sha256").update(png).digest("hex");
const artifact = {
  artifact_ref: digest, file_name: "photo.png", media_type: "image/png",
  byte_size: png.length, sha256: digest, download_url: `/api/v1/attachments/${digest}`,
};
const content = (text) => [{ body: { kind: "inline", value: text } }];
const calls = [], runs = new Map(), streams = new Set();
let uploadCount = 0, viewReads = 0, nativeEcho = false;
const summary = () => ({
  connector_id: "fixture/local", session_id: "images", title: "图片回显验证",
  state: "idle", created_at_unix_ms: now - 60000, updated_at_unix_ms: now,
  cwd: "/workspace/demo",
});
const inputContent = (input, signature) => [
  ...content(`${input}\n\n附件（内容已由 Host 按 SHA-256 校验）：\n1. photo.png（image/png，${png.length} bytes，sha256 ${digest}）`),
  {
    media_type: "image/png",
    body: { kind: "artifact", value: { artifact_ref: digest, digest } },
    ...(signature ? { access: { uri: `https://files.example/image?signature=${signature}` } } : {}),
  },
];
const view = (run) => ({
  execution: { run_id: run.id, session_id: "images" },
  state: { state: "running" }, last_run_seq: 2, pending_requests: [],
  input: inputContent(run.input, ++viewReads), created_at_unix_ms: run.created,
});
const record = (run, seq, type) => ({ event: {
  run_seq: seq, event_id: `${run.id}-${type}`, payload: {
    type, ...(type === "input_committed" ? { content: inputContent(run.input) } : {}),
  },
} });
const json = (res, data, status = 200) => {
  res.writeHead(status, { "content-type": "application/json", "cache-control": "no-store" });
  res.end(JSON.stringify(data));
};
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://127.0.0.1");
  if (url.pathname.startsWith("/api/v1/")) {
    const route = url.pathname.slice("/api/v1".length);
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const raw = Buffer.concat(chunks);
    if (route === "/attachments" && req.method === "POST") {
      assert.deepEqual(raw, png, "upload must contain the selected image bytes");
      assert.equal(req.headers["x-file-sha256"], digest);
      uploadCount++;
      return json(res, artifact);
    }
    if (route.startsWith("/attachments/")) {
      res.writeHead(200, { "content-type": "image/png" });
      return res.end(png);
    }
    const body = raw.length ? JSON.parse(raw.toString()) : {};
    if (route === "/me") return json(res, { auth_mode: "gateway_jwt" });
    if (route === "/devices" || route === "/sessions") return json(res, []);
    if (route === "/agent-connectors") return json(res, [{
      connector_id: "fixture/local", display_name: "Image Agent", agent_family: "fixture",
      capabilities: { list: true, read: true, create: true },
    }]);
    if (route === "/agent-sessions") return json(res, { sessions: [summary()] });
    if (route === "/agent-session") return json(res, {
      summary: summary(), stream_cursor: 0,
      turns: nativeEcho ? [{ turn_id: "native-turn", status: "running", activities: Array.from(runs.values(), (run) => ({
        activity_id: `native-${run.id}`, kind: "user_message", status: "completed",
        content: content(run.input), details: { clientId: `orchestral:${run.id}:${digest}` },
      })) }] : [], pending_requests: [],
      controlled_runs: Array.from(runs.values(), view), next_cursor: null,
    });
    if (route.endsWith("/stream")) {
      res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
      res.write(": ready\n\n");
      streams.add(res);
      const timer = setInterval(() => res.write(": keep-alive\n\n"), 1000);
      res.on("close", () => { clearInterval(timer); streams.delete(res); });
      return;
    }
    if (route === "/agent-runs" && req.method === "POST") {
      calls.push(body);
      assert.equal(body.attachments.length, 1);
      assert.equal(body.attachments[0].artifact_ref, digest);
      const run = { id: body.run_id, input: body.input, created: Date.now() };
      runs.set(run.id, run);
      await new Promise((resolve) => setTimeout(resolve, 250));
      return json(res, { run_id: run.id, operation: "started", view: view(run) });
    }
    const match = route.match(/^\/runs\/([^/]+)(\/events)?$/);
    if (match && runs.has(match[1])) {
      const run = runs.get(match[1]);
      if (!match[2]) return json(res, view(run));
      return json(res, { records: [record(run, 1, "input_committed"), record(run, 2, "run_started")]
        .filter((item) => item.event.run_seq > Number(url.searchParams.get("after") || 0)) });
    }
    return json(res, { code: "mock_missing", message: route }, 404);
  }
  const requested = url.pathname === "/" ? "/index.html" : decodeURIComponent(url.pathname);
  const file = path.join(dist, requested);
  if (!file.startsWith(dist + path.sep) || !fs.existsSync(file)) {
    res.writeHead(404); return res.end("missing");
  }
  const type = { ".html": "text/html", ".js": "text/javascript", ".wasm": "application/wasm",
    ".css": "text/css", ".svg": "image/svg+xml", ".png": "image/png" }[path.extname(file)];
  res.writeHead(200, { "content-type": type || "application/octet-stream" });
  fs.createReadStream(file).pipe(res);
});

(async () => {
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const browser = await chromium.launch({ headless: true, channel: process.env.PWA_SMOKE_CHANNEL || "chrome" });
  const context = await browser.newContext({
    viewport: { width, height: 844 }, deviceScaleFactor: 1, isMobile: true,
    hasTouch: true, serviceWorkers: process.env.PWA_SMOKE_SW === "1" ? "allow" : "block",
  });
  await context.route("https://files.example/**", (route) => route.fulfill({ contentType: "image/png", body: png }));
  const page = await context.newPage();
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.stack || error.message));
  page.on("console", (message) => { if (message.type() === "error") errors.push(message.text()); });
  page.setDefaultTimeout(12000);
  try {
    await page.goto(`http://127.0.0.1:${server.address().port}`);
    const drawer = page.getByRole("button", { name: "打开会话列表" });
    await drawer.click();
    await page.getByRole("tab", { name: /Image Agent/ }).click();
    await page.locator(".thread-button").first().click();
    await page.locator(".message-input").waitFor();
    // Identical content sent twice is two distinct submissions. Every HTTP,
    // durable event and signed snapshot for either submission is one bubble.
    for (let sent = 1; sent <= 2; sent++) {
      await page.locator("#composer-file-input").setInputFiles({ name: "photo.png", mimeType: "image/png", buffer: png });
      await page.locator(".composer-attachment").waitFor();
      await page.getByRole("textbox", { name: "消息草稿" }).fill("看看这张图");
      await page.getByRole("button", { name: "发送消息", exact: true }).click();
      await page.waitForFunction(() => document.querySelector(".message-input").value === "");
      await page.waitForResponse((res) => /\/runs\/[^/]+\/events/.test(res.url()) && res.status() === 200);
      // Let the asynchronous Dioxus render apply both the event and snapshot.
      await page.waitForTimeout(300);
      const messages = page.locator(".message--user");
      const ids = await messages.evaluateAll((elements) => elements.map((element) => element.dataset.messageId));
      // Inspect the drawer even on a failing bundle to distinguish a DOM crash
      // from duplicate display alone.
      await drawer.click();
      const drawerOpen = await drawer.getAttribute("aria-expanded") === "true";
      console.log(JSON.stringify({ sent, bubbles: ids.length, uniqueIds: new Set(ids).size, drawerOpen, errors }));
      assert.equal(ids.length, sent, "each image submission must have exactly one user bubble");
      assert.equal(new Set(ids).size, ids.length, "timeline DOM identities must be unique");
      assert.equal(drawerOpen, true, "drawer must still open after image reconciliation");
      await Promise.all([
        page.waitForResponse((res) => new URL(res.url()).pathname === "/api/v1/agent-session"),
        page.locator(".thread-button").first().click(),
      ]);
      await page.waitForTimeout(300);
      assert.equal(await messages.count(), sent, "session refresh with renewed links must not duplicate input");
      await drawer.click();
      assert.equal(await drawer.getAttribute("aria-expanded"), "true", "drawer must remain interactive after another snapshot");
      await page.locator(".thread-button").first().click();
    }
    // Once the native transcript catches up, its client identities replace
    // the controlled projections even though native text omits attachments.
    nativeEcho = true;
    await drawer.click();
    await Promise.all([
      page.waitForResponse((res) => new URL(res.url()).pathname === "/api/v1/agent-session"),
      page.locator(".thread-button").first().click(),
    ]);
    await page.waitForFunction(() => document.querySelectorAll('.message--user[data-message-id^="native-"]').length === 2);
    assert.equal(await page.locator(".message--user").count(), 2);
    await drawer.click();
    assert.equal(await drawer.getAttribute("aria-expanded"), "true", "native echo must preserve drawer interaction");
    await page.locator(".thread-button").first().click();
    assert.equal(calls.length, 2, "each submit must send exactly one request");
    assert.equal(uploadCount, 2);
    assert.deepEqual(errors, [], "browser must remain free of runtime errors");
    fs.mkdirSync("target/pwa-smoke", { recursive: true });
    await page.screenshot({ path: `target/pwa-smoke/image-messages-${width}.png` });
    console.log(`Image-message browser regression passed at ${width}px`);
  } finally {
    await browser.close();
    for (const res of streams) res.end();
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  }
})().catch((error) => { console.error(error); process.exitCode = 1; });
