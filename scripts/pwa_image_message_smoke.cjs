// Run from the repository root with Playwright on NODE_PATH (see the PWA README).
const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");
const assert = require("node:assert/strict");
const { createHash } = require("node:crypto");
const { chromium } = require("playwright");
const dist = path.join(process.cwd(), "web/orchestral-web/dist");
const width = Number(process.env.PWA_SMOKE_WIDTH || 390);
const testWorker = process.env.PWA_SMOKE_SW === "1";
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
let workerRevision = 0, holdNextUpload = false, holdNextSend = false, releaseUpload, releaseSend;
let retryNextSend = false, retryRunId, releaseRetry;
const summary = (id = "images") => ({
  connector_id: "codex/local", session_id: id, title: id === "images" ? "图片回显验证" : "下屏上传验证",
  state: "idle", created_at_unix_ms: now - 60000, updated_at_unix_ms: now - (id === "images" ? 0 : 1000),
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
  execution: { run_id: run.id, session_id: run.sessionId },
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
      if (holdNextUpload) {
        holdNextUpload = false;
        await new Promise(resolve => { releaseUpload = resolve; });
      }
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
      connector_id: "codex/local", display_name: "Image Agent", agent_family: "coding-agent", agent_product: "codex",
      capabilities: { list: true, read: true, create: true },
    }]);
    if (route === "/agent-sessions") return json(res, { sessions: [summary(), summary("image-lower")] });
    if (route === "/agent-session") {
      const id = url.searchParams.get("session_id");
      const sessionRuns = [...runs.values()].filter(run => run.sessionId === id);
      return json(res, {
        summary: summary(id), stream_cursor: 0,
        turns: nativeEcho ? [{ turn_id: "native-turn", status: "running", activities: sessionRuns.map(run => ({
          activity_id: `native-${run.id}`, kind: "user_message", status: "completed",
          content: content(run.input), details: { clientId: `orchestral:${run.id}:${digest}` },
        })) }] : [], pending_requests: [],
        controlled_runs: sessionRuns.map(view), next_cursor: null,
      });
    }
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
      const run = { id: body.run_id, sessionId: body.session_id, input: body.input, created: Date.now() };
      runs.set(run.id, run);
      if (retryNextSend) {
        retryNextSend = false;
        retryRunId = run.id;
        return json(res, { code: "provider_unavailable", message: "Local outbox confirmation fixture" }, 503);
      }
      if (run.id === retryRunId) await new Promise(resolve => { releaseRetry = resolve; });
      if (holdNextSend) {
        holdNextSend = false;
        await new Promise(resolve => { releaseSend = resolve; });
      }
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
  if (requested === "/sw.js") {
    let worker = fs.readFileSync(file, "utf8");
    if (workerRevision) worker = worker.replace(/BUILD_ID="([^"]+)"/, 'BUILD_ID="$1-image-smoke-new"');
    res.writeHead(200, { "content-type": "text/javascript", "cache-control": "no-store" });
    return res.end(worker);
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
    hasTouch: true, serviceWorkers: testWorker ? "allow" : "block",
  });
  await context.route("https://files.example/**", (route) => route.fulfill({ contentType: "image/png", body: png }));
  const page = await context.newPage();
  const pane = page.locator('.session-pane[data-pane="primary"]');
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.stack || error.message));
  page.on("console", message => {
    const expectedRetry = retryRunId && /^Failed to load resource: the server responded with a status of 503/.test(message.text());
    if (message.type() === "error" && !expectedRetry) errors.push(message.text());
  });
  page.setDefaultTimeout(12000);
  try {
    await page.goto(`http://127.0.0.1:${server.address().port}`);
    const drawer = pane.getByRole("button", { name: "打开会话列表" });
    await drawer.click();
    await pane.locator(".thread-button").first().click();
    await pane.locator(".message-input").waitFor();
    // Identical content sent twice is two distinct submissions. Every HTTP,
    // durable event and signed snapshot for either submission is one bubble.
    for (let sent = 1; sent <= 2; sent++) {
      await pane.locator('input[type="file"]').setInputFiles({ name: "photo.png", mimeType: "image/png", buffer: png });
      await pane.locator(".composer-attachment").waitFor();
      await pane.getByRole("textbox", { name: "消息草稿" }).fill("看看这张图");
      await pane.getByRole("button", { name: "发送消息", exact: true }).click();
      await page.waitForFunction(() => document.querySelector('.session-pane[data-pane="primary"] .message-input').value === "");
      await page.waitForResponse((res) => /\/runs\/[^/]+\/events/.test(res.url()) && res.status() === 200);
      // Let the asynchronous Dioxus render apply both the event and snapshot.
      await page.waitForTimeout(300);
      const messages = pane.locator(".message--user");
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
        pane.locator(".thread-button").first().click(),
      ]);
      await page.waitForTimeout(300);
      assert.equal(await messages.count(), sent, "session refresh with renewed links must not duplicate input");
      await drawer.click();
      assert.equal(await drawer.getAttribute("aria-expanded"), "true", "drawer must remain interactive after another snapshot");
      await pane.locator(".thread-button").first().click();
    }
    // Once the native transcript catches up, its client identities replace
    // the controlled projections even though native text omits attachments.
    nativeEcho = true;
    await drawer.click();
    await Promise.all([
      page.waitForResponse((res) => new URL(res.url()).pathname === "/api/v1/agent-session"),
      pane.locator(".thread-button").first().click(),
    ]);
    await page.waitForFunction(() => document.querySelectorAll('.session-pane[data-pane="primary"] .message--user[data-message-id^="native-"]').length === 2);
    assert.equal(await pane.locator(".message--user").count(), 2);
    await drawer.click();
    assert.equal(await drawer.getAttribute("aria-expanded"), "true", "native echo must preserve drawer interaction");
    await pane.locator(".thread-button").first().click();
    assert.equal(calls.length, 2, "each submit must send exactly one request");
    assert.equal(uploadCount, 2);
    if (testWorker) {
      const lower = page.locator('.session-pane[data-pane="secondary"]');
      const wait = async predicate => { const deadline = Date.now() + 12000; while (!predicate()) { assert(Date.now() < deadline, "held image fixture request timed out"); await new Promise(resolve => setTimeout(resolve, 25)); } };
      await page.waitForFunction(async () => {
        const registration = await navigator.serviceWorker.getRegistration();
        return registration?.active && navigator.serviceWorker.controller;
      });
      // Finish any baseline update job before changing the served worker.
      // Revision 1 is discovered only through the app's visibility listener.
      await page.evaluate(async () => {
        const registration = await navigator.serviceWorker.ready;
        await registration.update();
      });
      workerRevision = 1;
      await page.evaluate(() => document.dispatchEvent(new Event("visibilitychange", { bubbles: true })));
      const update = page.getByRole("button", { name: "刷新更新", exact: true });
      await update.waitFor();
      assert.equal(await update.count(), 1, "one global update action serves both panes");

      await lower.locator(".pane-activate").click();
      holdNextUpload = true;
      await lower.locator('input[type="file"]').setInputFiles({ name: "photo.png", mimeType: "image/png", buffer: png });
      await wait(() => releaseUpload !== undefined);
      await page.waitForFunction(() => document.querySelector(".workspace-update button").disabled);
      assert.equal(await update.isDisabled(), true, "an upload in the lower pane blocks the global reload");
      releaseUpload();
      await lower.locator(".composer-attachment").waitFor();
      await page.waitForFunction(() => !document.querySelector(".workspace-update button").disabled);

      holdNextSend = true;
      await lower.getByRole("textbox", { name: "消息草稿" }).fill("下屏发送进行中");
      await lower.getByRole("button", { name: "发送消息", exact: true }).click();
      await wait(() => releaseSend !== undefined);
      assert.equal(calls.at(-1).session_id, "image-lower");
      await page.waitForFunction(() => document.querySelector(".workspace-update button").disabled);
      assert.equal(await update.isDisabled(), true, "a pending send in the lower pane blocks the global reload");
      releaseSend();
      await page.waitForFunction(() => !document.querySelector(".workspace-update button").disabled);
      assert.equal(calls.length, 3);
      assert.equal(uploadCount, 3);

      await lower.locator('input[type="file"]').setInputFiles({ name: "photo.png", mimeType: "image/png", buffer: png });
      await lower.locator(".composer-attachment").waitFor();
      await lower.getByRole("textbox", { name: "消息草稿" }).fill("切换来源后仍需后台确认的消息");
      retryNextSend = true;
      await lower.getByRole("button", { name: "发送消息", exact: true }).click();
      await wait(() => releaseRetry !== undefined);
      await page.getByRole("tab", { name: "Orch", exact: true }).click();
      await page.waitForFunction(() => document.querySelectorAll(".pane-empty").length === 2);
      assert.equal(await update.count(), 1, "the update action remains available on an empty source");
      assert.equal(await update.isDisabled(), true, "a background outbox retry blocks global refresh after its originating pane unmounts");
      assert.equal(calls.length, 5);
      assert.equal(calls[3].run_id, calls[4].run_id, "background confirmation retains the original operation identity");
      const retryResponse = page.waitForResponse(response => new URL(response.url()).pathname === "/api/v1/agent-runs"
        && response.status() === 200 && response.request().postDataJSON().run_id === retryRunId);
      releaseRetry();
      await retryResponse;
      await page.waitForFunction(() => !document.querySelector(".workspace-update button").disabled);
      assert.equal(await page.getByRole("tab", { name: "Orch", exact: true }).getAttribute("aria-selected"), "true");
      await page.getByRole("tab", { name: "Codex", exact: true }).click();
      const mainSession = page.locator(".session-pane").filter({ has: page.getByText("看看这张图", { exact: true }) });
      const uploadSession = page.locator(".session-pane").filter({ has: page.getByText("下屏发送进行中", { exact: true }) });
      await mainSession.locator(".pane-activate").click();
      await mainSession.getByRole("textbox", { name: "消息草稿" }).fill("升级后保留上屏草稿");
      await uploadSession.locator(".pane-activate").click();
      await uploadSession.getByRole("textbox", { name: "消息草稿" }).fill("升级后保留下屏草稿");
      await Promise.all([page.waitForNavigation({ waitUntil: "domcontentloaded" }), update.click()]);
      await mainSession.locator(".pane-activate").click();
      await mainSession.getByRole("textbox", { name: "消息草稿" }).waitFor();
      await uploadSession.locator(".pane-activate").click();
      await uploadSession.getByRole("textbox", { name: "消息草稿" }).waitFor();
      assert.equal(await mainSession.locator(".message-input").inputValue(), "升级后保留上屏草稿");
      assert.equal(await uploadSession.locator(".message-input").inputValue(), "升级后保留下屏草稿");
      console.log(JSON.stringify({ singleGlobalUpdate: true, lowerUploadBlocksUpdate: true, lowerSendBlocksUpdate: true, backgroundRetryBlocksUpdateAfterSourceSwitch: true, bothDraftsSurviveUpdate: true }));
    }
    assert.deepEqual(errors, [], "browser must remain free of runtime errors");
    fs.mkdirSync("target/pwa-smoke", { recursive: true });
    await page.screenshot({ path: `target/pwa-smoke/image-messages-${width}.png` });
    console.log(`Image-message browser regression passed at ${width}px`);
  } catch (error) {
    fs.mkdirSync("target/pwa-smoke", { recursive: true });
    await page.screenshot({ path: `target/pwa-smoke/image-messages-failed-${width}.png` });
    console.error(JSON.stringify({ uploadCount, submissions: calls.length, workerRevision, errors }));
    console.error((await page.locator("body").innerText()).slice(-3000));
    throw error;
  } finally {
    if (releaseUpload) releaseUpload();
    if (releaseSend) releaseSend();
    if (releaseRetry) releaseRetry();
    await browser.close();
    for (const res of streams) res.end();
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  }
})().catch((error) => { console.error(error); process.exitCode = 1; });
