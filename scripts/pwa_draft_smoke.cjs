// Browser draft ownership regression. Local fixtures only, no model requests.
const http = require("node:http"), fs = require("node:fs"), path = require("node:path");
const assert = require("node:assert/strict");
const { createHash } = require("node:crypto");
const { chromium } = require("playwright");
const dist = path.join(process.cwd(), "web/orchestral-web/dist");
const width = Number(process.env.PWA_SMOKE_WIDTH || 320), now = Date.now();
const png = Buffer.from("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=", "base64");
const digest = createHash("sha256").update(png).digest("hex");
const content = text => [{ body: { kind: "inline", value: text } }];
const summary = id => ({ connector_id: "fixture/editor", session_id: id, title: `会话 ${id.toUpperCase()}`, state: "idle", created_at_unix_ms: now - 60000, updated_at_unix_ms: now - ["a", "b", "c"].indexOf(id) * 1000 });
let releaseUpload;
const streams = new Set();
const json = (res, value) => { res.writeHead(200, { "content-type": "application/json", "cache-control": "no-store" }); res.end(JSON.stringify(value)); };
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://127.0.0.1");
  const route = url.pathname.slice(7);
  if (url.pathname.startsWith("/api/v1/")) {
    if (route === "/me") return json(res, { auth_mode: "gateway_jwt" });
    if (route === "/devices" || route === "/sessions") return json(res, []);
    if (route === "/agent-connectors") return json(res, [{ connector_id: "fixture/editor", display_name: "Draft Agent", agent_family: "coding-agent", agent_product: "codex", capabilities: { list: true, read: true, create: true } }]);
    if (route === "/agent-sessions") return json(res, { sessions: ["a", "b", "c"].map(summary) });
    if (route === "/agent-session") { const id = url.searchParams.get("session_id"); return json(res, { summary: summary(id), turns: [{ turn_id: id, status: "completed", activities: [{ activity_id: `history-${id}`, kind: "agent_message", status: "completed", content: content(`会话 ${id.toUpperCase()} 历史`) }] }], controlled_runs: [], pending_requests: [], stream_cursor: 0 }); }
    if (route === "/agent-session/stream") {
      res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-store" }); res.write(": ready\n\n");
      streams.add(res); res.on("close", () => streams.delete(res)); return;
    }
    if (route === "/attachments" && req.method === "POST") {
      const chunks = []; for await (const chunk of req) chunks.push(chunk);
      assert.deepEqual(Buffer.concat(chunks), png);
      await new Promise(resolve => { releaseUpload = resolve; });
      return json(res, { artifact_ref: digest, file_name: "photo.png", media_type: "image/png", byte_size: png.length, sha256: digest, download_url: `/api/v1/attachments/${digest}` });
    }
    if (route.startsWith("/attachments/")) { res.writeHead(200, { "content-type": "image/png" }); res.end(png); return; }
    res.writeHead(404); res.end(route); return;
  }
  const file = path.join(dist, url.pathname === "/" ? "index.html" : url.pathname);
  if (!file.startsWith(dist + path.sep) || !fs.existsSync(file)) { res.writeHead(404); res.end(); return; }
  const mime = { ".html": "text/html", ".js": "application/javascript", ".wasm": "application/wasm", ".css": "text/css", ".svg": "image/svg+xml", ".png": "image/png" };
  res.writeHead(200, { "content-type": mime[path.extname(file)] || "application/octet-stream" }); fs.createReadStream(file).pipe(res);
});
async function checkDrafts(browser, scenario) {
  const context = await browser.newContext({ viewport: { width, height: 844 }, isMobile: true, hasTouch: true, serviceWorkers: "block" });
  const page = await context.newPage(), errors = [];
  page.setDefaultTimeout(10000);
  page.on("pageerror", error => errors.push(error.message));
  const upper = page.locator('.session-pane[data-pane="primary"]'), lower = page.locator('.session-pane[data-pane="secondary"]');
  const upperInput = upper.getByRole("textbox", { name: "消息草稿" }), lowerInput = lower.getByRole("textbox", { name: "消息草稿" });
  const select = async (pane, id) => {
    await pane.getByRole("button", { name: "打开会话列表" }).click();
    await pane.locator(".thread-button").filter({ hasText: `会话 ${id.toUpperCase()}` }).click();
    await pane.getByText(`会话 ${id.toUpperCase()} 历史`, { exact: true }).waitFor();
  };
  try {
    await page.goto(`http://127.0.0.1:${server.address().port}`);
    await upper.getByText("会话 A 历史", { exact: true }).waitFor();
    await lower.getByText("会话 B 历史", { exact: true }).waitFor();
    if (scenario === "storage-failure") {
      await page.evaluate(() => {
        const original = Storage.prototype.setItem;
        Storage.prototype.setItem = function (key, value) {
          if (key === "orchestral.drafts.v1") throw new DOMException("Local draft quota fixture", "QuotaExceededError");
          return original.call(this, key, value);
        };
      });
      await upperInput.fill("A 尚未持久化的草稿");
      await lowerInput.fill("B 尚未持久化的草稿");
      await upper.getByText("草稿保存失败，请先复制草稿再刷新", { exact: true }).waitFor();
      await select(upper, "c");
      assert.equal(await upperInput.inputValue(), "");
      await upperInput.fill("C 独立草稿");
      await select(upper, "a");
      assert.equal(await upperInput.inputValue(), "A 尚未持久化的草稿", "storage rejection must not discard the in-memory draft during a session switch");
      assert.equal(await lowerInput.inputValue(), "B 尚未持久化的草稿", "one pane's storage failure must not erase the other draft");
      await select(upper, "c");
      assert.equal(await upperInput.inputValue(), "C 独立草稿");
    } else {
      await upperInput.fill("A 上传前草稿");
      await upper.locator('input[type="file"]').setInputFiles({ name: "photo.png", mimeType: "image/png", buffer: png });
      const deadline = Date.now() + 10000;
      while (!releaseUpload) { assert(Date.now() < deadline, "upload did not reach local fixture"); await new Promise(resolve => setTimeout(resolve, 25)); }
      await select(upper, "c");
      await upperInput.fill("C 不应收到 A 的附件");
      await select(lower, "a");
      await lowerInput.fill("A 已在下屏继续编辑");
      releaseUpload();
      await lower.locator(".composer-attachment").waitFor();
      assert.equal(await lowerInput.inputValue(), "A 已在下屏继续编辑", "late upload must merge with the draft now edited in the other pane");
      assert.equal(await upperInput.inputValue(), "C 不应收到 A 的附件");
      assert.equal(await upper.locator(".composer-attachment").count(), 0, "late upload belongs to the originating session, not the original pane");
      await lowerInput.fill("A 附件到达后继续编辑");
      assert.equal(await lower.locator(".composer-attachment").count(), 1, "typing after upload preserves the merged attachment");
    }
    assert.deepEqual(errors, []);
    console.log(JSON.stringify({ draftBrowser: scenario, passed: true, paidModelRequests: 0 }));
    return { scenario, passed: true };
  } catch (error) {
    fs.mkdirSync("target/pwa-smoke", { recursive: true });
    await page.screenshot({ path: `target/pwa-smoke/draft-${scenario}-failed-${width}.png` });
    return { scenario, passed: false, error: error.message };
  } finally { if (releaseUpload) releaseUpload(); await context.close(); }
}
(async () => {
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  const browser = await chromium.launch({ headless: true, channel: process.env.PWA_SMOKE_CHANNEL || "chrome" });
  try {
    const results = [];
    for (const scenario of ["storage-failure", "late-upload"]) results.push(await checkDrafts(browser, scenario));
    assert.deepEqual(results.filter(result => !result.passed), [], "all draft ownership regressions must pass");
  } finally {
    await browser.close(); for (const stream of streams) stream.end(); server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
