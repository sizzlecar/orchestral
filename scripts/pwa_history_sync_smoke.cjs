// Local fixture: native history advances while an SSE connection stays healthy.
const assert = require("node:assert/strict");
const fs = require("node:fs");
const http = require("node:http");
const path = require("node:path");
const { chromium } = require("playwright");
const dist = process.env.PWA_SMOKE_DIST || path.join(process.cwd(), "web/orchestral-web/dist");
const content = text => [{ body: { kind: "inline", value: text } }];
const message = (id, text, kind = "agent_message") => ({ activity_id: id, kind, status: "completed", content: content(text) });
const summary = { connector_id: "fixture/editor", session_id: "native-history", title: "历史同步回归", state: "active", updated_at_unix_ms: Date.now() };
let activities = [message("question", "检查任务进度", "user_message"), message("progress", "初始进度")];
let failNextRead = false, reads = 0, failedReads = 0, cursorReads = 0;
const streams = new Set(), errors = [];
const detail = (items, next = "older") => ({ summary, turns: [{ turn_id: "turn", status: "active", activities: items }], next_cursor: next, controlled_runs: [], pending_requests: [], stream_cursor: 0 });
const json = (res, value, status = 200) => { res.writeHead(status, { "content-type": "application/json", "cache-control": "no-store" }); res.end(JSON.stringify(value)); };
const server = http.createServer((req, res) => {
  const url = new URL(req.url, "http://127.0.0.1"), route = url.pathname.slice(7);
  if (url.pathname.startsWith("/api/v1/")) {
    if (route === "/me") return json(res, { auth_mode: "gateway_jwt" });
    if (route === "/devices" || route === "/sessions") return json(res, []);
    if (route === "/agent-connectors") return json(res, [{ connector_id: summary.connector_id, display_name: "Codex", agent_family: "coding-agent", agent_product: "codex", capabilities: { list: true, read: true, create: false }, actions: [] }]);
    if (route === "/agent-sessions") return json(res, { sessions: [summary] });
    if (route === "/agent-session") {
      reads++;
      if (url.searchParams.has("cursor")) { cursorReads++; return json(res, detail([message("older", "已加载的更早消息")], null)); }
      if (failNextRead) { failNextRead = false; failedReads++; return json(res, { code: "provider_unavailable", message: "本地快照读取暂时失败" }, 503); }
      return json(res, detail(activities));
    }
    if (route === "/agent-session/stream") {
      res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-store" });
      res.write(": ready\n\n"); streams.add(res);
      const timer = setInterval(() => res.write(": keep-alive\n\n"), 100);
      res.on("close", () => { streams.delete(res); clearInterval(timer); });
      return;
    }
    return json(res, { code: "not_found" }, 404);
  }
  const file = path.resolve(dist, url.pathname === "/" ? "index.html" : `.${url.pathname}`);
  if (!file.startsWith(dist + path.sep) || !fs.existsSync(file)) { res.writeHead(404); return res.end(); }
  const mime = { ".html": "text/html", ".js": "application/javascript", ".wasm": "application/wasm", ".css": "text/css", ".svg": "image/svg+xml", ".png": "image/png" };
  res.writeHead(200, { "content-type": mime[path.extname(file)] || "application/octet-stream" }); fs.createReadStream(file).pipe(res);
});
(async () => {
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  const browser = await chromium.launch({ headless: true, channel: process.env.PWA_SMOKE_CHANNEL || "chrome" });
  try {
    const context = await browser.newContext({ viewport: { width: 1200, height: 900 }, serviceWorkers: "block" });
    const page = await context.newPage(); page.on("pageerror", error => errors.push(error.message)); page.setDefaultTimeout(14000);
    await page.goto(`http://127.0.0.1:${server.address().port}`);
    const pane = page.locator('.session-pane[data-pane="primary"]');
    await pane.getByText("初始进度", { exact: true }).waitFor();
    await pane.getByRole("button", { name: "加载更早的会话记录" }).click();
    await pane.getByText("已加载的更早消息", { exact: true }).waitFor({ state: "attached" });
    assert.equal(cursorReads, 1);
    const before = reads;
    // No session_changed event accompanies these persisted native items.
    activities = [...activities, message("middle", "原生后续进度"), message("final", "原生最终回复")];
    failNextRead = true;
    await pane.getByText("原生最终回复", { exact: true }).waitFor({ state: "attached" });
    assert.equal(failedReads, 1, "a failed reconciliation must be retried while SSE remains open");
    assert.equal(streams.size, 1, "repair must retain the existing SSE subscription");
    assert.equal(await pane.getByText("原生后续进度", { exact: true }).count(), 1);
    assert.equal(await pane.getByText("已加载的更早消息", { exact: true }).count(), 1);
    assert.ok(reads - before <= 4, "frequent heartbeats must not cause a snapshot request for each frame");

    activities = [...activities, message("reconnected", "断线期间写入的回复")];
    for (const stream of [...streams]) stream.end();
    await pane.getByText("断线期间写入的回复", { exact: true }).waitFor({ state: "attached", timeout: 4000 });
    for (const text of ["初始进度", "原生后续进度", "原生最终回复", "已加载的更早消息", "断线期间写入的回复"]) assert.equal(await pane.getByText(text, { exact: true }).count(), 1);
    assert.equal(cursorReads, 1, "reconciliation must preserve the older history cursor");
    assert.deepEqual(errors, []);
    console.log(JSON.stringify({ checks: ["idle SSE history reconciliation", "failed snapshot retry", "reconnect backfill", "loaded history preservation", "message uniqueness"], reads, failedReads, cursorReads }));
  } finally { await browser.close(); server.closeAllConnections(); server.close(); }
})().catch(error => { console.error(error); server.closeAllConnections(); server.close(); process.exitCode = 1; });
