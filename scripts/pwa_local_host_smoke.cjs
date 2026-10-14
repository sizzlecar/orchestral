// Run from the repository root. Browser regression for terminal-created Runs;
// the HTTP fixture and all responses are local, with no model API calls.
const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");
const assert = require("node:assert/strict");
const { chromium } = require("playwright");
const dist = path.join(process.cwd(), "web/orchestral-web/dist");
const runs = new Map(), streams = new Map(), streamReads = new Map();
let polls = 0, available = true;
const now = Date.now();
const content = text => [{ body: { kind: "inline", value: text } }];
const session = () => ({ id: "terminal", title: "本机 Orchestral", cwd: "/fixture/project", created_at_unix_ms: now, updated_at_unix_ms: Date.now(), run_ids: [...runs.keys()] });
const view = run => ({ execution: { run_id: run.id, session_id: "terminal" }, state: run.done ? { state: "terminal", terminal: { type: "delivered", delivery_id: `delivery-${run.id}` } } : { state: "running" }, pending_requests: [], last_run_seq: run.records.length, input: content(run.input) });
const json = (res, data, status = 200) => { res.writeHead(status, { "content-type": "application/json", "cache-control": "no-store" }); res.end(JSON.stringify(data)); };
function publish(run, payload) {
  const seq = run.records.length + 1;
  const record = { event: { run_id: run.id, run_seq: seq, event_id: `${run.id}-${seq}`, payload } };
  run.records.push(record);
  for (const res of streams.get(run.id) || []) res.write(`id: ${seq}\nevent: durable\ndata: ${JSON.stringify(record)}\n\n`);
}
function start(id, input) { const run = { id, input, done: false, records: [] }; publish(run, { type: "run_started" }); runs.set(id, run); return run; }
function finish(run, text) { run.done = true; publish(run, { type: "output_committed", output_id: `${run.id}-answer`, content: content(text) }); publish(run, { type: "delivery_committed", delivery: { delivery_id: `delivery-${run.id}` } }); }
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://localhost");
  if (url.pathname.startsWith("/api/v1")) {
    const route = url.pathname.slice(7);
    if (route === "/me") return json(res, { auth_mode: "gateway_jwt" });
    if (route === "/devices" || route === "/agent-connectors") return json(res, []);
    if (route === "/sessions") return json(res, [session()]);
    if (route === "/sessions/terminal") { polls++; return available ? json(res, session()) : json(res, { code: "local_host_unavailable", message: "Terminal temporarily unavailable" }, 503); }
    const match = route.match(/^\/runs\/([^/]+)(?:\/(events|stream))?$/);
    if (match && runs.has(match[1])) {
      const run = runs.get(match[1]);
      if (!match[2]) return json(res, view(run));
      const after = Number(url.searchParams.get("after") || 0);
      const records = run.records.filter(record => record.event.run_seq > after);
      if (match[2] === "events") return json(res, { after, next: run.records.length, records });
      res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-store" });
      res.write(": ready\n\n");
      for (const record of records) res.write(`id: ${record.event.run_seq}\nevent: durable\ndata: ${JSON.stringify(record)}\n\n`);
      const group = streams.get(run.id) || new Set(); streams.set(run.id, group); group.add(res);
      streamReads.set(run.id, (streamReads.get(run.id) || 0) + 1);
      res.on("close", () => group.delete(res));
      return;
    }
    return json(res, { code: "not_found", message: route }, 404);
  }
  const file = path.join(dist, url.pathname === "/" ? "index.html" : url.pathname);
  if (!file.startsWith(dist + path.sep) || !fs.existsSync(file) || !fs.statSync(file).isFile()) { res.writeHead(404); res.end(); return; }
  const mime = { ".html": "text/html", ".js": "application/javascript", ".wasm": "application/wasm", ".css": "text/css", ".svg": "image/svg+xml", ".png": "image/png" };
  res.writeHead(200, { "content-type": mime[path.extname(file)] || "application/octet-stream" }); fs.createReadStream(file).pipe(res);
});
(async () => {
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  const browser = await chromium.launch({ headless: true, channel: process.env.PWA_SMOKE_CHANNEL || "chrome" });
  const context = await browser.newContext({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true, serviceWorkers: "block" });
  const page = await context.newPage(), errors = [];
  page.on("pageerror", error => errors.push(error.message));
  page.setDefaultTimeout(15000);
  const wait = async predicate => { const deadline = Date.now() + 15000; while (!predicate()) { assert(Date.now() < deadline, "local host observer timed out"); await new Promise(resolve => setTimeout(resolve, 50)); } };
  try {
    await page.goto(`http://127.0.0.1:${server.address().port}`);
    await page.getByRole("button", { name: "打开会话列表" }).click();
    await page.locator(".thread-button").first().click();
    await wait(() => polls > 0);
    const first = start("terminal-1", "终端发来的第一条消息");
    await page.getByRole("log", { name: "对话内容" }).getByText(first.input, { exact: true }).waitFor();
    await wait(() => (streams.get(first.id)?.size || 0) > 0);
    const initialPolls = polls;
    await wait(() => polls >= initialPolls + 2);
    assert.equal(streamReads.get(first.id), 1, "session polling must not restart a healthy Run stream");
    finish(first, "第一轮已经完成");
    await page.getByText("第一轮已经完成", { exact: true }).waitFor();
    const second = start("terminal-2", "完成后终端又发了一条消息");
    await page.getByRole("log", { name: "对话内容" }).getByText(second.input, { exact: true }).waitFor();
    await wait(() => (streams.get(second.id)?.size || 0) > 0);
    finish(second, "第二轮自动同步成功");
    await page.getByText("第二轮自动同步成功", { exact: true }).waitFor();
    available = false;
    await page.locator('.connection-status[data-state="reconnecting"]').waitFor({ state: "attached" });
    available = true;
    const third = start("terminal-3", "重连后的新消息"); finish(third, "恢复后没有重复消息");
    await page.getByText("恢复后没有重复消息", { exact: true }).waitFor();
    assert.equal(await page.locator(".message--user").count(), 3);
    assert.equal(await page.locator(".message--assistant").count(), 3);
    assert.deepEqual(errors, []);
    console.log(JSON.stringify({ localHostBrowser: true, idleDiscovery: true, subsequentTurns: true, stableSse: true, reconnect: true, paidModelRequests: 0 }));
  } finally {
    await browser.close();
    for (const group of streams.values()) for (const res of group) res.end();
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
