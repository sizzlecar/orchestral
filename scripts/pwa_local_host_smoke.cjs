// Run from the repository root. Browser regression for terminal-created Runs;
// the HTTP fixture and all responses are local, with no model API calls.
const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");
const assert = require("node:assert/strict");
const { chromium } = require("playwright");
const dist = path.join(process.cwd(), "web/orchestral-web/dist");
const runs = new Map(), streams = new Map(), streamReads = new Map(), sessionPolls = new Map();
const runReads = [];
const titles = new Map([["terminal", "本机 Orchestral"]]);
let polls = 0, available = true;
const now = Date.now();
const content = text => [{ body: { kind: "inline", value: text } }];
const session = (id = "terminal") => ({ id, title: titles.get(id), cwd: "/fixture/project", created_at_unix_ms: now, updated_at_unix_ms: now + (id === "long-history" ? 1000 : id === "reserved" ? 500 : 0), run_ids: [...runs.values()].filter(run => run.sessionId === id).map(run => run.id) });
const view = run => ({ execution: { run_id: run.id, session_id: run.sessionId }, state: run.done ? { state: "terminal", terminal: { type: "delivered", delivery_id: `delivery-${run.id}` } } : { state: "running" }, pending_requests: [], last_run_seq: run.records.length, input: content(run.input) });
const json = (res, data, status = 200) => { res.writeHead(status, { "content-type": "application/json", "cache-control": "no-store" }); res.end(JSON.stringify(data)); };
function publish(run, payload) {
  const seq = run.records.length + 1;
  const record = { event: { run_id: run.id, run_seq: seq, event_id: `${run.id}-${seq}`, payload } };
  run.records.push(record);
  for (const res of streams.get(run.id) || []) res.write(`id: ${seq}\nevent: durable\ndata: ${JSON.stringify(record)}\n\n`);
}
function start(id, input, sessionId = "terminal") { const run = { id, input, sessionId, done: false, records: [] }; publish(run, { type: "run_started" }); runs.set(id, run); return run; }
function finish(run, text) { run.done = true; publish(run, { type: "output_committed", output_id: `${run.id}-answer`, content: content(text) }); publish(run, { type: "delivery_committed", delivery: { delivery_id: `delivery-${run.id}` } }); }
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://localhost");
  if (url.pathname.startsWith("/api/v1")) {
    const route = url.pathname.slice(7);
    if (route === "/me") return json(res, { auth_mode: "gateway_jwt" });
    if (route === "/devices" || route === "/agent-connectors") return json(res, []);
    if (route === "/sessions") return json(res, [...titles.keys()].map(id => session(id)));
    const sessionMatch = route.match(/^\/sessions\/([^/]+)$/);
    if (sessionMatch && titles.has(sessionMatch[1])) {
      const id = sessionMatch[1];
      sessionPolls.set(id, (sessionPolls.get(id) || 0) + 1);
      if (id === "terminal") polls++;
      return id !== "terminal" || available ? json(res, session(id)) : json(res, { code: "local_host_unavailable", message: "Terminal temporarily unavailable" }, 503);
    }
    const match = route.match(/^\/runs\/([^/]+)(?:\/(events|stream))?$/);
    if (match && runs.has(match[1])) {
      const run = runs.get(match[1]);
      runReads.push({ sessionId: run.sessionId, runId: run.id, kind: match[2] || "view" });
      if (!match[2]) {
        // Reverse completion order within the client's request window; initial
        // latest positioning must survive more DOM rows arriving afterwards.
        if (run.sessionId === "long-history") await new Promise(resolve => setTimeout(resolve, Number(run.id.split("-").at(-1)) % 3 * 20));
        if (!res.destroyed) json(res, view(run));
        return;
      }
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

const historyRunId = index => `history-${String(index).padStart(3, "0")}`;
const historyInput = index => `历史输入 ${index}`;
const historyMarker = index => `本机长会话完成标记 ${index}`;
function seedLongHistory() {
  titles.set("long-history", "本机长会话回归");
  titles.set("reserved", "下屏独立空会话");
  for (let index = 1; index <= 48; index++) {
    const run = start(historyRunId(index), historyInput(index), "long-history");
    finish(run, `历史答复 ${index}。\n\n${"这是用于验证滚动位置的本地历史内容。".repeat(12)}\n\n${historyMarker(index)}`);
  }
}

async function verifyLongHistory(page, wait) {
  const pane = page.locator('.session-pane[data-pane="primary"]');
  const log = pane.getByRole("log", { name: "对话内容" });
  const openSession = async title => {
    const labels = title === "本机长会话回归" ? /本机长会话回归|历史输入 49/ : /本机 Orchestral|重连后的新消息/;
    const button = pane.locator(".thread-button").filter({ hasText: labels });
    const drawer = pane.getByRole("button", { name: "打开会话列表" });
    if (await drawer.getAttribute("aria-expanded") !== "true") await drawer.click();
    await button.click();
  };
  const readsSince = offset => runReads.slice(offset).filter(read => read.sessionId === "long-history");
  const viewIdsSince = offset => readsSince(offset).filter(read => read.kind === "view").map(read => read.runId);
  const frames = async () => page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  const atBottom = async () => page.waitForFunction(() => {
    const el = document.querySelector('.session-pane[data-pane="primary"] .message-list');
    return el && el.scrollHeight - el.clientHeight - el.scrollTop <= 3;
  });
  const markerVisible = async index => {
    await log.getByText(historyMarker(index), { exact: true }).waitFor();
    await page.waitForFunction(text => {
      const log = document.querySelector('.session-pane[data-pane="primary"] .message-list');
      const marker = [...log.querySelectorAll("p")].find(el => el.textContent === text);
      if (!marker) return false;
      const viewport = log.getBoundingClientRect(), bounds = marker.getBoundingClientRect();
      return bounds.top >= viewport.top - 2 && bounds.bottom <= viewport.bottom + 2;
    }, historyMarker(index));
  };
  const anchorTop = locator => locator.evaluate(el => el.getBoundingClientRect().top - el.closest(".message-list").getBoundingClientRect().top);
  seedLongHistory();
  const firstRead = runReads.length;
  await page.reload();
  await page.getByRole("tab", { name: "Orch", exact: true }).click();
  await wait(() => viewIdsSince(firstRead).length > 0);
  assert.equal(viewIdsSince(firstRead)[0], historyRunId(48), "initial native history must request the newest Run first");
  await wait(() => new Set(viewIdsSince(firstRead)).size >= 20);
  await markerVisible(48);
  await atBottom();
  assert.equal(new Set(viewIdsSince(firstRead)).size, 20, "initial load is limited to the newest 20 Runs");
  assert.ok(readsSince(firstRead).every(read => Number(read.runId.split("-").at(-1)) >= 29), "initial load must not fetch old Run views, events, or streams");
  const observedPolls = sessionPolls.get("long-history") || 0;
  await wait(() => (sessionPolls.get("long-history") || 0) >= observedPolls + 2);
  assert.equal(new Set(viewIdsSince(firstRead)).size, 20, "session polling must not silently backfill all unloaded old Runs");
  assert.ok(readsSince(firstRead).every(read => Number(read.runId.split("-").at(-1)) >= 29), "polling must leave old history unloaded until requested");
  await markerVisible(48);

  await log.evaluate(el => { el.scrollTop = 0; el.dispatchEvent(new Event("scroll")); });
  await pane.getByRole("button", { name: "跳到最新消息" }).waitFor();
  const anchor = log.getByText(historyInput(29), { exact: true });
  await frames();
  const beforePrepend = await anchorTop(anchor);
  const olderStart = runReads.length;
  await pane.getByRole("button", { name: "加载更早的会话记录" }).click();
  await wait(() => new Set(viewIdsSince(firstRead)).size >= 40);
  await log.getByText(historyInput(9), { exact: true }).waitFor({ state: "attached" });
  await frames();
  assert.ok(Math.abs(await anchorTop(anchor) - beforePrepend) <= 4, "prepending an older page must preserve the visible message anchor");
  assert.equal(new Set(viewIdsSince(firstRead)).size, 40, "one explicit history action loads one older page");
  assert.ok(readsSince(olderStart).every(read => Number(read.runId.split("-").at(-1)) >= 9), "one history page must not eagerly fetch the oldest eight Runs");

  const beforeLive = await anchorTop(anchor);
  const liveStart = runReads.length;
  const live = start(historyRunId(49), historyInput(49), "long-history");
  finish(live, historyMarker(49));
  await log.getByText(historyMarker(49), { exact: true }).waitFor({ state: "attached" });
  await frames();
  assert.ok(Math.abs(await anchorTop(anchor) - beforeLive) <= 4, "live updates must not pull a user reading earlier messages back to the bottom");
  assert.ok(readsSince(liveStart).every(read => Number(read.runId.split("-").at(-1)) >= 9), "live discovery must not backfill history omitted by pagination");
  await pane.getByRole("button", { name: "跳到最新消息" }).click();
  await markerVisible(49);
  await atBottom();

  const heightBefore = await log.evaluate(el => el.scrollHeight);
  // Model a delayed image/font/details layout change without emitting another
  // server event: following latest must respond to actual rendered dimensions.
  await log.getByText(historyMarker(49), { exact: true }).evaluate(el => {
    el.closest("article").style.paddingBottom = "360px";
  });
  await page.waitForFunction(previous => document.querySelector('.session-pane[data-pane="primary"] .message-list').scrollHeight > previous + 300, heightBefore);
  await atBottom();

  await log.evaluate(el => { el.scrollTop = 0; el.dispatchEvent(new Event("scroll")); });
  await pane.getByRole("button", { name: "跳到最新消息" }).waitFor();
  await openSession("本机 Orchestral");
  await pane.getByText("恢复后没有重复消息", { exact: true }).waitFor();
  await openSession("本机长会话回归");
  await markerVisible(49);
  await atBottom();

  await log.evaluate(el => { el.scrollTop = 0; el.dispatchEvent(new Event("scroll")); });
  await pane.getByRole("button", { name: "跳到最新消息" }).waitFor();
  const reloadStart = runReads.length;
  await page.reload();
  // Reload selects Codex by default; choosing Orch reopens its latest sessions.
  await page.getByRole("tab", { name: "Orch", exact: true }).click();
  await markerVisible(49);
  await atBottom();
  await wait(() => new Set(viewIdsSince(reloadStart)).size >= 20);
  assert.equal(viewIdsSince(reloadStart)[0], historyRunId(49));
  assert.equal(new Set(viewIdsSince(reloadStart)).size, 20);
  assert.ok(readsSince(reloadStart).every(read => Number(read.runId.split("-").at(-1)) >= 30), "reload must not fetch the old end of a long Session");
}

(async () => {
  await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
  const browser = await chromium.launch({ headless: true, channel: process.env.PWA_SMOKE_CHANNEL || "chrome" });
  const context = await browser.newContext({ viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true, serviceWorkers: "block" });
  const page = await context.newPage(), errors = [];
  const pane = page.locator('.session-pane[data-pane="primary"]');
  page.on("pageerror", error => errors.push(error.message));
  page.on("console", message => {
    if (message.type() === "error" && message.text().includes("Orchestral browser error:")) errors.push(message.text());
  });
  page.setDefaultTimeout(15000);
  const wait = async predicate => { const deadline = Date.now() + 15000; while (!predicate()) { assert(Date.now() < deadline, "local host observer timed out"); await new Promise(resolve => setTimeout(resolve, 50)); } };
  try {
    await page.goto(`http://127.0.0.1:${server.address().port}`);
    await page.getByRole("tab", { name: "Orch", exact: true }).click();
    await wait(() => polls > 0);
    const first = start("terminal-1", "终端发来的第一条消息");
    await pane.getByRole("log", { name: "对话内容" }).getByText(first.input, { exact: true }).waitFor();
    await wait(() => (streams.get(first.id)?.size || 0) > 0);
    const initialPolls = polls;
    await wait(() => polls >= initialPolls + 2);
    assert.equal(streamReads.get(first.id), 1, "session polling must not restart a healthy Run stream");
    finish(first, "第一轮已经完成");
    await page.getByText("第一轮已经完成", { exact: true }).waitFor();
    const second = start("terminal-2", "完成后终端又发了一条消息");
    await pane.getByRole("log", { name: "对话内容" }).getByText(second.input, { exact: true }).waitFor();
    await wait(() => (streams.get(second.id)?.size || 0) > 0);
    finish(second, "第二轮自动同步成功");
    await page.getByText("第二轮自动同步成功", { exact: true }).waitFor();
    await pane.getByRole("button", { name: "打开会话列表" }).click();
    available = false;
    await pane.locator('.connection-status[data-state="reconnecting"]').waitFor({ state: "attached" });
    available = true;
    await pane.locator(".pane-drawer-header").getByRole("button", { name: "关闭会话列表" }).click();
    const third = start("terminal-3", "重连后的新消息"); finish(third, "恢复后没有重复消息");
    await pane.getByText("恢复后没有重复消息", { exact: true }).waitFor();
    assert.equal(await pane.locator(".message--user").count(), 3);
    assert.equal(await pane.locator(".message--assistant").count(), 3);
    await verifyLongHistory(page, wait);
    assert.deepEqual(errors, []);
    console.log(JSON.stringify({ localHostBrowser: true, idleDiscovery: true, subsequentTurns: true, stableSse: true, reconnect: true, longHistoryRuns: 49, latestFirst: true, boundedInitialHistory: true, pollingDoesNotBackfill: true, prependAnchor: true, manualScrollPreserved: true, delayedLayoutFollow: true, switchAndReloadLatest: true, paidModelRequests: 0 }));
  } catch (error) {
    fs.mkdirSync("target/pwa-smoke", { recursive: true });
    await page.screenshot({ path: "target/pwa-smoke/local-host-failed.png" });
    console.error(JSON.stringify({ recentReads: runReads.slice(-40), errors }));
    console.error(await pane.locator(".message-list").evaluate(el => ({ scrollTop: el.scrollTop, scrollHeight: el.scrollHeight, clientHeight: el.clientHeight, dataset: { ...el.dataset } })));
    console.error((await pane.innerText()).slice(-2000));
    throw error;
  } finally {
    await browser.close();
    for (const group of streams.values()) for (const res of group) res.end();
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
