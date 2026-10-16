// Two independently controlled sessions, using local HTTP/SSE fixtures only.
const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");
const assert = require("node:assert/strict");
const { chromium } = require("playwright");
const dist = path.join(process.cwd(), "web/orchestral-web/dist");
const width = Number(process.env.PWA_SMOKE_WIDTH || 390);
const now = Date.now();
const connectorId = product => product === "codex" ? "fixture/editor-a" : "fixture/editor-b";
const content = text => [{ body: { kind: "inline", value: text } }];
const sessions = new Map(), requests = [], submissions = [];
const actionPlans = new Map(), pendingReplies = [];
let nextActionMode;
for (const product of ["codex", "claude"]) {
  for (let rank = 1; rank <= 3; rank++) {
    const id = `${product}-${rank}`;
    sessions.set(id, { id, product, rank, streams: new Set(), changes: [], turns: Array.from({ length: 24 }, (_, index) => ({
      turn_id: `${id}-turn-${index}`, status: "completed", activities: [{
        activity_id: `${id}-history-${index}`, kind: "agent_message", status: "completed",
        content: content(`${id} 历史 ${index + 1}。${"本地双屏滚动回归内容。".repeat(12)}`),
      }],
    })) });
  }
}
const summary = session => ({
  connector_id: connectorId(session.product), session_id: session.id, title: `会话 ${session.id}`,
  state: "busy_elsewhere", input_action: "fixture.input", cwd: "/fixture/project",
  created_at_unix_ms: now - 60000, updated_at_unix_ms: now - session.rank * 1000,
});
const detail = session => ({ summary: summary(session), turns: session.turns, controlled_runs: [], pending_requests: [], stream_cursor: session.changes.length });
const json = (res, value, status = 200) => { res.writeHead(status, { "content-type": "application/json", "cache-control": "no-store" }); res.end(JSON.stringify(value)); };
function publish(session, text, kind = "agent_message", clientId) {
  const number = session.changes.length + 1;
  const activity = { activity_id: `${session.id}-live-${number}`, kind, status: "completed", content: content(text), ...(clientId ? { details: { clientId } } : {}) };
  const turn = { turn_id: `${session.id}-live-turn-${number}`, status: "completed", activities: [activity] };
  session.turns.push(turn);
  const event = { connector_id: connectorId(session.product), session_id: session.id, sequence: number,
    change: { type: "activity_upsert", turn_id: turn.turn_id, turn_status: "completed", activity } };
  session.changes.push(event);
  for (const stream of session.streams) stream.write(`id: ${number}\nevent: session_changed\ndata: ${JSON.stringify(event)}\n\n`);
}
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://127.0.0.1");
  if (url.pathname.startsWith("/api/v1/")) {
    const route = url.pathname.slice(7), chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    const body = chunks.length ? JSON.parse(Buffer.concat(chunks).toString()) : {};
    if (route === "/me") return json(res, { auth_mode: "gateway_jwt" });
    if (route === "/devices" || route === "/sessions") return json(res, []);
    if (route === "/agent-connectors") return json(res, ["codex", "claude"].map(product => ({
      connector_id: connectorId(product), display_name: product === "codex" ? "Codex" : "Claude Code", agent_family: "coding-agent", agent_product: product,
      capabilities: { list: true, read: true, create: false },
      actions: [{ action_id: "fixture.input", title: "发送文字", description: "Local two-pane input fixture", input_channel: true, execution: "immediate" }],
    })));
    if (route === "/agent-sessions") return json(res, { sessions: [...sessions.values()].filter(session => connectorId(session.product) === url.searchParams.get("connector_id")).map(summary) });
    if (route === "/agent-session" || route === "/agent-session/stream") {
      const session = sessions.get(url.searchParams.get("session_id"));
      if (!session) return json(res, { code: "missing_session" }, 404);
      assert.equal(url.searchParams.get("connector_id"), connectorId(session.product));
      requests.push({ route, session: session.id });
      if (route === "/agent-session") return json(res, detail(session));
      res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-store" });
      res.write(": ready\n\n");
      for (const event of session.changes.filter(event => event.sequence > Number(url.searchParams.get("after") || 0))) {
        res.write(`id: ${event.sequence}\nevent: session_changed\ndata: ${JSON.stringify(event)}\n\n`);
      }
      session.streams.add(res);
      res.on("close", () => session.streams.delete(res));
      return;
    }
    if (route === "/agent-session/actions" && req.method === "POST") {
      const session = sessions.get(body.session_id);
      assert.equal(body.connector_id, connectorId(session.product));
      assert.equal(body.action_id, "fixture.input");
      submissions.push(body);
      const id = body.arguments.submission_id;
      if (nextActionMode) {
        actionPlans.set(id, { mode: nextActionMode, attempts: 0 });
        nextActionMode = undefined;
      }
      const plan = actionPlans.get(id);
      if (plan) {
        plan.attempts++;
        if (plan.mode === "retry" && plan.attempts === 1) return json(res, { code: "provider_unavailable", message: "Local retry fixture" }, 503);
        await new Promise(release => pendingReplies.push({ sessionId: session.id, submissionId: id, release }));
      }
      publish(session, body.arguments.text, "user_message", body.arguments.submission_id);
      return json(res, { status: { state: "completed" }, details: { delivery_status: "submitted" }, content: content("已提交") });
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
  const context = await browser.newContext({ viewport: { width, height: 844 }, isMobile: width < 600, hasTouch: true, serviceWorkers: "block" });
  const page = await context.newPage(), errors = [];
  page.on("pageerror", error => errors.push(error.message));
  page.on("console", message => {
    if (message.type() === "error" && message.text().includes("Orchestral browser error:")) errors.push(message.text());
  });
  page.setDefaultTimeout(15000);
  const primary = page.locator('.session-pane[data-pane="primary"]');
  const secondary = page.locator('.session-pane[data-pane="secondary"]');
  const wait = async predicate => { const deadline = Date.now() + 15000; while (!predicate()) { assert(Date.now() < deadline, "dual-session fixture timed out"); await new Promise(resolve => setTimeout(resolve, 50)); } };
  const atBottom = pane => pane.locator(".message-list").evaluate(el => el.scrollHeight - el.clientHeight - el.scrollTop <= 3);
  try {
    await page.goto(`http://127.0.0.1:${server.address().port}`);
    const tabs = page.getByRole("tablist", { name: "会话来源" });
    await tabs.getByRole("tab", { name: "Codex", exact: true }).waitFor();
    assert.deepEqual(await tabs.getByRole("tab").allTextContents(), ["Codex", "Claude Code", "Orch"]);
    assert.equal(await tabs.getByRole("tab", { name: "Codex", exact: true }).getAttribute("aria-selected"), "true");
    await primary.getByText("codex-1 历史 24。", { exact: false }).waitFor();
    await secondary.getByText("codex-2 历史 24。", { exact: false }).waitFor();
    await wait(() => sessions.get("codex-1").streams.size && sessions.get("codex-2").streams.size);
    assert.ok(requests.every(request => !request.session.endsWith("-3")), "opening a source only loads its two most recent sessions");
    const primaryBox = await primary.boundingBox(), secondaryBox = await secondary.boundingBox();
    assert.ok(primaryBox.y + primaryBox.height <= secondaryBox.y + 2, "sessions occupy independent top and bottom panes");
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);

    publish(sessions.get("codex-1"), "上屏独立实时消息");
    publish(sessions.get("codex-2"), "下屏独立实时消息");
    await primary.getByText("上屏独立实时消息", { exact: true }).waitFor();
    await secondary.getByText("下屏独立实时消息", { exact: true }).waitFor();
    assert.equal(await secondary.getByText("上屏独立实时消息", { exact: true }).count(), 0);
    assert.equal(await primary.getByText("下屏独立实时消息", { exact: true }).count(), 0);
    const upperInput = primary.getByRole("textbox", { name: "消息草稿" });
    const lowerInput = secondary.getByRole("textbox", { name: "消息草稿" });
    await upperInput.fill("上屏消息");
    await lowerInput.fill("下屏保留草稿");
    await primary.getByRole("button", { name: "发送消息", exact: true }).click();
    await primary.locator(".message--user").getByText("上屏消息", { exact: true }).waitFor();
    await wait(() => submissions.length === 1);
    assert.equal(submissions[0].session_id, "codex-1", "upper composer routes to its own running session");
    assert.equal(await lowerInput.inputValue(), "下屏保留草稿", "upper send must not clear the lower draft");
    await upperInput.fill("上屏后续草稿");
    await secondary.getByRole("button", { name: "发送消息", exact: true }).click();
    await secondary.locator(".message--user").getByText("下屏保留草稿", { exact: true }).waitFor();
    await wait(() => submissions.length === 2);
    assert.equal(submissions[1].session_id, "codex-2");
    assert.notEqual(submissions[0].arguments.submission_id, submissions[1].arguments.submission_id);
    assert.equal(await upperInput.inputValue(), "上屏后续草稿");

    await primary.locator(".message-list").evaluate(el => { el.scrollTop = 0; el.dispatchEvent(new Event("scroll")); });
    await primary.getByRole("button", { name: "跳到最新消息" }).waitFor();
    const oldTop = await primary.locator(".message-list").evaluate(el => el.scrollTop);
    publish(sessions.get("codex-1"), "上屏阅读历史时的新消息");
    publish(sessions.get("codex-2"), "下屏继续跟随最新消息");
    await primary.getByText("上屏阅读历史时的新消息", { exact: true }).waitFor({ state: "attached" });
    await secondary.getByText("下屏继续跟随最新消息", { exact: true }).waitFor();
    await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
    assert.ok(Math.abs(await primary.locator(".message-list").evaluate(el => el.scrollTop) - oldTop) <= 4, "upper manual scroll survives events in both panes");
    assert.equal(await atBottom(secondary), true, "lower pane still follows independently");
    await primary.getByRole("button", { name: "跳到最新消息" }).click();
    await page.waitForFunction(() => { const el = document.querySelector('.session-pane[data-pane="primary"] .message-list'); return el.scrollHeight - el.clientHeight - el.scrollTop <= 3; });

    await primary.getByRole("button", { name: "打开会话列表" }).click();
    assert.equal(await primary.locator(".thread-button").count(), 2, "another pane's occupied session is not offered twice");
    await primary.locator(".thread-button").filter({ hasText: "会话 codex-3" }).click();
    await primary.getByText("codex-3 历史 24。", { exact: false }).waitFor();
    assert.equal(await lowerInput.inputValue(), "");
    publish(sessions.get("codex-2"), "上屏切换后下屏流仍然存活");
    await secondary.getByText("上屏切换后下屏流仍然存活", { exact: true }).waitFor();

    nextActionMode = "hold";
    await lowerInput.fill("切换来源前下屏正在提交的消息");
    await secondary.getByRole("button", { name: "发送消息", exact: true }).click();
    await wait(() => pendingReplies.length === 1);
    await tabs.getByRole("tab", { name: "Claude Code", exact: true }).click();
    await primary.getByText("claude-1 历史 24。", { exact: false }).waitFor();
    await secondary.getByText("claude-2 历史 24。", { exact: false }).waitFor();
    await wait(() => sessions.get("claude-1").streams.size && sessions.get("claude-2").streams.size);
    publish(sessions.get("claude-1"), "Claude 上屏流");
    publish(sessions.get("claude-2"), "Claude 下屏流");
    await primary.getByText("Claude 上屏流", { exact: true }).waitFor();
    await secondary.getByText("Claude 下屏流", { exact: true }).waitFor();
    const beforeOldReply = requests.length;
    const oldReply = page.waitForResponse(response => new URL(response.url()).pathname === "/api/v1/agent-session/actions"
      && response.request().postDataJSON().session_id === "codex-2");
    pendingReplies[0].release();
    await oldReply;
    // The callback also awaits IndexedDB cleanup before it can restore any
    // transport. Let that completion run, then prove the new stream still works.
    await page.waitForTimeout(200);
    publish(sessions.get("claude-2"), "旧来源提交完成后新来源流仍然存活");
    await secondary.getByText("旧来源提交完成后新来源流仍然存活", { exact: true }).waitFor();
    assert.equal(await tabs.getByRole("tab", { name: "Claude Code", exact: true }).getAttribute("aria-selected"), "true");
    assert.equal(requests.slice(beforeOldReply).filter(request => request.session === "codex-2").length, 0, "an old POST completion must not reload or reopen the old session after switching source");
    assert.equal(await secondary.getByText("切换来源前下屏正在提交的消息", { exact: true }).count(), 0);
    await tabs.getByRole("tab", { name: "Codex", exact: true }).click();
    // Recent activity can reorder the pair on a source switch. Draft and
    // routing assertions follow session identity rather than its former slot.
    const firstSession = page.locator(".session-pane").filter({ has: page.getByText("上屏阅读历史时的新消息", { exact: true }) });
    const secondSession = page.locator(".session-pane").filter({ has: page.getByText("下屏继续跟随最新消息", { exact: true }) });
    await firstSession.waitFor();
    await secondSession.waitFor();
    const firstInput = firstSession.getByRole("textbox", { name: "消息草稿" });
    const secondInput = secondSession.getByRole("textbox", { name: "消息草稿" });
    assert.equal(await firstInput.inputValue(), "上屏后续草稿", "source switching restores the session-specific draft");

    nextActionMode = "retry";
    await secondInput.fill("会话二需要后台确认的消息");
    await secondSession.getByRole("button", { name: "发送消息", exact: true }).click();
    await wait(() => pendingReplies.length === 2);
    await secondInput.fill("会话二等待中的下一条草稿");
    assert.equal(await secondSession.getByRole("button", { name: "发送消息", exact: true }).isDisabled(), true, "a pending retry blocks only its own session");
    await firstInput.fill("会话一不受另一屏后台重试影响");
    assert.equal(await firstSession.getByRole("button", { name: "发送消息", exact: true }).isEnabled(), true, "another pane stays writable while shared outbox retries");
    await firstSession.getByRole("button", { name: "发送消息", exact: true }).click();
    await wait(() => submissions.some(body => body.session_id === "codex-1" && body.arguments.text === "会话一不受另一屏后台重试影响"));
    await firstSession.locator(".message--user").getByText("会话一不受另一屏后台重试影响", { exact: true }).waitFor();
    assert.equal(actionPlans.get(pendingReplies[1].submissionId).attempts, 2, "one retry retains the original submission identity");
    const retriedReply = page.waitForResponse(response => new URL(response.url()).pathname === "/api/v1/agent-session/actions"
      && response.request().postDataJSON().arguments.submission_id === pendingReplies[1].submissionId);
    pendingReplies[1].release();
    await retriedReply;
    await page.waitForFunction(paneId => !document.querySelector(`.session-pane[data-pane="${paneId}"] .send-button`).disabled, await secondSession.getAttribute("data-pane"));
    assert.equal(await secondInput.inputValue(), "会话二等待中的下一条草稿");
    assert.deepEqual(errors, []);
    fs.mkdirSync("target/pwa-smoke", { recursive: true });
    await page.screenshot({ path: `target/pwa-smoke/dual-session-${width}.png` });
    console.log(JSON.stringify({ dualSessionBrowser: true, defaultCodex: true, latestTwo: true, independentStreams: true, isolatedDraftsAndRoutes: true, independentScroll: true, paneHistorySwitch: true, sourceSwitch: true, lateReplyKeepsNewSource: true, retryDoesNotBlockOtherPane: true, width, paidModelRequests: 0 }));
  } catch (error) {
    fs.mkdirSync("target/pwa-smoke", { recursive: true });
    await page.screenshot({ path: `target/pwa-smoke/dual-session-failed-${width}.png` });
    console.error(JSON.stringify({ requests, submissions: submissions.map(({ session_id, action_id }) => ({ session_id, action_id })), errors }));
    console.error((await page.locator("body").innerText()).slice(-3000));
    throw error;
  } finally {
    for (const pending of pendingReplies) pending.release();
    await browser.close();
    for (const session of sessions.values()) for (const stream of session.streams) stream.end();
    server.closeAllConnections();
    await new Promise(resolve => server.close(resolve));
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
