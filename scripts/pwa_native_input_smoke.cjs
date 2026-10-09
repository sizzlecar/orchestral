// Native input confirmation and SSE regression; uses only a local protocol fixture.
const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");
const assert = require("node:assert/strict");
const { chromium } = require("playwright");

const dist = path.join(process.cwd(), "web/orchestral-web/dist");
const width = Number(process.env.PWA_SMOKE_WIDTH || 390);
const now = Date.now();
const content = (text) => [{ body: { kind: "inline", value: text } }];
const calls = [], submissions = [], streams = new Set(), changes = [];
let sequence = 0, pendingReply, releaseReply;
const summary = () => ({
  connector_id: "fixture/local", session_id: "native", title: "原生会话输入验证",
  state: "busy_elsewhere", input_action: "fixture.input",
  created_at_unix_ms: now - 60000, updated_at_unix_ms: now,
  cwd: "/workspace/demo",
});
const activity = (entry) => ({
  activity_id: `${entry.echo ? "native" : "pending"}:${entry.id}`,
  kind: "user_message", status: entry.echo ? "completed" : "pending",
  occurred_at_unix_ms: entry.time, content: content(entry.text),
  details: { clientId: entry.id, ...(entry.echo ? {} : { phase: "deferred" }) },
});
const detail = () => ({
  summary: summary(), pending_requests: [], controlled_runs: [], stream_cursor: sequence,
  turns: submissions.map((entry) => ({
    turn_id: `${entry.echo ? "native" : "pending"}:${entry.id}`,
    status: entry.echo ? "completed" : "pending", activities: [activity(entry)],
  })),
});
const publish = (change) => {
  const item = { connector_id: "fixture/local", session_id: "native", sequence: ++sequence, change };
  changes.push(item);
  for (const stream of streams) stream.write(`id: ${item.sequence}\nevent: session_changed\ndata: ${JSON.stringify(item)}\n\n`);
};
const json = (res, value, status = 200) => {
  res.writeHead(status, { "content-type": "application/json", "cache-control": "no-store" });
  res.end(JSON.stringify(value));
};
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://127.0.0.1");
  if (url.pathname.startsWith("/api/v1/")) {
    const route = url.pathname.slice("/api/v1".length);
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    if (route === "/me") return json(res, { auth_mode: "gateway_jwt" });
    if (route === "/devices" || route === "/sessions") return json(res, []);
    if (route === "/agent-connectors") return json(res, [{
      connector_id: "fixture/local", provider_binding: "fixture/local", agent_family: "coding-agent",
      display_name: "Native Agent", capabilities: { list: true, read: true, create: false },
      actions: [{ action_id: "fixture.input", title: "发送文字", description: "Native input fixture", input_channel: true, execution: "immediate" }],
    }]);
    if (route === "/agent-sessions") return json(res, { sessions: [summary()] });
    if (route === "/agent-session") return json(res, detail());
    if (route === "/agent-session/stream") {
      res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
      res.flushHeaders();
      const after = Number(url.searchParams.get("after") || 0);
      for (const change of changes.filter((item) => item.sequence > after)) res.write(`id: ${change.sequence}\nevent: session_changed\ndata: ${JSON.stringify(change)}\n\n`);
      streams.add(res);
      res.on("close", () => streams.delete(res));
      return;
    }
    if (route === "/agent-session/actions" && req.method === "POST") {
      const body = JSON.parse(Buffer.concat(chunks).toString());
      assert.equal(body.action_id, "fixture.input");
      calls.push(body);
      const entry = { id: body.arguments.submission_id, text: body.arguments.text, time: Date.now(), echo: false };
      submissions.push(entry);
      pendingReply = new Promise((resolve) => { releaseReply = resolve; });
      await pendingReply;
      return json(res, { status: { state: "completed" }, details: { delivery_status: "submitted" }, content: content("消息已提交，等待确认") });
    }
    return json(res, { code: "not_found", message: route }, 404);
  }
  const file = path.join(dist, url.pathname === "/" ? "index.html" : url.pathname);
  if (!file.startsWith(dist + path.sep) || !fs.existsSync(file) || !fs.statSync(file).isFile()) { res.writeHead(404); res.end(); return; }
  const type = { ".html": "text/html", ".js": "application/javascript", ".wasm": "application/wasm", ".css": "text/css", ".svg": "image/svg+xml", ".png": "image/png" }[path.extname(file)];
  res.writeHead(200, { "content-type": type || "application/octet-stream" });
  fs.createReadStream(file).pipe(res);
});

(async () => {
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const browser = await chromium.launch({ headless: true, channel: process.env.PWA_SMOKE_CHANNEL || "chrome" });
  const context = await browser.newContext({ viewport: { width, height: 844 }, isMobile: true, hasTouch: true, serviceWorkers: process.env.PWA_SMOKE_SW === "1" ? "allow" : "block" });
  const page = await context.newPage(), errors = [];
  page.on("pageerror", (error) => errors.push(error.stack || error.message));
  page.on("console", (message) => { if (message.type() === "error") errors.push(message.text()); });
  page.setDefaultTimeout(12000);
  try {
    await page.goto(`http://127.0.0.1:${server.address().port}`);
    const drawer = page.getByRole("button", { name: "打开会话列表" });
    await drawer.click();
    await page.getByRole("tab", { name: /Native Agent/ }).click();
    await page.locator(".thread-button").first().click();
    for (let sent = 1; sent <= 2; sent++) {
      await page.getByRole("textbox", { name: "消息草稿" }).fill("继续检查");
      await page.getByRole("button", { name: "发送消息", exact: true }).click();
      await page.waitForFunction((count) => document.querySelectorAll(".message--user").length === count, sent);
      await page.waitForFunction(() => Array.from(document.querySelectorAll(".message__meta")).some((item) => item.textContent.includes("发送中")));
      // Read another snapshot while HTTP is still pending. The message must
      // remain visible, even when the native transcript has no echo yet.
      await drawer.click();
      await Promise.all([page.waitForResponse((res) => new URL(res.url()).pathname === "/api/v1/agent-session"), page.locator(".thread-button").first().click()]);
      assert.equal(await page.locator(".message--user").count(), sent);
      releaseReply();
      await page.waitForFunction(() => document.querySelector(".message-input").value === "");
      await page.waitForFunction(() => Array.from(document.querySelectorAll(".message__meta")).some((item) => item.textContent.includes("等待原 Agent 确认")));
      const entry = submissions[sent - 1];
      entry.echo = true;
      publish({ type: "activity_upsert", turn_id: `native:${entry.id}`, turn_status: "completed", activity: activity(entry) });
      await page.waitForFunction((id) => document.querySelector(`[data-message-id="native:${id}"]`) !== null, entry.id);
      assert.equal(await page.locator(".message--user").count(), sent, "native echo must replace pending input by identity");
      assert.equal(await page.locator(".message--deferred").count(), 0);
    }
    assert.equal(calls.length, 2, "each input must have one dispatch");
    assert.notEqual(calls[0].arguments.submission_id, calls[1].arguments.submission_id, "identical text is two distinct submissions");
    await page.reload();
    await drawer.click();
    await page.getByRole("tab", { name: /Native Agent/ }).click();
    await page.locator(".thread-button").first().click();
    await page.waitForFunction(() => document.querySelectorAll(".message--user").length === 2);
    assert.deepEqual(errors, [], "native input must not produce browser runtime errors");
    fs.mkdirSync("target/pwa-smoke", { recursive: true });
    await page.screenshot({ path: `target/pwa-smoke/native-input-${width}.png` });
    console.log(JSON.stringify({ nativeInputBrowser: true, immediateMessage: true, delayedConfirmation: true, nativeEchoDeduplication: true, reload: true, paidModelRequests: 0, width }));
  } finally {
    if (releaseReply) releaseReply();
    await browser.close();
    for (const stream of streams) stream.end();
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  }
})().catch((error) => { console.error(error); process.exitCode = 1; });
