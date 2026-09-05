// Run from the repository root with Playwright on NODE_PATH (see the PWA README).
const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");
const assert = require("node:assert/strict");
const { chromium } = require("playwright");
const dist = path.join(process.cwd(), "web/orchestral-web/dist");
const width = Number(process.env.PWA_SMOKE_WIDTH || 390);
const now = Date.now();
const content = (text) => [{ body: { kind: "inline", value: text } }];
const approval = {
  request_id: "native-approval",
  blocking: true,
  payload: {
    type: "approval",
    reason: "Update the requested workspace file; Reason: 保存你要求的修改",
    requested_scope: ["filesystem_write:workspace", "process:workspace"],
  },
};
const inputRequest = {
  request_id: "host-input",
  blocking: true,
  payload: { type: "input", prompt: content("请选择下一步要检查的模块") },
};
let nativePending = [approval],
  hostPending = [inputRequest],
  sequence = 20,
  approvalAttempts = 0,
  calls = [],
  effects = new Map(),
  sockets = new Set();
let records = [
  {
    event: { run_seq: 1, event_id: "start", payload: { type: "run_started" } },
  },
  {
    event: {
      run_seq: 2,
      event_id: "input",
      payload: { type: "input_committed", content: content("检查当前工作区") },
    },
  },
  {
    event: {
      run_seq: 3,
      event_id: "request",
      payload: { type: "request_opened", request: inputRequest },
    },
  },
];
const summary = (id) => ({
  connector_id: "fixture/local",
  session_id: id,
  title: id === "a" ? "同步验证会话" : "另一个会话",
  state: "active",
  created_at_unix_ms: now - 60000,
  updated_at_unix_ms: now - 30000,
  cwd: "/workspace/demo",
});
const view = () => ({
  execution: { run_id: "owner", session_id: "a" },
  state: { state: hostPending.length ? "waiting" : "running" },
  last_run_seq: records.length,
  pending_requests: hostPending,
  input: content("检查当前工作区"),
});
const history = (id) => ({
  summary: summary(id),
  stream_cursor: sequence,
  turns: [
    {
      turn_id: "native-turn",
      status: "active",
      activities: Array.from({ length: 8 }, (_, i) => ({
        activity_id: "native-" + i,
        kind: i % 2 ? "agent_message" : "user_message",
        status: "completed",
        content: content(
          i % 2 ? "已完成这一项检查，继续整理结果。" : "历史任务 " + (i + 1),
        ),
      })),
    },
  ],
  pending_requests: id === "a" ? nativePending : [],
  controlled_runs:
    id === "a"
      ? [
          {
            ...view(),
            created_at_unix_ms: now - 10000,
            after_activity_id: "native-7",
          },
        ]
      : [],
  next_cursor: null,
});
const json = (res, data, status = 200) => {
  res.writeHead(status, {
    "content-type": "application/json",
    "cache-control": "no-store",
  });
  res.end(JSON.stringify(data));
};
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://127.0.0.1");
  if (url.pathname.startsWith("/api/v1/")) {
    const route = url.pathname.slice("/api/v1".length);
    let raw = "";
    for await (const chunk of req) raw += chunk;
    const body = raw ? JSON.parse(raw) : {};
    if (route === "/me") return json(res, { auth_mode: "gateway_jwt" });
    if (route === "/devices" || route === "/sessions") return json(res, []);
    if (route === "/agent-connectors")
      return json(res, [
        {
          connector_id: "fixture/local",
          display_name: "Test Agent",
          agent_family: "fixture",
          capabilities: {
            list: true,
            read: true,
            create: true,
            resolve_requests: true,
          },
        },
      ]);
    if (route === "/agent-sessions")
      return json(res, { sessions: [summary("a"), summary("b")] });
    if (route === "/agent-session")
      return json(res, history(url.searchParams.get("session_id")));
    if (route === "/runs/owner") return json(res, view());
    if (route === "/runs/owner/events")
      return json(res, {
        records: records.filter(
          (x) => x.event.run_seq > Number(url.searchParams.get("after") || 0),
        ),
      });
    if (route.endsWith("/stream")) {
      res.writeHead(200, {
        "content-type": "text/event-stream",
        "cache-control": "no-cache",
      });
      res.write(": ready\n\n");
      sockets.add(res);
      const timer = setInterval(() => res.write(": keep-alive\n\n"), 1000);
      res.on("close", () => {
        clearInterval(timer);
        sockets.delete(res);
      });
      return;
    }
    if (route === "/agent-runs") {
      calls.push(body);
      const first = !effects.has(body.run_id);
      if (first) {
        effects.set(body.run_id, body.input);
        records.push({
          event: {
            run_seq: records.length + 1,
            event_id: "command-" + body.run_id,
            payload: {
              type: "command_received",
              command: {
                command_id: "agent-submit-" + body.run_id,
                payload: { type: "steer", content: content(body.input) },
                extensions: {
                  "orchestral.dev/session-history-anchor": {
                    after_activity_id: body.after_activity_id,
                  },
                },
              },
            },
          },
        });
      }
      await new Promise((r) => setTimeout(r, first ? 500 : 1500));
      if (first)
        return json(
          res,
          { code: "run_recovery_pending", message: "暂时无法确认，请稍候" },
          503,
        );
      return json(res, {
        run_id: "owner",
        operation: "steered",
        command_id: "agent-submit-" + body.run_id,
        view: view(),
      });
    }
    if (route === "/agent-session/requests/native-approval/approval") {
      approvalAttempts++;
      if (approvalAttempts === 1)
        return json(
          res,
          { code: "approval_retry", message: "审批暂未成功，请重试" },
          409,
        );
      nativePending = [];
      return json(res, { resolved: true });
    }
    if (route === "/runs/owner/requests/host-input/input") {
      hostPending = [];
      return json(res, { command_id: "answer", state: { state: "applied" } });
    }
    return json(res, { code: "mock_missing", message: route }, 404);
  }
  let requested = decodeURIComponent(url.pathname);
  if (requested === "/") requested = "/index.html";
  const file = path.join(dist, requested);
  if (!file.startsWith(dist) || !fs.existsSync(file)) {
    res.writeHead(404);
    return res.end("missing");
  }
  const type =
    {
      ".html": "text/html",
      ".js": "text/javascript",
      ".wasm": "application/wasm",
      ".css": "text/css",
      ".svg": "image/svg+xml",
      ".png": "image/png",
    }[path.extname(file)] || "application/octet-stream";
  res.writeHead(200, { "content-type": type });
  fs.createReadStream(file).pipe(res);
});
(async () => {
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  const browser = await chromium.launch({
    headless: true,
    channel: process.env.PWA_SMOKE_CHANNEL || "chrome",
  });
  const context = await browser.newContext({
    viewport: { width, height: 844 },
    deviceScaleFactor: 1,
    isMobile: true,
    hasTouch: true,
    serviceWorkers: "block",
  });
  const page = await context.newPage();
  let errors = [];
  page.on("pageerror", (e) => errors.push(e.message));
  page.setDefaultTimeout(12000);
  const openSession = async (title) => {
    await page.getByRole("button", { name: "打开会话列表" }).click();
    await page.getByRole("tab", { name: /Test Agent/ }).click();
    await page
      .locator(".thread-button")
      .nth(title === "同步验证会话" ? 0 : 1)
      .click();
    await page.locator(".message-input").waitFor();
  };
  try {
    await page.goto("http://127.0.0.1:" + server.address().port);
    await openSession("同步验证会话");
    await page
      .locator(".pending-card")
      .filter({ hasText: "保存你要求的修改" })
      .waitFor();
    assert.equal(
      await page.locator(".pending-card").count(),
      2,
      "both Host input and native approval must be visible",
    );
    const input = page.getByRole("textbox", { name: "消息草稿" });
    await input.fill("保留在会话 A 的草稿");
    await openSession("另一个会话");
    assert.equal(await input.inputValue(), "");
    await input.fill("会话 B 草稿");
    await openSession("同步验证会话");
    assert.equal(await input.inputValue(), "保留在会话 A 的草稿");
    await input.fill("中文确认不会误发送");
    await input.dispatchEvent("keydown", {
      key: "Enter",
      code: "Enter",
      isComposing: true,
    });
    assert.equal(calls.length, 0, "IME confirmation must not send");
    await page.getByRole("button", { name: "发送消息", exact: true }).click();
    await page.getByRole("button", { name: "正在发送", exact: true }).waitFor();
    await page.waitForFunction(
      () => document.querySelector(".message-input").value === "",
    );
    await page
      .getByText("正在确认上一条消息的发送状态，可以继续编辑草稿", {
        exact: true,
      })
      .waitFor();
    await input.fill("下一条草稿");
    assert.equal(
      await page
        .getByRole("button", { name: "发送消息", exact: true })
        .isDisabled(),
      true,
      "later sends wait for earlier confirmation",
    );
    await input.press("Enter");
    assert.equal(
      await input.inputValue(),
      "下一条草稿",
      "blocked Enter preserves the next draft",
    );
    await page.getByRole("button", { name: "发送消息", exact: true }).waitFor();
    await page.waitForFunction(
      () => !document.querySelector(".send-button").disabled,
    );
    await input.fill("");
    assert.equal(
      effects.size,
      1,
      "one logical send across ambiguous HTTP retry",
    );
    assert.ok(calls.length >= 2, "503 was retried");
    assert.equal(
      new Set(calls.map((x) => x.run_id)).size,
      1,
      "retry retains the original operation id",
    );
    assert.equal(
      await page
        .locator(".message--user .message__content")
        .filter({ hasText: "中文确认不会误发送" })
        .count(),
      1,
      "redirected outbox echo appears once",
    );
    const card = page
      .locator(".pending-card")
      .filter({ hasText: "保存你要求的修改" });
    await card.getByRole("button", { name: "允许一次" }).click();
    await card.locator("[role=alert]").waitFor();
    assert.equal(
      await card.getByRole("button", { name: "允许一次" }).isEnabled(),
      true,
      "approval can retry after failure",
    );
    assert.equal(
      await page.evaluate(
        () => document.documentElement.scrollWidth > innerWidth,
      ),
      false,
      "no horizontal page overflow",
    );
    await page.locator(".pending-panel__body").evaluate((el) => {
      el.scrollTop = el.scrollHeight;
    });
    const headerBottom = await page
      .locator(".pending-panel__header")
      .evaluate((el) => el.getBoundingClientRect().bottom);
    const bodyTop = await page
      .locator(".pending-panel__body")
      .evaluate((el) => el.getBoundingClientRect().top);
    assert.ok(
      headerBottom <= bodyTop,
      "pending header must not cover the scrolling cards",
    );
    const button = card.getByRole("button", { name: "允许一次" });
    const box = await button.boundingBox();
    assert.ok(box.height >= 44, "approval touch target");
    const composerTop = await page
      .locator(".composer-dock")
      .evaluate((el) => el.getBoundingClientRect().top);
    assert.ok(
      box.y + box.height <= composerTop,
      "approval actions stay above the composer",
    );
    await page.getByRole("button", { name: "关闭提示" }).click();
    fs.mkdirSync("target/pwa-smoke", { recursive: true });
    await page.screenshot({
      path: `target/pwa-smoke/mobile-${width}.png`,
      fullPage: true,
    });
    await card.getByRole("button", { name: "允许一次" }).click();
    await card.waitFor({ state: "detached" });
    await context.setOffline(true);
    await input.fill("离线草稿");
    assert.equal(await input.isEnabled(), true);
    assert.equal(
      await page
        .getByRole("button", { name: "发送消息", exact: true })
        .isDisabled(),
      true,
    );
    await context.setOffline(false);
    assert.deepEqual(errors, [], "no browser runtime exceptions");
    console.log(
      JSON.stringify({
        passed: true,
        checks: [
          `${width}px layout`,
          "union of pending requests",
          "session drafts",
          "IME enter",
          "503 outbox retry",
          "stable send identity",
          "ordered send confirmation",
          "44px approval targets",
          "unobscured approval actions",
          "single message projection",
          "approval failure and retry",
          "offline draft",
        ],
        httpSubmissions: calls.length,
        effects: effects.size,
        errors,
      }),
    );
  } catch (error) {
    fs.mkdirSync("target/pwa-smoke", { recursive: true });
    await page.screenshot({
      path: "target/pwa-smoke/failure.png",
      fullPage: true,
    });
    console.error(error);
    console.error("Browser errors:", errors);
    console.error((await page.locator("body").innerText()).slice(-4000));
    process.exitCode = 1;
  } finally {
    await browser.close();
    for (const res of sockets) res.end();
    server.close();
  }
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
