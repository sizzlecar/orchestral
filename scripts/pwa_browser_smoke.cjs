// Run from the repository root with Playwright on NODE_PATH (see the PWA README).
const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");
const assert = require("node:assert/strict");
const { chromium } = require("playwright");
const dist = path.join(process.cwd(), "web/orchestral-web/dist");
const width = Number(process.env.PWA_SMOKE_WIDTH || 390);
const now = Date.now();
const testWorker = process.env.PWA_SMOKE_SW === "1";
let workerRevision = 0;
const workerRequests = [];
const sessionStreams = new Map();
const createdSessionChanges = [];
let createdSession = false;
let sessionActionCompleted = false;
let lifecycleSnapshotReads = 0;
const recoveryChanges = [];
let recoveryPhase = "cancelled";
let recoveryEventReads = 0;
const recoveryFailure = { code: "provider_unavailable", message: "当前回合连接失败" };
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
  exposeNativePending = false,
  requestRouteReads = 0,
  failNextRequestRouteRead = false,
  hostPending = [inputRequest, approval],
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
records.push({
  event: {
    run_seq: 4,
    event_id: "native-approval-mirror",
    payload: { type: "request_opened", request: approval },
  },
});
const summary = (id) => ({
  connector_id: "fixture/local",
  session_id: id,
  title: id === "a" ? "同步验证会话" : id === "created" ? "新建实时会话" : id === "recovery" ? "运行恢复验证" : "另一个会话",
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
const cancelledRun = () => ({
  execution: { run_id: "recovery-old", session_id: "recovery" },
  state: { state: "cancelled" },
  last_run_seq: 1,
  input: content("保留旧回合的输入记录"),
  created_at_unix_ms: now - 5000,
  after_activity_id: "recovery-context",
});
const recoveryHistory = () => ({
  summary: summary("recovery"),
  stream_cursor: recoveryChanges.length,
  turns: [{
    turn_id: "recovery-native-turn",
    status: recoveryPhase === "failed" ? "failed" : "active",
    failure: recoveryPhase === "failed" ? recoveryFailure : null,
    activities: [
      { activity_id: "recovery-context", kind: "agent_message", status: "completed", content: content("已有会话上下文") },
      ...(recoveryPhase === "cancelled" ? [] : [{
        activity_id: "recovery-input", kind: "user_message", status: "completed",
        content: content("继续新的正常回合"),
        details: { clientId: "orchestral:recovery-latest:sha256:fixture" },
      }]),
    ],
  }],
  controlled_runs: [cancelledRun(), ...(recoveryPhase === "cancelled" ? [] : [{
    execution: { run_id: "recovery-latest", session_id: "recovery" },
    state: { state: "running" },
    last_run_seq: 0,
    input: content("继续新的正常回合"),
    created_at_unix_ms: now - 1000,
    after_activity_id: "recovery-context",
  }])],
  pending_requests: [],
  next_cursor: null,
});
const history = (id) => id === "recovery" ? recoveryHistory() : ({
  summary: summary(id),
  stream_cursor: id === "created" ? createdSessionChanges.length : sequence,
  turns: id === "created" ? [{
    turn_id: "created-turn",
    status: "active",
    activities: [
      ...createdSessionChanges.map(change => change.change.activity),
      ...(sessionActionCompleted ? [{
        activity_id: "action-snapshot",
        kind: "agent_message",
        status: "completed",
        content: content("会话操作后的快照已同步"),
      }] : []),
    ],
  }] : [
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
  pending_requests: id === "a" && exposeNativePending ? nativePending : [],
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
const writeSessionChange = (res, change) => {
  res.write(`id: ${change.sequence}\nevent: session_changed\ndata: ${JSON.stringify(change)}\n\n`);
};
const publishCreatedSessionMessage = (text) => {
  const change = {
    connector_id: "fixture/local",
    session_id: "created",
    sequence: createdSessionChanges.length + 1,
    change: {
      type: "activity_upsert",
      turn_id: "created-turn",
      turn_status: "active",
      activity: {
        activity_id: `live-${createdSessionChanges.length + 1}`,
        kind: "agent_message",
        status: "completed",
        content: content(text),
      },
    },
  };
  createdSessionChanges.push(change);
  for (const [res, sessionId] of sessionStreams)
    if (sessionId === "created") writeSessionChange(res, change);
};
const publishRecoveryChange = (change) => {
  const event = {
    connector_id: "fixture/local",
    session_id: "recovery",
    sequence: recoveryChanges.length + 1,
    change,
  };
  recoveryChanges.push(event);
  for (const [res, sessionId] of sessionStreams)
    if (sessionId === "recovery") writeSessionChange(res, event);
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
          actions: [{
            action_id: "session.refresh",
            title: "更新会话配置",
            description: "验证对话框关闭后的快照与实时更新",
            execution: "immediate",
          }],
        },
      ]);
    if (route === "/agent-sessions") {
      if (req.method === "POST") {
        assert.equal(body.connector_id, "fixture/local");
        createdSession = true;
        return json(res, summary("created"));
      }
      return json(res, { sessions: [summary("a"), summary("b"), summary("recovery"), ...(createdSession ? [summary("created")] : [])] });
    }
    if (route === "/agent-session/actions") {
      assert.equal(body.session_id, "created");
      assert.equal(body.action_id, "session.refresh");
      sessionActionCompleted = true;
      return json(res, { status: { state: "completed" }, session: summary("created") });
    }
    if (route === "/agent-session") {
      if (url.searchParams.get("session_id") === "created") {
        lifecycleSnapshotReads++;
        // The response must arrive after the action dialog has unmounted.
        // A task owned by that dialog would be cancelled during this await.
        if (sessionActionCompleted) await new Promise(resolve => setTimeout(resolve, 200));
      }
      if (url.searchParams.get("limit") === "1") {
        requestRouteReads++;
        if (failNextRequestRouteRead) {
          failNextRequestRouteRead = false;
          return json(res, {code: "agent_provider_unavailable", message: "暂时无法确认请求，请重试"}, 503);
        }
      }
      return json(res, history(url.searchParams.get("session_id")));
    }
    if (route === "/runs/owner") return json(res, view());
    if (route === "/runs/recovery-old") return json(res, cancelledRun());
    if (route === "/runs/recovery-old/events") {
      recoveryEventReads++;
      const after = Number(url.searchParams.get("after") || 0);
      return json(res, { after, next: 1, records: after < 1 ? [{ event: {
        run_seq: 1,
        event_id: "recovery-cancelled",
        payload: { type: "run_cancelled", reason: "旧回合被 watchdog 安全停止" },
      } }] : [] });
    }
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
      if (route === "/agent-session/stream") {
        const sessionId = url.searchParams.get("session_id");
        sessionStreams.set(res, sessionId);
        if (sessionId === "created")
          for (const change of createdSessionChanges)
            if (change.sequence > Number(url.searchParams.get("after") || 0))
              writeSessionChange(res, change);
        if (sessionId === "recovery")
          for (const change of recoveryChanges)
            if (change.sequence > Number(url.searchParams.get("after") || 0))
              writeSessionChange(res, change);
      }
      const timer = setInterval(() => res.write(": keep-alive\n\n"), 1000);
      res.on("close", () => {
        clearInterval(timer);
        sockets.delete(res);
        sessionStreams.delete(res);
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
      assert.deepEqual(
        Object.keys(body),
        ["decision"],
        "native approval is not a Run command",
      );
      approvalAttempts++;
      if (approvalAttempts === 1)
        return json(
          res,
          { code: "approval_unavailable", message: "审批暂未成功，请重试" },
          409,
        );
      nativePending = [];
      hostPending = hostPending.filter(request => request.request_id !== approval.request_id);
      return json(res, { resolved: true });
    }
    if (route === "/agent-session/requests/native-input/input") {
      assert.deepEqual(
        Object.keys(body),
        ["text"],
        "native input is not a Run command",
      );
      nativePending = nativePending.filter(
        (request) => request.request_id !== "native-input",
      );
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
  if (requested === "/sw.js") {
    workerRequests.push(workerRevision);
    let worker = fs.readFileSync(file, "utf8");
    if (workerRevision)
      worker = worker.replace(/BUILD_ID="([^"]+)"/, 'BUILD_ID="$1-smoke-new"');
    res.writeHead(200, {
      "content-type": "text/javascript",
      "cache-control": "no-store",
    });
    return res.end(worker);
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
    serviceWorkers: testWorker ? "allow" : "block",
  });
  const page = await context.newPage();
  let documentLoads = 0;
  page.on("framenavigated", frame => {
    if (frame === page.mainFrame()) documentLoads++;
  });
  let errors = [];
  page.on("pageerror", (e) => errors.push(e.stack || e.message));
  page.on("console", (message) => {
    if (message.type() === "error")
      console.error("Browser console:", message.text());
  });
  page.setDefaultTimeout(12000);
  const openSession = async (title) => {
    const sessionId = title === "同步验证会话" ? "a" : "b";
    // A loaded session uses its latest user message as the display title;
    // live updates also reorder the list. Select by its fixture content,
    // never by a fixed list position or only its initial summary title.
    const displayTitles = new RegExp(`^(?:${[
      title,
      ...(sessionId === "a" ? ["检查当前工作区", ...effects.values()] : ["历史任务 7"]),
    ].map(value => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|")})$`);
    await page.getByRole("button", { name: "打开会话列表" }).click();
    await page.getByRole("tab", { name: /Test Agent/ }).click();
    await Promise.all([
      page.waitForResponse(response => {
        const url = new URL(response.url());
        return url.pathname === "/api/v1/agent-session" && url.searchParams.get("session_id") === sessionId;
      }),
      page.locator(".thread-button")
        .filter({ has: page.locator(".thread-button__title", { hasText: displayTitles }) })
        .click(),
    ]);
    await page.getByRole("heading", { name: displayTitles }).waitFor();
    await page.locator(".message-input").waitFor();
  };
  try {
    await page.goto("http://127.0.0.1:" + server.address().port);
    await page.getByRole("button", { name: "新建会话", exact: true }).click();
    await page.locator(".session-create-card").filter({ hasText: "Test Agent" })
      .getByRole("button", { name: "创建会话", exact: true }).click();
    await page.getByRole("button", { name: "关闭新建会话" }).waitFor({ state: "detached" });
    const lifecycleDocumentLoads = documentLoads;
    publishCreatedSessionMessage("创建弹窗关闭后实时消息仍然到达");
    await page.getByText("创建弹窗关闭后实时消息仍然到达", { exact: true }).waitFor();
    assert.equal(lifecycleSnapshotReads, 0, "new session message must arrive through the live stream");

    await page.getByRole("button", { name: "打开会话操作" }).click();
    await page.locator(".session-action-card").filter({ hasText: "更新会话配置" })
      .getByRole("button", { name: "执行", exact: true }).click();
    await page.getByRole("button", { name: "关闭会话操作" }).waitFor({ state: "detached" });
    await page.getByText("会话操作后的快照已同步", { exact: true }).waitFor();
    assert.ok(lifecycleSnapshotReads > 0, "session action must finish the delayed snapshot refresh");
    publishCreatedSessionMessage("操作弹窗关闭后实时消息仍然到达");
    await page.getByText("操作弹窗关闭后实时消息仍然到达", { exact: true }).waitFor();
    assert.equal(documentLoads, lifecycleDocumentLoads, "session creation and actions must not require a page reload");

    await page.getByRole("button", { name: "打开会话列表" }).click();
    await page.locator(".thread-button").filter({ hasText: "运行恢复验证" }).click();
    const oldFailure = page.locator(".run-failure").filter({ hasText: "cancelled" });
    await oldFailure.filter({ hasText: "旧回合被 watchdog 安全停止" }).waitFor();
    const recoveryDocumentLoads = documentLoads;
    recoveryPhase = "running";
    publishRecoveryChange({ type: "refresh_required", reason: "new_controlled_run" });
    await page.locator(".message--user .message__content").filter({ hasText: "继续新的正常回合" }).waitFor();
    await oldFailure.waitFor({ state: "detached" });
    assert.equal(await page.locator(".message--user .message__content").filter({ hasText: "继续新的正常回合" }).count(), 1,
      "native client identity must deduplicate the latest Host mirror");
    await page.locator(".message--user .message__content").filter({ hasText: "保留旧回合的输入记录" }).waitFor();
    recoveryPhase = "failed";
    publishRecoveryChange({ type: "turn_status", turn_id: "recovery-native-turn", status: "failed", failure: recoveryFailure });
    await page.locator(".run-failure").filter({ hasText: "provider_unavailable" }).filter({ hasText: "当前回合连接失败" }).waitFor();
    assert.equal(await oldFailure.count(), 0, "current failure must not revive a cancelled historical footer");
    assert.equal(documentLoads, recoveryDocumentLoads, "recovery and current failure must update without a page reload");

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
    // The browser only knows the Run mirror. The request now exists in the
    // server snapshot, but no session event/refresh announces it to the page.
    exposeNativePending = true;
    failNextRequestRouteRead = true;
    await card.getByRole("button", { name: "允许一次" }).click();
    await card.locator("[role=alert]").waitFor();
    assert.equal(approvalAttempts, 0, "failed ownership read must not submit any approval");
    assert.ok(requestRouteReads > 0, "a Run card must resolve current request ownership");
    const readsBeforeRetry = requestRouteReads;
    await Promise.all([
      page.waitForResponse(response => response.url().includes("/agent-session/requests/native-approval/approval")),
      card.getByRole("button", { name: "允许一次" }).click(),
    ]);
    await card.locator("[role=alert]").waitFor();
    assert.ok(requestRouteReads > readsBeforeRetry, "retry must resolve ownership again");
    assert.equal(approvalAttempts, 1, "mirrored approval must use the native session endpoint");
    assert.equal(await page.getByText("该审批已由其他客户端处理", { exact: true }).count(), 0);
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
    nativePending.push({
      request_id: "native-input",
      blocking: true,
      payload: { type: "input", prompt: content("补充输入回归") },
    });
    await openSession("另一个会话");
    await openSession("同步验证会话");
    const nativeInput = page.locator('[data-request-id="native-input"]');
    await nativeInput.getByRole("textbox").fill("继续检查");
    await nativeInput
      .getByRole("button", { name: "继续", exact: true })
      .click();
    await nativeInput.waitFor({ state: "detached" });
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
    await page.waitForFunction(
      () =>
        !document
          .querySelector(".composer-hint")
          .textContent.includes("当前离线"),
    );
    if (testWorker) {
      await page.waitForFunction(async () => {
        const registration = await navigator.serviceWorker.getRegistration();
        return registration?.active && navigator.serviceWorker.controller;
      });
      await input.fill("升级后保留的草稿");
      workerRevision = 1;
      // Exercise the real foreground update check, including worker install,
      // cache activation, client notification and the app's explicit update UI.
      await page.evaluate(() =>
        document.dispatchEvent(
          new Event("visibilitychange", { bubbles: true }),
        ),
      );
      const update = page.getByRole("button", {
        name: "刷新更新",
        exact: true,
      });
      await update.waitFor();
      assert.equal(
        await input.inputValue(),
        "升级后保留的草稿",
        "update discovery preserves active draft",
      );
      await Promise.all([
        page.waitForNavigation({ waitUntil: "domcontentloaded" }),
        update.click(),
      ]);
      await openSession("同步验证会话");
      assert.equal(
        await input.inputValue(),
        "升级后保留的草稿",
        "draft survives update reload",
      );
      await openSession("另一个会话");
      assert.equal(
        await input.inputValue(),
        "会话 B 草稿",
        "other session draft survives update reload",
      );
      await openSession("同步验证会话");
    }
    assert.deepEqual(errors, [], "no browser runtime exceptions");
    console.log(
      JSON.stringify({
        passed: true,
        checks: [
          `${width}px layout`,
          "new session live updates after dialog unmount",
          "session action snapshot after dialog unmount",
          "session action live updates without reload",
          "recovered native turn supersedes old cancelled footer",
          "latest Host mirror deduplicates without reviving old failure",
          "current native failure remains visible without reload",
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
          "native input payload",
          ...(testWorker
            ? [
                "worker update discovery",
                "explicit update action",
                "drafts survive reload",
              ]
            : []),
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
    console.error("Worker requests:", workerRequests);
    console.error("Lifecycle state:", {
      createdSession,
      sessionActionCompleted,
      lifecycleSnapshotReads,
      activeSessionStreams: [...sessionStreams.values()],
      publishedSessionChanges: createdSessionChanges.length,
      recoveryPhase,
      recoveryEventReads,
      publishedRecoveryChanges: recoveryChanges.length,
      documentLoads,
    });
    if (testWorker)
      console.error(
        "Worker state:",
        await page.evaluate(async () => {
          const r = await navigator.serviceWorker.getRegistration();
          return {
            visible: document.visibilityState,
            online: navigator.onLine,
            caches: await caches.keys(),
            active: r?.active?.state,
            waiting: r?.waiting?.state,
            installing: r?.installing?.state,
          };
        }),
      );
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
