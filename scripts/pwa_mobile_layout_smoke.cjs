// Exercises real layout against iOS-style visual viewport resize/scroll events.
// No paid model calls. The HTTP/SSE fixture is shared with the dual-pane test.
const assert = require("node:assert/strict");
const fs = require("node:fs");

async function installViewportFixture(page) {
  await page.addInitScript(() => {
    const viewport = window.visualViewport;
    const values = {};
    for (const name of ["width", "height", "offsetTop", "offsetLeft"]) {
      const read = Object.getOwnPropertyDescriptor(Object.getPrototypeOf(viewport), name).get;
      Object.defineProperty(viewport, name, { configurable: true, get: () => name in values ? values[name] : read.call(viewport) });
    }
    window.setFixtureViewport = changes => {
      Object.assign(values, changes);
      viewport.dispatchEvent(new Event("resize"));
      viewport.dispatchEvent(new Event("scroll"));
    };
  });
}

async function checkMobileLayout({ page, primary, secondary, sessions, submissions, publish, wait, width }) {
  fs.mkdirSync("target/pwa-smoke", { recursive: true });
  const engine = process.env.PWA_SMOKE_ENGINE || "chromium";
  const activate = pane => pane.locator(".pane-activate").click();
  const input = pane => pane.getByRole("textbox", { name: "消息草稿" });
  const viewportSettled = () => page.waitForFunction(() => {
    const bounds = document.getElementById("main").getBoundingClientRect();
    return Math.abs(bounds.width - visualViewport.width) < 1
      && Math.abs(bounds.height - visualViewport.height) < 1
      && Math.abs(bounds.top - visualViewport.offsetTop) < 1;
  });
  const geometry = () => page.evaluate(() => {
    const visible = element => element && element.getClientRects().length > 0;
    const rect = element => { const r = element.getBoundingClientRect(); return { top: r.top, bottom: r.bottom, height: r.height, left: r.left, right: r.right }; };
    const panes = [...document.querySelectorAll(".session-pane")].filter(visible);
    const composers = [...document.querySelectorAll(".composer-dock")].filter(visible);
    return {
      top: visualViewport.offsetTop, bottom: visualViewport.offsetTop + visualViewport.height, height: visualViewport.height,
      panes: panes.map(rect), reading: panes.map(p => rect(p.querySelector(".message-list"))),
      headers: panes.map(p => rect(p.querySelector(".conversation-header"))),
      composers: composers.map(rect),
      controls: composers.flatMap(p => [p.querySelector("textarea"), p.querySelector(".send-button")].map(rect)),
      overflow: document.documentElement.scrollWidth > innerWidth,
    };
  });
  const assertInputVisible = async () => {
    const g = await geometry();
    assert.equal(g.composers.length, 1, "mobile exposes one shared composer");
    assert.equal(g.overflow, false);
    for (const r of g.controls) {
      assert(r.top >= g.top - 1 && r.bottom <= g.bottom + 1, "input and send button stay entirely above keyboard/browser chrome");
      assert(r.height >= 40, "composer controls retain a usable touch height");
    }
    return g;
  };
  await page.evaluate(() => setFixtureViewport({ height: 708 }));
  await page.waitForFunction(() => Math.abs(document.getElementById("main").getBoundingClientRect().height - 708) < 1);
  await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  let g = await assertInputVisible();
  assert.equal(g.panes.length, 2);
  assert(g.headers.every(r => r.height <= 48), "long session metadata must not expand phone headers");
  const readingRatio = g.reading.reduce((total, r) => total + r.height, 0) / g.height;
  assert(readingRatio >= 0.65, `transcripts should occupy at least 65% of the visible page, got ${readingRatio}`);
  assert(g.reading[0].height >= 200 && g.reading[1].height >= 200);
  await input(primary).fill("上屏独立草稿");
  await activate(secondary);
  await input(secondary).fill("下屏独立草稿");
  await activate(primary);
  assert.equal(await input(primary).inputValue(), "上屏独立草稿");
  await primary.locator(".composer-target__label").filter({ hasText: "发送到 上屏" }).waitFor();
  await page.screenshot({ path: `target/pwa-smoke/mobile-reading-${engine}-${width}.png` });

  await input(primary).focus();
  await page.evaluate(() => setFixtureViewport({ height: 350, offsetTop: 70 }));
  await page.waitForFunction(() => document.documentElement.dataset.keyboardPane === "primary");
  g = await assertInputVisible();
  assert.equal(g.panes.length, 1, "keyboard gives the editing session the available reading area");
  assert(g.reading[0].height >= 100);
  await page.screenshot({ path: `target/pwa-smoke/mobile-keyboard-upper-${engine}-${width}.png` });
  await primary.getByRole("button", { name: "切换发送会话" }).click();
  await page.waitForFunction(() => document.documentElement.dataset.keyboardPane === "secondary"
    && document.activeElement.closest(".session-pane")?.dataset.pane === "secondary");
  await assertInputVisible();
  assert.equal(await input(secondary).inputValue(), "下屏独立草稿");
  assert(sessions.get("codex-1").streams.size && sessions.get("codex-2").streams.size, "folding a pane preserves both live streams");
  await input(secondary).fill("键盘中发送到下屏");
  await secondary.getByRole("button", { name: "发送消息", exact: true }).click();
  await secondary.locator(".message--user").getByText("键盘中发送到下屏", { exact: true }).waitFor();
  await assertInputVisible();
  assert.equal((await geometry()).panes.length, 1, "sending does not restore split panes while the keyboard still covers the page");
  assert.equal(submissions.at(-1).session_id, "codex-2");
  // Submission acknowledgement refreshes the selected native session. Wait
  // for that transport handoff before publishing another event.
  await wait(() => sessions.get("codex-1").streams.size && sessions.get("codex-2").streams.size);
  publish(sessions.get("codex-1"), "键盘期间上屏继续接收");
  await primary.getByText("键盘期间上屏继续接收", { exact: true }).waitFor({ state: "attached" });
  await page.screenshot({ path: `target/pwa-smoke/mobile-keyboard-lower-${engine}-${width}.png` });

  await page.evaluate(() => { document.activeElement.blur(); setFixtureViewport({ height: 708, offsetTop: 0 }); });
  await page.waitForFunction(() => !document.documentElement.hasAttribute("data-keyboard-pane"));
  await primary.getByText("键盘期间上屏继续接收", { exact: true }).waitFor();
  g = await assertInputVisible();
  assert.equal(g.panes.length, 2);
  await activate(primary);
  assert.equal(await input(primary).inputValue(), "上屏独立草稿");

  await page.setViewportSize({ width: 844, height: 390 });
  await page.evaluate(() => setFixtureViewport({ width: 844, height: 390, offsetTop: 0 }));
  await viewportSettled();
  await assertInputVisible();
  await page.setViewportSize({ width, height: 844 });
  await page.evaluate(w => setFixtureViewport({ width: w, height: 708, offsetTop: 0 }), width);
  await viewportSettled();
  await assertInputVisible();
  console.log(JSON.stringify({ mobileLayout: true, engine, width, readingRatio, singleComposer: true, keyboardUpperAndLower: true, targetRouting: true, retainedDraftsAndStreams: true, rotate: true, paidModelRequests: 0 }));
}

module.exports = { installViewportFixture, checkMobileLayout };
if (require.main === module) {
  process.env.PWA_SMOKE_MOBILE_LAYOUT = "1";
  require("./pwa_dual_session_smoke.cjs");
}
