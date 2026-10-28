// Local mobile fixture: idle rendering, conditional refresh, and hidden-page work.
const assert = require("node:assert/strict");

async function installPowerFixture(page) {
  await page.addInitScript(() => {
    let hidden = false;
    Object.defineProperty(document, "visibilityState", { configurable: true, get: () => hidden ? "hidden" : "visible" });
    Object.defineProperty(document, "hidden", { configurable: true, get: () => hidden });
    window.setFixtureHidden = value => {
      hidden = value;
      document.dispatchEvent(new Event("visibilitychange", { bubbles: true }));
    };
  });
}

async function checkPower({ page, primary, secondary, sessions, requests, publish, wait }) {
  const headers = page.locator(".conversation-header");
  assert.equal(await headers.evaluateAll(elements => elements.flatMap(element => element.getAnimations({ subtree: true })).filter(animation => animation.playState === "running").length), 0,
    "idle status badges must not keep repainting the page");
  await wait(() => requests.some(request => request.notModified));
  const conditionalReads = requests.filter(request => request.notModified).length;
  assert(conditionalReads > 0, "unchanged heartbeat snapshots must use conditional requests");
  const cdp = await page.context().newCDPSession(page);
  await cdp.send("Performance.enable");
  const metrics = async () => Object.fromEntries((await cdp.send("Performance.getMetrics")).metrics.map(metric => [metric.name, metric.value]));
  const before = await metrics();
  await page.waitForTimeout(3000);
  const after = await metrics();
  const idleStyles = after.RecalcStyleCount - before.RecalcStyleCount;
  assert(idleStyles < 30, `idle page must settle instead of restyling every animation frame: ${idleStyles}`);

  const input = primary.getByRole("textbox", { name: "消息草稿" });
  await input.fill("后台保留的草稿"); await input.blur();
  await page.evaluate(() => setFixtureHidden(true));
  await wait(() => !sessions.get("codex-1").streams.size && !sessions.get("codex-2").streams.size);
  const backgroundReads = requests.length;
  for (let index = 1; index <= 12; index++) {
    publish(sessions.get("codex-1"), `上屏后台进度 ${index}`);
    publish(sessions.get("codex-2"), `下屏后台进度 ${index}`);
  }
  await page.waitForTimeout(6000);
  const hiddenRequestCount = requests.length - backgroundReads;
  assert.equal(hiddenRequestCount, 0, "hidden panes must stop snapshots and reconnects");
  assert.equal(await primary.getByText("上屏后台进度 12", { exact: true }).count(), 0, "hidden pages must not process transcript updates");

  await page.evaluate(() => setFixtureHidden(false));
  await primary.getByText("上屏后台进度 12", { exact: true }).waitFor();
  await secondary.getByText("下屏后台进度 12", { exact: true }).waitFor();
  assert.equal(await input.inputValue(), "后台保留的草稿");
  for (const pane of [primary, secondary]) assert.equal(await pane.locator('.message--assistant').count(), 36, "foreground repair must preserve history without duplicating replies");
  await wait(() => sessions.get("codex-1").streams.size === 1 && sessions.get("codex-2").streams.size === 1);
  const streamsBefore = requests.filter(request => request.route === "/agent-session/stream").length;
  await page.evaluate(() => window.dispatchEvent(new Event("pagehide")));
  await wait(() => !sessions.get("codex-1").streams.size && !sessions.get("codex-2").streams.size);
  await page.evaluate(() => window.dispatchEvent(new Event("pageshow")));
  await wait(() => requests.filter(request => request.route === "/agent-session/stream").length >= streamsBefore + 2);
  console.log(JSON.stringify({ mobilePower: true, idleSeconds: 3, idleStyleRecalculations: idleStyles, idleTaskSeconds: after.TaskDuration - before.TaskDuration,
    conditionalReads, backgroundRequests: hiddenRequestCount, hiddenStreamsPaused: true, foregroundHistoryRestored: true, draftsPreserved: true, pageLifecycle: true, paidModelRequests: 0 }));
}

module.exports = { installPowerFixture, checkPower };
if (require.main === module) {
  process.env.PWA_SMOKE_POWER = "1";
  require("./pwa_dual_session_smoke.cjs");
}
