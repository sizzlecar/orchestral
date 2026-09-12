const assert = require('node:assert/strict');
const fs = require('node:fs');
const http = require('node:http');
const path = require('node:path');
const { chromium } = require('playwright');
const root = path.resolve('deploy/cloudflare/orchestral-site/public');
const server = http.createServer((req, res) => {
  const file = path.join(root, req.url === '/' ? 'index.html' : req.url.slice(1));
  if (!file.startsWith(root + path.sep) || !fs.existsSync(file)) { res.writeHead(404); return res.end('Not found'); }
  const types = { '.html': 'text/html', '.css': 'text/css', '.js': 'text/javascript', '.svg': 'image/svg+xml' };
  res.setHeader('content-type', types[path.extname(file)] || 'text/plain');
  res.end(fs.readFileSync(file));
});
(async () => {
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const url = `http://127.0.0.1:${server.address().port}`;
  const browser = await chromium.launch({ channel: process.env.PWA_SMOKE_CHANNEL || 'chrome', headless: true });
  try {
    const context = await browser.newContext({ permissions: ['clipboard-read', 'clipboard-write'], viewport: { width: 1440, height: 1000 } });
    const page = await context.newPage();
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));
    await page.route('https://api.github.com/**', route => route.fulfill({ status: 404, body: '{}' }));
    await page.goto(url);
    await page.screenshot({ path: process.env.SITE_SCREENSHOT || '/tmp/orchestral-site-desktop.png', fullPage: true });
    await page.locator('#tab-windows').click();
    assert.equal(await page.locator('#panel-windows').isVisible(), true);
    await page.locator('[data-copy="install-windows"]').click();
    assert.equal(await page.evaluate(() => navigator.clipboard.readText()), 'irm https://orch.pandaailabs.com/install.ps1 | iex');
    await page.locator('#tab-windows').focus();
    await page.keyboard.press('ArrowLeft');
    assert.equal(await page.locator('#tab-unix').getAttribute('aria-selected'), 'true');
    for (const width of [390, 320]) {
      await page.setViewportSize({ width, height: 844 });
      assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true, `horizontal overflow at ${width}px`);
    }
    await page.screenshot({ path: '/tmp/orchestral-site-mobile.png', fullPage: true });
    await page.unroute('https://api.github.com/**');
    const targets = ['aarch64-apple-darwin.tar.gz', 'x86_64-apple-darwin.tar.gz', 'x86_64-unknown-linux-gnu.tar.gz', 'x86_64-pc-windows-msvc.zip'];
    await page.route('https://api.github.com/**', route => route.fulfill({ json: { tag_name: 'v0.3.0', draft: false, prerelease: false, assets: targets.flatMap(target => ['', '.sha256'].map(suffix => ({ name: `orchestral-v0.3.0-${target}${suffix}` }))) } }));
    await page.reload();
    await page.waitForFunction(() => document.getElementById('release-status').textContent.includes('is available'));
    assert.ok(!(await page.locator('.footnote').textContent()).includes('preparation'));
    assert.deepEqual(errors, []);
    console.log('Website checks passed: release states, tabs, keyboard, copy, desktop and narrow mobile.');
  } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exitCode = 1; }).finally(() => server.close());
