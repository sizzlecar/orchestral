const fs = require('node:fs');
const path = require('node:path');
const { pathToFileURL } = require('node:url');
const { chromium } = require('playwright');

(async () => {
  const root = path.resolve(__dirname, '../assets/brand');
  const browser = await chromium.launch({ channel: process.env.PWA_SMOKE_CHANNEL || 'chrome', headless: true });
  try {
    const page = await browser.newPage({ viewport: { width: 1280, height: 640 }, deviceScaleFactor: 1 });
    await page.goto(pathToFileURL(path.join(root, 'social-card.svg')).href);
    await page.evaluate(() => document.fonts.ready);
    const output = path.join(root, 'social-card.png');
    await page.screenshot({ path: output, omitBackground: false });
    if (fs.statSync(output).size >= 1024 * 1024) throw new Error('GitHub social preview must be under 1 MB');
    console.log(`Brand social preview exported: ${output}`);
  } finally {
    await browser.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
