const fs = require('node:fs');
const path = require('node:path');
const root = path.resolve(__dirname, '..');
const output = path.join(root, 'deploy/cloudflare/orchestral-site/public');
fs.mkdirSync(path.join(output, 'brand'), { recursive: true });
for (const file of fs.readdirSync(path.join(root, 'assets/brand'))) {
  if (/\.(svg|png)$/.test(file)) fs.copyFileSync(path.join(root, 'assets/brand', file), path.join(output, 'brand', file));
}
for (const file of ['install.sh', 'install.ps1']) {
  fs.copyFileSync(path.join(__dirname, file), path.join(output, file));
}
console.log('Website brand assets and installers refreshed.');
