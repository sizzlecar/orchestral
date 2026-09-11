const fs = require('node:fs');
const path = require('node:path');
const root = path.resolve(__dirname, '..');
const output = path.join(root, 'deploy/cloudflare/orchestral-site/public');
for (const file of ['install.sh', 'install.ps1']) {
  fs.copyFileSync(path.join(__dirname, file), path.join(output, file));
}
console.log('Website installers refreshed from scripts/.');
