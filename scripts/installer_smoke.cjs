// Validate real installer replacement behavior with a locally packaged executable.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const http = require('node:http');
const crypto = require('node:crypto');
const { execFileSync, spawn } = require('node:child_process');
const windows = process.platform === 'win32';
function windowsPowerShellEnvironment(extra) {
  const environment = { ...process.env, ...extra };
  // pwsh -> Node -> Windows PowerShell does not get pwsh's direct-child
  // module-path filtering. Let Windows PowerShell construct its own defaults.
  for (const key of Object.keys(environment)) {
    if (key.toUpperCase() === 'PSMODULEPATH') delete environment[key];
  }
  return environment;
}
const binary = path.resolve(process.argv[2] || `target/debug/orchestral${windows ? '.exe' : ''}`);
const version = execFileSync(binary, ['--version'], { encoding: 'utf8' }).trim().split(' ')[1];
const target = windows ? 'x86_64-pc-windows-msvc' : process.platform === 'darwin' ? `${process.arch === 'arm64' ? 'aarch64' : 'x86_64'}-apple-darwin` : 'x86_64-unknown-linux-gnu';
const root = fs.mkdtempSync(path.join(os.tmpdir(), 'orchestral install 中文 '));
const name = `orchestral-v${version}-${target}`;
const asset = `${name}.${windows ? 'zip' : 'tar.gz'}`;
fs.mkdirSync(path.join(root, name));
fs.copyFileSync(binary, path.join(root, name, windows ? 'orchestral.exe' : 'orchestral'));
if (windows) {
  // Use an encoded command and positional paths passed via process-only variables.
  const encoded = Buffer.from(`Add-Type -AssemblyName System.IO.Compression.FileSystem; [IO.Compression.ZipFile]::CreateFromDirectory($env:ORCHESTRAL_PACKAGE_INPUT,$env:ORCHESTRAL_PACKAGE_OUTPUT,0,$true)`, 'utf16le').toString('base64');
  execFileSync('powershell.exe', ['-NoProfile', '-EncodedCommand', encoded], { env: windowsPowerShellEnvironment({ ORCHESTRAL_PACKAGE_INPUT: path.join(root, name), ORCHESTRAL_PACKAGE_OUTPUT: path.join(root, asset) }) });
} else {
  execFileSync('tar', ['-czf', path.join(root, asset), '-C', root, name]);
}
const archive = fs.readFileSync(path.join(root, asset));
const hash = crypto.createHash('sha256').update(archive).digest('hex');
let badHash = false;
const server = http.createServer((req, res) => {
  if (req.url.endsWith('.sha256')) res.end(`${badHash ? '0'.repeat(64) : hash}  ${asset}\n`);
  else if (req.url.endsWith(asset)) res.end(archive);
  else { res.writeHead(404); res.end(); }
});
const installDir = path.join(root, "program's folder 中文");
const installed = path.join(installDir, windows ? 'orchestral.exe' : 'orchestral');
function install(base) {
  const args = windows ? ['-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', path.resolve('scripts/install.ps1'), '-Version', version, '-InstallDir', installDir, '-NoModifyPath', '-ReleaseBaseUrl', base] : [path.resolve('scripts/install.sh'), '--version', version, '--dir', installDir, '--no-modify-path'];
  return new Promise((resolve, reject) => {
    const env = windows ? windowsPowerShellEnvironment({ ORCHESTRAL_RELEASE_BASE_URL: base }) : { ...process.env, ORCHESTRAL_RELEASE_BASE_URL: base };
    const child = spawn(windows ? 'powershell.exe' : 'sh', args, { env, stdio: ['ignore', 'pipe', 'pipe'] });
    let output = '';
    child.stdout.on('data', chunk => { output += chunk; });
    child.stderr.on('data', chunk => { output += chunk; });
    const timer = setTimeout(() => { child.kill(); reject(new Error('installer timed out')); }, 60000);
    child.on('error', error => { clearTimeout(timer); reject(error); });
    child.on('exit', code => { clearTimeout(timer); resolve({ code, output }); });
  });
}
(async () => {
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const base = `http://127.0.0.1:${server.address().port}`;
  for (const attempt of ['fresh installation', 'repeat installation']) {
    const result = await install(base);
    assert.equal(result.code, 0, result.output);
    assert.equal(execFileSync(installed, ['--version'], { encoding: 'utf8' }).trim(), `orchestral ${version}`);
    console.log(`PASS ${attempt} in a path with spaces, apostrophe and Unicode`);
  }
  const previous = crypto.createHash('sha256').update(fs.readFileSync(installed)).digest('hex');
  badHash = true;
  const result = await install(base);
  assert.notEqual(result.code, 0);
  assert.match(result.output, /Checksum mismatch/);
  assert.equal(crypto.createHash('sha256').update(fs.readFileSync(installed)).digest('hex'), previous);
  assert.ok(!fs.readdirSync(installDir).some(file => file.startsWith('.orchestral-install')));
  console.log('PASS checksum failure preserves the existing executable and cleans temporary files');
})().catch(error => { console.error(error); process.exitCode = 1; }).finally(() => {
  server.closeAllConnections(); server.close();
  fs.rmSync(root, { force: true, recursive: true });
});
