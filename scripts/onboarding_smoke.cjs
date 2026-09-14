// End-to-end onboarding checks against a local protocol fixture; no paid models.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const http = require('node:http');
const { spawn } = require('node:child_process');

const binary = path.resolve(process.argv[2] || `target/debug/orchestral${process.platform === 'win32' ? '.exe' : ''}`);
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'orchestral onboarding 中文 '));
let mode = 'single';
let requests = [];
const server = http.createServer(async (req, res) => {
  let raw = '';
  for await (const chunk of req) raw += chunk;
  const body = raw ? JSON.parse(raw) : undefined;
  requests.push({ url: req.url, auth: req.headers.authorization, body });
  if (mode === 'unauthorized') { res.writeHead(401); res.end('{}'); return; }
  if (mode === 'missing models endpoint') { res.writeHead(404); res.end('{}'); return; }
  if (mode === 'invalid discovery JSON') { res.writeHead(200); res.end('<html>Gateway page</html>'); return; }
  if (mode === 'interrupted discovery') {
    res.writeHead(200, { 'content-type': 'application/json', 'content-length': '1000' });
    res.write('{"data":');
    setImmediate(() => res.destroy());
    return;
  }
  if (req.url.endsWith('/models')) {
    const ids = mode === 'multiple' ? ['local-one', 'local-two'] : mode === 'empty' ? [] : ['local-test'];
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ data: ids.map(id => ({ id })) }));
    return;
  }
  if (!req.url.endsWith('/chat/completions')) { res.writeHead(404); res.end('{}'); return; }
  assert.equal(body.model, 'local-test');
  res.writeHead(200, { 'content-type': 'text/event-stream' });
  const toolCall = mode === 'tool' && !body.messages.some(message => message.role === 'tool');
  const chunks = toolCall ? [
    { choices: [{ delta: { tool_calls: [{ index: 0, id: 'write-1', type: 'function', function: { name: 'file_write', arguments: JSON.stringify({ path: 'hello 中文.txt', content: 'hello from the local model\n', mode: 'create' }) } }] }, finish_reason: 'tool_calls' }] },
  ] : [
    { choices: [{ delta: { content: 'Local connection ' } }] },
    { choices: [{ delta: { content: 'works. 本地连接成功。' }, finish_reason: 'stop' }] },
  ];
  for (const chunk of chunks) res.write(`data: ${JSON.stringify(chunk)}\n\n`);
  res.end('data: [DONE]\n\n');
});

function run(args, cwd, extraEnv = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(binary, args, {
      cwd,
      env: { ...process.env, ORCHESTRAL_HOME: path.join(cwd, 'user config'), OPENAI_API_KEY: 'must-not-leak-cloud-key', GOOGLE_API_KEY: 'must-not-autoselect-google', OPENAI_BASE_URL: '', OPENAI_MODEL: '', ...extraEnv },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let stdout = '', stderr = '';
    child.stdout.on('data', data => { stdout += data.toString(); });
    child.stderr.on('data', data => { stderr += data.toString(); });
    const timer = setTimeout(() => { child.kill(); reject(new Error(`CLI timed out: ${args.join(' ')}`)); }, 30000);
    child.once('error', error => { clearTimeout(timer); reject(error); });
    child.once('exit', code => { clearTimeout(timer); resolve({ code, stdout, stderr }); });
  });
}

(async () => {
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const base = `http://127.0.0.1:${server.address().port}`;
  let count = 0;
  async function scenario(name, fn) {
    mode = 'single'; requests = [];
    const cwd = path.join(scratch, name);
    fs.mkdirSync(cwd);
    await fn(cwd);
    for (const request of requests) assert.notEqual(request.auth, 'Bearer must-not-leak-cloud-key');
    count++;
    console.log(`PASS ${name}`);
  }
  const prompt = ['--no-mcp', '--no-skills', 'Check this local model connection.'];
  await scenario('URL only discovers a model', async cwd => {
    const result = await run(['--base-url', base, ...prompt], cwd);
    assert.equal(result.code, 0, result.stderr);
    assert.match(result.stdout, /本地连接成功/);
    assert.deepEqual(requests.map(req => req.url), ['/v1/models', '/v1/chat/completions']);
    assert.ok(requests.every(req => req.auth === undefined));
  });
  await scenario('full endpoint and explicit model skip discovery', async cwd => {
    const result = await run(['--base-url', `${base}/proxy/v1/chat/completions`, '--model', 'local-test', ...prompt], cwd);
    assert.equal(result.code, 0, result.stderr);
    assert.deepEqual(requests.map(req => req.url), ['/proxy/v1/chat/completions']);
  });
  await scenario('environment URL and model work without YAML', async cwd => {
    const result = await run(prompt, cwd, { OPENAI_BASE_URL: `${base}/v1`, OPENAI_MODEL: 'local-test' });
    assert.equal(result.code, 0, result.stderr);
    assert.equal(requests.length, 1);
  });
  await scenario('explicit key is sent only when selected', async cwd => {
    const result = await run(['--base-url', base, '--api-key-env', 'LOCAL_TEST_API_KEY', ...prompt], cwd, { LOCAL_TEST_API_KEY: 'local-only' });
    assert.equal(result.code, 0, result.stderr);
    assert.ok(requests.every(req => req.auth === 'Bearer local-only'));
  });
  await scenario('missing custom key never falls back to cloud key', async cwd => {
    const result = await run(['--base-url', base, '--api-key-env', 'ORCHESTRAL_ABSENT_TEST_KEY', ...prompt], cwd, { ORCHESTRAL_ABSENT_TEST_KEY: '' });
    assert.notEqual(result.code, 0);
    assert.match(result.stderr, /no API key configured/);
    assert.equal(requests.length, 0);
  });
  for (const [selectedMode, expected] of [
    ['multiple', /--model.*local-one.*local-two/s],
    ['empty', /no models loaded/],
    ['unauthorized', /HTTP 401/],
    ['missing models endpoint', /HTTP 404/],
    ['invalid discovery JSON', /did not return JSON/],
    ['interrupted discovery', /interrupted|request failed/],
  ]) {
    await scenario(`${selectedMode} produces an actionable error`, async cwd => {
      mode = selectedMode;
      const result = await run(['--base-url', base, ...prompt], cwd);
      assert.notEqual(result.code, 0);
      assert.match(result.stderr, expected);
      assert.ok(requests.every(req => !req.body));
    });
  }
  await scenario('explicit configuration never switches to another cloud provider', async cwd => {
    fs.writeFileSync(path.join(cwd, 'selected.yaml'), `version: 1\nagent:\n  backend: custom\n  model: local-test\nproviders:\n  backends:\n    - name: custom\n      kind: openai\n      endpoint: ${base}/v1\n      api_key_env: ORCHESTRAL_ABSENT_TEST_KEY\n`);
    const result = await run(['--config', 'selected.yaml', 'doctor', '--json'], cwd, { ORCHESTRAL_ABSENT_TEST_KEY: '' });
    assert.notEqual(result.code, 0);
    assert.equal(JSON.parse(result.stdout).backend, 'custom');
    assert.equal(JSON.parse(result.stdout).authentication, 'missing');
    assert.equal(requests.length, 0);
  });
  await scenario('doctor is read only and can check connection', async cwd => {
    const local = await run(['--base-url', base, 'doctor', '--json'], cwd);
    assert.equal(local.code, 0, local.stderr);
    assert.equal(JSON.parse(local.stdout).connection_checked, false);
    assert.equal(requests.length, 0);
    assert.deepEqual(fs.readdirSync(cwd), []);
    const connected = await run(['--base-url', base, 'doctor', '--json', '--check-connection'], cwd);
    assert.equal(connected.code, 0, connected.stderr);
    assert.deepEqual(JSON.parse(connected.stdout).available_models, ['local-test']);
    assert.deepEqual(fs.readdirSync(cwd), []);
    assert.ok(!connected.stdout.includes('must-not-leak'));
  });
  await scenario('local model executes a file tool and resumes the session', async cwd => {
    mode = 'tool';
    const result = await run(['--base-url', base, '--session-id', 'onboarding-tool-session', ...prompt], cwd);
    assert.equal(result.code, 0, result.stderr);
    assert.equal(fs.readFileSync(path.join(cwd, 'hello 中文.txt'), 'utf8'), 'hello from the local model\n');
    assert.equal(requests.filter(req => req.body).length, 2);
    const history = await run(['sessions', 'list', '--json'], cwd);
    assert.equal(history.code, 0, history.stderr);
    assert.match(history.stdout, /onboarding-tool-session/);
    const resumed = await run(['--base-url', base, '--no-mcp', '--no-skills', 'resume', 'onboarding-tool-session', 'Continue our conversation.'], cwd);
    assert.equal(resumed.code, 0, resumed.stderr);
    const turns = requests.filter(req => req.body);
    assert.equal(turns.length, 3);
    assert.ok(turns[2].body.messages.some(message => message.role === 'tool'));
  });
  await scenario('serve starts with the embedded PWA and protected API', async cwd => {
    const reservation = http.createServer();
    await new Promise(resolve => reservation.listen(0, '127.0.0.1', resolve));
    const port = reservation.address().port;
    await new Promise(resolve => reservation.close(resolve));
    const origin = `http://127.0.0.1:${port}`;
    const child = spawn(binary, ['--base-url', base, '--model', 'local-test', '--no-mcp', '--no-skills', 'serve', '--pair', '--listen', `127.0.0.1:${port}`, '--public-url', origin], { cwd, env: { ...process.env, ORCHESTRAL_HOME: path.join(cwd, 'user config') }, stdio: ['ignore', 'pipe', 'pipe'] });
    let output = '';
    child.stdout.on('data', chunk => { output += chunk; });
    child.stderr.on('data', chunk => { output += chunk; });
    const exited = new Promise((resolve, reject) => { child.once('exit', resolve); child.once('error', reject); });
    try {
      let health;
      for (let attempt = 0; attempt < 100; attempt++) {
        if (child.exitCode !== null) throw new Error(`serve exited before startup: ${output}`);
        health = await fetch(`${origin}/api/v1/health`).catch(() => undefined);
        if (health?.ok) break;
        await new Promise(resolve => setTimeout(resolve, 100));
      }
      assert.ok(health?.ok, 'serve health endpoint did not become ready');
      const page = await fetch(origin);
      assert.equal(page.status, 200);
      assert.match(await page.text(), /orchestral-web/);
      assert.equal((await fetch(`${origin}/api/v1/sessions`)).status, 401);
      assert.equal(requests.length, 0, 'starting the server must not generate model output');
    } finally {
      child.kill();
      await exited;
    }
  });
  console.log(`${count} onboarding scenarios passed; no paid API calls.`);
})().catch(error => { console.error(error); process.exitCode = 1; }).finally(() => {
  server.closeAllConnections(); server.close();
  fs.rmSync(scratch, { recursive: true, force: true });
});
