// Real PWA bundle against local session-control fixtures; no model API calls.
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const { chromium, webkit } = require('playwright');
const dist = path.join(process.cwd(), 'web/orchestral-web/dist');
const profiles = new Map(['codex-a', 'codex-b', 'native-a', 'native-b'].map(id => [id, { model: 'model-a', reasoning_effort: 'low', permissions: {} }]));
const changes = [];
const now = Date.now();
let rejectNext = true;
const summary = id => ({ connector_id: 'fixture-codex', session_id: id, title: id, state: 'idle', cwd: '/fixture', updated_at_unix_ms: now + (id.endsWith('a') ? 2 : 1), execution_profile: profiles.get(id) });
const native = id => ({ id, title: id, created_at_unix_ms: now, updated_at_unix_ms: now + (id.endsWith('a') ? 2 : 1), run_ids: [], execution_profile: profiles.get(id) });
const schema = (field, values) => ({ type: 'object', required: [field], properties: { [field]: { type: 'string', enum: values } } });
const connector = { connector_id: 'fixture-codex', display_name: 'Codex', agent_product: 'codex', agent_family: 'coding-agent', capabilities: { list: true, read: true, create: false }, actions: [
  { action_id: 'session.set_model', title: 'Model', description: 'Local model fixture', execution: 'immediate', input_schema: schema('model', ['model-a', 'model-b']) },
  { action_id: 'session.set_reasoning', title: 'Reasoning', description: 'Local effort fixture', execution: 'immediate', input_schema: schema('reasoning_effort', ['default', 'low', 'high', 'xhigh']) },
] };
const json = (res, body, status = 200) => { res.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' }); res.end(JSON.stringify(body)); };
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname.startsWith('/api/v1/')) {
    const route = url.pathname.slice(7);
    const chunks = []; for await (const chunk of req) chunks.push(chunk);
    const body = chunks.length ? JSON.parse(Buffer.concat(chunks).toString()) : {};
    if (route === '/me') return json(res, { auth_mode: 'gateway_jwt' });
    if (route === '/devices') return json(res, []);
    if (route === '/agent-connectors') return json(res, [connector]);
    if (route === '/agent-sessions') return json(res, { sessions: ['codex-a', 'codex-b'].map(summary) });
    if (route === '/sessions') return json(res, ['native-a', 'native-b'].map(native));
    if (route === '/agent-session') return json(res, { summary: summary(url.searchParams.get('session_id')), turns: [], controlled_runs: [], pending_requests: [], stream_cursor: 0 });
    if (route === '/agent-session/stream') {
      res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store' });
      res.write(': ready\n\n'); const timer = setInterval(() => res.write(': heartbeat\n\n'), 2000); res.on('close', () => clearInterval(timer)); return;
    }
    const nativeMatch = route.match(/^\/sessions\/(native-[ab])(?:\/(model-settings))?$/);
    if (nativeMatch && !nativeMatch[2]) return json(res, native(nativeMatch[1]));
    if (nativeMatch && req.method === 'GET') {
      const profile = profiles.get(nativeMatch[1]);
      return json(res, { session_id: nativeMatch[1], profile, models: ['model-a', 'model-b'].map(value => ({ value, label: value })), reasoning: (profile.model === 'model-b' ? ['default', 'high'] : ['default', 'low', 'high', 'xhigh']).map(value => ({ value, label: value })), model_supported: true, reasoning_supported: true, allow_custom_model: true, reasoning_declared: true, notice: null });
    }
    if (route === '/agent-session/actions' || nativeMatch) {
      const external = route === '/agent-session/actions';
      const id = external ? body.session_id : nativeMatch[1];
      const field = external ? (body.action_id === 'session.set_model' ? 'model' : 'reasoning_effort') : (body.model !== undefined ? 'model' : 'reasoning_effort');
      const value = external ? body.arguments[field] : body[field];
      if (external) { assert.equal(body.connector_id, 'fixture-codex'); assert.deepEqual(Object.keys(body.arguments), [field]); }
      else assert.equal(body.expected_model, profiles.get(id).model);
      changes.push({ id, field, value });
      if (field === 'reasoning_effort' && value === 'high' && rejectNext) { rejectNext = false; return json(res, { code: 'fixture_rejected', message: '设置未应用，请重试' }, 409); }
      profiles.get(id)[field] = value;
      return json(res, external ? { status: { state: 'completed' }, session: summary(id), content: [], details: null } : profiles.get(id));
    }
    return json(res, { code: 'missing_fixture_route', message: route }, 404);
  }
  let relative = decodeURIComponent(url.pathname).replace(/^\/+/, ''); if (!relative || relative === 'index.html') relative = 'index.html';
  const file = path.join(dist, relative);
  if (!file.startsWith(dist) || !fs.existsSync(file) || fs.statSync(file).isDirectory()) { res.writeHead(404); res.end(); return; }
  const mime = { '.html': 'text/html', '.js': 'application/javascript', '.wasm': 'application/wasm', '.css': 'text/css', '.png': 'image/png', '.svg': 'image/svg+xml' };
  res.writeHead(200, { 'content-type': mime[path.extname(file)] || 'application/octet-stream' }); fs.createReadStream(file).pipe(res);
});

(async () => {
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const browser = process.env.PWA_SMOKE_ENGINE === 'webkit' ? await webkit.launch({ headless: true }) : await chromium.launch({ headless: true, channel: process.env.PWA_SMOKE_CHANNEL || 'chromium' });
  const width = Number(process.env.PWA_SMOKE_WIDTH || 390);
  const context = await browser.newContext({ viewport: { width, height: 844 }, isMobile: width < 600, hasTouch: true, serviceWorkers: 'block' });
  const page = await context.newPage(); const errors = []; page.on('pageerror', error => errors.push(error.message)); page.setDefaultTimeout(15000);
  try {
    await page.goto(`http://127.0.0.1:${server.address().port}`);
    const primary = page.locator('.session-pane[data-pane="primary"]'); const secondary = page.locator('.session-pane[data-pane="secondary"]');
    const open = async pane => { await pane.getByRole('button', { name: '切换模型与推理强度' }).click(); const panel = pane.getByRole('dialog', { name: '模型与推理强度' }); await panel.getByLabel('选择模型', { exact: true }).waitFor(); return panel; };
    let panel = await open(primary);
    assert.match(await panel.getByRole('definition').allTextContents().then(values => values.join(' ')), /model-a.*low/);
    await panel.getByLabel('选择模型', { exact: true }).selectOption('model-b'); await panel.getByRole('button', { name: '应用模型', exact: true }).click();
    await page.waitForFunction(() => document.querySelector('.model-settings-current dd')?.textContent === 'model-b');
    panel = primary.getByRole('dialog', { name: '模型与推理强度' });
    assert.equal(await panel.getByLabel('选择模型', { exact: true }).inputValue(), 'model-b');
    await panel.getByLabel('推理强度', { exact: true }).selectOption('high'); await panel.getByRole('button', { name: '应用推理强度', exact: true }).click();
    await panel.getByRole('alert').getByText('设置未应用，请重试').waitFor(); assert.equal(profiles.get('codex-a').reasoning_effort, 'low');
    assert.equal(await panel.getByLabel('推理强度', { exact: true }).inputValue(), 'high');
    await panel.getByRole('button', { name: '应用推理强度', exact: true }).click();
    await page.waitForFunction(() => [...document.querySelectorAll('.model-settings-current dd')].some(element => element.textContent === 'high'));
    await panel.getByRole('button', { name: '关闭模型设置' }).click();
    panel = await open(secondary); assert.equal(await panel.getByLabel('选择模型', { exact: true }).inputValue(), 'model-a'); assert.equal(profiles.get('codex-b').reasoning_effort, 'low');
    await panel.getByRole('button', { name: '关闭模型设置' }).click();
    await page.getByRole('tab', { name: 'Orch', exact: true }).click();
    panel = await open(primary); await panel.getByLabel('选择模型', { exact: true }).selectOption('model-b'); await panel.getByRole('button', { name: '应用模型', exact: true }).click();
    await page.waitForFunction(() => document.querySelector('.model-settings-current dd')?.textContent === 'model-b');
    panel = primary.getByRole('dialog', { name: '模型与推理强度' });
    assert.equal(await panel.getByLabel('选择模型', { exact: true }).inputValue(), 'model-b');
    assert.deepEqual(await panel.getByLabel('推理强度', { exact: true }).locator('option:not([disabled])').allTextContents(), ['default', 'high']);
    await panel.getByLabel('推理强度', { exact: true }).selectOption('high'); await panel.getByRole('button', { name: '应用推理强度', exact: true }).click();
    await page.waitForFunction(() => [...document.querySelectorAll('.model-settings-current dd')].some(element => element.textContent === 'high'));
    assert.equal(await panel.getByLabel('推理强度', { exact: true }).inputValue(), 'high');
    assert.equal(profiles.get('native-b').model, 'model-a');
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false, 'settings must fit mobile width');
    assert.deepEqual(errors, []);
    const output = path.join(process.cwd(), 'target/pwa-model-controls'); fs.mkdirSync(output, { recursive: true }); await page.screenshot({ path: path.join(output, `${width}-${process.env.PWA_SMOKE_ENGINE || 'chromium'}.png`) });
    console.log(JSON.stringify({ modelControls: true, width, changes: changes.length, externalAndBuiltin: true, independentSessions: true, rejectionRetry: true, refreshedReasoning: true, paidModelRequests: 0 }));
  } finally { await context.close(); await browser.close(); server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
})().catch(error => { console.error(error); server.closeAllConnections(); server.close(); process.exitCode = 1; });
