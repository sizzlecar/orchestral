// Discovery sources fail independently; real PWA bundle, local fixtures only.
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const { chromium, webkit } = require('playwright');
const dist = path.join(process.cwd(), 'web/orchestral-web/dist');
const now = Date.now();
let catalogUnavailable = true, builtinFailures = 0, creates = 0;
const sessions = ['existing-a', 'existing-b'];
const summary = id => ({ connector_id: 'fixture-codex', session_id: id, title: id, state: 'idle', cwd: '/fixture', updated_at_unix_ms: now + (id === 'created' ? 3 : id.endsWith('a') ? 2 : 1) });
const connector = { connector_id: 'fixture-codex', display_name: 'Fixture Codex', agent_product: 'codex', agent_family: 'coding-agent', capabilities: { list: true, read: true, create: true }, creation: { accepts_cwd: false, input_schema: { type: 'object', properties: {}, additionalProperties: false } }, actions: [] };
const json = (res, body, status = 200) => { res.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' }); res.end(JSON.stringify(body)); };
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');
  if (url.pathname.startsWith('/api/v1/')) {
    const route = url.pathname.slice(7);
    const chunks = []; for await (const chunk of req) chunks.push(chunk);
    const body = chunks.length ? JSON.parse(Buffer.concat(chunks).toString()) : {};
    if (route === '/me') return json(res, { auth_mode: 'gateway_jwt' });
    if (route === '/devices') return json(res, []);
    if (route === '/sessions') { builtinFailures++; return json(res, { code: 'agent_control_failed', message: 'old provider contract' }, 500); }
    if (route === '/agent-connectors') return catalogUnavailable ? json(res, { code: 'fixture_unavailable', message: '能力目录暂不可用' }, 503) : json(res, [connector]);
    if (route === '/agent-sessions' && req.method === 'POST') {
      assert.equal(body.connector_id, 'fixture-codex'); creates++; sessions.unshift('created'); return json(res, summary('created'), 201);
    }
    if (route === '/agent-sessions') return json(res, { sessions: sessions.map(summary) });
    if (route === '/agent-session') return json(res, { summary: summary(url.searchParams.get('session_id')), turns: [], controlled_runs: [], pending_requests: [], stream_cursor: 0 });
    if (route === '/agent-session/stream') {
      res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store' }); res.write(': ready\n\n');
      const timer = setInterval(() => res.write(': heartbeat\n\n'), 2000); res.on('close', () => clearInterval(timer)); return;
    }
    return json(res, { code: 'missing_fixture_route', message: route }, 404);
  }
  const relative = decodeURIComponent(url.pathname).replace(/^\/+/, '') || 'index.html';
  const file = path.join(dist, relative);
  if (!file.startsWith(dist + path.sep) || !fs.existsSync(file) || fs.statSync(file).isDirectory()) { res.writeHead(404); res.end(); return; }
  const mime = { '.html': 'text/html', '.js': 'application/javascript', '.wasm': 'application/wasm', '.css': 'text/css' };
  res.writeHead(200, { 'content-type': mime[path.extname(file)] || 'application/octet-stream' }); fs.createReadStream(file).pipe(res);
});
(async () => {
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const browser = process.env.PWA_SMOKE_ENGINE === 'webkit' ? await webkit.launch({ headless: true }) : await chromium.launch({ headless: true, channel: process.env.PWA_SMOKE_CHANNEL || 'chromium' });
  const page = await browser.newPage({ viewport: { width: 390, height: 844 }, serviceWorkers: 'block' }); const errors = [];
  page.on('pageerror', error => errors.push(error.message)); page.setDefaultTimeout(15000);
  try {
    await page.goto(`http://127.0.0.1:${server.address().port}`);
    await page.getByRole('button', { name: '新建会话', exact: true }).click();
    const panel = page.locator('.session-control-dialog');
    await panel.getByText('外部 Agent 暂不可用：能力目录暂不可用', { exact: true }).waitFor();
    assert.equal(await panel.getByText(/尚未提供.*会话创建功能/).count(), 0, 'a failed capability read is not unsupported creation');
    catalogUnavailable = false;
    await panel.getByRole('button', { name: '重试加载 Agent 能力' }).click();
    await panel.getByRole('button', { name: '创建会话', exact: true }).click();
    await page.getByRole('button', { name: '关闭新建会话' }).waitFor({ state: 'detached' });
    assert.equal(creates, 1);
    await page.locator('.session-pane[data-pane="primary"]').getByRole('heading', { name: 'created', exact: true }).waitFor();
    // Losing the directory later preserves the last confirmed capabilities.
    catalogUnavailable = true;
    await page.getByRole('button', { name: '刷新会话', exact: true }).click();
    await page.getByRole('button', { name: '新建会话', exact: true }).click();
    await panel.getByText('外部 Agent 暂不可用：能力目录暂不可用', { exact: true }).waitFor();
    assert.equal(await panel.getByRole('button', { name: '创建会话', exact: true }).count(), 1);
    assert.equal(await panel.getByText(/尚未提供.*会话创建功能/).count(), 0);
    assert.ok(builtinFailures >= 2, 'external creation must work while built-in discovery continues failing');
    assert.deepEqual(errors, []);
    fs.mkdirSync('target/pwa-discovery', { recursive: true }); await page.screenshot({ path: `target/pwa-discovery/${process.env.PWA_SMOKE_ENGINE || 'chromium'}.png` });
    console.log(JSON.stringify({ discoveryFailure: true, independentSources: true, capabilityRetry: true, cachedCapabilities: true, createdSessions: creates, paidModelRequests: 0 }));
  } finally { await browser.close(); server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
})().catch(error => { console.error(error); server.closeAllConnections(); server.close(); process.exitCode = 1; });
