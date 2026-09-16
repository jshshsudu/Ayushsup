const express = require('express');
const pLimit = require('p-limit');

const app = express();
const PORT = process.env.PORT || 3000;
const APIFY_TOKEN = process.env.APIFY_TOKEN;

if (!APIFY_TOKEN) {
    console.error('❌ APIFY_TOKEN environment variable is required.');
    process.exit(1);
}

// ---------------------------------------------------------------------------
//  CONFIGURATION
// ---------------------------------------------------------------------------
const APIFY_ACTOR = 'apify~cheerio-scraper';
const APIFY_BASE = 'https://api.apify.com/v2';
const MAX_CONCURRENT_RUNS = 25;      // 25 Apify runs in parallel
const MEMORY_MB = 256;               // 256 MB per run (single HEAD is tiny)
const REFRESH_BUFFER_SECONDS = 1800; // Refresh 30 min before expiry
const RETRY_DELAY_MS = 120_000;      // Retry failed channels after 2 min

// Global concurrency gate — shared by initial batch AND refreshes
const apifyLimit = pLimit(MAX_CONCURRENT_RUNS);

// ---------------------------------------------------------------------------
//  IN-MEMORY STORAGE
// ---------------------------------------------------------------------------
const channelStore = new Map();
// channelId -> { channelId, name, originalUrl, currentUrl, cookie, expires, lastUpdated, status, error }

const stats = {
    totalAdded: 0,
    queued: 0,
    processing: 0,
    succeeded: 0,
    failed: 0,
    refreshCount: 0,
    startedAt: null,
    lastEventAt: null,
};

const eventLog = [];
function log(msg, level = 'info') {
    const entry = { time: getFormattedDate(), msg, level };
    eventLog.push(entry);
    if (eventLog.length > 500) eventLog.shift();
    const prefix = level === 'error' ? '❌' : level === 'warn' ? '⚠️' : 'ℹ️';
    console.log(`${prefix} ${msg}`);
}

// ---------------------------------------------------------------------------
//  HELPERS
// ---------------------------------------------------------------------------
function getFormattedDate() {
    const d = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
    return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}:${String(d.getSeconds()).padStart(2, '0')} ${String(d.getDate()).padStart(2, '0')}-${String(d.getMonth() + 1).padStart(2, '0')}-${d.getFullYear()}`;
}

function extractExpiry(cookieStr) {
    const match = cookieStr.match(/exp=(\d+)/);
    return match ? parseInt(match[1], 10) : 0;
}

// ---------------------------------------------------------------------------
//  APIFY: SINGLE-URL RUN
//  1 URL = 1 run = 1 result, so cookie mismatch is impossible.
// ---------------------------------------------------------------------------
async function runApifySingle(url) {
    const apifyInput = {
        startUrls: [{ url, method: 'HEAD' }],
        proxyConfiguration: {
            useApifyProxy: true,
            apifyProxyGroups: ['RESIDENTIAL'],
            apifyProxyCountry: 'IN',
        },
        maxConcurrency: 1,
        maxRequestsPerCrawl: 1,
        additionalMimeTypes: ['*/*'],
        ignoreSslErrors: true,
        pageFunction: `async function pageFunction(context) {
            return {
                url: context.request.url,
                status: context.response.statusCode,
                headers: context.response.headers
            };
        }`,
    };

    const apiUrl = `${APIFY_BASE}/acts/${APIFY_ACTOR}/run-sync-get-dataset-items?token=${APIFY_TOKEN}&memory=${MEMORY_MB}`;

    const res = await fetch(apiUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(apifyInput),
    });

    if (!res.ok) {
        throw new Error(`Apify HTTP ${res.status}: ${(await res.text()).slice(0, 200)}`);
    }

    const items = await res.json();
    if (!Array.isArray(items) || items.length === 0) {
        throw new Error('Apify returned 0 dataset items');
    }

    return items[0];
}

// ---------------------------------------------------------------------------
//  CORE: process one channel
// ---------------------------------------------------------------------------
async function processChannel(channelId) {
    const ch = channelStore.get(channelId);
    if (!ch) return;

    channelStore.set(channelId, { ...ch, status: 'processing' });
    stats.processing++;

    try {
        const item = await runApifySingle(ch.currentUrl || ch.originalUrl);

        const headers = item.headers || {};
        const setCookie = headers['set-cookie'] || headers['Set-Cookie'];

        if (!setCookie) {
            throw new Error(`No set-cookie header (HTTP ${item.status})`);
        }

        const cookieStr = Array.isArray(setCookie) ? setCookie[0] : setCookie;
        const pureCookie = cookieStr.split(';')[0];
        const exp = extractExpiry(pureCookie);

        if (exp <= 0) {
            throw new Error('Cookie missing exp= parameter');
        }

        // Build new URL by replacing the __hdnea__ value
        const baseUrl = ch.originalUrl.split('__hdnea__')[0];
        const newUrl = baseUrl + pureCookie;

        const now = Math.floor(Date.now() / 1000);
        let waitSeconds = exp - now - REFRESH_BUFFER_SECONDS;
        if (waitSeconds < 60) waitSeconds = 60;

        channelStore.set(channelId, {
            channelId,
            name: ch.name,
            originalUrl: ch.originalUrl,
            currentUrl: newUrl,
            cookie: pureCookie,
            expires: exp,
            lastUpdated: getFormattedDate(),
            status: 'active',
            error: null,
        });

        stats.processing--;
        stats.succeeded++;
        stats.lastEventAt = getFormattedDate();

        log(`[${channelId}] active — next refresh in ${Math.round(waitSeconds / 60)} min`);

        // Schedule next refresh through the shared gate
        setTimeout(() => {
            apifyLimit(() => processChannel(channelId)).catch(() => {});
        }, waitSeconds * 1000);

    } catch (err) {
        stats.processing--;
        stats.failed++;
        stats.lastEventAt = getFormattedDate();

        const current = channelStore.get(channelId);
        channelStore.set(channelId, {
            ...(current || {}),
            channelId,
            name: ch.name,
            originalUrl: ch.originalUrl,
            currentUrl: ch.currentUrl || ch.originalUrl,
            cookie: '',
            expires: 0,
            lastUpdated: getFormattedDate(),
            status: 'error',
            error: err.message,
        });

        log(`[${channelId}] FAILED: ${err.message}`, 'error');

        setTimeout(() => {
            apifyLimit(() => processChannel(channelId)).catch(() => {});
        }, RETRY_DELAY_MS);
    }
}

// ---------------------------------------------------------------------------
//  QUEUE
// ---------------------------------------------------------------------------
function scheduleChannels(channels) {
    stats.startedAt = stats.startedAt || getFormattedDate();

    for (const ch of channels) {
        const exists = channelStore.has(ch.channelId);
        const prev = exists ? channelStore.get(ch.channelId) : null;

        channelStore.set(ch.channelId, {
            channelId: ch.channelId,
            name: ch.name || ch.channelId,
            originalUrl: ch.url,
            currentUrl: prev?.currentUrl || ch.url,
            cookie: prev?.cookie || '',
            expires: prev?.expires || 0,
            lastUpdated: prev?.lastUpdated || '',
            status: 'queued',
            error: null,
        });

        if (!exists) stats.totalAdded++;
        stats.queued++;

        apifyLimit(() => processChannel(ch.channelId))
            .then(() => { stats.queued--; })
            .catch(() => { stats.queued--; });
    }

    log(`Queued ${channels.length} channels. Active workers: ${apifyLimit.activeCount}, pending: ${apifyLimit.pendingCount}`);
}

// ---------------------------------------------------------------------------
//  INPUT PARSER — accepts JSON array of {Id, url}
// ---------------------------------------------------------------------------
function parseChannelInput(text) {
    const trimmed = text.trim();
    if (!trimmed) return [];

    // Try JSON first
    try {
        const parsed = JSON.parse(trimmed);
        if (Array.isArray(parsed)) {
            return parsed
                .map(item => {
                    const channelId = String(item.Id ?? item.id ?? item.channelId ?? item.channel_id ?? '').trim();
                    const url = String(item.url ?? item.URL ?? '').trim();
                    if (!channelId || !url.startsWith('http')) return null;
                    return { channelId, name: item.name || channelId, url };
                })
                .filter(Boolean);
        }
    } catch (_) {
        // fall through to line-based parsing
    }

    // Fallback: line-based
    const lines = trimmed.split('\n').map(l => l.trim()).filter(l => l && !l.startsWith('#'));
    const channels = [];

    for (const line of lines) {
        let parts;
        if (line.includes('|')) parts = line.split('|').map(s => s.trim());
        else if (line.includes('\t')) parts = line.split('\t').map(s => s.trim());
        else parts = line.split(/\s+/).map(s => s.trim());

        const url = parts.find(p => p.startsWith('http'));
        if (!url) continue;

        const nonUrl = parts.filter(p => p && !p.startsWith('http'));
        const channelId = nonUrl[0] || `ch-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
        const name = nonUrl[1] || channelId;

        channels.push({ channelId: String(channelId), name, url });
    }

    return channels;
}

// ---------------------------------------------------------------------------
//  ROUTES
// ---------------------------------------------------------------------------

app.get('/', (req, res) => res.send('OK'));

// ---- Admin page ----
app.get('/ayush8481/admin', (req, res) => {
    res.send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>JioTV Admin</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0f172a;color:#e2e8f0;padding:24px;font-size:14px}
h1{font-size:1.5rem;margin-bottom:16px;color:#38bdf8}
h2{font-size:1.1rem;margin:16px 0 8px;color:#94a3b8}
.card{background:#1e293b;border-radius:12px;padding:20px;margin-bottom:16px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px}
.stat{background:#0f172a;border-radius:8px;padding:12px}
.stat-label{font-size:.7rem;color:#64748b;text-transform:uppercase;letter-spacing:.05em}
.stat-value{font-size:1.3rem;font-weight:700;color:#38bdf8;margin-top:4px}
.green{color:#4ade80!important}.red{color:#f87171!important}.yellow{color:#facc15!important}
textarea{width:100%;min-height:240px;background:#0f172a;color:#e2e8f0;border:1px solid #334155;border-radius:8px;padding:12px;font-family:ui-monospace,Menlo,Consolas,monospace;font-size:12px;resize:vertical;line-height:1.5}
textarea:focus{outline:none;border-color:#38bdf8}
button{background:#38bdf8;color:#0f172a;border:none;padding:10px 20px;border-radius:8px;font-weight:600;cursor:pointer;font-size:14px;margin-right:8px;margin-top:8px}
button:disabled{opacity:.5;cursor:not-allowed}
button.secondary{background:#334155;color:#e2e8f0}
button.danger{background:#7f1d1d;color:#fecaca}
.log{background:#0f172a;border-radius:8px;padding:12px;font-family:ui-monospace,monospace;font-size:12px;max-height:400px;overflow-y:auto;white-space:pre-wrap;line-height:1.5}
.log .err{color:#f87171}
.log .warn{color:#facc15}
a{text-decoration:none}
table{width:100%;border-collapse:collapse;font-size:12px;margin-top:8px}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid #334155}
th{color:#64748b;font-weight:600;text-transform:uppercase;font-size:.7rem}
.hint{color:#64748b;font-size:12px;margin-top:8px;line-height:1.6}
code{background:#0f172a;padding:2px 6px;border-radius:4px;color:#38bdf8;font-size:12px}
.progress{background:#0f172a;border-radius:8px;height:8px;overflow:hidden;margin-top:8px}
.progress-bar{background:linear-gradient(90deg,#38bdf8,#4ade80);height:100%;transition:width .3s}
</style>
</head>
<body>
<h1>JioTV Cookie Manager</h1>

<div class="card">
  <h2>Paste JSON (100 channels OK — 25 process in parallel)</h2>
  <textarea id="input" placeholder='[
  {"Id":"105","url":"https://jiotvpllive.cdn.jio.com/bpk-tv/.../index.mpd?__hdnea__=..."},
  {"Id":"106","url":"https://jiotvpllive.cdn.jio.com/bpk-tv/.../index.mpd?__hdnea__=..."}
]'></textarea>
  <div class="hint">Accepts JSON array <code>[{"Id":"...","url":"..."}]</code>. Also supports line format: <code>Id|url</code>.</div>
  <button id="processBtn" onclick="processInput()">▶ Add to Queue</button>
  <button class="secondary" onclick="refreshAll()">🔄 Refresh All Active</button>
  <button class="danger" onclick="clearAll()">🗑 Clear All</button>
  <a href="/jiostb.json" target="_blank"><button class="secondary">📥 jiostb.json</button></a>
</div>

<div class="card">
  <h2>Status</h2>
  <div class="grid">
    <div class="stat"><div class="stat-label">Total Tracked</div><div class="stat-value" id="s-total">0</div></div>
    <div class="stat"><div class="stat-label">Active</div><div class="stat-value green" id="s-active">0</div></div>
    <div class="stat"><div class="stat-label">Processing</div><div class="stat-value yellow" id="s-processing">0</div></div>
    <div class="stat"><div class="stat-label">Queued</div><div class="stat-value yellow" id="s-queued">0</div></div>
    <div class="stat"><div class="stat-label">Failed</div><div class="stat-value red" id="s-failed">0</div></div>
    <div class="stat"><div class="stat-label">Succeeded</div><div class="stat-value green" id="s-succeeded">0</div></div>
    <div class="stat"><div class="stat-label">Workers</div><div class="stat-value" id="s-workers">0/25</div></div>
  </div>
  <div class="progress"><div class="progress-bar" id="progressBar" style="width:0%"></div></div>
</div>

<div class="card">
  <h2>Channels</h2>
  <table>
    <thead><tr><th>ID</th><th>Status</th><th>Expires In</th><th>Updated</th><th>Error</th></tr></thead>
    <tbody id="channels"></tbody>
  </table>
</div>

<div class="card">
  <h2>Event Log</h2>
  <div class="log" id="log">Waiting…</div>
</div>

<script>
async function loadStatus(){
  try{
    const r = await fetch('/ayush8481/status');
    const d = await r.json();
    document.getElementById('s-total').textContent = d.stats.totalAdded;
    document.getElementById('s-active').textContent = d.stats.active;
    document.getElementById('s-processing').textContent = d.stats.processing;
    document.getElementById('s-queued').textContent = d.stats.queued;
    document.getElementById('s-failed').textContent = d.stats.failed;
    document.getElementById('s-succeeded').textContent = d.stats.succeeded;
    document.getElementById('s-workers').textContent = d.workerActive + '/' + d.workerMax;

    const total = d.stats.totalAdded || 1;
    const done = d.stats.active + d.stats.failed;
    document.getElementById('progressBar').style.width = Math.min(100, Math.round(done/total*100)) + '%';

    const tbody = document.getElementById('channels');
    tbody.innerHTML = '';
    for(const c of d.channels.slice(0, 200)){
      const tr = document.createElement('tr');
      const expTxt = c.expires ? Math.max(0, Math.round((c.expires - Date.now()/1000)/60)) + ' min' : '—';
      let cls = '';
      if(c.status === 'active') cls = 'green';
      else if(c.status === 'error' || c.status === 'failed') cls = 'red';
      else cls = 'yellow';
      tr.innerHTML = '<td>'+c.channelId+'</td><td class="'+cls+'">'+c.status+'</td><td>'+expTxt+'</td><td>'+(c.lastUpdated||'')+'</td><td style="color:#f87171">'+(c.error||'')+'</td>';
      tbody.appendChild(tr);
    }

    const logEl = document.getElementById('log');
    logEl.innerHTML = d.log.slice(-80).map(e => {
      const cls = e.level === 'error' ? 'err' : e.level === 'warn' ? 'warn' : '';
      return '<div class="'+cls+'">'+e.time+'  '+e.msg+'</div>';
    }).join('') || 'No events yet.';
    logEl.scrollTop = logEl.scrollHeight;
  } catch(e){ console.error(e); }
}

async function processInput(){
  const btn = document.getElementById('processBtn');
  const text = document.getElementById('input').value.trim();
  if(!text) return alert('Paste some channels first.');
  btn.disabled = true; btn.textContent = 'Adding…';
  try{
    const r = await fetch('/ayush8481/add', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ input: text })
    });
    const d = await r.json();
    if(d.error) throw new Error(d.error);
    document.getElementById('input').value = '';
    alert('✅ Added ' + d.added + ' channels to queue.');
  } catch(e){ alert('❌ ' + e.message); }
  btn.disabled = false; btn.textContent = '▶ Add to Queue';
  loadStatus();
}

async function refreshAll(){
  if(!confirm('Re-process ALL channels in the store?')) return;
  const r = await fetch('/ayush8481/refresh-all', {method:'POST'});
  const d = await r.json();
  alert(d.message || 'Done');
  loadStatus();
}

async function clearAll(){
  if(!confirm('Clear all channels?')) return;
  await fetch('/ayush8481/clear', {method:'POST'});
  loadStatus();
}

loadStatus();
setInterval(loadStatus, 3000);
</script>
</body></html>`);
});

// ---- Add channels ----
app.post('/ayush8481/add', express.json({ limit: '10mb' }), (req, res) => {
    try {
        const text = req.body.input || '';
        const channels = parseChannelInput(text);

        if (channels.length === 0) {
            return res.status(400).json({ error: 'No valid channels found. Expected JSON array [{"Id":"...","url":"..."}]' });
        }

        scheduleChannels(channels);
        res.json({ success: true, added: channels.length });
    } catch (err) {
        log(`Add failed: ${err.message}`, 'error');
        res.status(500).json({ error: err.message });
    }
});

// ---- Status ----
app.get('/ayush8481/status', (req, res) => {
    const all = [...channelStore.values()];
    const active = all.filter(c => c.status === 'active').length;

    res.json({
        stats: { ...stats, active },
        workerActive: apifyLimit.activeCount,
        workerMax: MAX_CONCURRENT_RUNS,
        channels: all,
        log: eventLog.slice(-100),
    });
});

// ---- Refresh all ----
app.post('/ayush8481/refresh-all', (req, res) => {
    const active = [...channelStore.values()].filter(c => c.status === 'active' || c.status === 'error');
    if (active.length === 0) return res.json({ message: 'No active channels.' });

    for (const c of active) {
        c.status = 'queued';
        apifyLimit(() => processChannel(c.channelId)).catch(() => {});
    }
    stats.refreshCount += active.length;
    log(`Manual refresh triggered for ${active.length} channels.`);
    res.json({ message: `Refreshing ${active.length} channels.` });
});

// ---- Clear ----
app.post('/ayush8481/clear', (req, res) => {
    channelStore.clear();
    stats.totalAdded = 0;
    stats.queued = 0;
    stats.succeeded = 0;
    stats.failed = 0;
    stats.refreshCount = 0;
    log('Store cleared.');
    res.json({ ok: true });
});

// ---- Public JSON ----
app.get('/jiostb.json', (req, res) => {
    const result = [...channelStore.values()]
        .filter(c => c.status === 'active')
        .map(c => ({
            channel_id: c.channelId,
            url: c.currentUrl,
            name: c.name,
            last_updated: c.lastUpdated,
        }));
    res.json(result);
});

// ---- Diagnostics ----
app.get('/debug/state', (req, res) => {
    res.json({
        stats,
        workerActive: apifyLimit.activeCount,
        workerPending: apifyLimit.pendingCount,
        workerMax: MAX_CONCURRENT_RUNS,
        nodeVersion: process.version,
        hasApifyToken: !!APIFY_TOKEN,
        channelCount: channelStore.size,
        log: eventLog.slice(-200),
    });
});

app.get('/debug/test-apify', async (req, res) => {
    const testUrl = 'https://jiotvpllive.cdn.jio.com/bpk-tv/Star_Sports_HD1_BTS/WDVLive/index.mpd?__hdnea__=st=1789546555~exp=1789568155~acl=/bpk-tv/Star_Sports_HD1_BTS/WDVLive/*~hmac=22eeb22a986a4f91a19ed2cb016c800ea412aaae2fe769b0962b03e31e19590f';
    try {
        const item = await runApifySingle(testUrl);
        res.json({
            ok: true,
            status: item.status,
            hasSetCookie: !!(item.headers && (item.headers['set-cookie'] || item.headers['Set-Cookie'])),
            cookiePreview: (item.headers?.['set-cookie'] || '').toString().slice(0, 80),
        });
    } catch (e) {
        res.status(500).json({ ok: false, error: e.message });
    }
});

// ---------------------------------------------------------------------------
//  START
// ---------------------------------------------------------------------------
app.listen(PORT, () => {
    log(`Server started on port ${PORT}`);
    log(`Concurrency: ${MAX_CONCURRENT_RUNS} × ${MEMORY_MB} MB = ${MAX_CONCURRENT_RUNS * MEMORY_MB} MB total`);
    console.log(`   Admin:   /ayush8481/admin`);
    console.log(`   Public:  /jiostb.json`);
    console.log(`   Node:    ${process.version}`);
});
