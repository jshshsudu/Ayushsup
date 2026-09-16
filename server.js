const express = require('express');
const multer = require('multer');
const pLimit = require('p-limit');
const { parsePlaylist } = require('iptv-m3u-playlist-parser');

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
const BATCH_SIZE = 50;
const MAX_CONCURRENT_RUNS = 2;
const REFRESH_BUFFER_SECONDS = 1800;
const MEMORY_MB = 4096;
const POLL_INTERVAL_MS = 3000;

// ---------------------------------------------------------------------------
//  IN-MEMORY STORAGE
// ---------------------------------------------------------------------------
const channelStore = new Map();

const processingState = {
    isProcessing: false,
    totalChannels: 0,
    processedChannels: 0,
    failedChannels: 0,
    currentBatch: 0,
    totalBatches: 0,
    startedAt: null,
    finishedAt: null,
    errors: [],
    lastLog: '',
};

// ---------------------------------------------------------------------------
//  HELPERS
// ---------------------------------------------------------------------------
function getFormattedDate() {
    const d = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
    return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')} ${String(d.getDate()).padStart(2, '0')}-${String(d.getMonth() + 1).padStart(2, '0')}-${d.getFullYear()}`;
}

function extractExpiry(cookieStr) {
    const match = cookieStr.match(/exp=(\d+)/);
    return match ? parseInt(match[1], 10) : 0;
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function chunkArray(arr, size) {
    const chunks = [];
    for (let i = 0; i < arr.length; i += size) chunks.push(arr.slice(i, i + size));
    return chunks;
}

// ---------------------------------------------------------------------------
//  APIFY HELPERS
// ---------------------------------------------------------------------------
async function startApifyRun(urls, memoryMb = MEMORY_MB) {
    const startUrls = urls.map(u => ({ url: u, method: 'HEAD' }));

    const apifyInput = {
        startUrls,
        proxyConfiguration: {
            useApifyProxy: true,
            apifyProxyGroups: ['RESIDENTIAL'],
            apifyProxyCountry: 'IN',
        },
        maxConcurrency: 50,
        maxRequestsPerCrawl: 0,
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

    const url = `${APIFY_BASE}/acts/${APIFY_ACTOR}/runs?token=${APIFY_TOKEN}&memory=${memoryMb}`;
    const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(apifyInput),
    });
    if (!res.ok) throw new Error(`Apify start failed (${res.status}): ${await res.text()}`);
    return (await res.json()).data;
}

async function waitForRun(runId) {
    const url = `${APIFY_BASE}/actor-runs/${runId}?token=${APIFY_TOKEN}`;
    while (true) {
        await sleep(POLL_INTERVAL_MS);
        const res = await fetch(url);
        if (!res.ok) throw new Error(`Apify poll failed (${res.status})`);
        const data = await res.json();
        const status = data.data.status;
        if (['SUCCEEDED', 'FAILED', 'ABORTED', 'TIMED-OUT'].includes(status)) return data.data;
    }
}

async function fetchDatasetItems(datasetId) {
    const url = `${APIFY_BASE}/datasets/${datasetId}/items?token=${APIFY_TOKEN}&clean=true`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`Dataset fetch failed (${res.status})`);
    return res.json();
}

async function processBatch(urls) {
    const run = await startApifyRun(urls);
    const finished = await waitForRun(run.id);
    if (finished.status !== 'SUCCEEDED') {
        throw new Error(`Apify run ${run.id} ended with status: ${finished.status}`);
    }
    return fetchDatasetItems(finished.defaultDatasetId);
}

// ---------------------------------------------------------------------------
//  PER-CHANNEL REFRESH
// ---------------------------------------------------------------------------
async function refreshChannel(channelId) {
    const ch = channelStore.get(channelId);
    if (!ch) return;

    try {
        const results = await processBatch([ch.currentUrl]);
        if (!results || results.length === 0) throw new Error('Empty Apify result');

        const headers = results[0].headers || {};
        let setCookie = headers['set-cookie'] || headers['Set-Cookie'];
        if (!setCookie) throw new Error('No set-cookie header');

        const cookieStr = Array.isArray(setCookie) ? setCookie[0] : setCookie;
        const pureCookie = cookieStr.split(';')[0];
        const exp = extractExpiry(pureCookie);
        if (exp <= 0) throw new Error('Cookie has no expiry');

        const now = Math.floor(Date.now() / 1000);
        let waitSeconds = exp - now - REFRESH_BUFFER_SECONDS;
        if (waitSeconds < 60) waitSeconds = 60;

        const newUrl = ch.currentUrl.replace(/__hdnea__=[^&]+/, pureCookie);

        channelStore.set(channelId, {
            ...ch,
            cookie: pureCookie,
            currentUrl: newUrl,
            expires: exp,
            lastUpdated: getFormattedDate(),
            status: 'active',
            error: undefined,
        });

        setTimeout(() => refreshChannel(channelId), waitSeconds * 1000);
        console.log(`✅ Refreshed ${channelId} (${ch.name}) — next in ${Math.round(waitSeconds / 60)} min`);
    } catch (err) {
        console.error(`❌ Refresh failed for ${channelId}: ${err.message}`);
        const current = channelStore.get(channelId);
        if (current) {
            channelStore.set(channelId, { ...current, status: 'error', error: err.message });
        }
        setTimeout(() => refreshChannel(channelId), 120_000);
    }
}

// ---------------------------------------------------------------------------
//  MASTER PROCESSOR
// ---------------------------------------------------------------------------
async function processAllChannels(channels) {
    const total = channels.length;
    const batches = chunkArray(channels, BATCH_SIZE);

    processingState.isProcessing = true;
    processingState.totalChannels = total;
    processingState.processedChannels = 0;
    processingState.failedChannels = 0;
    processingState.totalBatches = batches.length;
    processingState.currentBatch = 0;
    processingState.startedAt = getFormattedDate();
    processingState.finishedAt = null;
    processingState.errors = [];
    processingState.lastLog = `Started ${total} channels in ${batches.length} batches`;

    console.log(`🚀 [START] ${total} channels, ${batches.length} batches, ${MAX_CONCURRENT_RUNS} concurrent`);

    const limit = pLimit(MAX_CONCURRENT_RUNS);
    let batchCounter = 0;

    const tasks = batches.map(batch =>
        limit(async () => {
            const batchIndex = ++batchCounter;
            processingState.currentBatch = batchIndex;
            const urls = batch.map(ch => ch.url);
            const ids = batch.map(ch => ch.channelId).join(',');

            console.log(`▶️  [BATCH ${batchIndex}] starting ${urls.length} urls: ${ids}`);

            try {
                const results = await processBatch(urls);

                console.log(`◀️  [BATCH ${batchIndex}] got ${Array.isArray(results) ? results.length : 'NON-ARRAY'} results`);

                if (!Array.isArray(results)) {
                    throw new Error(`Apify returned non-array: ${JSON.stringify(results).slice(0, 200)}`);
                }

                for (let i = 0; i < batch.length; i++) {
                    const ch = batch[i];
                    const item = results[i];

                    if (!item) {
                        processingState.failedChannels++;
                        processingState.processedChannels++;
                        channelStore.set(ch.channelId, {
                            channelId: ch.channelId, name: ch.name,
                            originalUrl: ch.url, currentUrl: ch.url,
                            cookie: '', expires: 0, lastUpdated: '',
                            status: 'failed', error: 'No result returned for index ' + i,
                        });
                        console.warn(`  ⚠️ [${ch.channelId}] no result`);
                        continue;
                    }

                    const headers = item.headers || {};
                    let setCookie = headers['set-cookie'] || headers['Set-Cookie'];

                    if (!setCookie) {
                        processingState.failedChannels++;
                        processingState.processedChannels++;
                        channelStore.set(ch.channelId, {
                            channelId: ch.channelId, name: ch.name,
                            originalUrl: ch.url, currentUrl: ch.url,
                            cookie: '', expires: 0, lastUpdated: '',
                            status: 'no_cookie', error: 'No set-cookie header. Status: ' + item.status,
                        });
                        console.warn(`  ⚠️ [${ch.channelId}] no set-cookie (status=${item.status})`);
                        continue;
                    }

                    const cookieStr = Array.isArray(setCookie) ? setCookie[0] : setCookie;
                    const pureCookie = cookieStr.split(';')[0];
                    const exp = extractExpiry(pureCookie);

                    if (exp <= 0) {
                        processingState.failedChannels++;
                        processingState.processedChannels++;
                        channelStore.set(ch.channelId, {
                            channelId: ch.channelId, name: ch.name,
                            originalUrl: ch.url, currentUrl: ch.url,
                            cookie: pureCookie, expires: 0,
                            lastUpdated: getFormattedDate(),
                            status: 'no_expiry', error: 'Cookie missing exp=',
                        });
                        console.warn(`  ⚠️ [${ch.channelId}] cookie has no exp`);
                        continue;
                    }

                    const now = Math.floor(Date.now() / 1000);
                    let waitSeconds = exp - now - REFRESH_BUFFER_SECONDS;
                    if (waitSeconds < 60) waitSeconds = 60;

                    const newUrl = ch.url.replace(/__hdnea__=[^&]+/, pureCookie);

                    channelStore.set(ch.channelId, {
                        channelId: ch.channelId, name: ch.name,
                        originalUrl: ch.url, currentUrl: newUrl,
                        cookie: pureCookie, expires: exp,
                        lastUpdated: getFormattedDate(),
                        status: 'active',
                    });

                    setTimeout(() => refreshChannel(ch.channelId), waitSeconds * 1000);
                    processingState.processedChannels++;
                    console.log(`  ✅ [${ch.channelId}] active (expires in ${Math.round((exp-now)/60)} min)`);
                }
            } catch (err) {
                console.error(`❌ [BATCH ${batchIndex}] FAILED: ${err.message}`);
                console.error(err.stack);
                processingState.errors.push(`Batch ${batchIndex}: ${err.message}`);

                for (const ch of batch) {
                    processingState.failedChannels++;
                    processingState.processedChannels++;
                    channelStore.set(ch.channelId, {
                        channelId: ch.channelId, name: ch.name,
                        originalUrl: ch.url, currentUrl: ch.url,
                        cookie: '', expires: 0, lastUpdated: '',
                        status: 'failed', error: err.message,
                    });
                }
            }
        })
    );

    try {
        await Promise.all(tasks);
    } catch (e) {
        console.error('❌ [GLOBAL] processAllChannels crashed:', e);
        processingState.errors.push('Global: ' + e.message);
    }

    processingState.isProcessing = false;
    processingState.finishedAt = getFormattedDate();
    processingState.lastLog = `Finished. Active: ${[...channelStore.values()].filter(c => c.status === 'active').length}, Failed: ${processingState.failedChannels}`;
    console.log(`🏁 [DONE] ${processingState.lastLog}`);
}

// ---------------------------------------------------------------------------
//  MULTER
// ---------------------------------------------------------------------------
const upload = multer({ storage: multer.memoryStorage() });

// ---------------------------------------------------------------------------
//  ROUTES
// ---------------------------------------------------------------------------

app.get('/', (req, res) => res.send('OK'));

app.get('/ayush8481/admin', (req, res) => {
    res.send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>JioTV Admin</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0f172a;color:#e2e8f0;padding:24px}
h1{font-size:1.5rem;margin-bottom:16px;color:#38bdf8}
h2{font-size:1.1rem;margin:16px 0 8px;color:#94a3b8}
.card{background:#1e293b;border-radius:12px;padding:20px;margin-bottom:16px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px}
.stat{background:#0f172a;border-radius:8px;padding:12px}
.stat-label{font-size:.75rem;color:#64748b;text-transform:uppercase}
.stat-value{font-size:1.4rem;font-weight:700;color:#38bdf8}
.green{color:#4ade80!important}.red{color:#f87171!important}.yellow{color:#facc15!important}
input[type=file]{display:block;margin:12px 0;color:#cbd5e1}
button{background:#38bdf8;color:#0f172a;border:none;padding:10px 20px;border-radius:8px;font-weight:600;cursor:pointer}
button:disabled{opacity:.5;cursor:not-allowed}
button.secondary{background:#334155;color:#e2e8f0}
.log{background:#0f172a;border-radius:8px;padding:12px;font-family:monospace;font-size:.8rem;max-height:400px;overflow-y:auto;white-space:pre-wrap}
a{text-decoration:none}
</style>
</head>
<body>
<h1>JioTV Cookie Manager</h1>
<div class="card">
  <h2>Upload M3U8 Playlist</h2>
  <form id="uploadForm" enctype="multipart/form-data">
    <input type="file" name="playlist" accept=".m3u8,.m3u,.txt" required>
    <button type="submit" id="uploadBtn">Upload &amp; Process</button>
  </form>
</div>
<div class="card">
  <h2>Status</h2>
  <div class="grid">
    <div class="stat"><div class="stat-label">Total</div><div class="stat-value" id="total">0</div></div>
    <div class="stat"><div class="stat-label">Processed</div><div class="stat-value yellow" id="processed">0</div></div>
    <div class="stat"><div class="stat-label">Active</div><div class="stat-value green" id="active">0</div></div>
    <div class="stat"><div class="stat-label">Failed</div><div class="stat-value red" id="failed">0</div></div>
    <div class="stat"><div class="stat-label">Batch</div><div class="stat-value" id="batch">0/0</div></div>
  </div>
  <div style="margin-top:12px">
    <button class="secondary" onclick="refreshAll()">🔄 Refresh All</button>
    <button class="secondary" onclick="loadStatus()">📊 Refresh Status</button>
    <a href="/jiostb.json" target="_blank"><button class="secondary">📥 View jiostb.json</button></a>
    <a href="/debug/state" target="_blank"><button class="secondary">🔍 Debug State</button></a>
    <a href="/debug/test-apify" target="_blank"><button class="secondary">⚡ Test Apify</button></a>
    <a href="/debug/test-parse" target="_blank"><button class="secondary">📄 Test Parser</button></a>
  </div>
</div>
<div class="card"><h2>Log</h2><div class="log" id="log">Waiting…</div></div>
<script>
async function loadStatus(){
  try{
    const r=await fetch('/ayush8481/status');
    const d=await r.json();
    total.textContent=d.totalChannels;
    processed.textContent=d.processedChannels;
    active.textContent=d.activeChannels;
    failed.textContent=d.failedChannels;
    batch.textContent=d.currentBatch+'/'+d.totalBatches;
    document.getElementById('log').textContent=d.log||'No log yet.';
  }catch(e){document.getElementById('log').textContent='Error: '+e.message;}
}
document.getElementById('uploadForm').addEventListener('submit',async e=>{
  e.preventDefault();
  const btn=document.getElementById('uploadBtn');
  btn.disabled=true;btn.textContent='Processing…';
  try{
    const res=await fetch('/ayush8481/upload',{method:'POST',body:new FormData(e.target)});
    const d=await res.json();
    if(d.error)throw new Error(d.error);
    document.getElementById('log').textContent='✅ Upload accepted ('+d.channelCount+' channels). Processing started…';
  }catch(err){document.getElementById('log').textContent='❌ '+err.message;}
  btn.disabled=false;btn.textContent='Upload & Process';
  loadStatus();
});
async function refreshAll(){
  const b=event.target;b.disabled=true;b.textContent='Refreshing…';
  try{
    const r=await fetch('/ayush8481/refresh-all',{method:'POST'});
    const d=await r.json();
    document.getElementById('log').textContent=d.message||'Refresh triggered.';
  }catch(e){document.getElementById('log').textContent='❌ '+e.message;}
  b.disabled=false;b.textContent='🔄 Refresh All';
}
loadStatus();setInterval(loadStatus,5000);
</script>
</body></html>`);
});

app.post('/ayush8481/upload', upload.single('playlist'), async (req, res) => {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded.' });

    try {
        const text = req.file.buffer.toString('utf8');
        console.log(`📥 Upload received: ${req.file.size} bytes`);

        const parsed = parsePlaylist(text);

        if (!parsed.items || parsed.items.length === 0) {
            console.error('❌ Parser returned 0 items');
            return res.status(400).json({ error: 'No channels found in playlist. Check format.' });
        }

        console.log(`📄 Parsed ${parsed.items.length} channels`);

        const channels = parsed.items.map(item => ({
            channelId: String(item.tvg?.id || item.name || `ch-${Math.random().toString(36).slice(2, 8)}`),
            name: item.name || 'Unknown',
            url: item.url,
        })).filter(ch => ch.url && ch.url.startsWith('http'));

        if (channels.length === 0) {
            return res.status(400).json({ error: 'No valid URLs found in playlist.' });
        }

        console.log(`✅ ${channels.length} valid channels after filtering`);

        processAllChannels(channels).catch(e => {
            console.error('❌ Background processing crashed:', e);
        });

        res.json({ success: true, channelCount: channels.length });
    } catch (err) {
        console.error('❌ Upload parse error:', err);
        res.status(500).json({ error: `Failed to parse playlist: ${err.message}` });
    }
});

app.get('/ayush8481/status', (req, res) => {
    const all = [...channelStore.values()];
    const active = all.filter(c => c.status === 'active').length;
    const failed = all.filter(c => c.status !== 'active').length;

    let log;
    if (processingState.errors.length > 0) {
        log = processingState.errors.slice(-20).join('\n');
    } else if (processingState.isProcessing) {
        log = `Processing batch ${processingState.currentBatch}/${processingState.totalBatches}…\n` + processingState.lastLog;
    } else if (processingState.finishedAt) {
        log = processingState.lastLog + '\nFinished at ' + processingState.finishedAt;
    } else {
        log = 'Idle. Upload an M3U8 to begin.';
    }

    res.json({
        isProcessing: processingState.isProcessing,
        totalChannels: processingState.totalChannels,
        processedChannels: processingState.processedChannels,
        activeChannels: active,
        failedChannels: failed,
        currentBatch: processingState.currentBatch,
        totalBatches: processingState.totalBatches,
        startedAt: processingState.startedAt,
        finishedAt: processingState.finishedAt,
        log,
    });
});

app.post('/ayush8481/refresh-all', async (req, res) => {
    const channels = [...channelStore.values()].map(c => ({
        channelId: c.channelId,
        name: c.name,
        url: c.currentUrl || c.originalUrl,
    }));

    if (channels.length === 0) {
        return res.json({ message: 'No channels loaded. Upload a playlist first.' });
    }

    processAllChannels(channels).catch(e => console.error(e));
    res.json({ message: `Refresh triggered for ${channels.length} channels.` });
});

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

// ---------------------------------------------------------------------------
//  DIAGNOSTIC ENDPOINTS
// ---------------------------------------------------------------------------

app.get('/debug/test-apify', async (req, res) => {
    const testUrl = 'https://jiotvpllive.cdn.jio.com/bpk-tv/Star_Sports_HD1_BTS/WDVLive/index.mpd?__hdnea__=st=1789546555~exp=1789568155~acl=/bpk-tv/Star_Sports_HD1_BTS/WDVLive/*~hmac=22eeb22a986a4f91a19ed2cb016c800ea412aaae2fe769b0962b03e31e19590f';
    try {
        console.log('🧪 Testing Apify...');
        const run = await startApifyRun([testUrl]);
        res.json({ ok: true, message: 'Apify run started', runId: run.id, status: run.status });
    } catch (e) {
        console.error('❌ Apify test failed:', e.message);
        res.status(500).json({ ok: false, error: e.message, stack: e.stack });
    }
});

app.get('/debug/test-parse', (req, res) => {
    const sample = `#EXTM3U
#EXTINF:-1 tvg-id="167" tvg-name="Zee TV HD" tvg-logo="https://img.media.jio.com/tvpimages/66/3/300378_1753869902174_l_medium.jpg" group-title="Entertainment",Zee TV HD
#KODIPROP:inputstream=inputstream.adaptive
#KODIPROP:inputstream.adaptive.manifest_type=mpd
#KODIPROP:inputstream.adaptive.license_type=clearkey
#KODIPROP:inputstream.adaptive.license_key=a23b609b33e254a48e4fc6fa7af0fd8d:4064c52328bfe9e6008776b48a431e4b
https://jiotvpllive.cdn.jio.com/bpk-tv/ZeeTVHD_BTS/WDVLive/index.mpd?__hdnea__=st=1789558513~exp=1789580113~acl=/bpk-tv/ZeeTVHD_BTS/WDVLive/*~hmac=0e963dd530f7d872a6edc6e3141e8e81414f8da948a80168dfa239271f00fd8c`;
    try {
        const parsed = parsePlaylist(sample);
        res.json({
            ok: true,
            itemCount: parsed.items.length,
            firstItem: parsed.items[0],
        });
    } catch (e) {
        res.status(500).json({ ok: false, error: e.message, stack: e.stack });
    }
});

app.get('/debug/state', (req, res) => {
    const all = [...channelStore.values()];
    res.json({
        processingState,
        channelCount: all.length,
        activeCount: all.filter(c => c.status === 'active').length,
        failedCount: all.filter(c => c.status !== 'active').length,
        firstFive: all.slice(0, 5),
        envCheck: {
            hasApifyToken: !!APIFY_TOKEN,
            tokenPrefix: APIFY_TOKEN ? APIFY_TOKEN.slice(0, 12) + '...' : null,
            nodeVersion: process.version,
        },
    });
});

// ---------------------------------------------------------------------------
//  START
// ---------------------------------------------------------------------------
app.listen(PORT, () => {
    console.log(`✅ JioTV Cookie Manager running on port ${PORT}`);
    console.log(`   Admin:   /ayush8481/admin`);
    console.log(`   Public:  /jiostb.json`);
    console.log(`   Node:    ${process.version}`);
    console.log(`   Apify:   ${MAX_CONCURRENT_RUNS} runs × ${BATCH_SIZE} URLs = ${MAX_CONCURRENT_RUNS * BATCH_SIZE} parallel`);
});
