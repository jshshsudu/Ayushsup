const express = require('express');
const multer = require('multer');
const pLimit = require('p-limit');
const { parse } = require('iptv-playlist-parser');

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
const BATCH_SIZE = 50;               // URLs per Apify run
const MAX_CONCURRENT_RUNS = 2;       // ✅ 2 runs = 100 channels in parallel
const REFRESH_BUFFER_SECONDS = 1800; // 30 minutes before expiry
const MEMORY_MB = 4096;              // 4 GB per run (2 × 4 = 8 GB total, safe)
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
//  APIFY HELPERS (async — no 300 s timeout)
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
//  CHANNEL REFRESH (single channel)
// ---------------------------------------------------------------------------
async function refreshChannel(channelId) {
    const ch = channelStore.get(channelId);
    if (!ch) return;

    try {
        const results = await processBatch([ch.currentUrl]);
        if (!results || results.length === 0) throw new Error('Empty Apify result');

        const headers = results[0].headers || {};
        const setCookie = headers['set-cookie'];

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

    console.log(`🚀 Processing ${total} channels in ${batches.length} batches (${MAX_CONCURRENT_RUNS} concurrent)`);

    const limit = pLimit(MAX_CONCURRENT_RUNS);
    let batchCounter = 0;

    const tasks = batches.map(batch =>
        limit(async () => {
            const batchIndex = ++batchCounter;
            processingState.currentBatch = batchIndex;
            const urls = batch.map(ch => ch.url);

            try {
                const results = await processBatch(urls);

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
                            status: 'failed', error: 'No result returned',
                        });
                        continue;
                    }

                    const headers = item.headers || {};
                    const setCookie = headers['set-cookie'];

                    if (!setCookie) {
                        processingState.failedChannels++;
                        processingState.processedChannels++;
                        channelStore.set(ch.channelId, {
                            channelId: ch.channelId, name: ch.name,
                            originalUrl: ch.url, currentUrl: ch.url,
                            cookie: '', expires: 0, lastUpdated: '',
                            status: 'no_cookie', error: 'No set-cookie header',
                        });
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
                            status: 'no_expiry', error: 'Cookie has no expiry',
                        });
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

                    // Schedule per-channel refresh
                    setTimeout(() => refreshChannel(ch.channelId), waitSeconds * 1000);

                    processingState.processedChannels++;
                }

                console.log(`✅ Batch ${batchIndex}/${batches.length} done (${results.length} results)`);
            } catch (err) {
                console.error(`❌ Batch ${batchIndex} failed: ${err.message}`);
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

    await Promise.all(tasks);

    processingState.isProcessing = false;
    processingState.finishedAt = getFormattedDate();
    console.log(`🏁 Done. Active: ${[...channelStore.values()].filter(c => c.status === 'active').length}, Failed: ${processingState.failedChannels}`);
}

// ---------------------------------------------------------------------------
//  MULTER
// ---------------------------------------------------------------------------
const upload = multer({ storage: multer.memoryStorage() });

// ---------------------------------------------------------------------------
//  ROUTES
// ---------------------------------------------------------------------------

// ✅ Keep-alive — hit every 2 min via cron-job.org
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
.log{background:#0f172a;border-radius:8px;padding:12px;font-family:monospace;font-size:.8rem;max-height:300px;overflow-y:auto;white-space:pre-wrap}
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
    document.getElementById('log').textContent='✅ Upload accepted. Processing started…';
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

// ---- Upload M3U8 ----
app.post('/ayush8481/upload', upload.single('playlist'), async (req, res) => {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded.' });

    try {
        const text = req.file.buffer.toString('utf8');
        const parsed = parse(text);

        if (!parsed.items || parsed.items.length === 0) {
            return res.status(400).json({ error: 'No channels found in playlist.' });
        }

        const channels = parsed.items.map(item => ({
            channelId: item.tvg?.id || item.name || `ch-${Math.random().toString(36).slice(2, 8)}`,
            name: item.name || 'Unknown',
            url: item.url,
        }));

        // Start processing in background
        processAllChannels(channels);

        res.json({ success: true, channelCount: channels.length });
    } catch (err) {
        console.error('Upload parse error:', err);
        res.status(500).json({ error: `Failed to parse playlist: ${err.message}` });
    }
});

// ---- Status API ----
app.get('/ayush8481/status', (req, res) => {
    const all = [...channelStore.values()];
    const active = all.filter(c => c.status === 'active').length;
    const failed = all.filter(c => c.status !== 'active').length;

    let log;
    if (processingState.errors.length > 0) {
        log = processingState.errors.join('\n');
    } else if (processingState.isProcessing) {
        log = `Processing batch ${processingState.currentBatch}/${processingState.totalBatches}…`;
    } else if (processingState.finishedAt) {
        log = `Finished at ${processingState.finishedAt}`;
    } else {
        log = 'Idle';
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

// ---- Manual refresh all ----
app.post('/ayush8481/refresh-all', async (req, res) => {
    const channels = [...channelStore.values()].map(c => ({
        channelId: c.channelId,
        name: c.name,
        url: c.currentUrl || c.originalUrl,
    }));

    if (channels.length === 0) {
        return res.json({ message: 'No channels loaded. Upload a playlist first.' });
    }

    processAllChannels(channels);
    res.json({ message: `Refresh triggered for ${channels.length} channels.` });
});

// ---- Public jiostb.json ----
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

// ---- Debug ----
app.get('/debug/channels', (req, res) => {
    const all = [...channelStore.values()];
    res.json({
        total: all.length,
        active: all.filter(c => c.status === 'active').length,
        failed: all.filter(c => c.status !== 'active').length,
        channels: all,
    });
});

// ---------------------------------------------------------------------------
//  START
// ---------------------------------------------------------------------------
app.listen(PORT, () => {
    console.log(`✅ JioTV Cookie Manager running on port ${PORT}`);
    console.log(`   Admin:  /ayush8481/admin`);
    console.log(`   Public: /jiostb.json`);
    console.log(`   Apify concurrency: ${MAX_CONCURRENT_RUNS} runs × ${BATCH_SIZE} URLs = ${MAX_CONCURRENT_RUNS * BATCH_SIZE} channels in parallel`);
});
