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

const BATCH_SIZE = 50;                // URLs per Apify run (normal channels)
const BATCH_MEMORY_MB = 2048;         // 2 GB per batch run
const SINGLE_MEMORY_MB = 256;         // 256 MB per single-URL run (special channels)
const MAX_CONCURRENT_RUNS = 4;        // Apify Free plan limit is 5 — leave 1 spare
const REFRESH_BUFFER_SECONDS = 1800;  // 30 min before expiry
const RETRY_DELAY_MS = 120_000;       // Retry failed channel after 2 min

const apifyLimit = pLimit(MAX_CONCURRENT_RUNS);

// ---------------------------------------------------------------------------
//  STORAGE
// ---------------------------------------------------------------------------
const channelStore = new Map();

const stats = {
    totalAdded: 0,
    normalCount: 0,
    specialCount: 0,
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
    const p = level === 'error' ? '❌' : level === 'warn' ? '⚠️' : 'ℹ️';
    console.log(`${p} ${msg}`);
}

// ---------------------------------------------------------------------------
//  HELPERS
// ---------------------------------------------------------------------------
function getFormattedDate() {
    const d = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
    return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}:${String(d.getSeconds()).padStart(2, '0')} ${String(d.getDate()).padStart(2, '0')}-${String(d.getMonth() + 1).padStart(2, '0')}-${d.getFullYear()}`;
}

function extractExpiry(cookieStr) {
    const m = cookieStr.match(/exp=(\d+)/);
    return m ? parseInt(m[1], 10) : 0;
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function chunkArray(arr, size) {
    const chunks = [];
    for (let i = 0; i < arr.length; i += size) chunks.push(arr.slice(i, i + size));
    return chunks;
}

// ---------------------------------------------------------------------------
//  ACL EXTRACTION / NORMALISATION
// ---------------------------------------------------------------------------
function extractAclFromUrl(url) {
    const m = url.match(/~acl=([^~]+)~/);
    if (m) return m[1].trim();
    const m2 = url.match(/\/bpk-tv\/[^\/]+\/[^\/]+/);
    return m2 ? m2[0] + '/*' : null;
}

function extractAclFromCookie(cookieStr) {
    const m = cookieStr.match(/acl=([^~;]+)/);
    return m ? m[1].trim() : null;
}

function normaliseAcl(acl) {
    if (!acl) return '';
    return acl.toLowerCase().replace(/\/+$/, '').replace(/\*+$/, '').trim();
}

function applyCookieToUrl(originalUrl, pureCookie) {
    const idx = originalUrl.indexOf('__hdnea__=');
    if (idx === -1) return originalUrl;
    return originalUrl.substring(0, idx + '__hdnea__='.length) + pureCookie;
}

// ---------------------------------------------------------------------------
//  SPECIAL-CHANNEL DETECTION
//
//  A channel is "special" when the URL's own path (/bpk-tv/<X>/...) does NOT
//  match the URL's own ~acl=/bpk-tv/<Y>/...~ value.
//
//  Reason: the CDN only returns cookies whose acl= matches the URL path it
//  actually serves. For these mis-aliased entries, batching cannot resolve the
//  mapping by ACL, so we must process them individually.
//
//  Example special URL:
//    /bpk-tv/Dagdusheth_Pune_BTS/WDVLive/index.mpd?__hdnea__=...~acl=/bpk-tv/Dagdusheth_Ganpati/HLSPartner/*~...
//  Example normal URL:
//    /bpk-tv/ZeeTVHD_BTS/WDVLive/index.mpd?__hdnea__=...~acl=/bpk-tv/ZeeTVHD_BTS/WDVLive/*~...
// ---------------------------------------------------------------------------
function isSpecialChannel(url) {
    try {
        const u = new URL(url);
        // Extract the /bpk-tv/<segment>/ part from the URL's own path
        const m = u.pathname.match(/^\/bpk-tv\/([^\/]+)/i);
        if (!m) return true;                    // unknown path → treat as special
        const urlSegment = m[1].toLowerCase();

        const acl = extractAclFromUrl(url);
        if (!acl) return true;                  // no acl= → treat as special

        // ACL must contain the same /bpk-tv/<segment>/ as the URL path
        return !acl.toLowerCase().includes(`/bpk-tv/${urlSegment}/`);
    } catch {
        return true;
    }
}

// ---------------------------------------------------------------------------
//  APIFY: START / POLL / FETCH
// ---------------------------------------------------------------------------
async function startApifyRun(urls, memoryMb, maxConcurrency) {
    const startUrls = urls.map(u => ({ url: u, method: 'HEAD' }));

    const apifyInput = {
        startUrls,
        proxyConfiguration: {
            useApifyProxy: true,
            apifyProxyGroups: ['RESIDENTIAL'],
            apifyProxyCountry: 'IN',
        },
        maxConcurrency,
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

    const apiUrl = `${APIFY_BASE}/acts/${APIFY_ACTOR}/runs?token=${APIFY_TOKEN}&memory=${memoryMb}`;
    const res = await fetch(apiUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(apifyInput),
    });
    if (!res.ok) {
        throw new Error(`Apify HTTP ${res.status}: ${(await res.text()).slice(0, 300)}`);
    }
    return (await res.json()).data;
}

async function waitForRun(runId) {
    const url = `${APIFY_BASE}/actor-runs/${runId}?token=${APIFY_TOKEN}`;
    while (true) {
        await sleep(3000);
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

async function runApify(urls, memoryMb, maxConcurrency) {
    const run = await startApifyRun(urls, memoryMb, maxConcurrency);
    const finished = await waitForRun(run.id);
    if (finished.status !== 'SUCCEEDED') {
        throw new Error(`Apify run ${run.id} ended with status: ${finished.status}`);
    }
    return fetchDatasetItems(finished.defaultDatasetId);
}

// ---------------------------------------------------------------------------
//  NORMAL BATCH PROCESSING (50 URLs per run, 2 GB)
// ---------------------------------------------------------------------------
async function processBatchOfChannels(channels, batchIndex) {
    const urls = channels.map(c => c.url);
    log(`▶️  [BATCH ${batchIndex}] starting ${urls.length} urls @ ${BATCH_MEMORY_MB} MB`);

    for (const ch of channels) {
        const existing = channelStore.get(ch.channelId) || {};
        channelStore.set(ch.channelId, { ...existing, status: 'processing' });
    }

    const results = await runApify(urls, BATCH_MEMORY_MB, 50);
    if (!Array.isArray(results)) throw new Error('Apify returned non-array');

    log(`◀️  [BATCH ${batchIndex}] got ${results.length} results`);

    // Build ACL → cookie map from all returned cookies
    const aclToCookie = new Map();
    for (const item of results) {
        const headers = item.headers || {};
        let setCookie = headers['set-cookie'] || headers['Set-Cookie'];
        if (!setCookie) continue;
        const cookieStr = Array.isArray(setCookie) ? setCookie[0] : setCookie;
        const aclRaw = extractAclFromCookie(cookieStr);
        if (!aclRaw) continue;
        const acl = normaliseAcl(aclRaw);
        const exp = extractExpiry(cookieStr.split(';')[0]);
        const existing = aclToCookie.get(acl);
        if (!existing || exp > existing.exp) {
            aclToCookie.set(acl, { cookieStr, exp });
        }
    }
    log(`   [BATCH ${batchIndex}] ACL map has ${aclToCookie.size} unique ACLs`);

    let matched = 0, failed = 0;

    for (const ch of channels) {
        const aclUrl = normaliseAcl(extractAclFromUrl(ch.url));
        if (!aclUrl) {
            failed++;
            channelStore.set(ch.channelId, {
                channelId: ch.channelId, name: ch.name,
                originalUrl: ch.url, currentUrl: ch.url,
                cookie: '', expires: 0, lastUpdated: '',
                status: 'failed', error: 'No ACL in URL',
                isSpecial: false,
            });
            continue;
        }

        let hit = aclToCookie.get(aclUrl);
        if (!hit) {
            // Try prefix match on /bpk-tv/<segment>/
            const prefix = aclUrl.split('/').slice(0, 4).join('/');
            for (const [k, v] of aclToCookie.entries()) {
                if (k.startsWith(prefix)) { hit = v; break; }
            }
        }

        if (!hit) {
            failed++;
            channelStore.set(ch.channelId, {
                channelId: ch.channelId, name: ch.name,
                originalUrl: ch.url, currentUrl: ch.url,
                cookie: '', expires: 0,
                lastUpdated: getFormattedDate(),
                status: 'no_cookie', error: `No cookie for ACL ${aclUrl}`,
                isSpecial: false,
            });
            log(`  ⚠️ [${ch.channelId}] no cookie for ACL ${aclUrl}`, 'warn');
            continue;
        }

        aclToCookie.delete(aclUrl);

        const { cookieStr } = hit;
        const pureCookie = cookieStr.split(';')[0];
        const exp = extractExpiry(pureCookie);
        if (exp <= 0) { failed++; continue; }

        const now = Math.floor(Date.now() / 1000);
        let waitSeconds = exp - now - REFRESH_BUFFER_SECONDS;
        if (waitSeconds < 60) waitSeconds = 60;

        const newUrl = applyCookieToUrl(ch.url, pureCookie);

        channelStore.set(ch.channelId, {
            channelId: ch.channelId, name: ch.name,
            originalUrl: ch.url, currentUrl: newUrl,
            cookie: pureCookie, expires: exp,
            lastUpdated: getFormattedDate(),
            status: 'active', error: null,
            isSpecial: false,
        });

        setTimeout(() => apifyLimit(() => refreshChannel(ch.channelId)).catch(() => {}), waitSeconds * 1000);
        stats.succeeded++;
        matched++;
        log(`  ✅ [${ch.channelId}] matched ACL ${aclUrl}`);
    }

    stats.failed += failed;
    log(`   [BATCH ${batchIndex}] done: ${matched} matched, ${failed} failed, ${aclToCookie.size} unused`);
}

// ---------------------------------------------------------------------------
//  SPECIAL PROCESSING (1 URL per run, 256 MB)
// ---------------------------------------------------------------------------
async function processSpecialChannel(channelId, index, total) {
    const ch = channelStore.get(channelId);
    if (!ch) return;

    channelStore.set(channelId, { ...ch, status: 'processing' });

    try {
        log(`▶️  [SPECIAL ${index}/${total}] ${channelId} @ ${SINGLE_MEMORY_MB} MB`);
        const results = await runApify([ch.originalUrl], SINGLE_MEMORY_MB, 1);
        if (!Array.isArray(results) || results.length === 0) {
            throw new Error('Empty Apify result');
        }

        const headers = results[0].headers || {};
        let setCookie = headers['set-cookie'] || headers['Set-Cookie'];
        if (!setCookie) throw new Error('No set-cookie header');

        const cookieStr = Array.isArray(setCookie) ? setCookie[0] : setCookie;
        const pureCookie = cookieStr.split(';')[0];
        const exp = extractExpiry(pureCookie);
        if (exp <= 0) throw new Error('Cookie missing exp=');

        const now = Math.floor(Date.now() / 1000);
        let waitSeconds = exp - now - REFRESH_BUFFER_SECONDS;
        if (waitSeconds < 60) waitSeconds = 60;

        const newUrl = applyCookieToUrl(ch.originalUrl, pureCookie);

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
            isSpecial: true,       // ← marks this channel for single-URL refresh
        });

        stats.succeeded++;
        log(`  ✅ [${channelId}] special OK (expires in ${Math.round((exp - now) / 60)} min)`);

        setTimeout(() => apifyLimit(() => refreshChannel(channelId)).catch(() => {}), waitSeconds * 1000);
    } catch (err) {
        stats.failed++;
        const current = channelStore.get(channelId) || ch;
        channelStore.set(channelId, {
            ...current,
            status: 'error',
            error: err.message,
            isSpecial: true,
        });
        log(`  ❌ [${channelId}] special failed: ${err.message}`, 'error');

        const delay = /402|concurrent-runs-limit/i.test(err.message) ? 5_000 : RETRY_DELAY_MS;
        setTimeout(() => apifyLimit(() => processSpecialChannel(channelId, index, total)).catch(() => {}), delay);
    }
}

// ---------------------------------------------------------------------------
//  PER-CHANNEL REFRESH (mode depends on isSpecial)
// ---------------------------------------------------------------------------
async function refreshChannel(channelId) {
    const ch = channelStore.get(channelId);
    if (!ch) return;

    try {
        // Special channels use single-URL run at 256 MB; normal channels use single-URL run too
        // (single-URL refresh is same for both — the difference is memory and how it was first created)
        const memoryMb = ch.isSpecial ? SINGLE_MEMORY_MB : SINGLE_MEMORY_MB;
        const results = await runApify([ch.originalUrl], memoryMb, 1);
        if (!results || results.length === 0) throw new Error('Empty Apify result');

        const headers = results[0].headers || {};
        let setCookie = headers['set-cookie'] || headers['Set-Cookie'];
        if (!setCookie) throw new Error('No set-cookie header');

        const cookieStr = Array.isArray(setCookie) ? setCookie[0] : setCookie;
        const pureCookie = cookieStr.split(';')[0];
        const exp = extractExpiry(pureCookie);
        if (exp <= 0) throw new Error('Cookie missing exp=');

        // For normal channels: sanity-check ACL match (skip for special since they are known-mismatched)
        if (!ch.isSpecial) {
            const aclUrl = normaliseAcl(extractAclFromUrl(ch.originalUrl));
            const aclCookie = normaliseAcl(extractAclFromCookie(cookieStr));
            if (aclUrl && aclCookie && aclUrl !== aclCookie) {
                throw new Error(`ACL mismatch: cookie=${aclCookie} url=${aclUrl}`);
            }
        }

        const now = Math.floor(Date.now() / 1000);
        let waitSeconds = exp - now - REFRESH_BUFFER_SECONDS;
        if (waitSeconds < 60) waitSeconds = 60;

        const newUrl = applyCookieToUrl(ch.originalUrl, pureCookie);

        channelStore.set(channelId, {
            ...ch,
            currentUrl: newUrl,
            cookie: pureCookie,
            expires: exp,
            lastUpdated: getFormattedDate(),
            status: 'active',
            error: null,
        });

        stats.refreshCount++;
        setTimeout(() => apifyLimit(() => refreshChannel(channelId)).catch(() => {}), waitSeconds * 1000);
        log(`[${channelId}] refreshed${ch.isSpecial ? ' (special)' : ''} — next in ${Math.round(waitSeconds / 60)} min`);
    } catch (err) {
        const current = channelStore.get(channelId);
        if (current) channelStore.set(channelId, { ...current, status: 'error', error: err.message });
        log(`[${channelId}] refresh failed: ${err.message}`, 'error');

        const delay = /402|concurrent-runs-limit/i.test(err.message) ? 5_000 : RETRY_DELAY_MS;
        setTimeout(() => apifyLimit(() => refreshChannel(channelId)).catch(() => {}), delay);
    }
}

// ---------------------------------------------------------------------------
//  MASTER PROCESSOR — splits normal vs special
// ---------------------------------------------------------------------------
async function processAllChannels(channels) {
    // Split at input time
    const normal = [];
    const special = [];
    for (const ch of channels) {
        if (isSpecialChannel(ch.url)) special.push(ch);
        else normal.push(ch);
    }

    stats.startedAt = stats.startedAt || getFormattedDate();
    log(`🚀 [START] ${channels.length} channels → ${normal.length} normal (batched 50) + ${special.length} special (single 256 MB)`);
    log(`   Free plan limit is 5 concurrent runs — using ${MAX_CONCURRENT_RUNS}.`);

    // ---- 1) NORMAL CHANNELS (batched) ----
    if (normal.length > 0) {
        const batches = chunkArray(normal, BATCH_SIZE);
        let batchCounter = 0;
        const normalTasks = batches.map(batch =>
            apifyLimit(async () => {
                const idx = ++batchCounter;
                try {
                    await processBatchOfChannels(batch, idx);
                } catch (err) {
                    log(`❌ [BATCH ${idx}] ${err.message}`, 'error');
                    for (const ch of batch) {
                        const cur = channelStore.get(ch.channelId) || {};
                        channelStore.set(ch.channelId, {
                            ...cur,
                            channelId: ch.channelId,
                            name: ch.name,
                            originalUrl: ch.url,
                            currentUrl: cur.currentUrl || ch.url,
                            status: 'error',
                            error: err.message,
                            isSpecial: false,
                        });
                        stats.failed++;
                    }
                }
            })
        );
        await Promise.all(normalTasks);
    }

    // ---- 2) SPECIAL CHANNELS (single-URL, 256 MB) ----
    if (special.length > 0) {
        let i = 0;
        const specialTasks = special.map(ch =>
            apifyLimit(async () => {
                const idx = ++i;
                await processSpecialChannel(ch.channelId, idx, special.length);
            })
        );
        await Promise.all(specialTasks);
    }

    stats.lastEventAt = getFormattedDate();
    const active = [...channelStore.values()].filter(c => c.status === 'active').length;
    log(`🏁 [DONE] Active: ${active}, Failed: ${stats.failed}`);
}

// ---------------------------------------------------------------------------
//  INPUT PARSER
// ---------------------------------------------------------------------------
function parseChannelInput(text) {
    const trimmed = text.trim();
    if (!trimmed) return [];

    try {
        const parsed = JSON.parse(trimmed);
        if (Array.isArray(parsed)) {
            return parsed
                .map(item => {
                    const id = String(item.Id ?? item.id ?? item.channelId ?? item.channel_id ?? '').trim();
                    const url = String(item.url ?? item.URL ?? '').trim();
                    if (!id || !url.startsWith('http')) return null;
                    return { channelId: id, name: item.name || id, url };
                })
                .filter(Boolean);
        }
    } catch (_) {}

    // Fallback line-based
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
        const id = nonUrl[0] || `ch-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
        channels.push({ channelId: String(id), name: nonUrl[1] || id, url });
    }
    return channels;
}

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
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0f172a;color:#e2e8f0;padding:24px;font-size:14px}
h1{font-size:1.5rem;margin-bottom:16px;color:#38bdf8}
h2{font-size:1.1rem;margin:16px 0 8px;color:#94a3b8}
.card{background:#1e293b;border-radius:12px;padding:20px;margin-bottom:16px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:12px}
.stat{background:#0f172a;border-radius:8px;padding:12px}
.stat-label{font-size:.7rem;color:#64748b;text-transform:uppercase;letter-spacing:.05em}
.stat-value{font-size:1.3rem;font-weight:700;color:#38bdf8;margin-top:4px}
.green{color:#4ade80!important}.red{color:#f87171!important}.yellow{color:#facc15!important}.purple{color:#c084fc!important}
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
  <h2>Paste JSON (any size — normal in batches of 50, special 1-per-request @ 256 MB)</h2>
  <textarea id="input" placeholder='[
  {"Id":"167","url":"https://jiotvpllive.cdn.jio.com/bpk-tv/ZeeTVHD_BTS/WDVLive/index.mpd?__hdnea__=..."},
  {"Id":"146","url":"https://jiotvpllive.cdn.jio.com/bpk-tv/History_HD_BTS/WDVLive/index.mpd?__hdnea__=..."}
]'></textarea>
  <div class="hint">
    <strong>Normal:</strong> URL's own <code>~acl=/bpk-tv/X/…~</code> matches its own <code>/bpk-tv/X/</code> path → batched.<br>
    <strong>Special:</strong> URL path and ACL path differ → processed one-by-one at 256 MB.
  </div>
  <button id="processBtn" onclick="processInput()">▶ Add to Queue</button>
  <button class="secondary" onclick="refreshAll()">🔄 Refresh All</button>
  <button class="danger" onclick="clearAll()">🗑 Clear All</button>
  <a href="/jiostb.json" target="_blank"><button class="secondary">📥 jiostb.json</button></a>
</div>

<div class="card">
  <h2>Status</h2>
  <div class="grid">
    <div class="stat"><div class="stat-label">Total</div><div class="stat-value" id="s-total">0</div></div>
    <div class="stat"><div class="stat-label">Active</div><div class="stat-value green" id="s-active">0</div></div>
    <div class="stat"><div class="stat-label">Normal</div><div class="stat-value" id="s-normal">0</div></div>
    <div class="stat"><div class="stat-label">Special</div><div class="stat-value purple" id="s-special">0</div></div>
    <div class="stat"><div class="stat-label">Processing</div><div class="stat-value yellow" id="s-processing">0</div></div>
    <div class="stat"><div class="stat-label">Failed</div><div class="stat-value red" id="s-failed">0</div></div>
    <div class="stat"><div class="stat-label">Succeeded</div><div class="stat-value green" id="s-succeeded">0</div></div>
    <div class="stat"><div class="stat-label">Workers</div><div class="stat-value" id="s-workers">0/4</div></div>
  </div>
  <div class="progress"><div class="progress-bar" id="progressBar" style="width:0%"></div></div>
</div>

<div class="card">
  <h2>Channels</h2>
  <table>
    <thead><tr><th>ID</th><th>Name</th><th>Type</th><th>Status</th><th>Expires In</th><th>Updated</th><th>Error</th></tr></thead>
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
    document.getElementById('s-normal').textContent = d.stats.normalCount;
    document.getElementById('s-special').textContent = d.stats.specialCount;
    document.getElementById('s-processing').textContent = d.stats.processing;
    document.getElementById('s-failed').textContent = d.stats.failed;
    document.getElementById('s-succeeded').textContent = d.stats.succeeded;
    document.getElementById('s-workers').textContent = d.workerActive + '/' + d.workerMax;

    const total = d.stats.totalAdded || 1;
    const done = d.stats.active + d.stats.failed;
    document.getElementById('progressBar').style.width = Math.min(100, Math.round(done/total*100)) + '%';

    const tbody = document.getElementById('channels');
    tbody.innerHTML = '';
    for(const c of d.channels.slice(0, 300)){
      const tr = document.createElement('tr');
      const expTxt = c.expires ? Math.max(0, Math.round((c.expires - Date.now()/1000)/60)) + ' min' : '—';
      let cls = '';
      if(c.status === 'active') cls = 'green';
      else if(c.status === 'error' || c.status === 'failed') cls = 'red';
      else cls = 'yellow';
      const typeTxt = c.isSpecial ? '<span class="purple">special</span>' : 'normal';
      tr.innerHTML = '<td>'+c.channelId+'</td><td>'+(c.name||'')+'</td><td>'+typeTxt+'</td><td class="'+cls+'">'+c.status+'</td><td>'+expTxt+'</td><td>'+(c.lastUpdated||'')+'</td><td style="color:#f87171">'+(c.error||'')+'</td>';
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
    alert('✅ Added ' + d.added + ' (' + d.normal + ' normal, ' + d.special + ' special)');
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

        let normalAdded = 0, specialAdded = 0;

        for (const ch of channels) {
            const special = isSpecialChannel(ch.url);
            const existing = channelStore.get(ch.channelId);
            if (!existing) {
                channelStore.set(ch.channelId, {
                    channelId: ch.channelId,
                    name: ch.name,
                    originalUrl: ch.url,
                    currentUrl: ch.url,
                    cookie: '',
                    expires: 0,
                    lastUpdated: '',
                    status: 'queued',
                    isSpecial: special,
                });
                stats.totalAdded++;
                if (special) stats.specialCount++;
                else stats.normalCount++;
            } else {
                channelStore.set(ch.channelId, {
                    ...existing,
                    name: ch.name,
                    originalUrl: ch.url,
                    isSpecial: special,
                    status: existing.status === 'active' ? 'active' : 'queued',
                });
            }
            if (special) specialAdded++; else normalAdded++;
        }

        processAllChannels(channels).catch(e => log(`Global crash: ${e.message}`, 'error'));

        res.json({ success: true, added: channels.length, normal: normalAdded, special: specialAdded });
    } catch (err) {
        log(`Add failed: ${err.message}`, 'error');
        res.status(500).json({ error: err.message });
    }
});

app.get('/ayush8481/status', (req, res) => {
    const all = [...channelStore.values()];
    const active = all.filter(c => c.status === 'active').length;
    const processing = all.filter(c => c.status === 'processing').length;

    res.json({
        stats: { ...stats, active, processing },
        workerActive: apifyLimit.activeCount,
        workerMax: MAX_CONCURRENT_RUNS,
        channels: all,
        log: eventLog.slice(-100),
    });
});

app.post('/ayush8481/refresh-all', (req, res) => {
    const active = [...channelStore.values()].filter(c => c.status === 'active' || c.status === 'error');
    if (active.length === 0) return res.json({ message: 'No active channels.' });
    const channels = active.map(c => ({ channelId: c.channelId, name: c.name, url: c.originalUrl }));
    processAllChannels(channels).catch(e => log(`Global crash: ${e.message}`, 'error'));
    res.json({ message: `Refreshing ${active.length} channels.` });
});

app.post('/ayush8481/clear', (req, res) => {
    channelStore.clear();
    stats.totalAdded = 0;
    stats.normalCount = 0;
    stats.specialCount = 0;
    stats.queued = 0;
    stats.succeeded = 0;
    stats.failed = 0;
    stats.refreshCount = 0;
    log('Store cleared.');
    res.json({ ok: true });
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

app.get('/debug/state', (req, res) => {
    res.json({
        stats,
        workerActive: apifyLimit.activeCount,
        workerPending: apifyLimit.pendingCount,
        workerMax: MAX_CONCURRENT_RUNS,
        batchMemoryMB: BATCH_MEMORY_MB,
        singleMemoryMB: SINGLE_MEMORY_MB,
        nodeVersion: process.version,
        hasApifyToken: !!APIFY_TOKEN,
        channelCount: channelStore.size,
        log: eventLog.slice(-200),
    });
});

// ---------------------------------------------------------------------------
//  START
// ---------------------------------------------------------------------------
app.listen(PORT, () => {
    log(`Server started on port ${PORT}`);
    log(`Normal: batch ${BATCH_SIZE} @ ${BATCH_MEMORY_MB} MB | Special: 1 URL @ ${SINGLE_MEMORY_MB} MB`);
    log(`Concurrency: ${MAX_CONCURRENT_RUNS} (Free plan limit is 5)`);
    console.log(`   Admin:   /ayush8481/admin`);
    console.log(`   Public:  /jiostb.json`);
});
