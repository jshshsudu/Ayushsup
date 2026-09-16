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
const BATCH_SIZE = 50;                        // URLs per Apify ru
const BATCH_MEMORY_MB = 2048;                 // 2 GB per run
const MAX_CONCURRENT_RUNS = 4;                // 4 × 2 GB = 8 GB (within Free plan)

const GROUP_SIZE = 200;                       // channels per refresh group
const GROUP_STAGGER_MS = 5 * 60 * 1000;       // 5 min between initial upload groups
const GROUP_REFRESH_INTERVAL_S = 5 * 3600;    // refresh each group every 5 hours
const GROUP_RETRY_DELAY_MS = 30 * 60 * 1000;  // 30 min retry if a group refresh fails

const apifyLimit = pLimit(MAX_CONCURRENT_RUNS);

// ---------------------------------------------------------------------------
//  STORAGE
// ---------------------------------------------------------------------------
const channelStore = new Map();   // channelId -> channel object (includes groupId)

// groupId -> { groupId, channelIds:[], lastRefreshAt, nextRefreshAt, timer, processing }
const groups = new Map();

const stats = {
    totalAdded: 0,
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

function isSpecialChannel(url) {
    try {
        const u = new URL(url);
        const m = u.pathname.match(/^\/bpk-tv\/([^\/]+)/i);
        if (!m) return true;
        const urlSegment = m[1].toLowerCase();
        const acl = extractAclFromUrl(url);
        if (!acl) return true;
        return !acl.toLowerCase().includes(`/bpk-tv/${urlSegment}/`);
    } catch { return true; }
}

// ---------------------------------------------------------------------------
//  APIFY
// ---------------------------------------------------------------------------
async function startApifyRun(urls) {
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
    const apiUrl = `${APIFY_BASE}/acts/${APIFY_ACTOR}/runs?token=${APIFY_TOKEN}&memory=${BATCH_MEMORY_MB}`;
    const res = await fetch(apiUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(apifyInput),
    });
    if (!res.ok) throw new Error(`Apify HTTP ${res.status}: ${(await res.text()).slice(0, 300)}`);
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

async function deleteDataset(datasetId) {
    if (!datasetId) return;
    try { await fetch(`${APIFY_BASE}/datasets/${datasetId}?token=${APIFY_TOKEN}`, { method: 'DELETE' }); } catch (_) {}
}

async function runApify(urls) {
    const run = await startApifyRun(urls);
    const finished = await waitForRun(run.id);
    if (finished.status !== 'SUCCEEDED') {
        await deleteDataset(finished.defaultDatasetId);
        throw new Error(`Apify run ${run.id} ended with status: ${finished.status}`);
    }
    const items = await fetchDatasetItems(finished.defaultDatasetId);
    deleteDataset(finished.defaultDatasetId).catch(() => {});
    return items;
}

// ---------------------------------------------------------------------------
//  BATCH PROCESSOR
// ---------------------------------------------------------------------------
async function processBatchOfChannels(channels, label) {
    const urls = channels.map(c => c.url);
    log(`▶️  [${label}] starting ${urls.length} urls`);

    for (const ch of channels) {
        const existing = channelStore.get(ch.channelId) || {};
        channelStore.set(ch.channelId, { ...existing, status: 'processing' });
    }

    const results = await runApify(urls);
    if (!Array.isArray(results)) throw new Error('Apify returned non-array');
    log(`◀️  [${label}] got ${results.length} results`);

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
        if (!existing || exp > existing.exp) aclToCookie.set(acl, { cookieStr, exp });
    }
    log(`   [${label}] ACL map: ${aclToCookie.size} unique`);

    let matched = 0, failed = 0;

    for (const ch of channels) {
        const aclUrl = normaliseAcl(extractAclFromUrl(ch.url));
        let hit = null;
        if (aclUrl) {
            hit = aclToCookie.get(aclUrl);
            if (!hit) {
                const prefix = aclUrl.split('/').slice(0, 4).join('/');
                for (const [k, v] of aclToCookie.entries()) {
                    if (k.startsWith(prefix)) { hit = v; break; }
                }
            }
        }
        if (!hit && isSpecialChannel(ch.url) && aclToCookie.size === 1) {
            hit = aclToCookie.values().next().value;
        }

        if (!hit) {
            failed++;
            channelStore.set(ch.channelId, {
                ...(channelStore.get(ch.channelId) || {}),
                channelId: ch.channelId, name: ch.name,
                originalUrl: ch.url, currentUrl: ch.url,
                cookie: '', expires: 0,
                lastUpdated: getFormattedDate(),
                status: 'no_cookie', error: 'No cookie for ACL',
            });
            continue;
        }

        aclToCookie.delete(aclUrl);

        const pureCookie = hit.cookieStr.split(';')[0];
        const exp = extractExpiry(pureCookie);
        if (exp <= 0) { failed++; continue; }

        const newUrl = applyCookieToUrl(ch.url, pureCookie);
        channelStore.set(ch.channelId, {
            ...(channelStore.get(ch.channelId) || {}),
            channelId: ch.channelId, name: ch.name,
            originalUrl: ch.url, currentUrl: newUrl,
            cookie: pureCookie, expires: exp,
            lastUpdated: getFormattedDate(),
            status: 'active', error: null,
        });
        stats.succeeded++;
        matched++;
    }

    stats.failed += failed;
    log(`   [${label}] matched ${matched}, failed ${failed}, unused ${aclToCookie.size}`);
}

// ---------------------------------------------------------------------------
//  GROUP MANAGEMENT
// ---------------------------------------------------------------------------
function createGroup(channelIds) {
    const groupId = `grp-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    groups.set(groupId, {
        groupId,
        channelIds: [...channelIds],
        lastRefreshAt: null,
        nextRefreshAt: null,
        timer: null,
        processing: false,
    });
    return groupId;
}

function scheduleGroupRefresh(groupId, delayMs) {
    const g = groups.get(groupId);
    if (!g) return;
    if (g.timer) clearTimeout(g.timer);

    g.nextRefreshAt = Math.floor(Date.now() / 1000) + Math.round(delayMs / 1000);
    g.timer = setTimeout(() => {
        processGroup(groupId).catch(e => log(`Group ${groupId} crashed: ${e.message}`, 'error'));
    }, delayMs);

    const when = new Date(g.nextRefreshAt * 1000).toLocaleTimeString('en-IN', { timeZone: 'Asia/Kolkata' });
    log(`⏰ [${groupId}] next refresh at ${when} IST`);
}

async function processGroup(groupId) {
    const g = groups.get(groupId);
    if (!g) return;
    if (g.processing) {
        log(`⚠️ [${groupId}] already processing, skipping`, 'warn');
        return;
    }
    g.processing = true;

    try {
        const activeChannels = g.channelIds
            .map(id => channelStore.get(id))
            .filter(c => c && (c.status === 'active' || c.status === 'error'));

        if (activeChannels.length === 0) {
            log(`⚠️ [${groupId}] no active channels, rescheduling`);
            g.processing = false;
            scheduleGroupRefresh(groupId, GROUP_REFRESH_INTERVAL_S * 1000);
            return;
        }

        log(`🔄 [${groupId}] refreshing ${activeChannels.length} channels`);

        const inputs = activeChannels.map(c => ({
            channelId: c.channelId,
            name: c.name,
            url: c.originalUrl,
        }));

        const batches = chunkArray(inputs, BATCH_SIZE);
        let idx = 0;
        const tasks = batches.map(batch =>
            apifyLimit(async () => {
                const label = `${groupId}-B${++idx}`;
                try {
                    await processBatchOfChannels(batch, label);
                    stats.refreshCount += batch.length;
                } catch (err) {
                    log(`❌ [${label}] ${err.message}`, 'error');
                    for (const ch of batch) {
                        const cur = channelStore.get(ch.channelId) || {};
                        channelStore.set(ch.channelId, {
                            ...cur,
                            status: 'error',
                            error: err.message,
                        });
                    }
                }
            })
        );
        await Promise.all(tasks);

        g.lastRefreshAt = Math.floor(Date.now() / 1000);
        g.processing = false;

        // Schedule next refresh 5h from now
        scheduleGroupRefresh(groupId, GROUP_REFRESH_INTERVAL_S * 1000);
    } catch (err) {
        log(`❌ [${groupId}] group refresh failed: ${err.message}`, 'error');
        g.processing = false;
        // Retry sooner
        scheduleGroupRefresh(groupId, GROUP_RETRY_DELAY_MS);
    }
}

// ---------------------------------------------------------------------------
//  INITIAL PROCESSING — grouped with stagger
// ---------------------------------------------------------------------------
async function processAllChannels(channels) {
    const channelGroups = chunkArray(channels, GROUP_SIZE);
    log(`🚀 [START] ${channels.length} channels in ${channelGroups.length} groups of ${GROUP_SIZE} (stagger ${GROUP_STAGGER_MS / 60000} min)`);
    stats.startedAt = stats.startedAt || getFormattedDate();

    for (let i = 0; i < channelGroups.length; i++) {
        const groupChannels = channelGroups[i];

        // Create the group entry
        const groupId = createGroup(groupChannels.map(c => c.channelId));

        // Tag each channel with groupId
        for (const ch of groupChannels) {
            const cur = channelStore.get(ch.channelId) || {};
            channelStore.set(ch.channelId, { ...cur, groupId });
        }

        log(`📦 [GROUP ${i + 1}/${channelGroups.length}] ${groupChannels.length} channels → ${groupId}`);

        // Process the group in 50-batches (all through the shared apifyLimit)
        const batches = chunkArray(groupChannels, BATCH_SIZE);
        let idx = 0;
        const tasks = batches.map(batch =>
            apifyLimit(async () => {
                const label = `${groupId}-B${++idx}`;
                try {
                    await processBatchOfChannels(batch, label);
                } catch (err) {
                    log(`❌ [${label}] ${err.message}`, 'error');
                    for (const ch of batch) {
                        const cur = channelStore.get(ch.channelId) || {};
                        channelStore.set(ch.channelId, {
                            ...cur, channelId: ch.channelId, name: ch.name,
                            originalUrl: ch.url, currentUrl: cur.currentUrl || ch.url,
                            status: 'error', error: err.message,
                        });
                        stats.failed++;
                    }
                }
            })
        );
        await Promise.all(tasks);

        // Record completion time and schedule first refresh
        const g = groups.get(groupId);
        g.lastRefreshAt = Math.floor(Date.now() / 1000);
        scheduleGroupRefresh(groupId, GROUP_REFRESH_INTERVAL_S * 1000);

        // Stagger before the next group
        if (i < channelGroups.length - 1) {
            log(`⏳ Waiting ${GROUP_STAGGER_MS / 60000} min before group ${i + 2}…`);
            await sleep(GROUP_STAGGER_MS);
        }
    }

    stats.lastEventAt = getFormattedDate();
    const active = [...channelStore.values()].filter(c => c.status === 'active').length;
    log(`🏁 [DONE] Active: ${active}, Failed: ${stats.failed}, Groups: ${groups.size}`);
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
            return parsed.map(item => {
                const id = String(item.Id ?? item.id ?? item.channelId ?? item.channel_id ?? '').trim();
                const url = String(item.url ?? item.URL ?? '').trim();
                if (!id || !url.startsWith('http')) return null;
                return { channelId: id, name: item.name || id, url };
            }).filter(Boolean);
        }
    } catch (_) {}
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
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>JioTV Admin</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,sans-serif;background:#0f172a;color:#e2e8f0;padding:24px;font-size:14px}
h1{font-size:1.5rem;margin-bottom:16px;color:#38bdf8}
h2{font-size:1.1rem;margin:16px 0 8px;color:#94a3b8}
.card{background:#1e293b;border-radius:12px;padding:20px;margin-bottom:16px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:12px}
.stat{background:#0f172a;border-radius:8px;padding:12px}
.stat-label{font-size:.7rem;color:#64748b;text-transform:uppercase}
.stat-value{font-size:1.3rem;font-weight:700;color:#38bdf8;margin-top:4px}
.green{color:#4ade80!important}.red{color:#f87171!important}.yellow{color:#facc15!important}
textarea{width:100%;min-height:200px;background:#0f172a;color:#e2e8f0;border:1px solid #334155;border-radius:8px;padding:12px;font-family:monospace;font-size:12px}
button{background:#38bdf8;color:#0f172a;border:none;padding:10px 20px;border-radius:8px;font-weight:600;cursor:pointer;font-size:14px;margin-right:8px;margin-top:8px}
button:disabled{opacity:.5}
button.secondary{background:#334155;color:#e2e8f0}
button.danger{background:#7f1d1d;color:#fecaca}
.log{background:#0f172a;border-radius:8px;padding:12px;font-family:monospace;font-size:12px;max-height:400px;overflow-y:auto;white-space:pre-wrap;line-height:1.5}
.log .err{color:#f87171}.log .warn{color:#facc15}
a{text-decoration:none}
table{width:100%;border-collapse:collapse;font-size:12px;margin-top:8px}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid #334155}
th{color:#64748b;font-weight:600;text-transform:uppercase;font-size:.7rem}
</style></head><body>
<h1>JioTV Cookie Manager</h1>
<div class="card"><h2>Paste JSON</h2>
<textarea id="input" placeholder='[{"Id":"167","url":"https://..."}]'></textarea>
<button id="processBtn" onclick="processInput()">▶ Add to Queue</button>
<button class="secondary" onclick="refreshAll()">🔄 Refresh All Groups</button>
<button class="danger" onclick="clearAll()">🗑 Clear All</button>
<a href="/jiostb.json" target="_blank"><button class="secondary">📥 jiostb.json</button></a>
</div>
<div class="card"><h2>Status</h2>
<div class="grid">
<div class="stat"><div class="stat-label">Total</div><div class="stat-value" id="s-total">0</div></div>
<div class="stat"><div class="stat-label">Active</div><div class="stat-value green" id="s-active">0</div></div>
<div class="stat"><div class="stat-label">Failed</div><div class="stat-value red" id="s-failed">0</div></div>
<div class="stat"><div class="stat-label">Groups</div><div class="stat-value" id="s-groups">0</div></div>
<div class="stat"><div class="stat-label">Refreshes</div><div class="stat-value" id="s-refreshes">0</div></div>
<div class="stat"><div class="stat-label">Workers</div><div class="stat-value" id="s-workers">0/4</div></div>
</div>
</div>
<div class="card"><h2>Groups</h2>
<table><thead><tr><th>Group</th><th>Channels</th><th>Last Refresh</th><th>Next Refresh</th><th>Status</th></tr></thead>
<tbody id="groups"></tbody></table>
</div>
<div class="card"><h2>Channels</h2>
<table><thead><tr><th>ID</th><th>Name</th><th>Group</th><th>Status</th><th>Expires In</th><th>Updated</th></tr></thead>
<tbody id="channels"></tbody></table>
</div>
<div class="card"><h2>Event Log</h2><div class="log" id="log">Waiting…</div></div>
<script>
async function loadStatus(){
try{
const r=await fetch('/ayush8481/status');
const d=await r.json();
document.getElementById('s-total').textContent=d.stats.totalAdded;
document.getElementById('s-active').textContent=d.stats.active;
document.getElementById('s-failed').textContent=d.stats.failed;
document.getElementById('s-groups').textContent=d.stats.groupCount;
document.getElementById('s-refreshes').textContent=d.stats.refreshCount;
document.getElementById('s-workers').textContent=d.workerActive+'/'+d.workerMax;

const gt=document.getElementById('groups');
gt.innerHTML='';
for(const g of d.groups){
const tr=document.createElement('tr');
const next=g.nextRefreshAt?new Date(g.nextRefreshAt*1000).toLocaleTimeString('en-IN',{timeZone:'Asia/Kolkata'}):'—';
tr.innerHTML='<td>'+g.groupId+'</td><td>'+g.channelIds.length+'</td><td>'+(g.lastRefreshAt?new Date(g.lastRefreshAt*1000).toLocaleTimeString('en-IN',{timeZone:'Asia/Kolkata'}):'—')+'</td><td>'+next+'</td><td>'+(g.processing?'<span class="yellow">processing</span>':'idle')+'</td>';
gt.appendChild(tr);
}

const tbody=document.getElementById('channels');
tbody.innerHTML='';
for(const c of d.channels.slice(0,300)){
const tr=document.createElement('tr');
const expTxt=c.expires?Math.max(0,Math.round((c.expires-Date.now()/1000)/60))+' min':'—';
let cls=c.status==='active'?'green':(c.status==='error'||c.status==='failed')?'red':'yellow';
tr.innerHTML='<td>'+c.channelId+'</td><td>'+(c.name||'')+'</td><td style="font-size:10px">'+(c.groupId||'')+'</td><td class="'+cls+'">'+c.status+'</td><td>'+expTxt+'</td><td>'+(c.lastUpdated||'')+'</td>';
tbody.appendChild(tr);
}
const logEl=document.getElementById('log');
logEl.innerHTML=d.log.slice(-80).map(e=>'<div class="'+(e.level==='error'?'err':e.level==='warn'?'warn':'')+'">'+e.time+'  '+e.msg+'</div>').join('');
logEl.scrollTop=logEl.scrollHeight;
}catch(e){}
}
async function processInput(){
const btn=document.getElementById('processBtn');
const text=document.getElementById('input').value.trim();
if(!text)return alert('Paste channels first.');
btn.disabled=true;btn.textContent='Adding…';
try{
const r=await fetch('/ayush8481/add',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({input:text})});
const d=await r.json();
if(d.error)throw new Error(d.error);
document.getElementById('input').value='';
alert('✅ Added '+d.added);
}catch(e){alert('❌ '+e.message);}
btn.disabled=false;btn.textContent='▶ Add to Queue';
loadStatus();
}
async function refreshAll(){
if(!confirm('Force refresh ALL groups now?'))return;
const r=await fetch('/ayush8481/refresh-all',{method:'POST'});
const d=await r.json();
alert(d.message||'Done');
loadStatus();
}
async function clearAll(){
if(!confirm('Clear all?'))return;
await fetch('/ayush8481/clear',{method:'POST'});
loadStatus();
}
loadStatus();setInterval(loadStatus,5000);
</script></body></html>`);
});

app.post('/ayush8481/add', express.json({ limit: '10mb' }), (req, res) => {
    try {
        const text = req.body.input || '';
        const channels = parseChannelInput(text);
        if (channels.length === 0) return res.status(400).json({ error: 'No valid channels.' });

        for (const ch of channels) {
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
                    isSpecial: isSpecialChannel(ch.url),
                });
                stats.totalAdded++;
            } else {
                channelStore.set(ch.channelId, {
                    ...existing,
                    name: ch.name,
                    originalUrl: ch.url,
                    isSpecial: isSpecialChannel(ch.url),
                    status: existing.status === 'active' ? 'active' : 'queued',
                });
            }
        }

        // Run the staggered group upload in background
        processAllChannels(channels).catch(e => log(`Global crash: ${e.message}`, 'error'));

        res.json({ success: true, added: channels.length });
    } catch (err) {
        log(`Add failed: ${err.message}`, 'error');
        res.status(500).json({ error: err.message });
    }
});

app.get('/ayush8481/status', (req, res) => {
    const all = [...channelStore.values()];
    const active = all.filter(c => c.status === 'active').length;
    const groupArr = [...groups.values()].map(g => ({
        groupId: g.groupId,
        channelIds: g.channelIds,
        lastRefreshAt: g.lastRefreshAt,
        nextRefreshAt: g.nextRefreshAt,
        processing: g.processing,
    }));
    res.json({
        stats: { ...stats, active, groupCount: groups.size },
        workerActive: apifyLimit.activeCount,
        workerMax: MAX_CONCURRENT_RUNS,
        groups: groupArr,
        channels: all,
        log: eventLog.slice(-100),
    });
});

app.post('/ayush8481/refresh-all', async (req, res) => {
    if (groups.size === 0) return res.json({ message: 'No groups yet.' });
    res.json({ message: `Forcing refresh on ${groups.size} groups` });
    for (const g of groups.values()) {
        if (!g.processing) {
            processGroup(g.groupId).catch(e => log(`Group ${g.groupId} failed: ${e.message}`, 'error'));
        }
    }
});

app.post('/ayush8481/clear', (req, res) => {
    for (const g of groups.values()) {
        if (g.timer) clearTimeout(g.timer);
    }
    groups.clear();
    channelStore.clear();
    stats.totalAdded = 0;
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

app.get('/debug/groups', (req, res) => {
    const now = Math.floor(Date.now() / 1000);
    res.json([...groups.values()].map(g => ({
        groupId: g.groupId,
        channelCount: g.channelIds.length,
        lastRefreshAt: g.lastRefreshAt,
        nextRefreshAt: g.nextRefreshAt,
        secondsUntilRefresh: g.nextRefreshAt ? g.nextRefreshAt - now : null,
        processing: g.processing,
    })));
});

app.get('/debug/state', (req, res) => {
    res.json({
        stats: { ...stats, groupCount: groups.size },
        workerActive: apifyLimit.activeCount,
        workerMax: MAX_CONCURRENT_RUNS,
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
    log(`Batch: ${BATCH_SIZE} @ ${BATCH_MEMORY_MB} MB | Workers: ${MAX_CONCURRENT_RUNS}`);
    log(`Group size: ${GROUP_SIZE} | Stagger: ${GROUP_STAGGER_MS / 60000} min | Refresh interval: ${GROUP_REFRESH_INTERVAL_S / 3600} h`);
});
