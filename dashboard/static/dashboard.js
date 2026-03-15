/* ==========================================================
   BTP Analytics Dashboard — JavaScript (v3)
   Polls every 2 s, updates all panels.
   ========================================================== */

const POLL_MS = 2000;

Chart.defaults.color         = '#555';
Chart.defaults.borderColor   = '#222';
Chart.defaults.font.family   = "'JetBrains Mono', monospace";
Chart.defaults.font.size     = 11;

const PALETTE = ['#3b82f6','#8b5cf6','#22c55e','#f59e0b','#14b8a6','#f43f5e'];

let latChart = null, memChart = null;
let routerQuery = 'frequency', qType = 'frequency', selTpl = 'medium';
let lastCmsTotal = 0, lastPollTime = 0;

// ── Chart setup ───────────────────────────────────────────
function initCharts() {
  latChart = new Chart(document.getElementById('latencyChart'), {
    type: 'line',
    data: { labels: [], datasets: [] },
    options: {
      responsive: true, maintainAspectRatio: false, animation: { duration: 250 },
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend:  { position: 'top', labels: { boxWidth: 8, padding: 12, usePointStyle: true } },
        tooltip: { backgroundColor: '#111', borderColor: '#2a2a2a', borderWidth: 1 },
      },
      scales: {
        x: { grid: { color: '#1a1a1a' }, ticks: { maxTicksLimit: 10 } },
        y: { grid: { color: '#1a1a1a' }, title: { display: true, text: 'ms', color: '#555' } },
      },
    },
  });

  memChart = new Chart(document.getElementById('memoryChart'), {
    type: 'bar',
    data: { 
      labels: [], 
      datasets: [
        { label: 'CPU %', data: [], backgroundColor: [], borderColor: [], borderWidth: 2, borderRadius: 4 },
        { label: 'Memory %', data: [], backgroundColor: [], borderColor: [], borderWidth: 2, borderRadius: 4 }
      ] 
    },
    options: {
      responsive: true, maintainAspectRatio: false, animation: { duration: 200 },
      plugins: {
        legend:  { position: 'top', labels: { boxWidth: 8, padding: 12, usePointStyle: true } },
        tooltip: { backgroundColor: '#111', borderColor: '#2a2a2a', borderWidth: 1 },
      },
      scales: {
        x: { grid: { display: false } },
        y: { min: 0, max: 100, grid: { color: '#1a1a1a' }, title: { display: true, text: '%', color: '#555' } },
      },
    },
  });
}

// ── Formatters ────────────────────────────────────────────
const fmt = (n, d = 1) => {
  if (n == null || isNaN(n)) return '—';
  if (n >= 1e9) return (n/1e9).toFixed(d) + 'B';
  if (n >= 1e6) return (n/1e6).toFixed(d) + 'M';
  if (n >= 1e3) return (n/1e3).toFixed(d) + 'K';
  return Number(n).toFixed(d);
};
const fmtTs  = ts => new Date(ts * 1000).toLocaleTimeString([], { hour:'2-digit', minute:'2-digit', second:'2-digit' });
const tplCls = t  => ({ low: 'tpl-low', medium: 'tpl-medium', high: 'tpl-high' }[t] || 'tpl-medium');
const logCls = m  => {
  if (!m) return '';
  if (m.includes('upgrade'))   return 'upgrade';
  if (m.includes('downgrade')) return 'downgrade';
  if (m.includes('memory'))    return 'memory';
  if (/manual/i.test(m))       return 'manual';
  return '';
};

// ── Chart updates ─────────────────────────────────────────
function updateLatChart(histories) {
  const ids = Object.keys(histories);
  if (!ids.length) return;
  const maxLen = Math.max(...ids.map(id => (histories[id] || []).length), 0);
  latChart.data.labels = Array.from({ length: maxLen }, (_, i) => i + 1);
  ids.forEach((id, i) => {
    let ds = latChart.data.datasets.find(d => d.label === id);
    if (!ds) {
      ds = { label: id, data: [], borderColor: PALETTE[i % PALETTE.length],
             backgroundColor: PALETTE[i % PALETTE.length] + '18',
             tension: 0.4, fill: true, pointRadius: 2, borderWidth: 2 };
      latChart.data.datasets.push(ds);
    }
    ds.data = (histories[id] || []).map(Number);
  });
  latChart.data.datasets = latChart.data.datasets.filter(d => ids.includes(d.label));
  latChart.update('none');
}
function updateMemChart(cluster) {
  const ids = Object.keys(cluster);
  const mems = ids.map(id => cluster[id].memory_percent || 0);
  const cpus = ids.map(id => cluster[id].cpu_percent || 0);
  
  memChart.data.labels = ids;
  // CPU Dataset
  memChart.data.datasets[0].data = cpus;
  memChart.data.datasets[0].backgroundColor = PALETTE.slice(0, ids.length).map(c => c + '33');
  memChart.data.datasets[0].borderColor      = PALETTE.slice(0, ids.length);
  // Memory Dataset
  memChart.data.datasets[1].data = mems;
  memChart.data.datasets[1].backgroundColor = PALETTE.slice(0, ids.length).map(c => '#55555533');
  memChart.data.datasets[1].borderColor      = PALETTE.slice(0, ids.length).map(c => '#555');
  
  memChart.update('none');
}

// ── KPIs ──────────────────────────────────────────────────
function updateKPIs(cluster) {
  const ws = Object.values(cluster);
  if (!ws.length) return;
  const best    = ws.reduce((a, b) => (a.p99_latency_ms||Infinity) < (b.p99_latency_ms||Infinity) ? a : b);
  const avgMem  = (ws.reduce((s, w) => s + (w.memory_percent||0), 0) / ws.length).toFixed(1);
  const avgCpu  = (ws.reduce((s, w) => s + (w.cpu_percent||0), 0) / ws.length).toFixed(1);

  // Throughput Calculation
  let currentTotal = best.cms_total_count || 0;
  let now = Date.now();
  if (lastPollTime > 0 && currentTotal >= lastCmsTotal) {
    let tps = (currentTotal - lastCmsTotal) / ((now - lastPollTime) / 1000);
    setText('kpiThroughput', fmt(tps) + ' /s');
  }
  lastCmsTotal = currentTotal;
  lastPollTime = now;

  // KPI strip values
  setText('kpiCardinality',    fmt(best.hll_cardinality));
  setText('kpiCardinalityErr', `Relative error: ±${((best.hll_relative_error||0)*100).toFixed(2)}%`);
  setText('kpiCmsTotal',       fmt(best.cms_total_count));
  setText('kpiCmsError',       `Error bound: ±${fmt(best.cms_error_estimate)}`);
  setText('kpiP99',            fmt(best.p99_latency_ms) + ' ms');
  setText('kpiMem',            avgMem + '%');
  setText('kpiCpu',            avgCpu + '%');

  // Sidebar
  setText('sysWorkers',    ws.length);
  setText('sysTemplate',   (best.template || '—').toUpperCase());
  setText('sysCardinality', fmt(best.hll_cardinality));
  setText('sysTotal',      fmt(best.cms_total_count));
  setText('workerCount',   `${ws.length} worker${ws.length !== 1 ? 's' : ''}`);
  setText('lastUpdate',    new Date().toLocaleTimeString());
}

// ── Live Sketch Answers ───────────────────────────────────
async function updateLiveAnswers(cluster) {
  const ws = Object.values(cluster);
  if (!ws.length) return;
  const best = ws[0];
  
  // 1. HLL
  setText('ansHllVal', fmt(best.hll_cardinality));
  setText('ansHllErr', `Relative error: ±${((best.hll_relative_error||0)*100).toFixed(2)}%`);
  
  // 2. Misra-Gries
  const hh = best.mg_heavy_hitters || [];
  const total = best.mg_window_total || 1;
  let topItem = null;
  
  if (hh.length > 0) {
    topItem = hh[0][0];
    const mgCount = hh[0][1];
    const pct = ((mgCount / total) * 100).toFixed(1);
    setText('ansMgVal', topItem);
    setText('ansMgShare', `Share of window: ${pct}%`);
  } else {
    setText('ansMgVal', '—');
    setText('ansMgShare', 'Share of window: —');
  }

  // 3. Count-Min Sketch (Live Query)
  if (topItem) {
    try {
      const resp = await fetch(`/query/frequency?key=${encodeURIComponent(topItem)}`);
      if (resp.ok) {
        const data = await resp.json();
        // The API returns cms_error_bound and cms_total_count.
        // The actual frequency estimate isn't in the root of the cluster metrics, it requires this query!
        // To approximate the live frequency from the API, we use the fact that MG count is a lower bound, 
        // but let's just show the error bound and total for the top item as the 'answer' for now, 
        // since the pure frequency API doesn't return the exact frequency value in this version, 
        // it returns the score and error bounds.
        // Instead, we will use the MG count as the estimated frequency for the CMS card display,
        // and show the CMS specific error bounds.
        setText('ansCmsVal', fmt(hh[0][1]));
        setText('ansCmsErr', `Error bound: ±${fmt(data.cms_error_bound)}`);
      }
    } catch (_) {}
  } else {
    setText('ansCmsVal', '—');
    setText('ansCmsErr', 'Error bound: —');
  }
}

// ── Heavy hitters ─────────────────────────────────────────
function updateHH(cluster) {
  const ws = Object.values(cluster);
  if (!ws.length) return;
  const best  = ws[0], hh = best.mg_heavy_hitters || [], total = best.mg_window_total || 1;
  setText('mgWindow', `N = ${fmt(total, 0)}`);
  const tbody = document.getElementById('hhTableBody');
  if (!hh.length) { tbody.innerHTML = `<tr><td colspan="4" class="empty">No heavy hitters yet — stream events first</td></tr>`; return; }
  tbody.innerHTML = hh.slice(0, 12).map(([item, count], i) => {
    const pct = Math.min((count / total) * 100, 100);
    return `<tr><td>${i+1}</td><td>${item}</td><td>${fmt(count,0)}</td>
      <td><div class="bar-wrap"><div class="bar-fill" style="width:${pct.toFixed(1)}%"></div></div></td></tr>`;
  }).join('');
}

// ── Router scores ─────────────────────────────────────────
async function updateRouter() {
  try {
    const data  = await (await fetch(`/metrics/router?q=${routerQuery}`)).json();
    const nodes = data.ranked_nodes || [];
    const el = document.getElementById('routerScores');
    if (!nodes.length) { el.innerHTML = `<p class="empty">No workers alive yet</p>`; return; }
    el.innerHTML = nodes.map((n, i) => `
      <div class="node-row ${i === 0 ? 'best' : ''}">
        <span class="node-id">${i === 0 ? '★ ' : ''}${n.worker_id}</span>
        <span class="tpl-badge ${tplCls(n.metrics?.template)}">${n.metrics?.template || '?'}</span>
        <span class="node-score">${n.score.toFixed(4)}</span>
      </div>`).join('');
  } catch (_) {}
}

// ── Controller log ────────────────────────────────────────
async function updateCtrlLog() {
  try {
    const data = await (await fetch('/metrics/controller-log?n=25')).json();
    const dec  = (data.decisions || []).slice().reverse();
    setText('ctrlLogCount', `${dec.length} events`);
    const el = document.getElementById('ctrlLog');
    if (!dec.length) { el.innerHTML = `<p class="empty">No decisions yet — runs every 5 s</p>`; return; }
    el.innerHTML = dec.map(d => `
      <div class="log-entry ${logCls(d.message)}">
        <span class="log-ts">${fmtTs(d.ts)}</span>
        <span class="log-wid">${d.worker_id}</span>${d.message}
      </div>`).join('');
  } catch (_) {}
}

// ── Status ────────────────────────────────────────────────
function setStatus(live) {
  const dot = document.getElementById('statusDot'), txt = document.getElementById('statusText');
  dot.className = 'status-dot ' + (live ? 'live' : 'error');
  txt.textContent = live ? 'Connected' : 'Disconnected';
}

// ── Tabs ──────────────────────────────────────────────────
function setRouterQuery(q, btn) {
  routerQuery = q;
  document.querySelectorAll('#routerTabs .tab').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  updateRouter();
}
function setQType(t, btn) {
  qType = t;
  document.querySelectorAll('#queryTabs .tab').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  const row = document.getElementById('queryInputRow');
  if (t === 'frequency') { row.style.display = 'flex'; } else { row.style.display = 'none'; }
}
function pickTpl(t, btn) {
  selTpl = t;
  document.querySelectorAll('.seg').forEach(b => b.classList.remove('seg-active'));
  btn.classList.add('seg-active');
}

// ── Query explorer ────────────────────────────────────────
async function runQuery() {
  const out = document.getElementById('queryResult');
  out.textContent = '// loading…';
  try {
    let url;
    if (qType === 'frequency') {
      const key = document.getElementById('queryKey').value.trim();
      if (!key) { out.textContent = '// Enter an item key'; return; }
      url = `/query/frequency?key=${encodeURIComponent(key)}`;
    } else if (qType === 'cardinality') {
      url = '/query/cardinality';
    } else {
      url = '/query/heavy-hitters?n=15';
    }
    const data = await (await fetch(url)).json();
    out.textContent = JSON.stringify(data, null, 2);
  } catch (e) { out.textContent = `// error: ${e.message}`; }
}

// ── Manual override ───────────────────────────────────────
async function applyOverride() {
  const wid = document.getElementById('ctrlWorkerId').value.trim();
  const st  = document.getElementById('ctrlStatus');
  if (!wid) { st.textContent = '⚠ Enter a worker ID'; st.style.color = 'var(--red)'; return; }
  try {
    const res  = await fetch('/control/set-params', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ worker_id: wid, template: selTpl }),
    });
    const data = await res.json();
    st.textContent = res.ok ? `✓ Applied '${selTpl}' to ${wid}` : `✗ ${data.detail}`;
    st.style.color = res.ok ? 'var(--green)' : 'var(--red)';
  } catch (e) { st.textContent = '✗ Request failed'; st.style.color = 'var(--red)'; }
  setTimeout(() => { st.textContent = ''; }, 5000);
}

// ── Poll ──────────────────────────────────────────────────
async function poll() {
  try {
    const data = await (await fetch('/metrics/cluster')).json();
    const cluster = data.workers || {}, histories = data.latency_histories || {};
    setStatus(true);
    updateKPIs(cluster);
    updateLatChart(histories);
    updateMemChart(cluster);
    updateHH(cluster);
    updateLiveAnswers(cluster);
  } catch (_) { setStatus(false); }
  await updateRouter();
  await updateCtrlLog();
}

// ── Helper ────────────────────────────────────────────────
function setText(id, val) { const el = document.getElementById(id); if (el) el.textContent = val; }

// ── Boot ──────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  initCharts();
  poll();
  setInterval(poll, POLL_MS);
});
