/* ==========================================================
   BTP Analytics Dashboard — Real-time JS
   Polls /metrics/cluster every 2s and updates all widgets.
   ========================================================== */

const POLL_INTERVAL = 2000;

// ---- Chart.js defaults ----
Chart.defaults.color = '#64748b';
Chart.defaults.borderColor = 'rgba(99,179,237,0.08)';
Chart.defaults.font.family = "'JetBrains Mono', monospace";
Chart.defaults.font.size = 11;

const COLORS = ['#4f9eff', '#7c3aed', '#10b981', '#f59e0b', '#ef4444'];

// ---- State ----
let latencyChart = null;
let memoryChart  = null;
let workerIds    = [];

// ---- Initialise Charts ----
function initCharts() {
  const lCtx = document.getElementById('latencyChart').getContext('2d');
  latencyChart = new Chart(lCtx, {
    type: 'line',
    data: { labels: [], datasets: [] },
    options: {
      responsive: true, maintainAspectRatio: false,
      animation: { duration: 400 },
      plugins: { legend: { position: 'top', labels: { boxWidth: 10, padding: 12 } } },
      scales: {
        x: { grid: { color: 'rgba(99,179,237,0.05)' }, ticks: { maxTicksLimit: 10 } },
        y: { grid: { color: 'rgba(99,179,237,0.05)' },
             title: { display: true, text: 'ms' } },
      },
    }
  });

  const mCtx = document.getElementById('memoryChart').getContext('2d');
  memoryChart = new Chart(mCtx, {
    type: 'bar',
    data: { labels: [], datasets: [{
      label: 'Memory %',
      data: [],
      backgroundColor: COLORS.map(c => c + '55'),
      borderColor: COLORS,
      borderWidth: 2,
      borderRadius: 6,
    }] },
    options: {
      responsive: true, maintainAspectRatio: false,
      animation: { duration: 300 },
      plugins: { legend: { display: false } },
      scales: {
        x: { grid: { display: false } },
        y: { min: 0, max: 100, grid: { color: 'rgba(99,179,237,0.05)' },
             title: { display: true, text: '%' } },
      },
    }
  });
}

// ---- Utility ----
function fmt(n, dec=1) {
  if (n === null || n === undefined || isNaN(n)) return '—';
  if (n >= 1e9) return (n/1e9).toFixed(1) + 'B';
  if (n >= 1e6) return (n/1e6).toFixed(1) + 'M';
  if (n >= 1e3) return (n/1e3).toFixed(1) + 'K';
  return Number(n).toFixed(dec);
}

function fmtTs(ts) {
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function templateClass(t) {
  return { low: 't-low', medium: 't-medium', high: 't-high' }[t] || 't-medium';
}

function decisionClass(msg) {
  if (!msg) return '';
  if (msg.includes('upgrade'))   return 'upgrade';
  if (msg.includes('downgrade')) return 'downgrade';
  if (msg.includes('memory') || msg.includes('shrink')) return 'memory';
  if (msg.includes('Manual') || msg.includes('manual')) return 'manual';
  return '';
}

// ---- Latency history update ----
function updateLatencyChart(histories) {
  const newIds = Object.keys(histories);
  const maxLen = Math.max(...newIds.map(id => (histories[id] || []).length), 0);

  // Sync labels (tick numbers)
  latencyChart.data.labels = Array.from({ length: maxLen }, (_, i) => i + 1);

  // Sync datasets
  newIds.forEach((id, idx) => {
    let ds = latencyChart.data.datasets.find(d => d.label === id);
    if (!ds) {
      ds = {
        label: id,
        data: [],
        borderColor: COLORS[idx % COLORS.length],
        backgroundColor: COLORS[idx % COLORS.length] + '1a',
        tension: 0.4,
        fill: true,
        pointRadius: 2,
        borderWidth: 2,
      };
      latencyChart.data.datasets.push(ds);
    }
    ds.data = (histories[id] || []).map(Number);
  });

  // Remove stale datasets
  latencyChart.data.datasets = latencyChart.data.datasets.filter(
    ds => newIds.includes(ds.label)
  );
  latencyChart.update('none');
}

// ---- Memory bar update ----
function updateMemoryChart(cluster) {
  const ids  = Object.keys(cluster);
  const mems = ids.map(id => cluster[id].memory_percent || 0);
  memoryChart.data.labels = ids;
  memoryChart.data.datasets[0].data = mems;
  memoryChart.data.datasets[0].backgroundColor = COLORS.map(c => c + '55').slice(0, ids.length);
  memoryChart.data.datasets[0].borderColor      = COLORS.slice(0, ids.length);
  memoryChart.update('none');
}

// ---- KPIs ----
function updateKPIs(cluster) {
  if (!Object.keys(cluster).length) return;

  const workers = Object.values(cluster);
  const best = workers.reduce((a, b) =>
    (a.p99_latency_ms || Infinity) < (b.p99_latency_ms || Infinity) ? a : b
  );

  document.getElementById('kpiCardinality').textContent = fmt(best.hll_cardinality);
  document.getElementById('kpiCardinalityErr').textContent =
    `rel. error: ±${((best.hll_relative_error || 0) * 100).toFixed(2)}%`;

  document.getElementById('kpiCmsError').textContent = fmt(best.cms_error_estimate);
  document.getElementById('kpiCmsTotal').textContent = `N = ${fmt(best.cms_total_count)}`;

  document.getElementById('kpiP99').textContent = fmt(best.p99_latency_ms) + ' ms';

  const tpl = best.template || 'medium';
  document.getElementById('kpiTemplate').textContent = tpl.toUpperCase();
  document.getElementById('kpiTemplateDetail').textContent =
    `CMS ${best.cms_width}×${best.cms_depth} · HLL p=${best.hll_precision} · MG k=${best.mg_k}`;

  document.getElementById('workerCount').textContent = `${workers.length} worker${workers.length !== 1 ? 's' : ''}`;
}

// ---- Heavy Hitters ----
function updateHeavyHitters(cluster) {
  const tbody = document.getElementById('hhTableBody');
  if (!Object.keys(cluster).length) return;

  const best = Object.values(cluster)[0];
  const hh   = best.mg_heavy_hitters || [];
  const total = best.mg_window_total || 1;
  document.getElementById('mgWindow').textContent = `window N≈${fmt(total)}`;

  if (!hh.length) {
    tbody.innerHTML = `<tr><td colspan="4" class="empty-row">No heavy hitters yet</td></tr>`;
    return;
  }

  tbody.innerHTML = hh.map(([item, count], i) => {
    const share = Math.min((count / total) * 100, 100);
    return `<tr>
      <td>${i + 1}</td>
      <td>${item}</td>
      <td>${fmt(count, 0)}</td>
      <td>
        <div class="share-bar">
          <div class="share-fill" style="width:${share.toFixed(1)}%"></div>
        </div>
      </td>
    </tr>`;
  }).join('');
}

// ---- Router Scores ----
async function updateRouterScores() {
  try {
    const res = await fetch('/metrics/router?q=frequency');
    const data = await res.json();
    const nodes = data.ranked_nodes || [];
    const container = document.getElementById('routerScores');

    if (!nodes.length) {
      container.innerHTML = `<p class="empty-row">No workers alive</p>`;
      return;
    }

    container.innerHTML = nodes.map((n, i) => `
      <div class="router-node ${i === 0 ? 'best' : ''}">
        <span class="router-node-id">${i === 0 ? '🥇 ' : ''}${n.worker_id}</span>
        <span class="template-pill ${templateClass(n.metrics?.template)}">
          ${n.metrics?.template || '?'}
        </span>
        <span class="router-score">${n.score.toFixed(4)}</span>
      </div>
    `).join('');
  } catch (_) {}
}

// ---- Controller Log ----
async function updateControllerLog() {
  try {
    const res = await fetch('/metrics/controller-log?n=20');
    const data = await res.json();
    const decisions = (data.decisions || []).reverse();
    const container = document.getElementById('ctrlLog');
    document.getElementById('ctrlLogCount').textContent = `${decisions.length} events`;

    if (!decisions.length) {
      container.innerHTML = `<p class="empty-row">No decisions yet</p>`;
      return;
    }

    container.innerHTML = decisions.map(d => `
      <div class="log-entry ${decisionClass(d.message)}">
        <span class="log-ts">${fmtTs(d.ts)}</span>
        <span class="log-wid">${d.worker_id}</span>
        ${d.message}
      </div>
    `).join('');
  } catch (_) {}
}

// ---- Status indicator ----
function setStatus(live) {
  const dot  = document.getElementById('statusDot');
  const text = document.getElementById('statusText');
  dot.className = 'status-dot ' + (live ? 'live' : 'error');
  text.textContent = live ? 'Live' : 'No connection';
}

// ---- Main poll loop ----
async function poll() {
  try {
    const res = await fetch('/metrics/cluster');
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const data = await res.json();

    const cluster   = data.workers || {};
    const histories = data.latency_histories || {};

    setStatus(true);
    updateKPIs(cluster);
    updateLatencyChart(histories);
    updateMemoryChart(cluster);
    updateHeavyHitters(cluster);
  } catch (e) {
    setStatus(false);
  }

  // These can lag slightly
  await updateRouterScores();
  await updateControllerLog();
}

// ---- Manual override ----
async function applyOverride() {
  const workerId = document.getElementById('ctrlWorkerId').value.trim();
  const template = document.getElementById('ctrlTemplate').value;
  const status   = document.getElementById('ctrlStatus');

  if (!workerId) { status.textContent = '⚠ Enter a worker ID'; return; }

  try {
    const res = await fetch('/control/set-params', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ worker_id: workerId, template }),
    });
    const data = await res.json();
    status.textContent = res.ok ? `✓ Applied '${template}' to ${workerId}` : `✗ ${data.detail}`;
    status.style.color = res.ok ? 'var(--accent3)' : 'var(--danger)';
  } catch (e) {
    status.textContent = '✗ Request failed';
    status.style.color = 'var(--danger)';
  }

  setTimeout(() => { status.textContent = ''; }, 5000);
}

// ---- Boot ----
document.addEventListener('DOMContentLoaded', () => {
  initCharts();
  poll();
  setInterval(poll, POLL_INTERVAL);
});
