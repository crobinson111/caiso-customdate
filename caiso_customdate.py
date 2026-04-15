from flask import Flask, request, jsonify, render_template_string
import requests
import zipfile
import io
import csv
from datetime import datetime, timedelta
import pytz

app = Flask(__name__)

PACIFIC = pytz.timezone("America/Los_Angeles")

def dt_to_utc_str(dt_str):
    """Convert a local date string YYYY-MM-DD to UTC start/end strings for CAISO API."""
    local_dt = PACIFIC.localize(datetime.strptime(dt_str, "%Y-%m-%d"))
    utc_dt = local_dt.astimezone(pytz.utc)
    return utc_dt.strftime("%Y%m%dT%H:%M-0000")

def fetch_caiso_data(start_date, end_date, queryname, market_run_id, node):
    """Fetch LMP data from CAISO OASIS for a date range, one day at a time."""
    base_url = "https://oasis.caiso.com/oasisapi/SingleZip"
    results = []

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    current = start
    while current <= end:
        day_str = current.strftime("%Y-%m-%d")
        next_day = current + timedelta(days=1)

        startdatetime = dt_to_utc_str(day_str)
        enddatetime = dt_to_utc_str(next_day.strftime("%Y-%m-%d"))

        params = {
            "queryname": queryname,
            "market_run_id": market_run_id,
            "startdatetime": startdatetime,
            "enddatetime": enddatetime,
            "version": 1,
            "node": node,
            "resultformat": 6,
        }

        try:
            resp = requests.get(base_url, params=params, timeout=60)
            if resp.content[:1] == b"<":
                print(f"[WARN] XML error response for {day_str}", flush=True)
                current += timedelta(days=1)
                continue

            with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                for name in z.namelist():
                    if name.endswith(".csv"):
                        with z.open(name) as f:
                            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
                            for row in reader:
                                results.append(row)
        except Exception as e:
            print(f"[ERROR] {day_str}: {e}", flush=True)

        current += timedelta(days=1)

    return results


def parse_lmp_rows(rows, interval_field):
    """Extract timestamp and LMP value from raw rows."""
    data = []
    for row in rows:
        try:
            lmp = float(row.get("MW", row.get("LMP_TYPE", 0)))
            # Find the LMP_TYPE == LMP row
        except:
            pass

    # Filter only LMP rows and extract interval start + value
    lmp_rows = [r for r in rows if r.get("LMP_TYPE") == "LMP"]
    for row in lmp_rows:
        try:
            interval_start = row.get("INTERVALSTARTTIME_GMT") or row.get("INTERVAL_START_GMT") or ""
            mw = float(row.get("MW", 0))
            # Convert GMT to Pacific
            if interval_start:
                dt_utc = datetime.strptime(interval_start[:19], "%Y-%m-%dT%H:%M:%S")
                dt_utc = pytz.utc.localize(dt_utc)
                dt_pt = dt_utc.astimezone(PACIFIC)
                label = dt_pt.strftime("%Y-%m-%d %H:%M")
                data.append({"time": label, "lmp": round(mw, 2)})
        except Exception as e:
            print(f"[PARSE ERROR] {e} row={row}", flush=True)

    data.sort(key=lambda x: x["time"])
    return data


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>CAISO Custom Date</title>
<link href="https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Barlow+Condensed:wght@300;400;600;700&display=swap" rel="stylesheet"/>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  :root {
    --bg: #0a0c10;
    --panel: #10141a;
    --border: #1e2530;
    --rtm: #4fc3f7;
    --rtm-dim: rgba(79,195,247,0.15);
    --hasp: #ce93d8;
    --hasp-dim: rgba(206,147,216,0.15);
    --text: #dce8f0;
    --muted: #5a7080;
    --accent: #f0c040;
    --danger: #ef5350;
    --success: #66bb6a;
    --mono: 'Share Tech Mono', monospace;
    --sans: 'Barlow Condensed', sans-serif;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: var(--sans); min-height: 100vh; }

  /* scanline texture */
  body::before {
    content: '';
    position: fixed; inset: 0; pointer-events: none; z-index: 999;
    background: repeating-linear-gradient(0deg, transparent, transparent 2px, rgba(0,0,0,0.04) 2px, rgba(0,0,0,0.04) 4px);
  }

  header {
    border-bottom: 1px solid var(--border);
    padding: 20px 32px;
    display: flex; align-items: center; gap: 16px;
    background: linear-gradient(90deg, #0d1117 0%, #10141a 100%);
  }
  .logo-mark {
    width: 36px; height: 36px;
    border: 2px solid var(--accent);
    display: grid; place-items: center;
    font-family: var(--mono); font-size: 11px; color: var(--accent);
    letter-spacing: -1px;
  }
  header h1 {
    font-size: 22px; font-weight: 700; letter-spacing: 3px; text-transform: uppercase;
    color: var(--text);
  }
  header .subtitle {
    font-family: var(--mono); font-size: 11px; color: var(--muted); margin-left: auto;
  }

  .main { max-width: 1200px; margin: 0 auto; padding: 28px 24px; }

  /* Query form */
  .query-panel {
    background: var(--panel); border: 1px solid var(--border);
    padding: 24px 28px; margin-bottom: 28px;
    position: relative;
  }
  .query-panel::before {
    content: 'QUERY PARAMETERS';
    position: absolute; top: -9px; left: 20px;
    font-family: var(--mono); font-size: 10px; color: var(--muted);
    background: var(--panel); padding: 0 8px; letter-spacing: 2px;
  }
  .form-row {
    display: flex; gap: 20px; align-items: flex-end; flex-wrap: wrap;
  }
  .field { display: flex; flex-direction: column; gap: 6px; }
  .field label {
    font-family: var(--mono); font-size: 10px; color: var(--muted); letter-spacing: 2px; text-transform: uppercase;
  }
  .field input[type="date"] {
    background: #0d1117; border: 1px solid var(--border);
    color: var(--text); padding: 8px 12px;
    font-family: var(--mono); font-size: 13px;
    outline: none; transition: border-color 0.2s;
    color-scheme: dark;
  }
  .field input[type="date"]:focus { border-color: var(--accent); }

  .markets-group { display: flex; flex-direction: column; gap: 6px; }
  .markets-group label { font-family: var(--mono); font-size: 10px; color: var(--muted); letter-spacing: 2px; text-transform: uppercase; }
  .checkboxes { display: flex; gap: 16px; }
  .checkbox-item { display: flex; align-items: center; gap: 7px; cursor: pointer; }
  .checkbox-item input[type="checkbox"] { accent-color: var(--accent); width: 14px; height: 14px; }
  .checkbox-item span { font-family: var(--mono); font-size: 12px; }
  .rtm-label { color: var(--rtm); }
  .hasp-label { color: var(--hasp); }

  .fetch-btn {
    background: transparent; border: 1px solid var(--accent);
    color: var(--accent); font-family: var(--mono); font-size: 12px;
    letter-spacing: 2px; text-transform: uppercase;
    padding: 9px 24px; cursor: pointer;
    transition: background 0.2s, color 0.2s;
    white-space: nowrap;
  }
  .fetch-btn:hover { background: var(--accent); color: #000; }
  .fetch-btn:disabled { opacity: 0.4; cursor: not-allowed; }

  /* Status bar */
  .status-bar {
    font-family: var(--mono); font-size: 11px; color: var(--muted);
    margin-bottom: 20px; min-height: 18px;
    display: flex; align-items: center; gap: 8px;
  }
  .status-dot {
    width: 7px; height: 7px; border-radius: 50%; background: var(--muted);
    flex-shrink: 0;
  }
  .status-dot.loading { background: var(--accent); animation: pulse 1s infinite; }
  .status-dot.ok { background: var(--success); }
  .status-dot.err { background: var(--danger); }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.3} }

  /* Market sections */
  .market-section { margin-bottom: 32px; }
  .section-header {
    display: flex; align-items: center; gap: 12px;
    margin-bottom: 16px; padding-bottom: 10px;
    border-bottom: 1px solid var(--border);
  }
  .market-tag {
    font-family: var(--mono); font-size: 10px; letter-spacing: 3px;
    padding: 3px 10px; text-transform: uppercase; font-weight: 700;
  }
  .rtm-tag { background: var(--rtm-dim); color: var(--rtm); border: 1px solid var(--rtm); }
  .hasp-tag { background: var(--hasp-dim); color: var(--hasp); border: 1px solid var(--hasp); }

  .section-meta { font-family: var(--mono); font-size: 11px; color: var(--muted); margin-left: auto; }

  .chart-wrap {
    background: var(--panel); border: 1px solid var(--border);
    padding: 20px; margin-bottom: 16px;
    position: relative; height: 280px;
  }

  /* Table */
  .table-wrap {
    background: var(--panel); border: 1px solid var(--border);
    overflow-x: auto; max-height: 320px; overflow-y: auto;
  }
  table { width: 100%; border-collapse: collapse; font-family: var(--mono); font-size: 12px; }
  thead tr { background: #0d1117; position: sticky; top: 0; z-index: 1; }
  th {
    padding: 10px 16px; text-align: left;
    color: var(--muted); font-weight: 400; letter-spacing: 2px; font-size: 10px;
    border-bottom: 1px solid var(--border); text-transform: uppercase;
  }
  td { padding: 7px 16px; border-bottom: 1px solid rgba(30,37,48,0.6); color: var(--text); }
  tr:last-child td { border-bottom: none; }
  tr:hover td { background: rgba(255,255,255,0.02); }
  .lmp-positive { color: var(--danger); }
  .lmp-negative { color: var(--success); }
  .lmp-neutral { color: var(--text); }

  .empty-state {
    padding: 48px; text-align: center;
    font-family: var(--mono); font-size: 12px; color: var(--muted);
    letter-spacing: 1px;
  }

  /* Stats row */
  .stats-row {
    display: flex; gap: 12px; margin-bottom: 14px; flex-wrap: wrap;
  }
  .stat-chip {
    background: #0d1117; border: 1px solid var(--border);
    padding: 6px 14px; font-family: var(--mono); font-size: 11px;
  }
  .stat-chip .s-label { color: var(--muted); font-size: 9px; letter-spacing: 2px; text-transform: uppercase; display: block; margin-bottom: 2px; }
  .stat-chip .s-val { color: var(--text); }
</style>
</head>
<body>

<header>
  <div class="logo-mark">CA</div>
  <h1>CAISO Custom Date</h1>
  <span class="subtitle">ELAP_PACE-APND &nbsp;|&nbsp; LMP QUERY TOOL</span>
</header>

<div class="main">

  <div class="query-panel">
    <div class="form-row">
      <div class="field">
        <label>Start Date</label>
        <input type="date" id="startDate"/>
      </div>
      <div class="field">
        <label>End Date</label>
        <input type="date" id="endDate"/>
      </div>
      <div class="markets-group">
        <label>Markets</label>
        <div class="checkboxes">
          <label class="checkbox-item">
            <input type="checkbox" id="chkRTM" checked/>
            <span class="rtm-label">RTM 5-min</span>
          </label>
          <label class="checkbox-item">
            <input type="checkbox" id="chkHASP" checked/>
            <span class="hasp-label">HASP 15-min</span>
          </label>
        </div>
      </div>
      <button class="fetch-btn" id="fetchBtn" onclick="runQuery()">&#9654; Fetch Data</button>
    </div>
  </div>

  <div class="status-bar">
    <div class="status-dot" id="statusDot"></div>
    <span id="statusMsg">Select a date range and click Fetch Data.</span>
  </div>

  <div id="rtmSection" class="market-section" style="display:none">
    <div class="section-header">
      <span class="market-tag rtm-tag">RTM</span>
      <span style="font-size:13px;color:var(--muted)">Real-Time Market &nbsp;·&nbsp; 5-min intervals</span>
      <span class="section-meta" id="rtmMeta"></span>
    </div>
    <div class="stats-row" id="rtmStats"></div>
    <div class="chart-wrap"><canvas id="rtmChart"></canvas></div>
    <div class="table-wrap"><div id="rtmTable"></div></div>
  </div>

  <div id="haspSection" class="market-section" style="display:none">
    <div class="section-header">
      <span class="market-tag hasp-tag">HASP</span>
      <span style="font-size:13px;color:var(--muted)">Hour-Ahead Scheduling Process &nbsp;·&nbsp; 15-min intervals</span>
      <span class="section-meta" id="haspMeta"></span>
    </div>
    <div class="stats-row" id="haspStats"></div>
    <div class="chart-wrap"><canvas id="haspChart"></canvas></div>
    <div class="table-wrap"><div id="haspTable"></div></div>
  </div>

</div>

<script>
let rtmChartInst = null;
let haspChartInst = null;

// Default dates: yesterday and today
(function setDefaults() {
  const now = new Date();
  const today = now.toISOString().slice(0,10);
  const yd = new Date(now); yd.setDate(yd.getDate()-1);
  const yesterday = yd.toISOString().slice(0,10);
  document.getElementById('startDate').value = yesterday;
  document.getElementById('endDate').value = today;
})();

function setStatus(msg, state) {
  document.getElementById('statusMsg').textContent = msg;
  const dot = document.getElementById('statusDot');
  dot.className = 'status-dot' + (state ? ' '+state : '');
}

async function runQuery() {
  const start = document.getElementById('startDate').value;
  const end = document.getElementById('endDate').value;
  const doRTM = document.getElementById('chkRTM').checked;
  const doHASP = document.getElementById('chkHASP').checked;

  if (!start || !end) { setStatus('Please select both start and end dates.', 'err'); return; }
  if (!doRTM && !doHASP) { setStatus('Select at least one market.', 'err'); return; }

  const btn = document.getElementById('fetchBtn');
  btn.disabled = true;
  document.getElementById('rtmSection').style.display = 'none';
  document.getElementById('haspSection').style.display = 'none';

  const markets = [];
  if (doRTM) markets.push('RTM');
  if (doHASP) markets.push('HASP');

  setStatus(`Fetching ${markets.join(' + ')} data from ${start} to ${end} — this may take a moment…`, 'loading');

  try {
    const resp = await fetch(`/query?start=${start}&end=${end}&markets=${markets.join(',')}`);
    const data = await resp.json();

    if (data.error) {
      setStatus('Error: ' + data.error, 'err');
      btn.disabled = false;
      return;
    }

    let totalRows = 0;
    if (data.rtm) { renderSection('rtm', data.rtm); totalRows += data.rtm.length; }
    if (data.hasp) { renderSection('hasp', data.hasp); totalRows += data.hasp.length; }

    setStatus(`Done — ${totalRows} intervals loaded.`, 'ok');
  } catch(e) {
    setStatus('Fetch failed: ' + e.message, 'err');
  }
  btn.disabled = false;
}

function renderSection(market, rows) {
  const section = document.getElementById(market+'Section');
  section.style.display = 'block';

  // Stats
  const vals = rows.map(r => r.lmp);
  const avg = vals.reduce((a,b)=>a+b,0)/vals.length;
  const max = Math.max(...vals);
  const min = Math.min(...vals);
  document.getElementById(market+'Meta').textContent = rows.length + ' intervals';
  document.getElementById(market+'Stats').innerHTML = `
    <div class="stat-chip"><span class="s-label">Avg LMP</span><span class="s-val">$${avg.toFixed(2)}</span></div>
    <div class="stat-chip"><span class="s-label">Max LMP</span><span class="s-val lmp-positive">$${max.toFixed(2)}</span></div>
    <div class="stat-chip"><span class="s-label">Min LMP</span><span class="s-val lmp-negative">$${min.toFixed(2)}</span></div>
    <div class="stat-chip"><span class="s-label">Intervals</span><span class="s-val">${rows.length}</span></div>
  `;

  // Chart
  const color = market === 'rtm' ? '#4fc3f7' : '#ce93d8';
  const colorDim = market === 'rtm' ? 'rgba(79,195,247,0.12)' : 'rgba(206,147,216,0.12)';
  const labels = rows.map(r => r.time);
  const values = rows.map(r => r.lmp);

  // Thin out labels for readability if many rows
  const step = Math.max(1, Math.floor(labels.length / 48));
  const sparseLabels = labels.map((l,i) => i % step === 0 ? l : '');

  const canvasId = market+'Chart';
  const existing = market === 'rtm' ? rtmChartInst : haspChartInst;
  if (existing) existing.destroy();

  const ctx = document.getElementById(canvasId).getContext('2d');
  const inst = new Chart(ctx, {
    type: 'line',
    data: {
      labels: sparseLabels,
      datasets: [{
        label: market.toUpperCase()+' LMP ($/MWh)',
        data: values,
        borderColor: color,
        backgroundColor: colorDim,
        borderWidth: 1.5,
        pointRadius: rows.length > 200 ? 0 : 2,
        tension: 0.2,
        fill: true,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: '#10141a',
          borderColor: color,
          borderWidth: 1,
          titleColor: '#5a7080',
          bodyColor: color,
          titleFont: { family: 'Share Tech Mono', size: 10 },
          bodyFont: { family: 'Share Tech Mono', size: 12 },
          callbacks: {
            title: (items) => labels[items[0].dataIndex],
            label: (item) => ' $' + item.raw.toFixed(2) + ' /MWh'
          }
        }
      },
      scales: {
        x: {
          ticks: { color: '#3a5060', font: { family: 'Share Tech Mono', size: 9 }, maxRotation: 45 },
          grid: { color: 'rgba(30,37,48,0.8)' }
        },
        y: {
          ticks: { color: '#3a5060', font: { family: 'Share Tech Mono', size: 10 }, callback: v => '$'+v },
          grid: { color: 'rgba(30,37,48,0.8)' }
        }
      }
    }
  });
  if (market === 'rtm') rtmChartInst = inst; else haspChartInst = inst;

  // Table
  const tableDiv = document.getElementById(market+'Table');
  if (rows.length === 0) {
    tableDiv.innerHTML = '<div class="empty-state">NO DATA RETURNED FOR THIS RANGE</div>';
    return;
  }
  const tbodyRows = rows.map(r => {
    const cls = r.lmp > 50 ? 'lmp-positive' : r.lmp < 0 ? 'lmp-negative' : 'lmp-neutral';
    return `<tr><td>${r.time}</td><td class="${cls}">$${r.lmp.toFixed(2)}</td></tr>`;
  }).join('');
  tableDiv.innerHTML = `<table>
    <thead><tr><th>Interval (Pacific)</th><th>LMP $/MWh</th></tr></thead>
    <tbody>${tbodyRows}</tbody>
  </table>`;
}
</script>
</body>
</html>"""


@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route("/query")
def query():
    start = request.args.get("start")
    end = request.args.get("end")
    markets_str = request.args.get("markets", "RTM,HASP")
    markets = [m.strip().upper() for m in markets_str.split(",")]

    if not start or not end:
        return jsonify({"error": "start and end required"})

    try:
        s = datetime.strptime(start, "%Y-%m-%d")
        e = datetime.strptime(end, "%Y-%m-%d")
        if e < s:
            return jsonify({"error": "End date must be >= start date"})
        if (e - s).days > 31:
            return jsonify({"error": "Date range cannot exceed 31 days"})
    except ValueError:
        return jsonify({"error": "Invalid date format"})

    node = "ELAP_PACE-APND"
    result = {}

    if "RTM" in markets:
        print(f"[RTM] Fetching {start} to {end}", flush=True)
        rows = fetch_caiso_data(start, end, "PRC_INTVL_LMP", "RTM", node)
        result["rtm"] = parse_lmp_rows(rows, "INTERVALSTARTTIME_GMT")

    if "HASP" in markets:
        print(f"[HASP] Fetching {start} to {end}", flush=True)
        rows = fetch_caiso_data(start, end, "PRC_HASP_LMP", "HASP", node)
        result["hasp"] = parse_lmp_rows(rows, "INTERVALSTARTTIME_GMT")

    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True, port=5002)
