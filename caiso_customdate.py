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
    local_dt = PACIFIC.localize(datetime.strptime(dt_str, "%Y-%m-%d"))
    utc_dt = local_dt.astimezone(pytz.utc)
    return utc_dt.strftime("%Y%m%dT%H:%M-0000")

def fetch_caiso_data(start_date, end_date, queryname, market_run_id, node):
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
        if current <= end:
            import time
            time.sleep(5)  # Avoid CAISO rate limiting between requests
    return results


def parse_lmp_hourly(rows):
    lmp_rows = [r for r in rows if r.get("LMP_TYPE") == "LMP"]
    buckets = {}
    for row in lmp_rows:
        try:
            interval_start = row.get("INTERVALSTARTTIME_GMT") or row.get("INTERVAL_START_GMT") or ""
            mw = float(row.get("MW", 0))
            if interval_start:
                dt_utc = datetime.strptime(interval_start[:19], "%Y-%m-%dT%H:%M:%S")
                dt_utc = pytz.utc.localize(dt_utc)
                dt_pt = dt_utc.astimezone(PACIFIC)
                key = (dt_pt.strftime("%Y-%m-%d"), dt_pt.hour)
                buckets.setdefault(key, []).append(mw)
        except Exception as e:
            print(f"[PARSE ERROR] {e} row={row}", flush=True)
    result = []
    for (date_str, hour) in sorted(buckets.keys()):
        vals = buckets[(date_str, hour)]
        result.append({
            "date": date_str,
            "hour": hour,
            "avg": round(sum(vals)/len(vals), 4),
            "min": round(min(vals), 4),
            "max": round(max(vals), 4),
        })
    return result


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>CAISO Custom Date</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans:wght@400;500;600&display=swap" rel="stylesheet"/>
<style>
  :root {
    --bg: #f0f7f0;
    --surface: #ffffff;
    --surface2: #e8f5e8;
    --border: #b8d8b8;
    --border2: #d0e8d0;
    --text: #1a2e1a;
    --muted: #5a7a5a;
    --rtm: #1a6b3a;
    --rtm-bg: #e8f5ee;
    --hasp: #2d4f8a;
    --hasp-bg: #eaf0fa;
    --accent: #2a7a2a;
    --danger: #c0392b;
    --neg: #1a6b3a;
    --mono: 'IBM Plex Mono', monospace;
    --sans: 'IBM Plex Sans', sans-serif;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: var(--sans); min-height: 100vh; }

  header {
    background: var(--surface); border-bottom: 2px solid var(--border);
    padding: 16px 32px; display: flex; align-items: center; gap: 14px;
  }
  .logo {
    width: 38px; height: 38px; background: var(--accent);
    display: grid; place-items: center;
    font-family: var(--mono); font-size: 11px; color: #fff; font-weight: 500;
  }
  header h1 { font-size: 18px; font-weight: 600; letter-spacing: 1px; }
  header .node { font-family: var(--mono); font-size: 11px; color: var(--muted); margin-left: auto; }

  .main { max-width: 960px; margin: 0 auto; padding: 24px 20px; }

  .query-panel {
    background: var(--surface); border: 1px solid var(--border);
    padding: 20px 24px; margin-bottom: 20px;
    display: flex; gap: 20px; align-items: flex-end; flex-wrap: wrap;
  }
  .field { display: flex; flex-direction: column; gap: 5px; }
  .field label { font-size: 11px; font-weight: 500; color: var(--muted); text-transform: uppercase; letter-spacing: 1px; }
  .field input[type="date"] {
    background: var(--bg); border: 1px solid var(--border);
    color: var(--text); padding: 7px 10px;
    font-family: var(--mono); font-size: 13px; outline: none;
  }
  .field input[type="date"]:focus { border-color: var(--accent); }

  .markets-group { display: flex; flex-direction: column; gap: 5px; }
  .markets-group > span { font-size: 11px; font-weight: 500; color: var(--muted); text-transform: uppercase; letter-spacing: 1px; }
  .checkboxes { display: flex; gap: 18px; padding-top: 2px; }
  .checkbox-item { display: flex; align-items: center; gap: 6px; cursor: pointer; font-size: 13px; font-weight: 500; }
  .checkbox-item input[type="checkbox"] { accent-color: var(--accent); width: 14px; height: 14px; }
  .rtm-cb { color: var(--rtm); }
  .hasp-cb { color: var(--hasp); }

  .fetch-btn {
    background: var(--accent); border: none; color: #fff;
    font-family: var(--sans); font-size: 13px; font-weight: 600;
    padding: 8px 22px; cursor: pointer;
  }
  .fetch-btn:hover { background: #1e5c1e; }
  .fetch-btn:disabled { background: var(--border); cursor: not-allowed; }

  .status-bar {
    font-family: var(--mono); font-size: 11px; color: var(--muted);
    margin-bottom: 18px; min-height: 16px; display: flex; align-items: center; gap: 7px;
  }
  .status-dot { width: 7px; height: 7px; border-radius: 50%; background: var(--border); flex-shrink: 0; }
  .status-dot.loading { background: #e6a817; animation: pulse 0.8s infinite; }
  .status-dot.ok { background: var(--accent); }
  .status-dot.err { background: var(--danger); }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.3} }

  .market-section { margin-bottom: 28px; }
  .section-header {
    display: flex; align-items: center; gap: 10px;
    padding: 10px 16px; border-left: 4px solid var(--rtm); background: var(--rtm-bg);
  }
  .section-header.hasp-hdr { border-left-color: var(--hasp); background: var(--hasp-bg); }
  .market-label { font-size: 12px; font-weight: 600; letter-spacing: 2px; text-transform: uppercase; }
  .rtm-lbl { color: var(--rtm); }
  .hasp-lbl { color: var(--hasp); }
  .section-desc { font-size: 12px; color: var(--muted); }
  .section-count { font-family: var(--mono); font-size: 11px; color: var(--muted); margin-left: auto; }

  .table-wrap { overflow-x: auto; border: 1px solid var(--border); border-top: none; }
  table { width: 100%; border-collapse: collapse; font-family: var(--mono); font-size: 12px; }
  thead tr { background: var(--surface2); }
  th {
    padding: 8px 16px; text-align: right;
    color: var(--muted); font-weight: 500; font-size: 11px;
    border-bottom: 1px solid var(--border); border-right: 1px solid var(--border2);
    text-transform: uppercase; letter-spacing: 1px;
  }
  th:first-child { text-align: center; }
  th:last-child { border-right: none; }
  td { padding: 6px 16px; border-bottom: 1px solid var(--border2); border-right: 1px solid var(--border2); text-align: right; }
  td:first-child { text-align: center; color: var(--muted); }
  .td-date { text-align: left !important; font-size: 11px; white-space: nowrap; }
  td:last-child { border-right: none; }
  tr:last-child td { border-bottom: none; }
  tr:nth-child(even) td { background: var(--surface2); }
  tr:hover td { background: #d8edd8 !important; }
  .date-row td {
    background: var(--border2) !important; color: var(--muted);
    font-size: 11px; font-weight: 500; padding: 4px 16px;
    text-align: left; letter-spacing: 1px; border-bottom: 1px solid var(--border);
  }
  .vpos { color: var(--danger); }
  .vneg { color: var(--neg); }
  .vneu { color: var(--text); }
  .empty-state {
    padding: 36px; text-align: center; font-family: var(--mono); font-size: 12px; color: var(--muted);
    background: var(--surface); border: 1px solid var(--border); border-top: none;
  }
</style>
</head>
<body>
<header>
  <div class="logo">CA</div>
  <h1>CAISO Custom Date</h1>
  <span class="node">ELAP_PACE-APND</span>
</header>

<div class="main">
  <div class="query-panel">
    <div class="field">
      <label>Start Date</label>
      <input type="date" id="startDate"/>
    </div>
    <div class="field">
      <label>End Date</label>
      <input type="date" id="endDate"/>
    </div>
    <div class="markets-group">
      <span>Markets</span>
      <div class="checkboxes">
        <label class="checkbox-item"><input type="checkbox" id="chkRTM" checked/><span class="rtm-cb">RTM</span></label>
        <label class="checkbox-item"><input type="checkbox" id="chkHASP" checked/><span class="hasp-cb">HASP</span></label>
      </div>
    </div>
    <button class="fetch-btn" id="fetchBtn" onclick="runQuery()">Fetch Data</button>
  </div>

  <div class="status-bar">
    <div class="status-dot" id="statusDot"></div>
    <span id="statusMsg">Select a date range and click Fetch Data.</span>
  </div>

  <div id="rtmSection" class="market-section" style="display:none">
    <div class="section-header">
      <span class="market-label rtm-lbl">RTM</span>
      <span class="section-desc">Real-Time Market &middot; 5-min intervals aggregated hourly</span>
      <span class="section-count" id="rtmCount"></span>
    </div>
    <div class="table-wrap"><div id="rtmTable"></div></div>
  </div>

  <div id="haspSection" class="market-section" style="display:none">
    <div class="section-header hasp-hdr">
      <span class="market-label hasp-lbl">HASP</span>
      <span class="section-desc">Hour-Ahead Scheduling Process &middot; 15-min intervals aggregated hourly</span>
      <span class="section-count" id="haspCount"></span>
    </div>
    <div class="table-wrap"><div id="haspTable"></div></div>
  </div>
</div>

<script>
(function() {
  const now = new Date();
  const yd = new Date(now); yd.setDate(yd.getDate()-1);
  document.getElementById('startDate').value = yd.toISOString().slice(0,10);
  document.getElementById('endDate').value = now.toISOString().slice(0,10);
})();

function setStatus(msg, state) {
  document.getElementById('statusMsg').textContent = msg;
  document.getElementById('statusDot').className = 'status-dot' + (state ? ' '+state : '');
}

async function runQuery() {
  const start = document.getElementById('startDate').value;
  const end = document.getElementById('endDate').value;
  const doRTM = document.getElementById('chkRTM').checked;
  const doHASP = document.getElementById('chkHASP').checked;
  if (!start || !end) { setStatus('Please select both dates.', 'err'); return; }
  if (!doRTM && !doHASP) { setStatus('Select at least one market.', 'err'); return; }
  const btn = document.getElementById('fetchBtn');
  btn.disabled = true;
  document.getElementById('rtmSection').style.display = 'none';
  document.getElementById('haspSection').style.display = 'none';
  const markets = [];
  if (doRTM) markets.push('RTM');
  if (doHASP) markets.push('HASP');
  setStatus('Fetching ' + markets.join(' + ') + ' from ' + start + ' to ' + end + '...', 'loading');
  try {
    const resp = await fetch('/query?start=' + start + '&end=' + end + '&markets=' + markets.join(','));
    const data = await resp.json();
    if (data.error) { setStatus('Error: ' + data.error, 'err'); btn.disabled = false; return; }
    let total = 0;
    if (data.rtm) { renderTable('rtm', data.rtm); total += data.rtm.length; }
    if (data.hasp) { renderTable('hasp', data.hasp); total += data.hasp.length; }
    setStatus('Done — ' + total + ' hourly rows.', 'ok');
  } catch(e) { setStatus('Fetch failed: ' + e.message, 'err'); }
  btn.disabled = false;
}

function vc(v) { return v > 50 ? 'vpos' : v < 0 ? 'vneg' : 'vneu'; }
function fmt(v) { return v.toFixed(4); }

function renderTable(market, rows) {
  document.getElementById(market+'Section').style.display = 'block';
  document.getElementById(market+'Count').textContent = rows.length + ' hourly rows';
  const div = document.getElementById(market+'Table');
  if (!rows.length) { div.innerHTML = '<div class="empty-state">NO DATA RETURNED</div>'; return; }
  let tbody = '';
  rows.forEach(r => {
    tbody += '<tr>'
      + '<td class="td-date">' + r.date + '</td>'
      + '<td>' + (r.hour + 1) + '</td>'
      + '<td class="' + vc(r.avg) + '">' + fmt(r.avg) + '</td>'
      + '<td class="' + vc(r.min) + '">' + fmt(r.min) + '</td>'
      + '<td class="' + vc(r.max) + '">' + fmt(r.max) + '</td>'
      + '</tr>';
  });
  div.innerHTML = '<table><thead><tr><th>Date</th><th>Oper Hour</th><th>Avg ($/MWh)</th><th>Min</th><th>Max</th></tr></thead><tbody>' + tbody + '</tbody></table>';
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
        result["rtm"] = parse_lmp_hourly(rows)
    if "HASP" in markets:
        print(f"[HASP] Fetching {start} to {end}", flush=True)
        rows = fetch_caiso_data(start, end, "PRC_HASP_LMP", "HASP", node)
        result["hasp"] = parse_lmp_hourly(rows)
    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True, port=5002)
