#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import re
import json
import sqlite3
import configparser
from datetime import datetime, date
from calendar import monthrange
from flask import Flask, render_template_string, request, jsonify
import plotly.graph_objs as go
import plotly.offline as pyo

# ---------------- config.ini loader ----------------
def load_ini(path=None):
    path = path or os.environ.get(
        "PVPIPE_CONFIG",
        "/home/belle2/amubarak/Data_Format_Conversion/config.ini",
    )
    cfg = configparser.ConfigParser()
    read_ok = cfg.read(path)
    if not read_ok:
        raise RuntimeError(f"config.ini not found at {path}")
    return cfg

_CFG = load_ini()

def cfg_get(section, key, default=None):
    try:
        return _CFG.get(section, key, fallback=default)
    except Exception:
        return default

# paths
DB_PATH     = cfg_get("paths", "db_path", "/home/belle2/amubarak/Data_Format_Conversion/database/conversion_log_2025c.db")
LOGS_ROOT   = cfg_get("paths", "logs_root", "/home/belle2/amubarak/Data_Format_Conversion/logs")
LOGS_SUBDIR = cfg_get("paths", "logs_subdir_pb2parquet", "pb2parquet")
SUMMARY_DIR = os.path.join(LOGS_ROOT, LOGS_SUBDIR)

# web
PORT = 8124

# colors
COLOR_SUCCESS = "#4C6EB1"
COLOR_FAILURE = "#D62728"
COLOR_CHUNKED = "#2CA02C"
COLOR_SKIPPED = "#AAAAAA"
COLOR_R_CONVERTED = "#2CA02C"
COLOR_R_FAILED    = "#D62728"
COLOR_R_ACTIVE    = "#1F77B4"
COLOR_R_QUEUED    = "#C7C7C7"

app = Flask(__name__)

TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Conversion Monitor</title>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; }
    .flex { display:flex; }
    .cal-wrap { max-width: 1100px; }
    .cal-header { display:flex; align-items:center; gap:10px; margin:8px 0 6px 0; }
    .cal-nav-btn { padding:4px 8px; border:1px solid #ccc; border-radius:6px; cursor:pointer; background:#f7f7f7; }
    .cal-title { font-weight: 600; font-size: 18px; min-width: 220px; }
    .cal-legend { display:flex; gap:12px; font-size: 12px; color:#333; margin-left:12px; }
    .box { display:inline-block; width:10px; height:10px; margin-right:6px; vertical-align:middle; }
    .cal-grid { display:grid; grid-template-columns: repeat(7, 1fr); gap:6px; }
    .cal-dow { font-weight:600; text-align:center; padding:6px 0; background:#f3f3f3; border:1px solid #e2e2e2; border-radius:6px; }
    .cal-cell { border:1px solid #e2e2e2; border-radius:10px; min-height: 86px; padding:6px 8px; display:flex; flex-direction:column; gap:4px; }
    .cal-cell.inactive { background:#fbfbfb; color:#aaa; }
    .cal-day { font-weight:700; font-size: 14px; }
    .cal-line { font-size: 12px; display:flex; justify-content:space-between; }
    .cal-line .label { color:#555; }
    .cal-cell:hover { outline:2px solid #cfe0ff; cursor:pointer; }
    .sel { outline:2px solid #4C6EB1; }
    table { border-collapse: collapse; }
    th, td { border:1px solid #ddd; padding:6px 8px; }
  </style>
</head>
<body>
  <h1>📊 .pb → .parquet Conversion Dashboard</h1>

  <h2>Overall Progress</h2>
  <div style="max-width: 900px;">
    {{ progress_bar|safe }}
    <div style="margin-top: 6px; font-family: monospace;">{{ progress_text }}</div>
  </div>

  {% if retry_bar_html %}
    <h2 style="margin-top: 28px;">Retry Progress (latest run)</h2>
    <div style="max-width: 900px;">
      {{ retry_bar_html|safe }}
      <div style="margin-top: 6px; font-family: monospace;">{{ retry_text }}</div>
    </div>
  {% endif %}

  {% if retry_simple_bar_html %}
    <h3 style="margin-top: 18px;">Retry Completion</h3>
    <div style="max-width: 900px;">
      {{ retry_simple_bar_html|safe }}
      <div style="margin-top: 6px; font-family: monospace;">{{ retry_simple_text }}</div>
    </div>
  {% endif %}

  <hr/>

  <div class="cal-wrap">
    <div class="cal-header">
      <button id="prevMonth" class="cal-nav-btn">Prev</button>
      <div id="calTitle" class="cal-title"></div>
      <button id="nextMonth" class="cal-nav-btn">Next</button>
      <div class="cal-legend">
        <span><span class="box" style="background:{{ COLOR_SUCCESS }}"></span>Success</span>
        <span><span class="box" style="background:{{ COLOR_CHUNKED }}"></span>Chunked</span>
        <span><span class="box" style="background:{{ COLOR_SKIPPED }}"></span>Skipped</span>
        <span><span class="box" style="background:{{ COLOR_FAILURE }}"></span>Failed</span>
      </div>
    </div>

    <div class="cal-grid" id="calGridHeader">
      <div class="cal-dow">Mon</div>
      <div class="cal-dow">Tue</div>
      <div class="cal-dow">Wed</div>
      <div class="cal-dow">Thu</div>
      <div class="cal-dow">Fri</div>
      <div class="cal-dow">Sat</div>
      <div class="cal-dow">Sun</div>
    </div>
    <div class="cal-grid" id="calGrid"></div>
  </div>

  <div style="max-width: 900px; margin-top: 14px;">
    <div id="dayCountsText" style="font-family: monospace; margin-bottom: 4px;"></div>
    <div id="dayBar" style="max-width: 900px; margin-top: 8px;"></div>
  </div>

  <h3 style="margin-top: 18px;">Submission Summaries for Selected Day</h3>
  <div id="dayTable"></div>

  <hr/>

  <div style="width: 48%;">
    <h3>All Time Summary</h3>
    {{ pie_chart|safe }}
  </div>

  <h3>Last 5 Submission Summaries</h3>
  {{ summary_table|safe }}

  <p>Last updated: {{ timestamp }}</p>

  <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
  <script>
    const COLOR_SUCCESS = "{{ COLOR_SUCCESS }}";
    const COLOR_FAILURE = "{{ COLOR_FAILURE }}";
    const COLOR_CHUNKED = "{{ COLOR_CHUNKED }}";
    const COLOR_SKIPPED = "{{ COLOR_SKIPPED }}";

    let curYear, curMonth;  // month is 1..12
    let selectedDateISO = null;

    function pad2(x){ return (x<10 ? "0"+x : ""+x); }

    function monthName(y, m){
      const d = new Date(Date.UTC(y, m-1, 1));
      return d.toLocaleString('en-US', {month:'long', year:'numeric', timeZone:'UTC'});
    }

    function isoDate(y, m, d){
      return [y, pad2(m), pad2(d)].join("-");
    }

    async function fetchMonth(y, m){
      const r = await fetch(`/month?year=${y}&month=${m}`);
      if(!r.ok){ return {days:[]}; }
      return r.json();
    }

    async function fetchDay(iso){
      const r = await fetch(`/day?date=${encodeURIComponent(iso)}`);
      if(!r.ok){ return null; }
      return r.json();
    }

    function renderDayBar(data){
      if(!data){ return; }
      const x = [data.date];
      const fig = {
        data: [
          {type:"bar", name:"Success", x:x, y:[data.success], marker:{color:COLOR_SUCCESS}},
          {type:"bar", name:"Chunked", x:x, y:[data.chunked], marker:{color:COLOR_CHUNKED}},
          {type:"bar", name:"Skipped", x:x, y:[data.skipped], marker:{color:COLOR_SKIPPED}},
          {type:"bar", name:"Failed",  x:x, y:[data.failed],  marker:{color:COLOR_FAILURE}},
        ],
        layout: {barmode:"stack", xaxis:{title:"Date"}, yaxis:{title:"Files"}, showlegend:true}
      };
      const div = document.getElementById("dayBar");
      div.innerHTML = "";
      Plotly.newPlot(div, fig.data, fig.layout, {responsive:true});
      const t = document.getElementById("dayCountsText");
      t.textContent = `Date ${data.date}: success ${data.success}, chunked ${data.chunked}, skipped ${data.skipped}, failed ${data.failed}`;

      const rows = data.summaries || [];
      let body = "";
      if (!rows.length) {
        body = "<tr><td colspan='11'>No summaries found for this date.</td></tr>";
      } else {
        for (const s of rows) {
          body += "<tr>"
            + `<td>${s.file}</td><td>${s.jobs}</td><td>${s.pb_submitted}</td><td>${s.chunked}</td><td>${s.peak_ram_mb}</td>`
            + `<td>${s.total_pb_mb}</td><td>${s.total_pq_mb}</td><td>${s.numeric}</td><td>${s.text}</td><td>${s.scalar}</td><td>${s.vector}</td>`
            + "</tr>";
        }
      }
      const table = `
        <table>
          <tr>
            <th>Submission File</th>
            <th>Jobs Submitted</th>
            <th>PB Files Submitted</th>
            <th># Chunk Files</th>
            <th>Peak RAM (MB)</th>
            <th>Total .pb Size (MB)</th>
            <th>Total .parquet Size (MB)</th>
            <th>Numeric PVs</th>
            <th>Text PVs</th>
            <th>Scalar PVs</th>
            <th>Vector PVs</th>
          </tr>
          ${body}
        </table>`;
      document.getElementById("dayTable").innerHTML = table;
    }

    async function selectDay(y, m, d){
      selectedDateISO = isoDate(y, m, d);
      document.querySelectorAll(".cal-cell").forEach(el => el.classList.remove("sel"));
      const el = document.getElementById("cell-"+selectedDateISO);
      if(el) el.classList.add("sel");
      const data = await fetchDay(selectedDateISO);
      renderDayBar(data);
    }

    function firstDowMondayIndex(y, m){
      // We want Monday=0 ... Sunday=6
      // JS getUTCDay: Sunday=0..Saturday=6
      const js = new Date(Date.UTC(y, m-1, 1)).getUTCDay();
      return (js+6)%7;
    }

    async function renderMonth(y, m){
      curYear = y; curMonth = m;
      const title = document.getElementById("calTitle");
      title.textContent = monthName(y, m);

      const grid = document.getElementById("calGrid");
      grid.innerHTML = "";

      const res = await fetchMonth(y, m);
      const byDate = {};
      (res.days || []).forEach(d => { byDate[d.date] = d; });

      const daysInMonth = new Date(Date.UTC(y, m, 0)).getUTCDate(); // last day
      const lead = firstDowMondayIndex(y, m); // blanks before day 1
      const totalCells = Math.ceil((lead + daysInMonth) / 7) * 7;

      const prevYear = (m === 1) ? y-1 : y;
      const prevMonth = (m === 1) ? 12  : m-1;
      const prevDays = new Date(Date.UTC(prevYear, prevMonth, 0)).getUTCDate();

      for(let i=0;i<totalCells;i++){
        const cell = document.createElement("div");
        cell.className = "cal-cell";
        let showYear = y, showMonth = m, dayNum = 0, inactive = false;

        if(i < lead){
          // prev month
          dayNum = prevDays - (lead - 1 - i);
          showYear = prevYear; showMonth = prevMonth; inactive = true;
        } else if(i >= lead + daysInMonth){
          // next month
          dayNum = i - (lead + daysInMonth) + 1;
          showYear = (m === 12) ? y+1 : y;
          showMonth = (m === 12) ? 1   : m+1;
          inactive = true;
        } else {
          // current month
          dayNum = i - lead + 1;
        }

        const iso = [showYear, pad2(showMonth), pad2(dayNum)].join("-");
        cell.id = "cell-"+iso;
        if(inactive) cell.classList.add("inactive");

        const head = document.createElement("div");
        head.className = "cal-day";
        head.textContent = dayNum;
        cell.appendChild(head);

        const stats = byDate[iso] || {success:0, chunked:0, skipped:0, failed:0};

        const mkLine = (label, val, color) => {
          const row = document.createElement("div");
          row.className = "cal-line";
          const l = document.createElement("span"); l.className="label"; l.textContent = label;
          const r = document.createElement("span"); r.style.color = color; r.textContent = val;
          row.appendChild(l); row.appendChild(r);
          return row;
        };
        cell.appendChild(mkLine("S", stats.success, COLOR_SUCCESS));
        cell.appendChild(mkLine("C", stats.chunked, COLOR_CHUNKED));
        cell.appendChild(mkLine("Sk", stats.skipped, COLOR_SKIPPED));
        cell.appendChild(mkLine("F", stats.failed, COLOR_FAILURE));

        cell.addEventListener("click", () => {
          if(inactive){
            renderMonth(showYear, showMonth).then(() => selectDay(showYear, showMonth, dayNum));
          } else {
            selectDay(showYear, showMonth, dayNum);
          }
        });

        grid.appendChild(cell);
      }

      // default select today if in month, else select first day of month
      const today = new Date();
      const ty = today.getUTCFullYear(), tm = today.getUTCMonth()+1, td = today.getUTCDate();
      if(ty === y && tm === m){
        selectDay(y, m, td);
      } else {
        selectDay(y, m, 1);
      }
    }

    document.getElementById("prevMonth").addEventListener("click", () => {
      let y = curYear, m = curMonth - 1;
      if(m < 1){ m = 12; y -= 1; }
      renderMonth(y, m);
    });
    document.getElementById("nextMonth").addEventListener("click", () => {
      let y = curYear, m = curMonth + 1;
      if(m > 12){ m = 1; y += 1; }
      renderMonth(y, m);
    });

    // boot
    const now = new Date();
    renderMonth(now.getUTCFullYear(), now.getUTCMonth()+1);
  </script>
</body>
</html>
"""

# ---------- helpers ----------
_INPUT_FILES_RE = re.compile(r"^\s*Input files\s*:\s*(\d+)\s*$")
_PB_FILE_RE     = re.compile(r"^\s*PB file\s*:\s*(.+)$")

def safe_count(cur, query, params=()):
    try:
        cur.execute(query, params)
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except sqlite3.Error:
        return 0

def conversions_table_has_rows():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='conversions'")
        if not c.fetchone():
            conn.close()
            return False
        c.execute("SELECT COUNT(*) FROM conversions")
        n = c.fetchone()[0]
        conn.close()
        return (n or 0) > 0
    except sqlite3.Error:
        return False

def retry_tables_exist():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='retry_campaigns'")
        ok1 = bool(c.fetchone())
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='retry_items'")
        ok2 = bool(c.fetchone())
        conn.close()
        return ok1 and ok2
    except sqlite3.Error:
        return False

def find_summary_jsons(root_dir, limit=5):
    candidates = []
    for r, _, files in os.walk(root_dir):
        for fn in files:
            if not fn.endswith(".json"):
                continue
            if fn.startswith("submission_summary_") or "summary" in fn:
                full = os.path.join(r, fn)
                try:
                    mtime = os.path.getmtime(full)
                except OSError:
                    continue
                candidates.append((mtime, full))
    candidates.sort(reverse=True)
    return [p for _, p in candidates[:limit]]

def scan_log_dir_for_fail_skip(log_dir):
    if not log_dir or not os.path.isdir(log_dir):
        return (0, 0)
    fail = 0
    skip = 0
    try:
        for r, _, files in os.walk(log_dir):
            for fn in files:
                if not fn.endswith(".log"):
                    continue
                fp = os.path.join(r, fn)
                try:
                    with open(fp, "r", encoding="utf-8", errors="ignore") as f:
                        for ln in f:
                            s = ln.lstrip()
                            if s.startswith("FAIL:") or "❌ Failed" in s:
                                fail += 1
                            elif s.startswith("Skipped:") or s.startswith("⚠️ Skipped:"):
                                skip += 1
                except Exception:
                    continue
    except Exception:
        pass
    return (fail, skip)

def count_jobs_in_log_dir(log_dir):
    if not log_dir or not os.path.isdir(log_dir):
        return 0
    jobs = 0
    try:
        for r, _, files in os.walk(log_dir):
            for fn in files:
                if not fn.endswith(".log"):
                    continue
                fp = os.path.join(r, fn)
                try:
                    with open(fp, "r", encoding="utf-8", errors="ignore") as f:
                        txt = f.read()
                        if ("------ Chunk Summary ------" in txt) or ("------ Chunker Summary ------" in txt):
                            jobs += 1
                except Exception:
                    continue
    except Exception:
        pass
    return jobs

def count_pb_submitted_from_logs(log_dir):
    if not log_dir or not os.path.isdir(log_dir):
        return 0
    total = 0
    try:
        for r, _, files in os.walk(log_dir):
            for fn in files:
                if not fn.endswith(".log"):
                    continue
                fp = os.path.join(r, fn)
                try:
                    with open(fp, "r", encoding="utf-8", errors="ignore") as f:
                        saw_chunk_summary = False
                        pb_count_for_this_log = 0
                        for ln in f:
                            s = ln.rstrip("\n")
                            if "------ Chunk Summary ------" in s:
                                saw_chunk_summary = True
                                continue
                            if saw_chunk_summary:
                                m = _INPUT_FILES_RE.match(s)
                                if m:
                                    pb_count_for_this_log += int(m.group(1))
                            if "------ Chunker Summary ------" in s:
                                pb_count_for_this_log += 0
                            else:
                                m2 = _PB_FILE_RE.match(s)
                                if m2:
                                    pb_count_for_this_log += 1
                        total += pb_count_for_this_log
                except Exception:
                    continue
    except Exception:
        pass
    return total

def parse_summary(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    ts_txt = data.get("timestamp")
    if ts_txt:
        try:
            ts = datetime.fromisoformat(ts_txt)
        except Exception:
            ts = datetime.fromtimestamp(os.path.getmtime(path))
    else:
        ts = datetime.fromtimestamp(os.path.getmtime(path))

    counters = data.get("counters", {})
    saved_files_count = int(counters.get("saved_files_count", 0) or 0)
    chunk_files_count = int(counters.get("chunk_files_count", 0) or 0)

    jobs_submitted = int(data.get("small_bundle_logs", 0) or 0) \
                   + int(data.get("big_chunk_logs", 0) or 0) \
                   + int(data.get("legacy_chunk_logs", 0) or 0)

    sizes = data.get("sizes", {})
    total_pb_mb = sizes.get("total_pb_size_MB", data.get("total_pb_size_MB"))
    total_pq_mb = sizes.get("total_parquet_size_MB", data.get("total_parquet_size_MB"))

    peak_rss_gb = data.get("peak_RSS_GB_overall")
    peak_ram_mb = int(round(peak_rss_gb * 1024)) if isinstance(peak_rss_gb, (int, float)) else None

    ptb = data.get("pv_type_breakdown", {})
    small_ptb = ptb.get("small", {})
    big_ptb   = ptb.get("big", {})

    def _sum(d, keys): return sum(int(d.get(k, 0) or 0) for k in keys)
    scalar_cnt  = _sum(small_ptb, ["numeric_scalar", "text_scalar"]) + _sum(big_ptb, ["numeric_scalar", "text_scalar"])
    vector_cnt  = _sum(small_ptb, ["numeric_vector", "text_vector"]) + _sum(big_ptb, ["numeric_vector", "text_vector"])
    numeric_cnt = _sum(small_ptb, ["numeric_scalar", "numeric_vector"]) + _sum(big_ptb, ["numeric_scalar", "numeric_vector"])
    text_cnt    = _sum(small_ptb, ["text_scalar", "text_vector"]) + _sum(big_ptb, ["text_scalar", "text_vector"])

    log_dir = data.get("log_dir")
    if (not jobs_submitted) and log_dir and os.path.isdir(log_dir):
        jobs_submitted = count_jobs_in_log_dir(log_dir)

    pb_submitted = count_pb_submitted_from_logs(log_dir) if log_dir else 0

    return {
        "path": path,
        "timestamp": ts,
        "date": ts.date().isoformat(),
        "saved": saved_files_count,
        "chunked": chunk_files_count,
        "jobs": jobs_submitted,
        "pb_submitted": pb_submitted,
        "total_pb_mb": total_pb_mb,
        "total_pq_mb": total_pq_mb,
        "peak_ram_mb": peak_ram_mb,
        "scalar": scalar_cnt,
        "vector": vector_cnt,
        "numeric": numeric_cnt,
        "text": text_cnt,
        "log_dir": log_dir
    }

def extract_row_from_summary(path):
    s = parse_summary(path)
    peak = str(s["peak_ram_mb"]) if s["peak_ram_mb"] is not None else "-"
    pb   = f"{s['total_pb_mb']}" if s["total_pb_mb"] is not None else "-"
    pq   = f"{s['total_pq_mb']}" if s["total_pq_mb"] is not None else "-"
    return (
        os.path.basename(path),
        s["jobs"],
        s["pb_submitted"],
        s["chunked"],
        peak,
        pb,
        pq,
        s["numeric"],
        s["text"],
        s["scalar"],
        s["vector"],
    )

def progress_counts():
    total = converted = remaining = pct = 0
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pb_catalog'")
        if c.fetchone():
            c.execute("SELECT COUNT(*) FROM pb_catalog")
            row = c.fetchone()
            total = int(row[0]) if row and row[0] is not None else 0
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='conversions'")
        if c.fetchone():
            c.execute("SELECT COUNT(*) FROM conversions WHERE status IN ('success','skipped')")
            row = c.fetchone()
            converted = int(row[0]) if row and row[0] is not None else 0
        conn.close()
    except Exception:
        pass
    remaining = max(0, total - converted)
    pct = int(round(100 * (converted / total), 0)) if total else 0
    return total, converted, remaining, pct

def build_simple_bar_html(pct, color=COLOR_SUCCESS):
    pct = max(0, min(100, int(pct)))
    outer = "background:#eee;border-radius:8px;overflow:hidden;height:24px;border:1px solid #ddd;"
    inner = f"width:{pct}%;height:100%;background:{color};"
    return f"<div style='{outer}'><div style='{inner}'></div></div>"

# ----- retry helpers -----
def latest_retry_run():
    if not retry_tables_exist():
        return None
    try:
        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
        c.execute("""
            SELECT run_id, started_ts, COALESCE(label,''), COALESCE(total_planned,0)
            FROM retry_campaigns
            ORDER BY datetime(COALESCE(started_ts,'')) DESC
            LIMIT 1
        """)
        row = c.fetchone()
        if row:
            conn.close()
            return {"run_id": row[0], "started_ts": row[1], "label": row[2], "total_planned": int(row[3] or 0)}
        c.execute("""
            SELECT run_id, MAX(COALESCE(queued_ts, finished_ts))
            FROM retry_items
            GROUP BY run_id
            ORDER BY datetime(MAX(COALESCE(queued_ts, finished_ts))) DESC
            LIMIT 1
        """)
        row = c.fetchone(); conn.close()
        if row:
            return {"run_id": row[0], "started_ts": row[1], "label": "", "total_planned": 0}
        return None
    except sqlite3.Error:
        return None

def retry_progress_counts_for_run(run_id, total_planned_hint=0):
    out = {"planned": 0, "converted_ok": 0, "skipped": 0, "failed_again": 0, "in_progress": 0, "queued": 0, "pct_done": 0}
    if not run_id:
        return out
    try:
        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
        c.execute("SELECT COALESCE(total_planned,0) FROM retry_campaigns WHERE run_id=?", (run_id,))
        row = c.fetchone()
        planned = int(row[0]) if row and row[0] is not None else 0
        if planned == 0:
            c.execute("SELECT COUNT(*) FROM retry_items WHERE run_id=?", (run_id,))
            planned = int(c.fetchone()[0] or 0)
        if planned == 0 and total_planned_hint:
            planned = int(total_planned_hint)

        c.execute("SELECT COUNT(*) FROM retry_items WHERE run_id=? AND outcome IN ('converted_ok')", (run_id,))
        converted_ok = int(c.fetchone()[0] or 0)
        c.execute("SELECT COUNT(*) FROM retry_items WHERE run_id=? AND outcome IN ('skipped')", (run_id,))
        skipped = int(c.fetchone()[0] or 0)
        c.execute("SELECT COUNT(*) FROM retry_items WHERE run_id=? AND outcome='failed_again'", (run_id,))
        failed_again = int(c.fetchone()[0] or 0)
        c.execute("SELECT COUNT(*) FROM retry_items WHERE run_id=? AND start_ts IS NOT NULL AND finished_ts IS NULL", (run_id,))
        in_progress = int(c.fetchone()[0] or 0)
        c.execute("SELECT COUNT(*) FROM retry_items WHERE run_id=? AND start_ts IS NULL", (run_id,))
        queued = int(c.fetchone()[0] or 0)
        conn.close()

        done = converted_ok + skipped
        out.update({
            "planned": planned, "converted_ok": converted_ok, "skipped": skipped,
            "failed_again": failed_again, "in_progress": in_progress, "queued": queued,
            "pct_done": int(round(100 * (done / planned), 0)) if planned else 0
        })
        return out
    except sqlite3.Error:
        return out

def build_stacked_retry_bar_html(planned, converted_ok, skipped, failed_again, in_progress, queued):
    def pct(x): return max(0.0, (100.0 * x / planned)) if planned else 0.0
    segs = [
        (pct(converted_ok + skipped), COLOR_R_CONVERTED, "Done"),
        (pct(in_progress),            COLOR_R_ACTIVE,    "In Progress"),
        (pct(queued),                 COLOR_R_QUEUED,    "Queued"),
        (pct(failed_again),           COLOR_R_FAILED,    "Failed Again"),
    ]
    outer = "background:#eee;border-radius:8px;overflow:hidden;height:24px;border:1px solid #ddd;display:flex;"
    html = [f"<div style='{outer}'>"]
    for w, color, _ in segs:
        width = max(0.0, min(100.0, w))
        if width <= 0: continue
        html.append(f"<div style='width:{width:.3f}%;height:100%;background:{color};'></div>")
    html.append("</div>")
    return "".join(html)

def build_simple_retry_bar_html(done, planned):
    pct = int(round(100 * (done / planned), 0)) if planned else 0
    pct = max(0, min(100, pct))
    outer = "background:#eee;border-radius:8px;overflow:hidden;height:18px;border:1px solid #ddd;"
    inner = f"width:{pct}%;height:100%;background:{COLOR_R_CONVERTED};"
    return f"<div style='{outer}'><div style='{inner}'></div></div>", pct

# ----- daily counts API -----
def parse_all_summaries_grouped_by_date():
    """Fallback map date_iso -> aggregated counts by scanning SUMMARY_DIR."""
    out = {}
    for r, _, files in os.walk(SUMMARY_DIR):
      for fn in files:
        if not fn.endswith(".json"): continue
        if not (fn.startswith("submission_summary_") or "summary" in fn): continue
        p = os.path.join(r, fn)
        try:
            s = parse_summary(p)
        except Exception:
            continue
        d = s["date"]
        x = out.get(d, {"success":0,"chunked":0,"skipped":0,"failed":0, "summaries":[]})
        x["success"] += int(s["saved"])
        x["chunked"] += int(s["chunked"])
        # estimate skipped and failed from logs
        f, k = scan_log_dir_for_fail_skip(s["log_dir"])
        x["failed"] += int(f)
        x["skipped"] += int(k)
        x["summaries"].append(s)
        out[d] = x
    return out

def day_counts(date_str):
    success = chunked = skipped = failed = 0
    used_db = False
    try:
        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='conversions'")
        if c.fetchone():
            used_db = True
            success = safe_count(c, "SELECT COUNT(*) FROM conversions WHERE status='success' AND substr(timestamp,1,10)=?", (date_str,))
            chunked = safe_count(c, "SELECT COUNT(*) FROM conversions WHERE status='chunked' AND substr(timestamp,1,10)=?", (date_str,))
            skipped = safe_count(c, "SELECT COUNT(*) FROM conversions WHERE status='skipped' AND substr(timestamp,1,10)=?", (date_str,))
            failed  = safe_count(c, "SELECT COUNT(*) FROM conversions WHERE status='failed'  AND substr(timestamp,1,10)=?", (date_str,))
        conn.close()
    except Exception:
        used_db = False

    summaries = []
    if not used_db:
        # fallback grouped summaries
        grouped = parse_all_summaries_grouped_by_date()
        agg = grouped.get(date_str)
        if agg:
            success = agg["success"]; chunked = agg["chunked"]; skipped = agg["skipped"]; failed = agg["failed"]
            summaries_raw = agg["summaries"]
        else:
            summaries_raw = []
    else:
        # DB mode still wants to show per-day summary rows if files exist
        summaries_raw = []
        for r, _, files in os.walk(SUMMARY_DIR):
            for fn in files:
                if not fn.endswith(".json"): continue
                if not (fn.startswith("submission_summary_") or "summary" in fn): continue
                p = os.path.join(r, fn)
                try:
                    s = parse_summary(p)
                    if s["date"] == date_str:
                        summaries_raw.append(s)
                except Exception:
                    continue

    rows = []
    for s in summaries_raw:
        rows.append({
            "file": os.path.basename(s["path"]),
            "jobs": s["jobs"],
            "pb_submitted": s["pb_submitted"],
            "chunked": s["chunked"],
            "peak_ram_mb": s["peak_ram_mb"] if s["peak_ram_mb"] is not None else "-",
            "total_pb_mb": s["total_pb_mb"] if s["total_pb_mb"] is not None else "-",
            "total_pq_mb": s["total_pq_mb"] if s["total_pq_mb"] is not None else "-",
            "numeric": s["numeric"],
            "text": s["text"],
            "scalar": s["scalar"],
            "vector": s["vector"]
        })

    return {
        "date": date_str,
        "success": success,
        "chunked": chunked,
        "skipped": skipped,
        "failed": failed,
        "summaries": rows
    }

def month_counts(year, month):
    """Return counts for every day in the month. Prefer DB, fallback to JSON scan."""
    y = int(year); m = int(month)
    first = date(y, m, 1)
    ndays = monthrange(y, m)[1]

    use_db = conversions_table_has_rows()
    grouped = None
    if not use_db:
        grouped = parse_all_summaries_grouped_by_date()

    days = []
    totals = {"success":0, "chunked":0, "skipped":0, "failed":0}
    try:
        conn = sqlite3.connect(DB_PATH) if use_db else None
        c = conn.cursor() if conn else None
    except Exception:
        use_db = False
        conn = None; c = None

    for d in range(1, ndays+1):
        iso = f"{y}-{str(m).zfill(2)}-{str(d).zfill(2)}"
        if use_db and c is not None:
            s = safe_count(c, "SELECT COUNT(*) FROM conversions WHERE status='success' AND substr(timestamp,1,10)=?", (iso,))
            ch = safe_count(c, "SELECT COUNT(*) FROM conversions WHERE status='chunked' AND substr(timestamp,1,10)=?", (iso,))
            sk = safe_count(c, "SELECT COUNT(*) FROM conversions WHERE status='skipped' AND substr(timestamp,1,10)=?", (iso,))
            f  = safe_count(c, "SELECT COUNT(*) FROM conversions WHERE status='failed'  AND substr(timestamp,1,10)=?", (iso,))
        else:
            agg = grouped.get(iso, {"success":0,"chunked":0,"skipped":0,"failed":0})
            s, ch, sk, f = agg["success"], agg["chunked"], agg["skipped"], agg["failed"]

        days.append({"date": iso, "success": s, "chunked": ch, "skipped": sk, "failed": f})
        totals["success"] += s; totals["chunked"] += ch; totals["skipped"] += sk; totals["failed"] += f

    if conn: conn.close()
    return {"year": y, "month": m, "days": days, "totals": totals}

# ---------- web ----------
@app.route("/")
def dashboard():
    total, converted, remaining, pct = progress_counts()
    progress_bar_html = build_simple_bar_html(pct, color=COLOR_SUCCESS)
    progress_text = f"{converted:,} of {total:,} .pb converted ({pct}%) | remaining {remaining:,}"

    try:
        os.makedirs(SUMMARY_DIR, exist_ok=True)
        progress_log = os.path.join(SUMMARY_DIR, "progress.log")
        with open(progress_log, "a", encoding="utf-8") as fp:
            ts = datetime.now().isoformat(timespec="seconds")
            fp.write(f"{ts} converted={converted} total={total} pct={pct} remaining={remaining}\n")
    except Exception:
        pass

    retry_bar_html = ""
    retry_text = ""
    retry_simple_bar_html = ""
    retry_simple_text = ""
    lr = latest_retry_run()
    if lr:
        counts = retry_progress_counts_for_run(lr["run_id"], total_planned_hint=lr.get("total_planned", 0))
        if counts["planned"] > 0:
            retry_bar_html = build_stacked_retry_bar_html(
                counts["planned"],
                counts["converted_ok"],
                counts["skipped"],
                counts["failed_again"],
                counts["in_progress"],
                counts["queued"],
            )
            done = counts["converted_ok"] + counts["skipped"]
            pct_done = counts["pct_done"]
            label = (lr.get("label") or "").strip()
            started = (lr.get("started_ts") or "").strip()
            meta = f"run_id={lr['run_id']}"
            if label: meta += f", label={label}"
            if started: meta += f", started={started}"
            retry_text = (f"{done:,} of {counts['planned']:,} retried ({pct_done}%) | "
                          f"in-progress {counts['in_progress']:,} | queued {counts['queued']:,} | "
                          f"failed-again {counts['failed_again']:,}  [{meta}]")
            retry_simple_bar_html, pct_simple = build_simple_retry_bar_html(done, counts["planned"])
            retry_simple_text = (f"Completion: {done:,}/{counts['planned']:,} ({pct_simple}%)  "
                                 f"• active now: {counts['in_progress']:,}  • failed again: {counts['failed_again']:,}")

    # all-time pie
    if conversions_table_has_rows():
        try:
            conn = sqlite3.connect(DB_PATH); c = conn.cursor()
            total_success_all = safe_count(c, "SELECT COUNT(*) FROM conversions WHERE status='success'")
            total_failed_all  = safe_count(c, "SELECT COUNT(*) FROM conversions WHERE status='failed'")
            conn.close()
        except sqlite3.Error:
            total_success_all = total_failed_all = 0
    else:
        total_success_all = total_failed_all = 0
        for r, _, files in os.walk(SUMMARY_DIR):
            for fn in files:
                if fn.endswith(".json") and (fn.startswith("submission_summary_") or "summary" in fn):
                    p = os.path.join(r, fn)
                    try:
                        s = parse_summary(p)
                    except Exception:
                        continue
                    total_success_all += int(s["saved"])
                    f, _ = scan_log_dir_for_fail_skip(s["log_dir"])
                    total_failed_all  += int(f)

    if total_success_all + total_failed_all == 0:
        pie = go.Figure()
        pie.add_annotation(text="No conversion attempts yet", x=0.5, y=0.5, showarrow=False)
    else:
        pie = go.Figure(data=[go.Pie(labels=["Success", "Failed"],
                                     values=[total_success_all, total_failed_all],
                                     marker=dict(colors=[COLOR_SUCCESS, COLOR_FAILURE]))])
        pie.update_traces(hole=0.3)
    pie_html = pyo.plot(pie, include_plotlyjs="cdn", output_type="div")

    # recent 5 summaries
    json_paths = find_summary_jsons(SUMMARY_DIR, limit=5) if os.path.isdir(SUMMARY_DIR) else []
    rows = []
    for pth in json_paths:
        try:
            rows.append(extract_row_from_summary(pth))
        except Exception:
            continue
    if not rows:
        body_rows = "<tr><td colspan='11'>No summaries found yet.</td></tr>"
    else:
        body_rows = "".join(
            "<tr>"
            f"<td>{fn}</td><td>{jobs}</td><td>{pbs}</td><td>{chunks}</td><td>{ram}</td><td>{pb}</td><td>{pq}</td>"
            f"<td>{num}</td><td>{txt}</td><td>{sc}</td><td>{vc}</td>"
            "</tr>"
            for (fn, jobs, pbs, chunks, ram, pb, pq, num, txt, sc, vc) in rows
        )
    summary_table = (
        "<table>"
        "<tr>"
        "<th>Submission File</th>"
        "<th>Jobs Submitted</th>"
        "<th>PB Files Submitted</th>"
        "<th># Chunk Files</th>"
        "<th>Peak RAM (MB)</th>"
        "<th>Total .pb Size (MB)</th>"
        "<th>Total .parquet Size (MB)</th>"
        "<th>Numeric PVs</th>"
        "<th>Text PVs</th>"
        "<th>Scalar PVs</th>"
        "<th>Vector PVs</th>"
        "</tr>"
        f"{body_rows}"
        "</table>"
    )

    return render_template_string(
        TEMPLATE,
        pie_chart=pie_html,
        summary_table=summary_table,
        progress_bar=progress_bar_html,
        progress_text=progress_text,
        retry_bar_html=retry_bar_html,
        retry_text=retry_text,
        retry_simple_bar_html=retry_simple_bar_html,
        retry_simple_text=retry_simple_text,
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        COLOR_SUCCESS=COLOR_SUCCESS,
        COLOR_FAILURE=COLOR_FAILURE,
        COLOR_CHUNKED=COLOR_CHUNKED,
        COLOR_SKIPPED=COLOR_SKIPPED,
    )

@app.route("/day")
def day_api():
    date_str = request.args.get("date", "").strip()
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        return jsonify({"error": "invalid date"}), 400
    return jsonify(day_counts(date_str))

@app.route("/month")
def month_api():
    try:
        y = int(request.args.get("year", "").strip())
        m = int(request.args.get("month", "").strip())
        if not (1 <= m <= 12):
            raise ValueError("bad month")
    except Exception:
        return jsonify({"error": "invalid year or month"}), 400
    return jsonify(month_counts(y, m))

if __name__ == "__main__":
    print(f"Dashboard running on port {PORT}")
    app.run(host="0.0.0.0", port=PORT, debug=False)