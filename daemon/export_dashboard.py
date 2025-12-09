#!/usr/bin/env python3
"""
Export static HTML dashboard + JSON data for GitLab Pages.
Exports ALL months with data, not just current month.
"""

import os
import json
import sqlite3
from datetime import datetime
from typing import Dict, List

import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from monitor_dashboard import (
    progress_counts, latest_retry_run, retry_progress_counts_for_run,
    month_counts, find_summary_jsons, extract_row_from_summary,
    parse_summary, scan_log_dir_for_fail_skip, SUMMARY_DIR, DB_PATH,
    COLOR_SUCCESS, COLOR_FAILURE, COLOR_CHUNKED, COLOR_SKIPPED,
    COLOR_R_CONVERTED, COLOR_R_FAILED, COLOR_R_ACTIVE, COLOR_R_QUEUED,
    conversions_table_has_rows, parse_all_summaries_grouped_by_date
)


def find_all_months_with_data():
    """Find all year-month combinations that have conversion data."""
    months_set = set()
    
    # Try DB first
    if conversions_table_has_rows():
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            c.execute("""
                SELECT DISTINCT substr(timestamp, 1, 7) as ym
                FROM conversions
                WHERE timestamp IS NOT NULL
                ORDER BY ym
            """)
            for row in c.fetchall():
                ym = row[0]  # format: "YYYY-MM"
                if ym and len(ym) == 7:
                    year, month = ym.split('-')
                    months_set.add((int(year), int(month)))
            conn.close()
        except Exception as e:
            print(f"[EXPORT] DB query failed: {e}")
    
    # Also scan summary files as fallback
    grouped = parse_all_summaries_grouped_by_date()
    for date_str in grouped.keys():
        if date_str and len(date_str) >= 7:
            year, month = date_str.split('-')[:2]
            months_set.add((int(year), int(month)))
    
    # Sort chronologically
    return sorted(list(months_set))


def export_static_site(output_dir: str):
    """Generate static HTML + JSON for GitLab Pages"""
    
    os.makedirs(output_dir, exist_ok=True)
    
    now = datetime.now()
    
    # Progress data
    total, converted, remaining, pct = progress_counts()
    
    # Find ALL months with data
    all_months = find_all_months_with_data()
    if not all_months:
        # Default to current month if no data found
        all_months = [(now.year, now.month)]
    
    print(f"[EXPORT] Found {len(all_months)} months with data")
    
    # Export data for each month
    months_data = {}
    for year, month in all_months:
        month_key = f"{year}-{month:02d}"
        month_data = month_counts(year, month)
        months_data[month_key] = month_data
        print(f"[EXPORT]   {month_key}: {month_data['totals']['success']} successes")
    
    # Recent summaries
    json_paths = find_summary_jsons(SUMMARY_DIR, limit=5)
    summary_rows = []
    for p in json_paths:
        try:
            summary_rows.append({
                "file": os.path.basename(p),
                "data": extract_row_from_summary(p)
            })
        except Exception as e:
            print(f"[EXPORT] Failed to parse {p}: {e}")
    
    # Retry data
    retry_data = None
    lr = latest_retry_run()
    if lr:
        counts = retry_progress_counts_for_run(lr["run_id"], lr.get("total_planned", 0))
        if counts["planned"] > 0:
            retry_data = {
                "counts": counts,
                "run_info": lr
            }
    
    # All-time success/fail counts
    total_success = total_failed = 0
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='conversions'")
        if c.fetchone():
            c.execute("SELECT COUNT(*) FROM conversions WHERE status='success'")
            total_success = int(c.fetchone()[0] or 0)
            c.execute("SELECT COUNT(*) FROM conversions WHERE status='failed'")
            total_failed = int(c.fetchone()[0] or 0)
        conn.close()
    except Exception as e:
        print(f"[EXPORT] DB query failed: {e}")
    
    # Export main data file
    data = {
        "progress": {
            "total": total,
            "converted": converted,
            "remaining": remaining,
            "pct": pct
        },
        "available_months": all_months,  # List of [year, month] pairs
        "months": months_data,            # All month data keyed by "YYYY-MM"
        "summaries": summary_rows,
        "retry": retry_data,
        "all_time": {
            "success": total_success,
            "failed": total_failed
        },
        "colors": {
            "success": COLOR_SUCCESS,
            "failure": COLOR_FAILURE,
            "chunked": COLOR_CHUNKED,
            "skipped": COLOR_SKIPPED,
            "r_converted": COLOR_R_CONVERTED,
            "r_failed": COLOR_R_FAILED,
            "r_active": COLOR_R_ACTIVE,
            "r_queued": COLOR_R_QUEUED
        },
        "timestamp": now.isoformat()
    }
    
    data_path = os.path.join(output_dir, "data.json")
    with open(data_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    # Generate static HTML
    html = generate_static_html()
    html_path = os.path.join(output_dir, "index.html")
    with open(html_path, 'w') as f:
        f.write(html)
    
    print(f"[EXPORT] Static site exported to {output_dir}")
    print(f"[EXPORT]   - {html_path}")
    print(f"[EXPORT]   - {data_path}")
    print(f"[EXPORT] Total months exported: {len(all_months)}")
    
    return html_path, data_path


def generate_static_html() -> str:
    """Generate self-contained HTML that fetches data.json client-side"""
    
    return """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Belle II PB→Parquet Conversion Monitor</title>
  <style>
    body { 
      font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      max-width: 1200px;
      margin: 0 auto;
      padding: 20px;
      background: #fafafa;
    }
    h1, h2, h3 { color: #333; }
    .progress-bar {
      background: #eee;
      border-radius: 8px;
      overflow: hidden;
      height: 24px;
      border: 1px solid #ddd;
      margin: 10px 0;
    }
    .progress-fill {
      height: 100%;
      transition: width 0.3s;
    }
    .stats { 
      font-family: monospace;
      margin: 8px 0;
      font-size: 14px;
    }
    .cal-wrap { max-width: 1100px; margin: 20px 0; }
    .cal-header { 
      display: flex;
      align-items: center;
      gap: 10px;
      margin: 12px 0;
    }
    .cal-nav-btn {
      padding: 6px 12px;
      border: 1px solid #ccc;
      border-radius: 6px;
      cursor: pointer;
      background: #fff;
    }
    .cal-nav-btn:hover { background: #f0f0f0; }
    .cal-title { font-weight: 600; font-size: 18px; min-width: 220px; }
    .cal-legend {
      display: flex;
      gap: 12px;
      font-size: 12px;
      color: #333;
    }
    .box {
      display: inline-block;
      width: 10px;
      height: 10px;
      margin-right: 4px;
      vertical-align: middle;
    }
    .cal-grid {
      display: grid;
      grid-template-columns: repeat(7, 1fr);
      gap: 6px;
    }
    .cal-dow {
      font-weight: 600;
      text-align: center;
      padding: 8px 0;
      background: #f3f3f3;
      border: 1px solid #e2e2e2;
      border-radius: 6px;
    }
    .cal-cell {
      border: 1px solid #e2e2e2;
      border-radius: 10px;
      min-height: 86px;
      padding: 6px 8px;
      background: #fff;
      display: flex;
      flex-direction: column;
      gap: 4px;
    }
    .cal-cell.inactive {
      background: #fbfbfb;
      color: #aaa;
    }
    .cal-cell:hover {
      outline: 2px solid #cfe0ff;
      cursor: pointer;
    }
    .cal-day {
      font-weight: 700;
      font-size: 14px;
    }
    .cal-line {
      font-size: 12px;
      display: flex;
      justify-content: space-between;
    }
    .cal-line .label { color: #555; }
    table {
      border-collapse: collapse;
      width: 100%;
      background: #fff;
      margin: 10px 0;
    }
    th, td {
      border: 1px solid #ddd;
      padding: 8px;
      text-align: left;
    }
    th { background: #f5f5f5; font-weight: 600; }
    .timestamp {
      margin-top: 30px;
      color: #666;
      font-size: 14px;
    }
    .loading {
      text-align: center;
      padding: 40px;
      color: #666;
    }
  </style>
</head>
<body>
  <h1>📊 Belle II .pb → .parquet Conversion Monitor</h1>
  
  <div id="loading" class="loading">Loading dashboard data...</div>
  <div id="content" style="display:none;">
    
    <h2>Overall Progress</h2>
    <div class="progress-bar">
      <div id="progressFill" class="progress-fill"></div>
    </div>
    <div id="progressStats" class="stats"></div>
    
    <div id="retrySection"></div>
    
    <hr style="margin: 30px 0;">
    
    <div class="cal-wrap">
      <div class="cal-header">
        <button id="prevMonth" class="cal-nav-btn">◀ Prev</button>
        <div id="calTitle" class="cal-title"></div>
        <button id="nextMonth" class="cal-nav-btn">Next ▶</button>
        <div class="cal-legend" id="legend"></div>
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
    
    <h3>Recent Submission Summaries</h3>
    <div id="summaryTable"></div>
    
    <div class="timestamp" id="timestamp"></div>
  </div>

  <script>
    let DATA = null;
    let curYear, curMonth;
    
    // Fetch and render
    fetch('data.json')
      .then(r => r.json())
      .then(data => {
        DATA = data;
        document.getElementById('loading').style.display = 'none';
        document.getElementById('content').style.display = 'block';
        renderAll();
      })
      .catch(err => {
        document.getElementById('loading').textContent = 'Error loading data: ' + err;
      });
    
    function renderAll() {
      renderProgress();
      renderRetry();
      renderLegend();
      
      // Start with most recent month that has data
      const months = DATA.available_months;
      if (months && months.length > 0) {
        const latest = months[months.length - 1];
        curYear = latest[0];
        curMonth = latest[1];
      } else {
        const now = new Date();
        curYear = now.getFullYear();
        curMonth = now.getMonth() + 1;
      }
      
      renderMonth();
      renderSummaries();
      
      const ts = new Date(DATA.timestamp);
      document.getElementById('timestamp').textContent = 
        'Last updated: ' + ts.toLocaleString();
    }
    
    function renderProgress() {
      const p = DATA.progress;
      const fill = document.getElementById('progressFill');
      fill.style.width = p.pct + '%';
      fill.style.background = DATA.colors.success;
      
      document.getElementById('progressStats').textContent =
        `${p.converted.toLocaleString()} of ${p.total.toLocaleString()} converted (${p.pct}%) • remaining ${p.remaining.toLocaleString()}`;
    }
    
    function renderRetry() {
      if (!DATA.retry) return;
      
      const r = DATA.retry.counts;
      const done = r.converted_ok + r.skipped;
      const pct = r.pct_done;
      
      const html = `
        <h2 style="margin-top: 28px;">Retry Progress</h2>
        <div class="progress-bar">
          <div style="width:${pct}%;height:100%;background:${DATA.colors.r_converted};"></div>
        </div>
        <div class="stats">
          ${done.toLocaleString()} of ${r.planned.toLocaleString()} retried (${pct}%) •
          active ${r.in_progress.toLocaleString()} •
          queued ${r.queued.toLocaleString()} •
          failed ${r.failed_again.toLocaleString()}
        </div>
      `;
      document.getElementById('retrySection').innerHTML = html;
    }
    
    function renderLegend() {
      const c = DATA.colors;
      document.getElementById('legend').innerHTML = `
        <span><span class="box" style="background:${c.success}"></span>Success</span>
        <span><span class="box" style="background:${c.chunked}"></span>Chunked</span>
        <span><span class="box" style="background:${c.skipped}"></span>Skipped</span>
        <span><span class="box" style="background:${c.failure}"></span>Failed</span>
      `;
    }
    
    function renderMonth() {
      const monthKey = `${curYear}-${String(curMonth).padStart(2, '0')}`;
      const monthData = DATA.months[monthKey];
      
      if (!monthData) {
        document.getElementById('calTitle').textContent = 
          new Date(curYear, curMonth - 1, 1).toLocaleString('en-US', {month:'long', year:'numeric'});
        document.getElementById('calGrid').innerHTML = '<p>No data for this month</p>';
        return;
      }
      
      const title = new Date(curYear, curMonth - 1, 1)
        .toLocaleString('en-US', {month:'long', year:'numeric'});
      document.getElementById('calTitle').textContent = title;
      
      const grid = document.getElementById('calGrid');
      grid.innerHTML = '';
      
      const dayMap = {};
      monthData.days.forEach(d => dayMap[d.date] = d);
      
      const firstDOW = new Date(curYear, curMonth - 1, 1).getDay();
      const mondayOffset = (firstDOW + 6) % 7;
      
      const daysInMonth = new Date(curYear, curMonth, 0).getDate();
      const totalCells = Math.ceil((mondayOffset + daysInMonth) / 7) * 7;
      
      for (let i = 0; i < totalCells; i++) {
        const cell = document.createElement('div');
        cell.className = 'cal-cell';
        
        let dayNum = i - mondayOffset + 1;
        if (dayNum < 1 || dayNum > daysInMonth) {
          cell.classList.add('inactive');
          dayNum = dayNum < 1 ? dayNum + daysInMonth : dayNum - daysInMonth;
        }
        
        const iso = `${curYear}-${String(curMonth).padStart(2,'0')}-${String(dayNum).padStart(2,'0')}`;
        const stats = dayMap[iso] || {success:0, chunked:0, skipped:0, failed:0};
        
        cell.innerHTML = `
          <div class="cal-day">${dayNum}</div>
          <div class="cal-line"><span class="label">S</span><span style="color:${DATA.colors.success}">${stats.success}</span></div>
          <div class="cal-line"><span class="label">C</span><span style="color:${DATA.colors.chunked}">${stats.chunked}</span></div>
          <div class="cal-line"><span class="label">Sk</span><span style="color:${DATA.colors.skipped}">${stats.skipped}</span></div>
          <div class="cal-line"><span class="label">F</span><span style="color:${DATA.colors.failure}">${stats.failed}</span></div>
        `;
        
        grid.appendChild(cell);
      }
    }
    
    function renderSummaries() {
      if (!DATA.summaries.length) {
        document.getElementById('summaryTable').innerHTML = '<p>No recent summaries</p>';
        return;
      }
      
      let rows = '';
      DATA.summaries.forEach(s => {
        const d = s.data;
        rows += `<tr>
          <td>${d[0]}</td><td>${d[1]}</td><td>${d[2]}</td><td>${d[3]}</td>
          <td>${d[4]}</td><td>${d[5]}</td><td>${d[6]}</td><td>${d[7]}</td>
          <td>${d[8]}</td><td>${d[9]}</td><td>${d[10]}</td>
        </tr>`;
      });
      
      document.getElementById('summaryTable').innerHTML = `
        <table>
          <tr>
            <th>File</th><th>Jobs</th><th>PB Files</th><th>Chunks</th>
            <th>Peak RAM (MB)</th><th>PB Size (MB)</th><th>PQ Size (MB)</th>
            <th>Numeric PVs</th><th>Text PVs</th><th>Scalar PVs</th><th>Vector PVs</th>
          </tr>
          ${rows}
        </table>
      `;
    }
    
    function hasDataForMonth(y, m) {
      const key = `${y}-${String(m).padStart(2, '0')}`;
      return DATA.months[key] !== undefined;
    }
    
    document.getElementById('prevMonth').onclick = () => {
      let y = curYear, m = curMonth - 1;
      if (m < 1) { m = 12; y--; }
      
      // Keep going back until we find a month with data or hit limits
      let attempts = 0;
      while (!hasDataForMonth(y, m) && attempts < 24) {
        m--;
        if (m < 1) { m = 12; y--; }
        attempts++;
      }
      
      if (hasDataForMonth(y, m)) {
        curYear = y;
        curMonth = m;
        renderMonth();
      }
    };
    
    document.getElementById('nextMonth').onclick = () => {
      let y = curYear, m = curMonth + 1;
      if (m > 12) { m = 1; y++; }
      
      // Keep going forward until we find a month with data or hit limits
      let attempts = 0;
      while (!hasDataForMonth(y, m) && attempts < 24) {
        m++;
        if (m > 12) { m = 1; y++; }
        attempts++;
      }
      
      if (hasDataForMonth(y, m)) {
        curYear = y;
        curMonth = m;
        renderMonth();
      }
    };
  </script>
</body>
</html>
"""


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Export static dashboard to GitLab Pages")
    parser.add_argument("--output", default="/home/belle2/amubarak/Data_Format_Conversion/public",
                        help="Output directory for static site")
    args = parser.parse_args()
    
    export_static_site(args.output)