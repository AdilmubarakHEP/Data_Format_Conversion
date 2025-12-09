#!/usr/bin/env python3
# daemon/pb_conversion_once.py
# One-shot that:
#  1) initializes DB and catalog,
#  2) submits one conversion round,
#  3) does a quick log->DB scan,
#  4) triggers merges once (via 03c, honoring config),
#  5) prints progress and exits.

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import pb_conversion_daemon as d  # constants + helpers


def _scan_and_update(log_dir: str) -> int:
    try:
        files = sorted(
            f for f in os.listdir(log_dir)
            if f.endswith(".log") or f.endswith(".txt")
        )
    except FileNotFoundError:
        files = []
    all_status = {}
    for fn in files:
        p = os.path.join(log_dir, fn)
        try:
            status_dict = d.parse_log_file(p)
            all_status.update(status_dict)
        except Exception as e:
            print(f"[CRON] Could not parse {p}: {e}")
    if all_status:
        d.update_conversion_log(all_status)
    return len(all_status)


def main():
    # Idempotent setup
    d.initialize_db()
    d.load_pb_index_from_catalog()

    # Always submit a new round on each tick
    try:
        run_ts, log_dir = d.submit_conversion()
        print(f"[CRON] Submitted round {run_ts}. Logs: {log_dir}")
    except Exception as e:
        print(f"[CRON] submit_conversion failed: {e}")
        # Still try merges so the pipeline keeps moving
        d.submit_merge_jobs()
        total, conv, rem, pct = d._current_progress()
        d._log_progress_line(total, conv, rem, pct)
        return 1

    # Fast pass over logs so DB is not stale
    _scan_and_update(log_dir)

    # One merge kick per tick (via 03c orchestrator)
    d.submit_merge_jobs()

    # Progress snapshot
    total, conv, rem, pct = d._current_progress()
    d._log_progress_line(total, conv, rem, pct)
    
    # NEW: Export static dashboard
    try:
        import export_dashboard
        export_dir = os.path.join(d._REPO_ROOT, "public")
        export_dashboard.export_static_site(export_dir)
        print(f"[CRON] Exported dashboard to {export_dir}")
    except Exception as e:
        print(f"[CRON] Dashboard export failed: {e}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())