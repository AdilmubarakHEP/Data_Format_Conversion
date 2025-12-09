#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
03b-merge-date.py

Date Merge Worker
Merges all PV Parquets for a given date (YYYY_MM_DD) into one date-level Parquet.

Can be called:
- Via bsub with --internal --date YYYY_MM_DD for single date (typical use)
- Directly without --internal to submit jobs for all dates

Uses per-date locks to prevent concurrent merges of same date.

Config (configparser)
- [paths].output_base
- [merge].output_dir
- [paths].logs_root
- [paths].db_path
- [lsf].queue
- [merge].merge_delete_parquet_after_date
- [merge].merge_stale_running_minutes
- Optional env: PVPIPE_CONFIG
"""

import os
import re
import sys
import fcntl
import sqlite3
import argparse
import configparser
from datetime import datetime, timedelta
from collections import defaultdict
from typing import List, Dict, Tuple, Optional

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.types as pat

# ---- config ----
CONFIG_PATH = os.environ.get(
    "PVPIPE_CONFIG",
    "/home/belle2/amubarak/Data_Format_Conversion/config.ini",
)
_cfg = configparser.ConfigParser()
if not _cfg.read(CONFIG_PATH):
    raise SystemExit(f"[FATAL] config.ini not found or unreadable at {CONFIG_PATH}")

def _cfg_str(sec, key, default=None):
    try:
        v = _cfg.get(sec, key)
        return v.strip() if v is not None else default
    except Exception:
        return default

def _cfg_bool(sec, key, default=False):
    try:
        return _cfg.getboolean(sec, key)
    except Exception:
        return bool(default)

def _cfg_int(sec, key, default):
    try:
        return _cfg.getint(sec, key)
    except Exception:
        return int(default)

OUTPUT_BASE = _cfg_str("paths", "output_base", "/home/belle2/amubarak/PV_Output/converted_flat2")

MERGE_OUTPUT_DIR = _cfg_str("merge", "output_dir", None)
if not MERGE_OUTPUT_DIR:
    MERGE_OUTPUT_DIR = _cfg_str("merge", "merge_output_dir", "/home/belle2/amubarak/PV_Output/merged")

LOGS_ROOT = _cfg_str("paths", "logs_root", "/home/belle2/amubarak/Data_Format_Conversion/logs")
DB_PATH   = _cfg_str("paths", "db_path", "/home/belle2/amubarak/conversion_log.db")

LSF_QUEUE = _cfg_str("lsf", "queue", "h")

DELETE_AFTER_DATE_MERGE = _cfg_bool("merge", "merge_delete_parquet_after_date", False)
STALE_RUNNING_MINUTES   = _cfg_int("merge", "merge_stale_running_minutes", 60)

# ---------------- utils ----------------
DATE_RE      = re.compile(r"(\d{4}_\d{2}_\d{2})")
DATE_STRICT  = re.compile(r"^\d{4}_\d{2}_\d{2}$")
CHUNK_RE     = re.compile(r"^(?P<base>.+)_chunk(?P<idx>\d{4})\.parquet$")

def _ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def _rmdir_if_empty(path: str):
    try:
        if os.path.isdir(path) and not os.listdir(path):
            os.rmdir(path)
    except Exception:
        pass

def is_chunk_file(filename: str) -> bool:
    return bool(CHUNK_RE.match(filename))

def extract_date_ymd_from_filename(filename: str) -> Optional[str]:
    m = DATE_RE.search(filename)
    return m.group(1) if m else None

def _date_paths(date_str: str, create_dirs: bool) -> Tuple[str, str, str]:
    """Return (log_path, lock_path, state_path). Only create dirs when create_dirs=True."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir   = os.path.join(LOGS_ROOT, "merge")
    lock_dir  = os.path.join(base_dir, "locks", "merge")
    state_dir = os.path.join(base_dir, "state", "merge")
    if create_dirs:
        _ensure_dir(log_dir); _ensure_dir(lock_dir); _ensure_dir(state_dir)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path  = os.path.join(log_dir, f"merge_{date_str}_{ts}.log")
    lock_path = os.path.join(lock_dir, f"date-{date_str}.lock")
    state_path= os.path.join(state_dir, f"date-{date_str}.done")
    return log_path, lock_path, state_path

# Aggregate failure log
AGG_DIR  = os.path.join(LOGS_ROOT, "merge")
AGG_FAIL = os.path.join(AGG_DIR, "merge_failures_all.log")
_ensure_dir(AGG_DIR)

def _append_lines_locked(file_path: str, lines: List[str]) -> None:
    os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)
    fd = os.open(file_path, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o644)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
        except Exception:
            pass
        with os.fdopen(fd, "a", encoding="utf-8", buffering=1) as f:
            for ln in lines:
                f.write(ln.rstrip("\n") + "\n")
            try:
                f.flush(); os.fsync(f.fileno())
            except Exception:
                pass
        fd = None
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass

def _agg_fail(date_str: str, msg: str) -> None:
    ts = datetime.now().isoformat(timespec="seconds")
    _append_lines_locked(AGG_FAIL, [f"{ts}  date={date_str}  status=failed  msg={msg}"])

def _log_to(path: str, msg: str):
    print(msg)
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write(msg + "\n")
    except Exception:
        pass

def find_all_dates(input_dir: str) -> Dict[str, List[str]]:
    date_to_files: Dict[str, List[str]] = defaultdict(list)
    for root, _, files in os.walk(input_dir):
        for fn in files:
            if not fn.endswith(".parquet"):
                continue
            if is_chunk_file(fn):
                continue
            date_str = extract_date_ymd_from_filename(fn)
            if not date_str:
                continue
            full_path = os.path.join(root, fn)
            date_to_files[date_str].append(full_path)
    return date_to_files

# ---------------- schema helpers ----------------
def _is_list(dt: pa.DataType) -> bool:
    return pat.is_list(dt) or pat.is_large_list(dt)

def _elem_type(dt: pa.DataType) -> Optional[pa.DataType]:
    if _is_list(dt):
        return dt.value_type
    return None

def _promote_numeric_scalar_to() -> pa.DataType:
    return pa.float64()

def _promote_numeric_list_to() -> pa.DataType:
    return pa.list_(pa.float64())

def _promote_string_scalar_to() -> pa.DataType:
    return pa.string()

def _promote_string_list_to() -> pa.DataType:
    return pa.list_(pa.string())

def collect_promoted_field_types(tables: List[pa.Table]) -> Dict[str, pa.DataType]:
    from collections import defaultdict as _dd
    seen: Dict[str, Dict[str, bool]] = _dd(lambda: _dd(bool))
    examples: Dict[str, pa.DataType] = {}
    for t in tables:
        sch = t.schema
        for i, name in enumerate(sch.names):
            typ = sch.types[i]
            examples.setdefault(name, typ)
            if _is_list(typ):
                et = _elem_type(typ)
                if et and (pat.is_integer(et) or pat.is_floating(et)):
                    seen[name]["list_numeric"] = True
                elif et and pat.is_string(et):
                    seen[name]["list_string"] = True
                else:
                    seen[name]["other"] = True
            else:
                if pat.is_integer(typ) or pat.is_floating(typ):
                    seen[name]["scalar_numeric"] = True
                elif pat.is_string(typ):
                    seen[name]["scalar_string"] = True
                else:
                    seen[name]["other"] = True
    targets: Dict[str, pa.DataType] = {}
    for name, flags in seen.items():
        if flags.get("list_numeric") or (flags.get("scalar_numeric") and flags.get("list_numeric")):
            targets[name] = _promote_numeric_list_to()
        elif flags.get("scalar_numeric"):
            targets[name] = _promote_numeric_scalar_to()
        elif flags.get("list_string") or (flags.get("scalar_string") and flags.get("list_string")):
            targets[name] = _promote_string_list_to()
        elif flags.get("scalar_string"):
            targets[name] = _promote_string_scalar_to()
        else:
            targets[name] = examples[name]
    return targets

def _wrap_scalar_listify(py_vals, cast_elem):
    out = []
    for v in py_vals:
        out.append(None if v is None else [cast_elem(v)])
    return out

def _cast_list_elems(py_lists, cast_elem):
    out = []
    for lst in py_lists:
        out.append(None if lst is None else [cast_elem(x) for x in lst])
    return out

def align_table_to_types(tbl: pa.Table, targets: Dict[str, pa.DataType]) -> pa.Table:
    n = tbl.num_rows
    current_cols = set(tbl.column_names)
    cols = []
    names = []
    for name, target in targets.items():
        if name in current_cols:
            col = tbl[name]
            src_type = col.type
            if src_type == target:
                cols.append(col); names.append(name); continue
            if target.equals(_promote_numeric_scalar_to()):
                if pat.is_integer(src_type) or pat.is_floating(src_type):
                    cols.append(col.cast(pa.float64())); names.append(name); continue
            if target.equals(_promote_numeric_list_to()):
                if _is_list(src_type):
                    et = _elem_type(src_type)
                    if et and (pat.is_integer(et) or pat.is_floating(et)):
                        py = col.to_pylist()
                        casted = _cast_list_elems(py, float)
                        cols.append(pa.array(casted, type=pa.list_(pa.float64()))); names.append(name); continue
                if pat.is_integer(src_type) or pat.is_floating(src_type):
                    py = col.to_pylist()
                    wrapped = _wrap_scalar_listify(py, float)
                    cols.append(pa.array(wrapped, type=pa.list_(pa.float64()))); names.append(name); continue
            if target.equals(_promote_string_scalar_to()):
                if pat.is_string(src_type):
                    cols.append(col); names.append(name); continue
            if target.equals(_promote_string_list_to()):
                if _is_list(src_type):
                    et = _elem_type(src_type)
                    if et and pat.is_string(et):
                        py = col.to_pylist()
                        casted = _cast_list_elems(py, str)
                        cols.append(pa.array(casted, type=pa.list_(pa.string()))); names.append(name); continue
                if pat.is_string(src_type):
                    py = col.to_pylist()
                    wrapped = _wrap_scalar_listify(py, str)
                    cols.append(pa.array(wrapped, type=pa.list_(pa.string()))); names.append(name); continue
            try:
                cols.append(col.cast(target))
            except Exception:
                cols.append(pa.nulls(n, type=target))
            names.append(name)
        else:
            cols.append(pa.nulls(n, type=target))
            names.append(name)
    return pa.Table.from_arrays(cols, names=names)

def _dedupe_by_pv_keep_last(tbl: pa.Table) -> pa.Table:
    if "pv" not in tbl.column_names:
        return tbl
    pvs = tbl["pv"].to_pylist()
    last_index = {}
    for i, k in enumerate(pvs):
        last_index[k] = i
    keep_idxs = sorted(last_index.values())
    return tbl.take(pa.array(keep_idxs, type=pa.int64()))

# ---------------- DB helpers ----------------
def _write_merge_run(db_path: str, run_id: str, date_str: str, status: str, message: str,
                     out_path: Optional[str], n_sources: int):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("""
      CREATE TABLE IF NOT EXISTS merge_runs (
        run_id TEXT PRIMARY KEY,
        date TEXT,
        started_at TEXT,
        finished_at TEXT,
        status TEXT,
        message TEXT,
        out_path TEXT,
        n_sources INTEGER
      )
    """)
    now = datetime.now().isoformat(timespec="seconds")
    finished_val = None if status == "starting" else now
    c.execute("""
      INSERT INTO merge_runs(run_id, date, started_at, finished_at, status, message, out_path, n_sources)
      VALUES (?, ?, COALESCE((SELECT started_at FROM merge_runs WHERE run_id=?), ?), ?, ?, ?, ?, ?)
      ON CONFLICT(run_id) DO UPDATE SET
        finished_at=excluded.finished_at,
        status=excluded.status,
        message=excluded.message,
        out_path=excluded.out_path,
        n_sources=excluded.n_sources
    """, (run_id, date_str, run_id, now, finished_val, status, message, out_path, n_sources))
    conn.commit(); conn.close()

def _fingerprint(paths: List[str]) -> str:
    parts = []
    for p in paths:
        try:
            st = os.stat(p)
            parts.append(f"{p}|{st.st_size}|{int(st.st_mtime)}")
        except FileNotFoundError:
            continue
    return "\n".join(parts)

def _date_locked(date_str: str) -> bool:
    # Check without creating any directories or files
    _, lock_path, _ = _date_paths(date_str, create_dirs=False)
    lock_dir = os.path.dirname(lock_path)
    if not os.path.isdir(lock_dir):
        return False
    if not os.path.exists(lock_path):
        return False
    try:
        fd = os.open(lock_path, os.O_RDWR)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            # If we acquired it, then no one else holds it
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
            return False
        except BlockingIOError:
            os.close(fd)
            return True
    except Exception:
        return False

def _db_thinks_running(date_str: str) -> bool:
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
          SELECT started_at FROM merge_runs
          WHERE date=? AND status='starting' AND (finished_at IS NULL OR finished_at='')
          ORDER BY started_at DESC LIMIT 1
        """, (date_str,))
        row = c.fetchone()
        conn.close()
    except Exception:
        return False
    if not row or not row[0]:
        return False
    try:
        started = datetime.fromisoformat(row[0])
        age_min = (datetime.now() - started).total_seconds() / 60.0
        return age_min < STALE_RUNNING_MINUTES
    except Exception:
        return True

# ---------------- merge core ----------------
def merge_one_date(date_str: str, file_list: List[str], output_dir: str, delete_inputs: bool=False) -> bool:
    if not DATE_STRICT.match(date_str):
        msg = f"Invalid date token (expected YYYY_MM_DD): {date_str}"
        print(f"❌ {msg}")
        _agg_fail(date_str, msg)
        return False

    # Create dirs only for the actual run
    log_path, lock_path, done_marker = _date_paths(date_str, create_dirs=True)
    lock_fd = None
    sources = sorted(file_list)
    run_id = f"{date_str}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{os.getpid()}"
    _write_merge_run(DB_PATH, run_id, date_str, "starting", "starting merge", None, len(sources))

    def _cleanup_lock_and_state():
        # unlock and close handled in finally
        try:
            if os.path.exists(lock_path):
                os.unlink(lock_path)
        except Exception:
            pass
        try:
            if os.path.exists(done_marker):
                os.unlink(done_marker)
        except Exception:
            pass
        # try to remove empty leaf dirs and parents
        try:
            lock_dir = os.path.dirname(lock_path)
            locks_root = os.path.dirname(lock_dir)
            _rmdir_if_empty(lock_dir)
            _rmdir_if_empty(locks_root)
        except Exception:
            pass
        try:
            state_dir = os.path.dirname(done_marker)
            state_root = os.path.dirname(state_dir)
            _rmdir_if_empty(state_dir)
            _rmdir_if_empty(state_root)
        except Exception:
            pass

    _log_to(log_path, f"[MERGE {date_str}] Found {len(sources)} parquet parts to merge.")
    if not sources:
        _write_merge_run(DB_PATH, run_id, date_str, "noop", "no sources", None, 0)
        _log_to(log_path, f"[MERGE {date_str}] No sources; nothing to do.")
        _cleanup_lock_and_state()
        return False

    fp = _fingerprint(sources)
    if os.path.isfile(done_marker):
        try:
            prev = open(done_marker, "r", encoding="utf-8").read()
            if prev == fp:
                _write_merge_run(DB_PATH, run_id, date_str, "skipped", "fingerprint unchanged", None, len(sources))
                _log_to(log_path, f"[MERGE {date_str}] Snapshot unchanged; skipping.")
                _cleanup_lock_and_state()
                return True
        except Exception:
            pass

    # take lock
    lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        msg = "another merge holds the lock"
        _write_merge_run(DB_PATH, run_id, date_str, "locked", msg, None, len(sources))
        _log_to(log_path, f"[MERGE {date_str}] Another merge holds the lock; exiting.")
        _agg_fail(date_str, msg)
        # clean any stray marker from our attempt
        _cleanup_lock_and_state()
        return False

    output_path = os.path.join(output_dir, f"{date_str}.parquet")
    tmp_path    = output_path + ".tmp"
    try:
        os.makedirs(output_dir, exist_ok=True)
        tables: List[pa.Table] = []

        if os.path.exists(output_path):
            try:
                tables.append(pq.read_table(output_path))
            except Exception as e:
                _log_to(log_path, f"[MERGE {date_str}] Could not read existing {output_path}: {e}")

        for f in sources:
            try:
                tables.append(pq.read_table(f))
            except Exception as e:
                _log_to(log_path, f"[MERGE {date_str}] Failed to read {f}: {e}")

        if not tables:
            msg = "no valid inputs"
            _log_to(log_path, f"[MERGE {date_str}] No valid input tables.")
            _write_merge_run(DB_PATH, run_id, date_str, "failed", msg, None, 0)
            _agg_fail(date_str, msg)
            return False

        targets = collect_promoted_field_types(tables)
        tables_fixed = [align_table_to_types(t, targets) for t in tables]
        merged_all   = pa.concat_tables(tables_fixed, promote=True)
        merged_final = _dedupe_by_pv_keep_last(merged_all)

        pq.write_table(merged_final, tmp_path, compression="snappy")
        if os.path.exists(output_path):
            os.remove(output_path)
        os.replace(tmp_path, output_path)

        with open(done_marker, "w", encoding="utf-8") as f:
            f.write(fp)

        _write_merge_run(DB_PATH, run_id, date_str, "success", "merged OK", output_path, len(sources))
        _log_to(log_path, f"[MERGE {date_str}] ✅ merged {len(sources)} files → {output_path} (rows={merged_final.num_rows})")
        print(f"✅ Date {date_str}: merged {len(file_list)} files → {output_path} (rows={merged_final.num_rows})")

        if delete_inputs:
            for f in sources:
                try:
                    os.remove(f)
                except Exception as e:
                    _log_to(log_path, f"[MERGE {date_str}] Could not delete {f}: {e}")

        return True

    except Exception as e:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        msg = f"{type(e).__name__}: {e}"
        _write_merge_run(DB_PATH, run_id, date_str, "failed", msg, None, len(sources))
        _log_to(log_path, f"[MERGE {date_str}] merge failed: {msg}")
        _agg_fail(date_str, msg)
        return False

    finally:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            os.close(lock_fd)
        except Exception:
            pass
        # delete lock and state, then clean empty dirs
        _cleanup_lock_and_state()

# ---------------- submission ----------------
def submit_merge_date_job(script_path: str, date_str: str,
                          input_dir: str, output_dir: str,
                          delete_flag: bool, queue: str) -> int:
    argv = ["bsub", "-q", str(queue), "python3", script_path, "--internal", "--date", date_str,
            "--input_dir", input_dir, "--output_dir", output_dir]
    if delete_flag:
        argv.append("--delete_after_date")
    try:
        import subprocess
        r = subprocess.run(argv, check=False, capture_output=True, text=True)
        if r.returncode != 0:
            _agg_fail(date_str, f"bsub rc={r.returncode} stdout={r.stdout.strip()} stderr={r.stderr.strip()}")
            print(f"❌ bsub failed for {date_str}: rc={r.returncode}\n{r.stdout}\n{r.stderr}")
        return r.returncode
    except Exception as e:
        _agg_fail(date_str, f"bsub exception: {type(e).__name__}: {e}")
        print(f"❌ bsub exception for {date_str}: {e}")
        return 1

# ---------------- main ----------------
def main():
    ap = argparse.ArgumentParser(description="Submit or run per-date merges with guards.")
    ap.add_argument("--input_dir",  default=OUTPUT_BASE,      help="Where PV Parquets live")
    ap.add_argument("--output_dir", default=MERGE_OUTPUT_DIR, help="Where to write date-level merged Parquets")
    ap.add_argument("--queue",      default=LSF_QUEUE,        help="LSF queue for per-date submissions")
    ap.add_argument("--delete_after_date", action="store_true", default=DELETE_AFTER_DATE_MERGE,
                    help="Delete per-PV inputs after date merge")
    ap.add_argument("--internal", action="store_true",
                    help="Run a single date inline. Requires --date.")
    ap.add_argument("--date", help="YYYY_MM_DD. With --internal: run inline. Without --internal: submit only this date.")
    args = ap.parse_args()

    script_path = os.path.abspath(__file__)

    if args.internal:
        if not args.date or not re.match(r"^\d{4}_\d{2}_\d{2}$", args.date):
            print("❌ --internal requires --date YYYY_MM_DD")
            sys.exit(2)
        date = args.date
        date_map = find_all_dates(args.input_dir)
        files = date_map.get(date, [])
        if not files:
            print(f"❌ No PV Parquets found for date {date}")
            sys.exit(0)
        ok = merge_one_date(date, files, args.output_dir, delete_inputs=args.delete_after_date)
        sys.exit(0 if ok else 1)
        return

    date_map = find_all_dates(args.input_dir)
    if not date_map:
        print("No dates found for date-level merge.")
        return

    items = [(args.date, date_map.get(args.date, []))] if args.date else sorted(date_map.items())
    for date_str, file_list in items:
        if not date_str:
            continue
        # Check without creating any dirs or files
        if _date_locked(date_str) or _db_thinks_running(date_str):
            print(f"Skip {date_str}: merge already running (lock or DB).")
            continue
        rc = submit_merge_date_job(script_path, date_str, args.input_dir, args.output_dir,
                                   delete_flag=args.delete_after_date, queue=args.queue)
        if rc == 0:
            print(f"Submitted merge job for {date_str} (n={len(file_list)})")
        else:
            print(f"Failed to submit merge job for {date_str}")

if __name__ == "__main__":
    main()