#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 00-submit_pb2parquet_jobs.py — config.ini is the single source of truth.
# - Absolutely no hardcoded defaults.
# - If a required key is missing/blank, the script exits with a clear message
#   showing the exact lines to add to Data_Format_Conversion/config.ini.
#
# Required sections/keys (all must exist; some may be allowed blank as noted):
# [paths]
#   input_dir, chunks_dir, logs_root, logs_subdir_pb2parquet, db_path,
#   small_script, big_chunker_script, too_large_txt
# [exclude]
#   subpaths   (can be empty)
# [lsf]
#   queue, max_concurrent_bsubs, kill_after_hours, job_group, job_name_prefix,
#   poll_sec, lsf_aware_sweep, kill_on_sweep, queued_sweep_hours
# [submitter]
#   chunk_size, big_threshold_gb, max_pb_gb, rss_cap_gb,
#   split_mode, split_granularity, tz_offset_hours, emit_date_view,
#   date_view_root, ooo_max_open_buckets, max_bsub_per_submit_round,
#   allow_out_of_order
# [catalog]
#   delta_scan_minutes
#
# Optional CLI (no value fallbacks; only operational toggles):
#   --submit, --rebuild_catalog, --full_scan, --catalog_echo_cmd,
#   --max_files, --exclude_subpath (repeatable), --exclude_filelist,
#   --force_big_subpath (repeatable)

import os
import re
import sys
import argparse
import sqlite3
import glob
import socket
import subprocess
import time
import getpass
import shutil
from typing import Optional, List, Tuple, Dict
from datetime import datetime, timedelta
from importlib.machinery import SourceFileLoader

# -------------------- INI loader (no defaults) --------------------
import configparser

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(HERE))
CONFIG_PATH = os.environ.get("PVPIPE_CONFIG",
                             os.path.join(REPO_ROOT, "config.ini"))

cfg = configparser.ConfigParser()
if not cfg.read(CONFIG_PATH):
    raise SystemExit(f"[FATAL] config.ini not found or unreadable at {CONFIG_PATH}")

def _err_missing(section: str, lines: List[str]):
    block = ["", f"[{section}]"] + lines
    msg = [
        f"[FATAL] Missing or empty required config in [{section}].",
        "Add the following lines to Data_Format_Conversion/config.ini:",
        "\n".join(block),
        ""
    ]
    raise SystemExit("\n".join(msg))

def _has(section: str, key: str) -> bool:
    return cfg.has_option(section, key) and cfg.get(section, key).strip() != ""

def _get_str(section: str, key: str, example_line: str, allow_blank: bool = False) -> str:
    if not cfg.has_option(section, key):
        _err_missing(section, [example_line])
    val = cfg.get(section, key)
    if not allow_blank and (val is None or val.strip() == ""):
        _err_missing(section, [example_line])
    return val.strip()

def _get_int(section: str, key: str, example_line: str) -> int:
    s = _get_str(section, key, example_line)
    try:
        return int(s)
    except Exception:
        _err_missing(section, [example_line + "   # must be an integer"])
        return 0  # unreachable

def _get_float(section: str, key: str, example_line: str) -> float:
    s = _get_str(section, key, example_line)
    try:
        return float(s)
    except Exception:
        _err_missing(section, [example_line + "   # must be a number"])
        return 0.0  # unreachable

def _get_bool(section: str, key: str, example_line: str) -> bool:
    s = _get_str(section, key, example_line)
    v = s.strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off"):
        return False
    _err_missing(section, [example_line + "   # true/false"])
    return False  # unreachable

def _get_optional_int(section: str, key: str, example_line: str) -> Optional[int]:
    if not cfg.has_option(section, key):
        return None
    s = cfg.get(section, key)
    if s is None or s.strip() == "":
        return None
    try:
        return int(s.strip())
    except Exception:
        _err_missing(section, [example_line + "   # must be an integer or blank"])
        return None  # unreachable

def _split_tokens(s: str) -> List[str]:
    # split on comma, semicolon, whitespace, or newline
    raw = re.split(r"[,\n;\s]+", s.strip())
    return [t for t in raw if t]

# -------------------- Required config (strings/numbers/bools) --------------------
# [paths]
INPUT_DIR   = _get_str("paths", "input_dir", "input_dir = /path/to/pv/2024/AA/")
CHUNKS_DIR  = _get_str("paths", "chunks_dir", "chunks_dir = /home/belle2/amubarak/PV_Output")
LOGS_ROOT   = _get_str("paths", "logs_root", "logs_root = /home/belle2/amubarak/Data_Format_Conversion/logs")
LOGS_SUB    = _get_str("paths", "logs_subdir_pb2parquet", "logs_subdir_pb2parquet = pb2parquet")
DB_PATH     = _get_str("paths", "db_path", "db_path = /home/belle2/amubarak/conversion_log.db")
SMALL_SCRIPT = _get_str("paths", "small_script", "small_script = /home/belle2/amubarak/Data_Format_Conversion/01-small_script.py")
BIG_CHUNKER  = _get_str("paths", "big_chunker_script", "big_chunker_script = /home/belle2/amubarak/Data_Format_Conversion/02-big_chunker_script.py")
TOO_LARGE_TXT = _get_str("paths", "too_large_txt", "too_large_txt = /home/belle2/amubarak/Data_Format_Conversion/logs/pb2parquet/too_large_pb_files.txt")

LOG_PARENT = os.path.join(LOGS_ROOT, LOGS_SUB)

# [exclude]
EXCLUDE_SUBPATHS_RAW = _get_str("exclude", "subpaths",
                                "subpaths = /gpfs/.../LM/", allow_blank=True)
EXCLUDE_TOKENS_CFG = _split_tokens(EXCLUDE_SUBPATHS_RAW) if EXCLUDE_SUBPATHS_RAW else []

# [lsf]
QUEUE      = _get_str("lsf", "queue", "queue = h")
MAX_RUNNING = _get_int("lsf", "max_concurrent_bsubs", "max_concurrent_bsubs = 500")
KILL_AFTER_HOURS = _get_float("lsf", "kill_after_hours", "kill_after_hours = 24")
JOB_GROUP  = _get_str("lsf", "job_group", "job_group = /pvpipe", allow_blank=True)
JOB_PREFIX = _get_str("lsf", "job_name_prefix", "job_name_prefix = pvpipe_")
POLL_SEC   = _get_int("lsf", "poll_sec", "poll_sec = 20")
LSF_AWARE_SWEEP = _get_bool("lsf", "lsf_aware_sweep", "lsf_aware_sweep = true")
KILL_ON_SWEEP   = _get_bool("lsf", "kill_on_sweep", "kill_on_sweep = true")
QUEUED_SWEEP_HOURS = _get_float("lsf", "queued_sweep_hours", "queued_sweep_hours = 24")

# [submitter]
CHUNK_SIZE      = _get_int("submitter", "chunk_size", "chunk_size = 100")
BIG_THRESHOLD_GB = _get_float("submitter", "big_threshold_gb", "big_threshold_gb = 1.0")
MAX_PB_GB       = _get_float("submitter", "max_pb_gb", "max_pb_gb = 1000.0")
RSS_CAP_GB      = _get_float("submitter", "rss_cap_gb", "rss_cap_gb = 0.05")

SPLIT_MODE      = _get_str("submitter", "split_mode", "split_mode = auto")  # auto|on|off
SPLIT_GRAN      = _get_str("submitter", "split_granularity", "split_granularity = day")
TZ_OFFSET_RAW   = _get_str("submitter", "tz_offset_hours", "tz_offset_hours = ", allow_blank=True)
EMIT_DVIEW      = _get_bool("submitter", "emit_date_view", "emit_date_view = true")
DVIEW_ROOT      = _get_str("submitter", "date_view_root", "date_view_root = ", allow_blank=True)
OOO_MAX_OPEN    = _get_int("submitter", "ooo_max_open_buckets", "ooo_max_open_buckets = 8")
MAX_BSUB_JOBS   = _get_int("submitter", "max_bsub_per_submit_round", "max_bsub_per_submit_round = 1000")
ALLOW_OOO       = _get_bool("submitter", "allow_out_of_order", "allow_out_of_order = false")

# Optional clean-up window for bundle lists
BUNDLES_KEEP_DAYS = _get_optional_int("submitter", "bundles_keep_days", "bundles_keep_days = 7")

TZ_OFFSET = None
if TZ_OFFSET_RAW.strip() != "":
    try:
        TZ_OFFSET = int(TZ_OFFSET_RAW)
    except Exception:
        _err_missing("submitter", ["tz_offset_hours = 9   # example; must be integer or empty"])

# [catalog]
DELTA_SCAN_MIN = _get_int("catalog", "delta_scan_minutes", "delta_scan_minutes = 0")

# -------------------- CLI (operational toggles only) --------------------
parser = argparse.ArgumentParser(
    description="Submit .pb -> .parquet jobs using bsub (catalog-first). Config is the single source of truth."
)
parser.add_argument("--submit", action="store_true", help="Actually submit jobs (else dry run)")
parser.add_argument("--rebuild_catalog", action="store_true", help="Full filesystem scan -> persist catalog")
parser.add_argument("--full_scan", action="store_true", help="Bypass catalog for THIS run (no persist)")
parser.add_argument("--catalog_echo_cmd", action="store_true", help="Echo the 'find' command during rebuild/delta")
parser.add_argument("--max_files", type=int, default=None, help="Cap NEW .pb candidates for this run")
parser.add_argument("--exclude_subpath", action="append", default=[], help="Extra exclude token (repeatable)")
parser.add_argument("--exclude_filelist", default=None, help="Text file with one substring per line to exclude")
parser.add_argument("--force_big_subpath", action="append", default=[], help="Force-BIG token (repeatable)")
parser.add_argument("-f", default=None)  # compatibility
args = parser.parse_args()

# -------------------- Runtime dirs --------------------
RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_DIR = os.environ.get("LOG_DIR_EXTERNAL") or os.path.join(LOG_PARENT, RUN_TS)
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(CHUNKS_DIR, exist_ok=True)

# New: per-run bundle directory to avoid clobbering across batches
BUNDLES_ROOT = os.path.join(CHUNKS_DIR, "bundles")
BUNDLES_DIR  = os.path.join(BUNDLES_ROOT, RUN_TS)
os.makedirs(BUNDLES_ROOT, exist_ok=True)
os.makedirs(BUNDLES_DIR, exist_ok=True)

def _purge_old_bundles(root: str, keep_days: Optional[int]):
    if keep_days is None or keep_days <= 0:
        return
    cutoff = time.time() - keep_days * 86400
    try:
        for name in os.listdir(root):
            path = os.path.join(root, name)
            if not os.path.isdir(path):
                continue
            # expect names like YYYYmmdd_HHMMSS
            try:
                dt = datetime.strptime(name, "%Y%m%d_%H%M%S")
            except ValueError:
                continue
            if dt.timestamp() < cutoff:
                shutil.rmtree(path, ignore_errors=True)
    except Exception:
        pass

# purge once per run
_purge_old_bundles(BUNDLES_ROOT, BUNDLES_KEEP_DAYS)

# -------------------- DB connection + retry helpers --------------------
def _db_connect(timeout_s: int = 120) -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=timeout_s)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=60000;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def _with_write_txn_retry(op, attempts: int = 8, base_sleep: float = 0.5):
    last_exc = None
    for i in range(attempts):
        conn = None
        try:
            conn = _db_connect()
            c = conn.cursor()
            c.execute("BEGIN IMMEDIATE")
            op(c)
            conn.commit()
            conn.close()
            return
        except sqlite3.OperationalError as e:
            last_exc = e
            try:
                if conn:
                    conn.rollback()
                    conn.close()
            except Exception:
                pass
            if "locked" in str(e).lower():
                time.sleep(base_sleep * (2 ** i))
                continue
            raise
        except Exception:
            try:
                if conn:
                    conn.rollback()
                    conn.close()
            except Exception:
                pass
            raise
    raise last_exc if last_exc else RuntimeError("DB write failed after retries")

# -------------------- Load catalog utils --------------------
def _load_catalog_utils():
    candidate = os.path.join(REPO_ROOT, "05-catalog_utils.py")
    if not os.path.exists(candidate):
        raise RuntimeError("Cannot locate 05-catalog_utils.py next to this script.")
    return SourceFileLoader("catalog_utils_loader", candidate).load_module()

cu = _load_catalog_utils()

# -------------------- Small utils --------------------
def _bytes_to_gb(b: int) -> float:
    return b / (1024.0 ** 3)

def _safe_makedirs(path: str):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass

def _append_lines_locked(file_path: str, lines):
    _safe_makedirs(os.path.dirname(file_path) or ".")
    fd = os.open(file_path, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o644)
    try:
        try:
            import fcntl
            fcntl.flock(fd, fcntl.LOCK_EX)
        except Exception:
            pass
        with os.fdopen(fd, "a", encoding="utf-8", buffering=1) as f:
            for ln in lines:
                f.write(ln.rstrip("\n") + "\n")
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                pass
        fd = None
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass

# -------------------- DB helpers --------------------
def ensure_conversions_table():
    def _op(c):
        c.execute("""CREATE TABLE IF NOT EXISTS conversions (
            pb_path   TEXT PRIMARY KEY,
            timestamp TEXT,
            status    TEXT,
            message   TEXT
        )""")
    _with_write_txn_retry(_op)

def ensure_jobs_table():
    def _op(c):
        c.execute("""CREATE TABLE IF NOT EXISTS lsf_jobs (
            pb_path   TEXT PRIMARY KEY,
            job_id    TEXT,
            submit_ts TEXT
        )""")
    _with_write_txn_retry(_op)

def record_job_ids(pb_paths, job_id):
    if not pb_paths or not job_id:
        return
    now = datetime.now().isoformat(timespec="seconds")
    def _op(c):
        c.executemany("""INSERT OR REPLACE INTO lsf_jobs (pb_path, job_id, submit_ts)
                         VALUES (?, ?, ?)""",
                      [(p, job_id, now) for p in pb_paths])
    _with_write_txn_retry(_op)

def get_job_info_for_paths(pb_paths):
    if not pb_paths:
        return {}
    conn = _db_connect(timeout_s=60)
    c = conn.cursor()
    out = {}
    # Batch to avoid SQLite variable limit (max ~999)
    batch_size = 900
    for i in range(0, len(pb_paths), batch_size):
        batch = pb_paths[i:i + batch_size]
        qmarks = ",".join(["?"] * len(batch))
        c.execute(f"SELECT pb_path, job_id, submit_ts FROM lsf_jobs WHERE pb_path IN ({qmarks})", batch)
        out.update({row[0]: (row[1], row[2]) for row in c.fetchall()})
    conn.close()
    return out

def mark_queued(pb_paths, message):
    if not pb_paths:
        return
    now = datetime.now().isoformat(timespec="seconds")
    def _op(c):
        for p in pb_paths:
            c.execute("""INSERT OR IGNORE INTO conversions
                         (pb_path, timestamp, status, message)
                         VALUES (?, ?, 'queued', ?)""",
                      (p, now, message))
            c.execute("""UPDATE conversions
                         SET timestamp=?, status='queued', message=?
                         WHERE pb_path=?""",
                      (now, message, p))
    _with_write_txn_retry(_op)

def mark_submit_failed(pb_paths, message):
    if not pb_paths:
        return
    now = datetime.now().isoformat(timespec="seconds")
    def _op(c):
        for p in pb_paths:
            c.execute("""INSERT OR REPLACE INTO conversions
                         (pb_path, timestamp, status, message)
                         VALUES (?, ?, 'submit_failed', ?)""",
                      (p, now, message))
    _with_write_txn_retry(_op)

def mark_submit_failed_rows(rows_with_msgs: List[Tuple[str, str]]):
    if not rows_with_msgs:
        return
    now = datetime.now().isoformat(timespec="seconds")
    def _op(c):
        for p, msg in rows_with_msgs:
            c.execute("""INSERT OR REPLACE INTO conversions
                         (pb_path, timestamp, status, message)
                         VALUES (?, ?, 'submit_failed', ?)""",
                      (p, now, msg))
    _with_write_txn_retry(_op)

def mark_failed_rows(rows_with_msgs):
    if not rows_with_msgs:
        return
    now = datetime.now().isoformat(timespec="seconds")
    def _op(c):
        for p, msg in rows_with_msgs:
            c.execute("""INSERT OR REPLACE INTO conversions
                         (pb_path, timestamp, status, message)
                         VALUES (?, ?, 'failed', ?)""",
                      (p, now, msg))
    _with_write_txn_retry(_op)

# -------------------- LSF helpers --------------------
def _bjobs_lines(cmd: str):
    try:
        r = subprocess.run(["bash", "-lc", cmd], capture_output=True, text=True, timeout=20)
        if r.returncode != 0:
            return []
        lines = [ln.strip() for ln in (r.stdout or "").splitlines() if ln.strip()]
        return lines
    except Exception:
        return []

def _count_current_jobs(job_group: Optional[str], job_name_prefix: Optional[str]) -> Optional[int]:
    user = getpass.getuser()
    if job_group:
        cmd = f"bjobs -g {job_group} -noheader -o 'stat job_name'"
    else:
        cmd = f"bjobs -u {user} -noheader -o 'stat job_name'"
    lines = _bjobs_lines(cmd)
    if lines == []:
        probe = subprocess.run(["bash", "-lc", "which bjobs"], capture_output=True, text=True)
        if probe.returncode != 0:
            return None
    cnt = 0
    prefix = job_name_prefix or ""
    for ln in lines:
        parts = ln.split(None, 1)
        if not parts:
            continue
        stat = parts[0]
        name = parts[1] if len(parts) > 1 else ""
        if stat in ("PEND", "RUN"):
            if prefix:
                if name.startswith(prefix):
                    cnt += 1
            else:
                cnt += 1
    return cnt

def throttle_if_needed(max_running: int, poll_sec: int, job_group: Optional[str], job_name_prefix: Optional[str]):
    if max_running <= 0:
        return
    while True:
        cur = _count_current_jobs(job_group, job_name_prefix)
        if cur is None:
            print(f"[throttle] Could not query bjobs. Waiting {poll_sec}s before retry.")
            time.sleep(poll_sec)
            continue
        if cur < max_running:
            return
        print(f"[throttle] Current jobs {cur} >= limit {max_running}. Sleeping {poll_sec}s...")
        time.sleep(poll_sec)

# -------------------- Sweeper --------------------
def sweep_stale_queued_db_only(hours: float) -> int:
    if not hours or hours <= 0:
        return 0
    cutoff = datetime.now() - timedelta(hours=hours)
    cutoff_iso = cutoff.isoformat(timespec="seconds")
    conn = _db_connect(timeout_s=60)
    c = conn.cursor()
    c.execute("""SELECT pb_path FROM conversions
                 WHERE status IN ('queued','running') AND timestamp < ?""", (cutoff_iso,))
    rows = [r[0] for r in c.fetchall()]
    conn.close()
    if rows:
        mark_submit_failed(rows, "auto-sweep: stale (db-only)")
    return len(rows)

def _lsf_state(job_id: str) -> Optional[str]:
    try:
        r = subprocess.run(["bash", "-lc", f"bjobs -noheader -o stat {job_id}"],
                           capture_output=True, text=True, timeout=10)
        if r.returncode != 0:
            return None
        out = (r.stdout or "").strip().split()
        return out[0] if out else None
    except Exception:
        return None

def _lsf_active_jobs() -> Dict[str, str]:
    """Get all active job states in ONE bjobs call. Returns {job_id: state}."""
    try:
        r = subprocess.run(["bash", "-lc", "bjobs -noheader -o 'jobid stat' 2>/dev/null"],
                           capture_output=True, text=True, timeout=30)
        if r.returncode != 0:
            return {}
        result = {}
        for line in (r.stdout or "").strip().splitlines():
            parts = line.split()
            if len(parts) >= 2:
                result[parts[0]] = parts[1]
        return result
    except Exception:
        return {}

def _maybe_bkill(job_id: str):
    try:
        subprocess.run(["bash", "-lc", f"bkill {job_id}"], capture_output=True, text=True, timeout=10)
    except Exception:
        pass

def sweep_stale_queued_lsf(hours: float) -> int:
    if not hours or hours <= 0:
        return 0
    cutoff = datetime.now() - timedelta(hours=hours)
    cutoff_iso = cutoff.isoformat(timespec="seconds")

    conn = _db_connect(timeout_s=60)
    c = conn.cursor()
    c.execute("""SELECT pb_path, COALESCE(timestamp,'') FROM conversions
                 WHERE status IN ('queued','running') AND timestamp < ?""", (cutoff_iso,))
    queued_rows = c.fetchall()
    conn.close()
    if not queued_rows:
        return 0

    print(f"Sweeper: checking {len(queued_rows)} stale queued/running entries...")
    paths = [p for p, _ in queued_rows]
    job_map = get_job_info_for_paths(paths)

    # One bjobs call to get all active job states
    active_jobs = _lsf_active_jobs()

    kill_after = KILL_AFTER_HOURS if (KILL_ON_SWEEP and KILL_AFTER_HOURS) else None
    now = datetime.now()
    to_mark: List[Tuple[str, str]] = []
    killed = 0

    for pb_path, ts in queued_rows:
        job_id, submit_ts = job_map.get(pb_path, (None, None))
        base_ts_str = submit_ts or ts or ""
        try:
            base_ts = datetime.fromisoformat(base_ts_str) if base_ts_str else (now - timedelta(hours=hours+1))
        except Exception:
            base_ts = now - timedelta(hours=hours+1)
        age_hours = max(0.0, (now - base_ts).total_seconds() / 3600.0)

        if not job_id:
            to_mark.append((pb_path, "stale (lsf-aware): no job_id recorded"))
            continue

        # Look up state from the single batch call (missing = job finished/gone)
        state = active_jobs.get(job_id)
        if state in (None, "DONE", "EXIT", "ZOMBI", "UNKWN"):
            to_mark.append((pb_path, f"stale (lsf-aware): LSF state {state or 'missing'} for job {job_id}"))
            continue

        if kill_after is not None and age_hours > kill_after:
            _maybe_bkill(job_id)
            killed += 1
            to_mark.append((pb_path, f"killed stale {state} job {job_id} at ~{age_hours:.1f}h"))

    if to_mark:
        mark_submit_failed_rows(to_mark)
    if killed or to_mark:
        print(f"Sweeper (lsf-aware): marked {len(to_mark)} as submit_failed; killed {killed} PEND/RUN jobs.")
    return len(to_mark)

# -------------------- Planning helpers --------------------
DATE_IN_NAME_RE = re.compile(r"(19|20)\d{2}[-_/]?\d{2}[-_/]?\d{2}")

def load_exclude_tokens():
    toks = list(EXCLUDE_TOKENS_CFG)  # from config
    if args.exclude_filelist and os.path.isfile(args.exclude_filelist):
        with open(args.exclude_filelist, "r", encoding="utf-8", errors="ignore") as f:
            for ln in f:
                raw = ln.strip()
                if not raw or raw.startswith("#"):
                    continue
                toks.append(raw)
    if args.exclude_subpath:
        toks.extend(args.exclude_subpath)
    seen, out = set(), []
    for t in toks:
        if t not in seen:
            seen.add(t); out.append(t)
    return out

def path_is_excluded(path, exclude_tokens):
    npath = os.path.normpath(path)
    return any(tok in npath for tok in exclude_tokens)

def filenames_look_dated(sample_paths):
    if not sample_paths:
        return False
    hits = 0
    for p in sample_paths:
        base = os.path.basename(p)
        if DATE_IN_NAME_RE.search(base):
            hits += 1
    return (hits / len(sample_paths)) >= 0.50

def decide_effective_split(sample_for_names):
    if SPLIT_MODE == "on":
        return True
    if SPLIT_MODE == "off":
        return False
    # auto
    if "/2024c/" in INPUT_DIR or "/2024c/" in os.path.normpath(INPUT_DIR):
        return False
    return not filenames_look_dated(sample_for_names)

def build_passthrough_flags(effective_split) -> List[str]:
    parts: List[str] = []
    if effective_split:
        parts.append("--split_by_date")
        if SPLIT_GRAN:
            parts += ["--split_granularity", SPLIT_GRAN]
        if TZ_OFFSET is not None:
            parts += ["--tz_offset_hours", str(TZ_OFFSET)]
        if EMIT_DVIEW:
            parts.append("--emit_date_view")
        if DVIEW_ROOT:
            parts += ["--date_view_root", DVIEW_ROOT]
        if ALLOW_OOO:
            parts.append("--allow_out_of_order")
        if OOO_MAX_OPEN is not None:
            parts += ["--ooo_max_open_buckets", str(OOO_MAX_OPEN)]
    return parts

def get_already_converted_set():
    if not os.path.exists(DB_PATH):
        return set()
    try:
        conn = _db_connect(timeout_s=60)
        c = conn.cursor()
        c.execute("""
            SELECT pb_path FROM conversions
            WHERE status IN ('queued','running','success','skipped','failed')
               OR status IS NULL
        """)
        rows = c.fetchall()
        conn.close()
        return set(row[0] for row in rows)
    except Exception:
        return set()

# -------------------- Tables --------------------
ensure_conversions_table()
ensure_jobs_table()
cu.ensure_pb_catalog_table(DB_PATH)

# -------------------- Optional sweep first --------------------
if QUEUED_SWEEP_HOURS and QUEUED_SWEEP_HOURS > 0:
    if LSF_AWARE_SWEEP:
        swept = sweep_stale_queued_lsf(QUEUED_SWEEP_HOURS)
        if swept:
            print(f"Sweeper (lsf-aware): reclassified {swept} stale items to 'submit_failed'.")
        else:
            print("Sweeper (lsf-aware): nothing stale to reclassify.")
    else:
        swept = sweep_stale_queued_db_only(QUEUED_SWEEP_HOURS)
        if swept:
            print(f"Sweeper (db-only): reclassified {swept} stale items to 'submit_failed'.")
        else:
            print("Sweeper (db-only): nothing stale to reclassify.")

already_converted = get_already_converted_set()
threshold_bytes = int(BIG_THRESHOLD_GB * (1024 ** 3))
max_pb_bytes    = int(MAX_PB_GB * (1024 ** 3)) if MAX_PB_GB and MAX_PB_GB > 0 else None

# caps
target_small_candidates = MAX_BSUB_JOBS * CHUNK_SIZE
target_big_candidates   = MAX_BSUB_JOBS

small_files: List[str] = []
big_files:   List[str] = []
too_large_files: List[Tuple[str, int, int, float]] = []
excluded_files: List[str] = []
name_sample: List[str] = []

exclude_tokens = load_exclude_tokens()
force_tokens   = [t for t in (args.force_big_subpath or []) if t]

def is_forced_big(path: str) -> bool:
    npath = os.path.normpath(path)
    return any(tok in npath for tok in force_tokens)

# -------------------- Catalog-first collection --------------------
def build_from_catalog():
    if DELTA_SCAN_MIN and DELTA_SCAN_MIN > 0:
        if hasattr(cu, "delta_update_catalog"):
            print(f"[CATALOG] Delta update last {DELTA_SCAN_MIN} minutes …")
            cu.delta_update_catalog(DB_PATH, INPUT_DIR, DELTA_SCAN_MIN,
                                    exclude_tokens=exclude_tokens, echo_cmd=args.catalog_echo_cmd)
        else:
            print("[CATALOG] WARNING: delta_update_catalog not found in 05-catalog_utils.py; skipping delta scan.")

    catalog_map = cu.load_catalog_map(DB_PATH)  # path -> (size_b, mtime, inode)
    root_norm = os.path.normpath(INPUT_DIR)
    catalog_map = {p: v for p, v in catalog_map.items() if os.path.normpath(p).startswith(root_norm)}

    all_files = sorted(catalog_map.keys())

    new_files = []
    for p in all_files:
        if p in already_converted:
            continue
        if path_is_excluded(p, exclude_tokens):
            excluded_files.append(p); continue
        new_files.append(p)

    for p in new_files[:200]:
        name_sample.append(p)

    for p in new_files:
        sz, mt, ino = catalog_map.get(p, (None, None, None))
        if sz is None:
            try:
                st = os.stat(p); sz = st.st_size; mt = st.st_mtime; ino = st.st_ino
            except FileNotFoundError:
                continue

        if max_pb_bytes is not None and int(sz) > max_pb_bytes:
            too_large_files.append((p, int(sz), int(ino or 0), float(mt or 0.0)))
            continue

        if is_forced_big(p) or int(sz) >= threshold_bytes:
            big_files.append(p)
        else:
            small_files.append(p)

        if len(small_files) >= target_small_candidates and len(big_files) >= target_big_candidates:
            break

    return len(new_files)

def build_from_full_scan():
    all_files = sorted(glob.glob(os.path.join(INPUT_DIR, "**", "*.pb"), recursive=True))
    new_files = []
    for p in all_files:
        if p in already_converted:
            continue
        if path_is_excluded(p, exclude_tokens):
            excluded_files.append(p); continue
        new_files.append(p)
        if args.max_files and len(new_files) >= args.max_files:
            break

    for p in new_files[:200]:
        name_sample.append(p)

    for p in new_files:
        try:
            st = os.stat(p)
        except FileNotFoundError:
            continue
        size_b = st.st_size
        ino    = st.st_ino
        mtime  = st.st_mtime

        if max_pb_bytes is not None and size_b > max_pb_bytes:
            too_large_files.append((p, size_b, ino, mtime))
        elif is_forced_big(p) or size_b >= threshold_bytes:
            big_files.append(p)
        else:
            small_files.append(p)

    return len(new_files)

def rebuild_then_load_catalog():
    print("[CATALOG] Rebuilding full pb_catalog …")
    cu.rebuild_catalog(DB_PATH, INPUT_DIR, exclude_tokens=exclude_tokens, echo_cmd=args.catalog_echo_cmd)
    return build_from_catalog()

# Choose mode
if args.rebuild_catalog:
    scanned_candidates = rebuild_then_load_catalog()
elif args.full_scan:
    scanned_candidates = build_from_full_scan()
else:
    scanned_candidates = build_from_catalog()

# Enforce max_files if requested (after routing)
if args.max_files and args.max_files > 0:
    if len(small_files) > args.max_files:
        small_files = small_files[:args.max_files]
        big_files = []
    else:
        remain = args.max_files - len(small_files)
        big_files = big_files[:max(0, remain)]

# decide split policy using name sample
effective_split = decide_effective_split(name_sample)
passthrough = build_passthrough_flags(effective_split)

# -------------------- Report --------------------
print(f"Visited NEW candidates this cycle: {scanned_candidates}")
print("[catalog] mode:", "full rebuild + snapshot load" if args.rebuild_catalog
      else ("BYPASS (full walk; no persist)" if args.full_scan
            else ("snapshot + delta({}m)".format(DELTA_SCAN_MIN) if DELTA_SCAN_MIN > 0 else "snapshot only")))
if args.max_files:
    print(f"max_files cap applied: {args.max_files}")
print(f"Excluded by tokens (seen among candidates): {len(excluded_files)}")
print(f"Over-size (>{MAX_PB_GB:.2f} GB) skipped: {len(too_large_files)}")
if too_large_files:
    ep, esz, _, _ = too_large_files[0]
    print(f"  Example too-large: {ep} ({_bytes_to_gb(esz):.2f} GB)")
print(f"Routing with BIG_THRESHOLD_GB={BIG_THRESHOLD_GB:.2f}: "
      f"{len(small_files)} small, {len(big_files)} big (cap jobs={MAX_BSUB_JOBS})")
if force_tokens:
    print(f"Force-BIG subpath tokens: {force_tokens}")
if exclude_tokens:
    print("Active exclude tokens:", exclude_tokens)
print(f"Split-by-date policy: {'ON' if effective_split else 'OFF'} (mode={SPLIT_MODE})")
if passthrough:
    print("Forwarding converter flags:", " ".join(passthrough))
print(f"Max bsub jobs this run: {MAX_BSUB_JOBS}")
print(f"Concurrent job cap: max_running={MAX_RUNNING}, poll_sec={POLL_SEC}")

# Oversize: persist logs + (if --submit) DB mark failed
if too_large_files:
    host = socket.gethostname()
    pid  = os.getpid()
    now_iso = datetime.now().isoformat(timespec="seconds")
    header = (f"# run={now_iso} host={host} pid={pid} log_dir={LOG_DIR} "
              f"max_pb_gb={MAX_PB_GB:.3f}")
    entry_lines = []
    for p, sz, ino, mt in too_large_files:
        entry_lines.append(
            f"{now_iso} | host={host} pid={pid} | "
            f"size_bytes={sz} size_gb={_bytes_to_gb(sz):.3f} | "
            f"inode={ino} mtime={datetime.fromtimestamp(mt).isoformat(timespec='seconds')} | "
            f"limit_gb={MAX_PB_GB:.3f} | status=failed | path={p}"
        )
    run_tl_path = os.path.join(LOG_DIR, "too_large_pb.txt")
    try:
        _append_lines_locked(run_tl_path, [header] + entry_lines)
        print(f"Wrote too-large list to {run_tl_path}")
    except Exception as e:
        print(f"WARNING: could not write {run_tl_path}: {e}")
    try:
        _append_lines_locked(TOO_LARGE_TXT, [header] + entry_lines)
        print(f"Appended too-large entries to {TOO_LARGE_TXT}")
    except Exception as e:
        print(f"WARNING: could not append to {TOO_LARGE_TXT}: {e}")
    if args.submit:
        rows_with_msgs = [(p, f"too large: {_bytes_to_gb(sz):.3f} GB > max_pb_gb={MAX_PB_GB:.3f} GB (skipped)")
                          for (p, sz, _, _) in too_large_files]
        try:
            mark_failed_rows(rows_with_msgs)
        except Exception as e:
            print(f"WARNING: could not mark too-large rows in DB: {e}")

# -------------------- Build Bundles for SMALL files --------------------
small_bundles = []
if small_files:
    for i in range(0, len(small_files), CHUNK_SIZE):
        if len(small_bundles) >= MAX_BSUB_JOBS:
            break
        small_bundles.append(small_files[i:i + CHUNK_SIZE])

# -------------------- Submission --------------------
submit_count = 0
_JOB_RE = re.compile(r"Job <(\d+)>", re.IGNORECASE)

def submit_bsub(cmd_argv: List[str], pb_list: List[str], label: str):
    global submit_count
    if submit_count >= MAX_BSUB_JOBS:
        print("Reached maximum bsub job submission limit; skipping remaining.")
        return
    mark_queued(pb_list, f"queued by submitter: {label}")
    r = subprocess.run(cmd_argv, capture_output=True, text=True)
    if r.returncode == 0:
        text = (r.stdout or "") + "\n" + (r.stderr or "")
        m = _JOB_RE.search(text)
        if m:
            record_job_ids(pb_list, m.group(1))
        submit_count += 1
        print(f"Submitting {label} [bsub job #{submit_count}] with {len(pb_list)} file(s)")
    else:
        mark_submit_failed(pb_list, f"bsub rc={r.returncode}")
        print(f"{label} submit FAILED (rc={r.returncode})")
        if r.stdout:
            print(r.stdout.strip())
        if r.stderr:
            print(r.stderr.strip())

# 1) SMALL bundles -> SMALL_SCRIPT
for i, bundle in enumerate(small_bundles):
    if submit_count >= MAX_BSUB_JOBS:
        break
    # was: bundle_file = os.path.join(CHUNKS_DIR, f"bundle_small_{i}.txt")
    bundle_file = os.path.join(BUNDLES_DIR, f"bundle_small_{RUN_TS}_{i:04d}.txt")
    with open(bundle_file, "w") as f:
        f.write("\n".join(bundle))
    cmd_argv = [
        "bsub",
        "-o", os.path.join(LOG_DIR, f"small_bundle_{i}.log"),
        "-q", QUEUE,
    ]
    if JOB_PREFIX:
        job_name = f"{JOB_PREFIX}small_{RUN_TS}_{i:04d}"
        cmd_argv += ["-J", job_name]
    if JOB_GROUP:
        cmd_argv += ["-g", JOB_GROUP]
    cmd_argv += [
        sys.executable, SMALL_SCRIPT,
        "--input_dir", INPUT_DIR,
        "--chunk_file", bundle_file,
        "--db_path", DB_PATH,
    ] + passthrough
    if args.submit:
        throttle_if_needed(MAX_RUNNING, POLL_SEC, JOB_GROUP, JOB_PREFIX)
        submit_bsub(cmd_argv, bundle, f"SMALL bundle {i+1}/{len(small_bundles)}")
    else:
        print(f"[Dry Run] SMALL bundle {i + 1}/{len(small_bundles)}: {' '.join(cmd_argv)}")

# 2) BIG single-file -> BIG_CHUNKER
for j, pb_path in enumerate(big_files):
    if submit_count >= MAX_BSUB_JOBS:
        break
    base = os.path.splitext(os.path.basename(pb_path))[0].replace(":", "_")
    cmd_argv = [
        "bsub",
        "-o", os.path.join(LOG_DIR, f"big_chunk_{base}.log"),
        "-q", QUEUE,
    ]
    if JOB_PREFIX:
        job_name = f"{JOB_PREFIX}big_{base}"
        cmd_argv += ["-J", job_name]
    if JOB_GROUP:
        cmd_argv += ["-g", JOB_GROUP]
    cmd_argv += [
        sys.executable, BIG_CHUNKER,
        "--pb", pb_path,
        "--input_root", INPUT_DIR,
        "--rss_cap_gb", str(RSS_CAP_GB),
        "--db_path", DB_PATH,
    ] + passthrough
    if args.submit:
        throttle_if_needed(MAX_RUNNING, POLL_SEC, JOB_GROUP, JOB_PREFIX)
        submit_bsub(cmd_argv, [pb_path], f"BIG file {j+1}/{len(big_files)}")
    else:
        print(f"[Dry Run] BIG file {j + 1}/{len(big_files)}: {' '.join(cmd_argv)}")

print(f"Done. Total jobs {'submitted' if args.submit else 'planned'}: {submit_count}")