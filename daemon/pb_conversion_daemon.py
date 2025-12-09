#!/usr/bin/env python3
# pb_conversion_daemon.py
#
# Loop:
#   1) submit 00-submit_pb2parquet_jobs.py
#   2) parse logs -> update DB
#   3) when a submission looks finished -> run 04-recompute + merges (via 03c)

import os
import re
import time
import sqlite3
import subprocess
import configparser
from importlib.machinery import SourceFileLoader
from datetime import datetime, timedelta
from typing import Optional, List, Dict

# ---------------- config.ini loader ----------------
# near the top
def _load_ini(path=None):
    path = path or os.environ.get(
        "PVPIPE_CONFIG",
        "/home/belle2/amubarak/Data_Format_Conversion/config.ini",
    )
    cfg = configparser.ConfigParser()
    if not cfg.read(path):
        raise RuntimeError(f"config.ini not found at {path}")
    return cfg, path

_CFG, _CONFIG_PATH = _load_ini()

def _get(section, key, default=None):
    try:
        return _CFG.get(section, key, fallback=default)
    except Exception:
        return default

def _getint(section, key, default=None):
    try:
        return _CFG.getint(section, key, fallback=default)
    except Exception:
        return default

def _getfloat(section, key, default=None):
    try:
        return _CFG.getfloat(section, key, fallback=default)
    except Exception:
        return default

def _getbool(section, key, default=False):
    try:
        return _CFG.getboolean(section, key, fallback=default)
    except Exception:
        v = str(_get(section, key, default)).strip().lower()
        return v in ("1", "true", "yes", "on")

def _split_list(s: str) -> List[str]:
    if not s:
        return []
    # split on commas and newlines, strip blanks
    toks = []
    for line in str(s).splitlines():
        for part in line.split(","):
            t = part.strip()
            if t:
                toks.append(t)
    return toks

# ---------------- paths from config.ini ----------------
INPUT_DIR   = _get("paths", "input_dir")
OUTPUT_BASE = _get("paths", "output_base")
LOGS_ROOT   = _get("paths", "logs_root")
LOGS_SUBDIR = _get("paths", "logs_subdir_pb2parquet", "pb2parquet")
LOG_PARENT  = os.path.join(LOGS_ROOT, LOGS_SUBDIR)
CHUNKS_DIR  = _get("paths", "chunks_dir")
DB_PATH     = _get("paths", "db_path")

SMALL_SCRIPT = _get("paths", "small_script")
BIG_SCRIPT   = _get("paths", "big_chunker_script")
TOO_LARGE_TXT = _get("paths", "too_large_txt")

# derive script paths (00, 03c, 04 live in repo root)
_REPO_ROOT = "/home/belle2/amubarak/Data_Format_Conversion"
CONVERSION_SCRIPT      = os.path.join(_REPO_ROOT, "00-submit_pb2parquet_jobs.py")
RECOMPUTE_SCRIPT       = os.path.join(_REPO_ROOT, "04-recompute_submission_summaries.py")
MERGE_ORCHESTRATOR_SCRIPT = os.path.join(_REPO_ROOT, "03c-submit-merge-jobs.py")

# write failures next to logs
FAILED_TXT = os.path.join(LOG_PARENT, "failed_pb_files.txt")

# ---------------- daemon cadence ----------------
POLL_INTERVAL = _getint("daemon", "live_poll_seconds", 600)

# ---------------- LSF and submitter knobs ----------------
DEFAULT_QUEUE          = _get("lsf", "queue", "h")
JOB_GROUP              = _get("lsf", "job_group", "")
JOB_NAME_PREFIX        = _get("lsf", "job_name_prefix", "")
MAX_CONCURRENT_RUNNING = _getint("lsf", "max_concurrent_bsubs", 500)
QUEUED_SWEEP_HOURS     = _getfloat("lsf", "queued_sweep_hours", 24.0)
LSF_AWARE_SWEEP        = _getbool("lsf", "lsf_aware_sweep", True)
KILL_ON_SWEEP          = _getbool("lsf", "kill_on_sweep", True)
KILL_AFTER_HOURS       = _getfloat("lsf", "kill_after_hours", 24.0)

CHUNK_SIZE         = _getint("submitter", "chunk_size", 100)
BIG_THRESHOLD_GB   = _getfloat("submitter", "big_threshold_gb", 1.0)
MAX_PB_GB          = _getfloat("submitter", "max_pb_gb", 1000.0)
RSS_CAP_GB         = _getfloat("submitter", "rss_cap_gb", 0.05)

SPLIT_MODE         = _get("submitter", "split_mode", "auto").lower()
SPLIT_GRANULARITY  = _get("submitter", "split_granularity", "day")
TZ_OFFSET_HOURS_TXT= _get("submitter", "tz_offset_hours", "").strip()
TZ_OFFSET_HOURS    = int(TZ_OFFSET_HOURS_TXT) if TZ_OFFSET_HOURS_TXT not in ("", None) else None
EMIT_DATE_VIEW     = _getbool("submitter", "emit_date_view", True)
DATE_VIEW_ROOT     = _get("submitter", "date_view_root", "")
OOO_MAX_OPEN       = _getint("submitter", "ooo_max_open_buckets", 8)
MAX_BSUB_PER_ROUND = _getint("submitter", "max_bsub_per_submit_round", 1000)

EXCLUDE_SUBPATHS   = _split_list(_get("exclude", "subpaths", ""))

# ---------------- catalog knobs ----------------
_catalog_at = _get("catalog", "daily_rebuild_at", "").strip()
if _catalog_at:
    try:
        _CAT_HH = int(_catalog_at.split(":")[0])
    except Exception:
        _CAT_HH = 3
    CATALOG_REBUILD_DAILY_AT_HOUR = _CAT_HH
else:
    CATALOG_REBUILD_DAILY_AT_HOUR = None
CATALOG_REBUILD_INTERVAL_HOURS = 24
CATALOG_REBUILD_ON_START = True
CATALOG_ECHO_FIND = True
DELTA_SCAN_MINUTES = _getint("catalog", "delta_scan_minutes", 0)

# Catalog refresh policy
ALWAYS_RESCAN_ON_TICK = _getbool("catalog", "always_rescan_on_tick", False)
DELTA_SCAN_MINUTES    = _getint("catalog", "delta_scan_minutes", 0)
FORCE_REBUILD_ENV     = os.environ.get("PB_FORCE_REBUILD", "0") == "1"

# ---------------- merge knobs ----------------
ENABLE_PV_MERGE   = _getbool("merge", "enable_pv_merge", True)
ENABLE_DATE_MERGE = _getbool("merge", "enable_date_merge", False)
MERGE_DELETE_CHUNKS_AFTER_PV = _getbool("merge", "merge_delete_chunks_after_pv", False)
MERGE_DELETE_PARQUET_AFTER_DATE = _getbool("merge", "merge_delete_parquet_after_date", False)

# ---------------- naming heuristic ----------------
NAMING_MODE = "yearly"  # keep stable

# ---------------- dynamic-load catalog utils ----------------
def _load_catalog_utils():
    here = os.path.dirname(os.path.abspath(__file__))
    candidate = os.path.join(os.path.dirname(here), "05-catalog_utils.py")
    if not os.path.exists(candidate):
        candidate = os.path.join(_REPO_ROOT, "05-catalog_utils.py")
    if not os.path.exists(candidate):
        raise RuntimeError("Cannot locate 05-catalog_utils.py")
    return SourceFileLoader("catalog_utils_loader", candidate).load_module()

cu = _load_catalog_utils()

# ---------------- DB schema ----------------
def initialize_db():
    os.makedirs(LOG_PARENT, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS conversions (
        pb_path   TEXT PRIMARY KEY,
        timestamp TEXT,
        status    TEXT,
        message   TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS submissions (
        timestamp   TEXT PRIMARY KEY,
        log_dir     TEXT,
        is_complete INTEGER
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS kv_store (
        k TEXT PRIMARY KEY,
        v TEXT
    )""")
    conn.commit(); conn.close()
    cu.ensure_pb_catalog_table(DB_PATH)

def kv_get(key: str) -> Optional[str]:
    conn = sqlite3.connect(DB_PATH); c = conn.cursor()
    c.execute("SELECT v FROM kv_store WHERE k=?", (key,))
    row = c.fetchone(); conn.close()
    return row[0] if row else None

def kv_set(key: str, val: str) -> None:
    conn = sqlite3.connect(DB_PATH); c = conn.cursor()
    c.execute("INSERT OR REPLACE INTO kv_store (k, v) VALUES (?,?)", (key, val))
    conn.commit(); conn.close()

# ---------------- PB catalog index ----------------
PB_INDEX: Dict[str, str] = {}

def load_pb_index_from_catalog():
    PB_INDEX.clear()
    catalog = cu.load_catalog_map(DB_PATH)  # path -> (size_b, mtime, inode)
    for full in catalog.keys():
        base = os.path.basename(full)
        cur = PB_INDEX.get(base)
        if cur is None:
            PB_INDEX[base] = full
        else:
            if full.startswith(INPUT_DIR) and not cur.startswith(INPUT_DIR):
                PB_INDEX[base] = full

# ---------------- path resolution helpers ----------------
def resolve_conversion_script() -> str:
    override = os.environ.get("CONVERSION_SCRIPT")
    if override and os.path.isfile(override):
        return override
    if os.path.isfile(CONVERSION_SCRIPT):
        return CONVERSION_SCRIPT
    # fallback scan
    candidates = [
        os.path.join(_REPO_ROOT, "submit_pb2parquet_jobs.py"),
    ]
    for c in candidates:
        if os.path.isfile(c):
            return c
    raise FileNotFoundError("Could not locate the conversion submitter script.")

_CHUNK_SUFFIX_RE = re.compile(r"_chunk\d{4}$")
_DATE_SUFFIX_RE  = re.compile(r"[-_](\d{4}_\d{2}_\d{2}|\d{4}_\d{2}|(?:\d{4}_W\d{2}))$")
_BARE_YEAR_RE    = re.compile(r"[-_](?:19|20)\d{2}$")

def _base_from_parquet_path(parquet_path: str) -> str:
    base = os.path.splitext(os.path.basename(parquet_path))[0]
    return _CHUNK_SUFFIX_RE.sub("", base)

def _insert_colon_at_last_sep(stem: str) -> str:
    i_dash = stem.rfind("-"); i_und = stem.rfind("_"); i = max(i_dash, i_und)
    if i == -1:
        return stem + ".pb"
    return stem[:i] + ":" + stem[i+1:] + ".pb"

def decide_split_by_date(input_dir: str) -> bool:
    if SPLIT_MODE == "on":
        return True
    if SPLIT_MODE == "off":
        return False
    if NAMING_MODE == "yearly":
        return True
    if NAMING_MODE == "dated":
        return False
    norm = os.path.join("/", os.path.normpath(input_dir).strip(os.sep)) + "/"
    if "/2024c/" in norm:
        return False
    if "/2024/" in norm:
        return True
    return False

def resolve_pb_from_saved_target(saved_target: str) -> Optional[str]:
    t = saved_target.strip().strip('"').strip("'")
    if t.endswith(".pb") and os.path.isabs(t) and os.path.exists(t):
        return t
    base = _base_from_parquet_path(t) if t.endswith(".parquet") else os.path.splitext(os.path.basename(t))[0]
    m = _DATE_SUFFIX_RE.search(base)
    if m:
        prefix = base[:m.start()]
        pv_core = prefix if (NAMING_MODE == "yearly" or _BARE_YEAR_RE.search(prefix)) else base
    else:
        pv_core = base
    candidates = [_insert_colon_at_last_sep(pv_core)]
    if "-" in pv_core and "_" in pv_core:
        j = pv_core.rfind("_");  k = pv_core.rfind("-")
        if j != -1: candidates.append(pv_core[:j] + ":" + pv_core[j+1:] + ".pb")
        if k != -1: candidates.append(pv_core[:k] + ":" + pv_core[k+1:] + ".pb")

    if os.path.isabs(t) and t.startswith(OUTPUT_BASE):
        try:
            rel_dir = os.path.relpath(os.path.dirname(t), OUTPUT_BASE)
            for bn in candidates:
                cand = os.path.join(INPUT_DIR, rel_dir, bn)
                if os.path.isfile(cand):
                    return cand
        except Exception:
            pass

    for bn in candidates:
        p = PB_INDEX.get(bn)
        if p:
            return p
    return None

# ---------------- catalog rebuild policy ----------------
def _due_for_catalog_rebuild(now: datetime) -> bool:
    if CATALOG_REBUILD_DAILY_AT_HOUR is not None:
        last_date = kv_get("last_catalog_rebuild_date")  # YYYY-MM-DD
        today = now.strftime("%Y-%m-%d")
        if last_date == today:
            return False
        return now.hour >= int(CATALOG_REBUILD_DAILY_AT_HOUR)

    last_ts = kv_get("last_catalog_rebuild_ts")
    if last_ts is None:
        return CATALOG_REBUILD_ON_START
    try:
        last_dt = datetime.fromisoformat(last_ts)
    except Exception:
        return True
    return (now - last_dt) >= timedelta(hours=CATALOG_REBUILD_INTERVAL_HOURS)

def _stamp_catalog_rebuild(now: datetime) -> None:
    kv_set("last_catalog_rebuild_ts", now.isoformat(timespec="seconds"))
    kv_set("last_catalog_rebuild_date", now.strftime("%Y-%m-%d"))

# ---------------- submit conversion ----------------
def submit_conversion():
    os.makedirs(LOG_PARENT, exist_ok=True)

    script = resolve_conversion_script()
    now = datetime.now()
    now_str = now.strftime("%Y%m%d_%H%M%S")
    log_dir = os.path.join(LOG_PARENT, now_str)
    os.makedirs(log_dir, exist_ok=True)

    env = os.environ.copy()
    env["LOG_DIR_EXTERNAL"] = log_dir
    env["PVPIPE_CONFIG"] = _CONFIG_PATH  # force children to use the same config.ini

    cmd = ["python3", script, "--submit"]

    # Decide how to refresh the catalog for this tick
    force_rebuild = FORCE_REBUILD_ENV or ALWAYS_RESCAN_ON_TICK or (DELTA_SCAN_MINUTES == 0)
    if force_rebuild:
        cmd.append("--rebuild_catalog")
        if CATALOG_ECHO_FIND:
            cmd.append("--catalog_echo_cmd")
        print("[DAEMON] Catalog: full rebuild this tick.")
    else:
        # If you later add support in 00-submit for delta scanning, forward it here
        if isinstance(DELTA_SCAN_MINUTES, int) and DELTA_SCAN_MINUTES > 0:
            cmd += ["--delta_scan", str(DELTA_SCAN_MINUTES)]
            print(f"[DAEMON] Catalog: delta scan {DELTA_SCAN_MINUTES} min.")
        elif _due_for_catalog_rebuild(now):
            cmd.append("--rebuild_catalog")
            if CATALOG_ECHO_FIND:
                cmd.append("--catalog_echo_cmd")
            print("[DAEMON] Catalog: full rebuild by schedule.")
        else:
            print("[DAEMON] Catalog: using existing snapshot.")

    subprocess.run(cmd, check=True, env=env)

    # Only stamp when we purposely did a rebuild by schedule or force
    if "--rebuild_catalog" in cmd:
        _stamp_catalog_rebuild(datetime.now())

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        "INSERT OR REPLACE INTO submissions (timestamp, log_dir, is_complete) VALUES (?, ?, ?)",
        (now_str, log_dir, 0),
    )
    conn.commit(); conn.close()

    return now_str, log_dir

# ---------------- parse logs -> status dict ----------------
def parse_log_file(path):
    import re

    status_dict = {}

    # Accept absolute parquet/pb paths; ignore parens decorations.
    FILE_RE = re.compile(r'(/[^ \t()]+?\.(?:parquet|pb))')

    def _pick_target(text: str) -> str:
        m = FILE_RE.search(text)
        if m:
            return m.group(1)
        # fallback: take rhs after ":" and strip decorations
        t = text.split(":", 1)[-1].strip()
        t = t.split("  (", 1)[0].split(" (", 1)[0]
        return t

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            line = raw.strip()
            low  = line.lower()

            # success lines
            if low.startswith("saved:") or low.startswith("wrote:") or low.startswith("output:"):
                target = _pick_target(line)
                pb = resolve_pb_from_saved_target(target)
                if pb:
                    status_dict[pb] = ("success", "")
                # else: silently ignore unmappable "success" lines
                continue

            # chunk lines: mark as 'chunked' if we can map, else ignore quietly
            if low.startswith("saved chunk:") or low.startswith("saved final chunk:"):
                target = _pick_target(line)
                pb = resolve_pb_from_saved_target(target)
                if pb and pb not in status_dict:
                    status_dict[pb] = ("chunked", "")
                continue

            # merge lines: support both arrow styles; ignore if unmappable
            if line.startswith("✅ Merged") or "->" in line or "→" in line:
                rhs = line.split("→", 1)[-1] if "→" in line else line.split("->", 1)[-1]
                target = _pick_target(rhs.strip())
                pb = resolve_pb_from_saved_target(target)
                if pb:
                    status_dict[pb] = ("success", "")
                continue

            # per-date summary lines we can ignore
            if line.startswith("✅ Date "):
                continue

            # failures: try to map; if not, still ignore quietly (we already log FAIL elsewhere)
            if line.startswith("❌") or line.startswith("FAIL:") or "pb2tsv failed" in line or "FAILED" in line:
                body = line.split(":", 1)[-1].strip()
                target = _pick_target(body)
                msg = "failed"
                if "-" in body:
                    msg = body.split("-", 1)[1].strip()
                pb = resolve_pb_from_saved_target(target)
                if pb:
                    status_dict[pb] = ("failed", msg)
                continue

            # skips: same handling as failures
            if line.startswith("⚠️ Skipped:") or line.startswith("SKIP:") or "SKIPPED (empty)" in line:
                body = line.split(":", 1)[-1].strip()
                target = _pick_target(body)
                msg = "skipped"
                if "(" in body and body.rstrip().endswith(")"):
                    msg = body.split("(", 1)[1].rstrip(")")
                pb = resolve_pb_from_saved_target(target)
                if pb:
                    status_dict[pb] = ("skipped", msg)
                continue

    return status_dict

# ---------------- update DB and failure txt ----------------
def update_conversion_log(status_dict):
    if not status_dict:
        return
    now = datetime.now().isoformat(timespec="seconds")
    conn = sqlite3.connect(DB_PATH); c = conn.cursor()
    failed_lines = []
    for pb_path, (status, msg) in status_dict.items():
        if not pb_path.endswith(".pb"):
            continue
        c.execute(
            "INSERT OR REPLACE INTO conversions (pb_path, timestamp, status, message) VALUES (?, ?, ?, ?)",
            (pb_path, now, status, msg),
        )
        if status == "failed":
            failed_lines.append(f"{pb_path} — {msg}")
    conn.commit(); conn.close()

    if failed_lines:
        os.makedirs(os.path.dirname(FAILED_TXT), exist_ok=True)
        with open(FAILED_TXT, "a", encoding="utf-8") as f:
            for line in failed_lines:
                f.write(line + "\n")

# ---------------- completion check ----------------
_SUMMARY_MARKERS = (
    "------ Chunk Summary ------",
    "------ Chunker Summary ------",
    "PV-chunk merge summary",
    "✅ Date ",
    "✅ PV unsplit",
)

def logs_show_completion(log_dir: str) -> bool:
    try:
        logs = [f for f in os.listdir(log_dir) if f.endswith(".log")]
    except FileNotFoundError:
        return False
    if not logs:
        return False
    stale_cutoff = datetime.now() - timedelta(minutes=15)
    for name in logs:
        lp = os.path.join(log_dir, name)
        try:
            with open(lp, "r", encoding="utf-8", errors="ignore") as f:
                text = f.read()
                if any(marker in text for marker in _SUMMARY_MARKERS):
                    continue
            mtime = datetime.fromtimestamp(os.path.getmtime(lp))
            if mtime < stale_cutoff:
                continue
            return False
        except OSError:
            return False
    return True

# ---------------- finalize submissions and trigger merges ----------------
def check_for_finished_submissions():
    any_finalized = False
    conn = sqlite3.connect(DB_PATH); c = conn.cursor()
    c.execute("SELECT timestamp, log_dir FROM submissions WHERE is_complete = 0")
    rows = c.fetchall(); conn.close()

    for timestamp, log_dir in rows:
        if not os.path.isdir(log_dir):
            continue
        if not logs_show_completion(log_dir):
            continue

        summary_path = os.path.join(LOG_PARENT, f"submission_summary_{timestamp}.json")
        try:
            env = os.environ.copy()
            env.setdefault("PVPIPE_CONFIG", os.environ.get("PVPIPE_CONFIG", "/home/belle2/amubarak/Data_Format_Conversion/config.ini"))
            subprocess.run(["python3", RECOMPUTE_SCRIPT], check=True, env=env)
            if os.path.exists(summary_path):
                conn2 = sqlite3.connect(DB_PATH); c2 = conn2.cursor()
                c2.execute("UPDATE submissions SET is_complete = 1 WHERE timestamp = ?", (timestamp,))
                conn2.commit(); conn2.close()
                print(f"[DAEMON] Finalized submission: {timestamp}")
                print(f"[DAEMON] Summary: {summary_path}")
                any_finalized = True
            else:
                print(f"[DAEMON] Recompute ran but {summary_path} not found")
        except Exception as e:
            print(f"[DAEMON] Failed to finalize {timestamp}: {e}")
    return any_finalized

def submit_merge_jobs():
    """
    Submit merge jobs via 03c orchestrator (which submits bsub jobs for 03a and 03b).
    Respects enable_pv_merge and enable_date_merge config flags.
    """
    if not ENABLE_PV_MERGE and not ENABLE_DATE_MERGE:
        print("[DAEMON] Merges skipped. Both merge types disabled by config.")
        return

    try:
        env = os.environ.copy()
        env.setdefault("PVPIPE_CONFIG", _CONFIG_PATH)
        
        cmd = ["python3", MERGE_ORCHESTRATOR_SCRIPT]
        
        # Only submit enabled merge types
        if ENABLE_PV_MERGE and not ENABLE_DATE_MERGE:
            cmd.append("--pv_only")
            print("[DAEMON] Submitting PV merge jobs only (date merge disabled).")
        elif ENABLE_DATE_MERGE and not ENABLE_PV_MERGE:
            cmd.append("--date_only")
            print("[DAEMON] Submitting date merge jobs only (PV merge disabled).")
        else:
            print("[DAEMON] Submitting both PV and date merge jobs.")
        
        subprocess.run(cmd, check=True, env=env)
        print("[DAEMON] Merge job submission complete (03c).")
    except Exception as e:
        print(f"[DAEMON] 03c-submit-merge-jobs.py failed: {e}")

# ---------------- progress snapshot ----------------
def _safe_count(cur, q, params=()):
    try:
        cur.execute(q, params)
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except Exception:
        return 0

def _current_progress():
    total = converted = remaining = 0
    try:
        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pb_catalog'")
        if c.fetchone():
            total = _safe_count(c, "SELECT COUNT(*) FROM pb_catalog")
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='conversions'")
        if c.fetchone():
            converted = _safe_count(c, "SELECT COUNT(*) FROM conversions WHERE status IN ('success','skipped')")
        conn.close()
    except Exception:
        pass
    remaining = max(0, total - converted)
    pct = 100.0 * converted / total if total else 0.0
    return total, converted, remaining, pct

def _log_progress_line(total, converted, remaining, pct):
    bar_len = 40
    filled = int(round((pct / 100.0) * bar_len))
    bar = "[" + "#" * filled + "-" * (bar_len - filled) + "]"
    msg = f"[PROGRESS] {bar} {pct:.1f}% ({converted:,}/{total:,}) left {remaining:,}"
    print(msg)
    try:
        os.makedirs(LOG_PARENT, exist_ok=True)
        progress_log = os.path.join(LOG_PARENT, "progress.log")
        with open(progress_log, "a", encoding="utf-8") as fp:
            ts = datetime.now().isoformat(timespec="seconds")
            fp.write(f"{ts} converted={converted} total={total} pct={pct:.1f} remaining={remaining}\n")
    except Exception:
        pass

# ---------------- main loop ----------------
def main():
    initialize_db()
    while True:
        print("\n[DAEMON] Submitting conversion round")
        try:
            ts, log_dir = submit_conversion()
        except Exception as e:
            print(f"[DAEMON] Submission failed: {e}")
            total, conv, rem, pct = _current_progress()
            _log_progress_line(total, conv, rem, pct)
            time.sleep(POLL_INTERVAL)
            continue

        print("[DAEMON] Sleeping for job start window")
        time.sleep(POLL_INTERVAL)

        load_pb_index_from_catalog()

        print(f"[DAEMON] Scanning logs in: {log_dir}")
        try:
            log_files = sorted(f for f in os.listdir(log_dir) if f.endswith(".log") or f.endswith(".txt"))
        except FileNotFoundError:
            log_files = []

        all_status = {}
        for log_file in log_files:
            log_path = os.path.join(log_dir, log_file)
            try:
                status_dict = parse_log_file(log_path)
                all_status.update(status_dict)
            except Exception as e:
                print(f"[DAEMON] Could not parse {log_path}: {e}")

        update_conversion_log(all_status)
        print(f"[DAEMON] Updated DB with {len(all_status)} entries")

        if check_for_finished_submissions():
            submit_merge_jobs()

        total, conv, rem, pct = _current_progress()
        _log_progress_line(total, conv, rem, pct)

        print(f"[DAEMON] Waiting {POLL_INTERVAL} sec for next cycle")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()