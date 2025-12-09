#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
05-retry_failed_pb_files.py
Retry failed PB conversions with campaign tracking and a merge orchestrator.

Flow
  1) Query failed PBs from DB (or a file), split into SMALL vs BIG by size.
  2) Submit SMALL bundles to 01-small_script.py, BIG singles to 02-big_chunker_script.py.
  3) Submit an orchestrator job that waits for this run_id's jobs, then:
       a) runs 03a-pv-chunks.py (merge chunk parts → PV parquet),
       b) runs 03b-merge-date.py (per-date merge; that script handles locking).

All paths, thresholds, and queue come from config.ini.
"""

import os
import re
import sys
import json
import argparse
import sqlite3
import subprocess
import configparser
from datetime import datetime
from typing import List, Tuple, Optional, Dict

# -------------------- Config loader (INI) --------------------

DEFAULT_CONFIG = "/home/belle2/amubarak/Data_Format_Conversion/config.ini"

def _as_bool(s: Optional[str], default: bool=False) -> bool:
    if s is None: return default
    s = str(s).strip().lower()
    if s in ("1", "true", "yes", "on"): return True
    if s in ("0", "false", "no", "off"): return False
    return default

def _as_opt_int(s: Optional[str]) -> Optional[int]:
    if s is None: return None
    s = str(s).strip()
    if not s: return None
    try:
        return int(s)
    except Exception:
        return None

def _read_cfg(path: str):
    cp = configparser.ConfigParser()
    if not os.path.isfile(path):
        sys.stderr.write(f"[ERROR] config not found: {path}\n")
        sys.exit(2)
    cp.read(path)

    def g(sec, key, default=None):
        try:
            return cp.get(sec, key)
        except Exception:
            return default

    def gb(sec, key, default=False):
        try:
            return cp.getboolean(sec, key)
        except Exception:
            return default

    def gi(sec, key, default=0):
        try:
            return cp.getint(sec, key)
        except Exception:
            return default

    def gf(sec, key, default=0.0):
        try:
            return cp.getfloat(sec, key)
        except Exception:
            return default

    # Base paths
    repo_root = os.path.dirname(os.path.abspath(path))

    paths = {
        "small_script":          g("paths", "small_script"),
        "big_chunker_script":    g("paths", "big_chunker_script"),
        "too_large_txt":         g("paths", "too_large_txt"),
        "input_dir":             g("paths", "input_dir"),
        "output_base":           g("paths", "output_base"),
        "logs_root":             g("paths", "logs_root"),
        "logs_subdir_pb2parquet":g("paths", "logs_subdir_pb2parquet", "pb2parquet"),
        "chunks_dir":            g("paths", "chunks_dir"),
        "db_path":               g("paths", "db_path"),
        # Optional overrides for merge scripts
        "merge_chunks_script":   g("paths", "merge_chunks_script",
                                   os.path.join(repo_root, "03a-pv-chunks.py")),
        "merge_date_script":     g("paths", "merge_date_script",
                                   os.path.join(repo_root, "03b-merge-date.py")),
    }

    lsf = {
        "queue":                 g("lsf", "queue", "h"),
        "max_concurrent_bsubs":  gi("lsf", "max_concurrent_bsubs", 500),
        "job_group":             g("lsf", "job_group", "/pvpipe"),
        "job_name_prefix":       g("lsf", "job_name_prefix", "pvpipe_"),
    }

    submit = {
        "chunk_size":            gi("submitter", "chunk_size", 100),
        "big_threshold_gb":      gf("submitter", "big_threshold_gb", 1.0),
        "rss_cap_gb":            gf("submitter", "rss_cap_gb", 0.05),
        "split_mode":            g("submitter", "split_mode", "auto"),
        "split_granularity":     g("submitter", "split_granularity", "day"),
        "tz_offset_hours":       _as_opt_int(g("submitter", "tz_offset_hours", None)),
        "emit_date_view":        _as_bool(g("submitter", "emit_date_view", "false")),
        "date_view_root":        g("submitter", "date_view_root", None),
    }

    merge = {
        "delete_chunks_after_pv":   gb("merge", "merge_delete_chunks_after_pv", False),
        "delete_parquet_after_date":gb("merge", "merge_delete_parquet_after_date", False),
    }

    # Required checks
    required = ["small_script", "big_chunker_script", "input_dir", "output_base", "logs_root", "db_path"]
    for k in required:
        if not paths.get(k):
            sys.stderr.write(f"[ERROR] missing [paths].{k} in {path}\n")
            sys.exit(2)

    return paths, lsf, submit, merge, repo_root

# -------------------- SQLite helpers --------------------

def _connect_db(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=10.0, isolation_level=None)
    c = conn.cursor()
    try:
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA busy_timeout=8000")
        c.execute("PRAGMA synchronous=NORMAL")
    except Exception:
        pass
    return conn

def _ensure_tables_and_columns(db_path: str):
    conn = _connect_db(db_path); c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS conversions (
            pb_path   TEXT PRIMARY KEY,
            timestamp TEXT,
            status    TEXT,
            message   TEXT
        )
    """)
    c.execute("PRAGMA table_info(conversions)")
    cols = {row[1] for row in c.fetchall()}
    if "retries" not in cols:
        try:
            c.execute("ALTER TABLE conversions ADD COLUMN retries INTEGER DEFAULT 0")
        except Exception:
            pass
    c.execute("""
        CREATE TABLE IF NOT EXISTS lsf_jobs (
            pb_path   TEXT PRIMARY KEY,
            job_id    TEXT,
            submit_ts TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS retry_campaigns (
            run_id        TEXT PRIMARY KEY,
            started_ts    TEXT,
            args_json     TEXT,
            total_planned INTEGER,
            logs_dir      TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS retry_items (
            run_id         TEXT,
            pb_path        TEXT,
            initial_status TEXT,
            queued_ts      TEXT,
            job_id         TEXT,
            finished_ts    TEXT,
            outcome        TEXT,
            PRIMARY KEY(run_id, pb_path)
        )
    """)
    conn.commit(); conn.close()

def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")

# -------------------- Query / updates --------------------

def _get_failed_from_db(db_path: str,
                        include_submit_failed: bool = True,
                        include_too_large: bool = False,
                        max_retries: int = 2) -> List[str]:
    conn = _connect_db(db_path); c = conn.cursor()
    statuses = ["failed"] + (["submit_failed"] if include_submit_failed else [])
    qmarks = ",".join(["?"] * len(statuses))
    c.execute(f"""
        SELECT pb_path, COALESCE(retries,0), COALESCE(message,'')
        FROM conversions
        WHERE status IN ({qmarks})
    """, statuses)
    out, seen = [], set()
    for pb_path, retries, msg in c.fetchall():
        if not pb_path or pb_path in seen: continue
        seen.add(pb_path)
        if int(retries or 0) >= max_retries: continue
        if not os.path.isfile(pb_path):     continue
        if (not include_too_large) and ("too large" in (msg or "").lower()): continue
        out.append(pb_path)
    conn.close()
    return out

def _get_initial_status(db_path: str, pb_path: str) -> str:
    conn = _connect_db(db_path); c = conn.cursor()
    c.execute("SELECT COALESCE(status,'') FROM conversions WHERE pb_path=?", (pb_path,))
    row = c.fetchone(); conn.close()
    return (row[0] if row else "") or ""

def _increment_retries_and_mark_queued(db_path: str, pb_paths: List[str], message: str):
    if not pb_paths: return
    now = _now_iso(); conn = _connect_db(db_path); c = conn.cursor()
    for p in pb_paths:
        c.execute("SELECT COALESCE(retries,0) FROM conversions WHERE pb_path=?", (p,))
        row = c.fetchone(); r = int(row[0]) if row else 0
        r = min(r+1, 2)
        c.execute("""
            INSERT INTO conversions (pb_path, timestamp, status, message, retries)
            VALUES (?, ?, 'queued', ?, ?)
            ON CONFLICT(pb_path) DO UPDATE SET
                timestamp=excluded.timestamp,
                status='queued',
                message=excluded.message,
                retries=excluded.retries
        """, (p, now, message, r))
    conn.commit(); conn.close()

def _mark_submit_failed(db_path: str, pb_paths: List[str], message: str):
    if not pb_paths: return
    now = _now_iso(); conn = _connect_db(db_path); c = conn.cursor()
    for p in pb_paths:
        c.execute("""
            INSERT INTO conversions (pb_path, timestamp, status, message)
            VALUES (?, ?, 'submit_failed', ?)
            ON CONFLICT(pb_path) DO UPDATE SET
                timestamp=excluded.timestamp,
                status='submit_failed',
                message=excluded.message
        """, (p, now, message))
    conn.commit(); conn.close()

def _record_job_ids(db_path: str, pb_paths: List[str], job_id: Optional[str]):
    if not pb_paths or not job_id: return
    now = _now_iso(); conn = _connect_db(db_path); c = conn.cursor()
    c.executemany("""
        INSERT OR REPLACE INTO lsf_jobs (pb_path, job_id, submit_ts)
        VALUES (?, ?, ?)
    """, [(p, job_id, now) for p in pb_paths])
    conn.commit(); conn.close()

def _new_run_id() -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = f"{os.getpid():x}"[-3:]
    return f"retry_{ts}_{suffix}"

def _create_retry_campaign(db_path: str, run_id: str, logs_dir: str, planned: List[str], args: Dict):
    conn = _connect_db(db_path); c = conn.cursor()
    c.execute("""
        INSERT OR REPLACE INTO retry_campaigns (run_id, started_ts, args_json, total_planned, logs_dir)
        VALUES (?, ?, ?, ?, ?)
    """, (run_id, _now_iso(), json.dumps(args, ensure_ascii=False), len(planned), logs_dir))
    rows = [(run_id, p, "", None, None, None, None) for p in planned]
    c.executemany("""
        INSERT OR IGNORE INTO retry_items
            (run_id, pb_path, initial_status, queued_ts, job_id, finished_ts, outcome)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit(); conn.close()

def _set_initial_statuses(db_path: str, run_id: str, pb_paths: List[str]):
    if not pb_paths: return
    conn = _connect_db(db_path); c = conn.cursor()
    for p in pb_paths:
        c.execute("UPDATE retry_items SET initial_status=? WHERE run_id=? AND pb_path=?",
                  (_get_initial_status(db_path, p), run_id, p))
    conn.commit(); conn.close()

def _mark_retry_queued(db_path: str, run_id: str, pb_paths: List[str], job_id: Optional[str]):
    if not pb_paths: return
    now = _now_iso(); conn = _connect_db(db_path); c = conn.cursor()
    for p in pb_paths:
        c.execute("""
            UPDATE retry_items
               SET queued_ts = COALESCE(queued_ts, ?),
                   job_id    = COALESCE(?, job_id)
             WHERE run_id=? AND pb_path=?
        """, (now, job_id, run_id, p))
    conn.commit(); conn.close()

# -------------------- Size / split flags --------------------

DATE_TOKEN_RE = re.compile(r"\d{4}_\d{2}_\d{2}")  # YYYY_MM_DD

def _has_day_token_in_filename(pb_path: str) -> bool:
    return bool(DATE_TOKEN_RE.search(os.path.basename(pb_path)))

def _classify_small_big(pb_paths: List[str], big_threshold_gb: float) -> Tuple[List[str], List[str]]:
    small, big = [], []
    thr = int(big_threshold_gb * (1024 ** 3))
    for p in pb_paths:
        try: sz = os.path.getsize(p)
        except Exception: sz = 0
        (big if sz >= thr else small).append(p)
    return small, big

def _want_split_by_date(split_mode: str, pb_path: str) -> bool:
    if split_mode == "on":  return True
    if split_mode == "off": return False
    npath = os.path.normpath(pb_path)
    if "/2024c/" in npath: return False
    if "/2024/" in npath and not _has_day_token_in_filename(pb_path): return True
    return False

def _build_split_flags(split_mode: str, pb_path: str,
                       split_granularity: Optional[str],
                       tz_offset_hours: Optional[int],
                       emit_date_view: bool,
                       date_view_root: Optional[str]) -> List[str]:
    flags = []
    if _want_split_by_date(split_mode, pb_path):
        flags.append("--split_by_date")
        if split_granularity: flags += ["--split_granularity", split_granularity]
        if tz_offset_hours is not None: flags += ["--tz_offset_hours", str(int(tz_offset_hours))]
        if emit_date_view: flags.append("--emit_date_view")
        if date_view_root: flags += ["--date_view_root", date_view_root]
    return flags

# -------------------- LSF helpers --------------------

_JOB_ID_RE = re.compile(r"Job <(\d+)>", re.IGNORECASE)

def _extract_job_id(text: str) -> Optional[str]:
    m = _JOB_ID_RE.search(text or "")
    return m.group(1) if m else None

def _current_job_count(jobname_prefixes: Tuple[str, ...],
                       count_all_jobs_if_unknown: bool = True) -> int:
    try:
        r = subprocess.run("bjobs -noheader -o job_name",
                           shell=True, capture_output=True, text=True, timeout=10)
        if r.returncode != 0: return 0
        names = [ln.strip() for ln in (r.stdout or "").splitlines() if ln.strip()]
        match_cnt = sum(1 for n in names if any(n.startswith(pref) for pref in jobname_prefixes))
        if match_cnt == 0 and count_all_jobs_if_unknown:
            return len(names)
        return match_cnt
    except Exception:
        return 0

# -------------------- Submitters (01/02) --------------------

def _submit_small_bundle(bundled_paths: List[str],
                         split_flags: List[str],
                         cfg_paths: dict,
                         queue: str,
                         job_group: Optional[str],
                         job_name_prefix: str,
                         log_parent: str,
                         dry_run: bool,
                         run_id: str) -> Optional[str]:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(log_parent, run_id)
    os.makedirs(run_dir, exist_ok=True)
    bundle_name = f"{run_id}_small_{ts}_{os.getpid()}.txt"
    bundle_file = os.path.join(run_dir, bundle_name)
    with open(bundle_file, "w", encoding="utf-8") as f:
        f.write("\n".join(bundled_paths) + "\n")

    small_script = cfg_paths["small_script"]
    run_cmd = (
        f'python3 "{small_script}" '
        f'--input_dir "{cfg_paths["input_dir"]}" '
        f'--chunk_file "{bundle_file}" '
        f'--db_path "{cfg_paths["db_path"]}" '
        f'--retry-run-id "{run_id}" '
        + " ".join(split_flags)
    ).strip()

    log_file = os.path.join(run_dir, f"{bundle_name}.log")
    job_name = f'{job_name_prefix}small_retry.{run_id}'
    bsub_parts = ["bsub"]
    if job_group: bsub_parts += ["-g", job_group]
    bsub_parts += ["-J", job_name, "-o", log_file, "-q", queue, run_cmd]
    bsub_cmd = " ".join(f'"{p}"' if (" " in p and not p.startswith("-")) else p for p in bsub_parts)

    print(f"[bsub small] {bsub_cmd}")
    if dry_run: return None
    r = subprocess.run(bsub_cmd, shell=True, capture_output=True, text=True)
    if r.returncode != 0:
        sys.stderr.write(f"[WARN] small bundle submit rc={r.returncode}\n{r.stdout}\n{r.stderr}\n")
        return None
    return _extract_job_id((r.stdout or "") + (r.stderr or ""))

def _submit_big_job(pb_path: str,
                    split_flags: List[str],
                    cfg_paths: dict,
                    rss_cap_gb: float,
                    queue: str,
                    job_group: Optional[str],
                    job_name_prefix: str,
                    log_parent: str,
                    dry_run: bool,
                    run_id: str) -> Optional[str]:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = os.path.splitext(os.path.basename(pb_path))[0].replace(":", "_")
    run_dir = os.path.join(log_parent, run_id)
    os.makedirs(run_dir, exist_ok=True)
    log_file = os.path.join(run_dir, f"{run_id}_big_{ts}_{base}.log")

    big_chunker = cfg_paths["big_chunker_script"]
    run_cmd = (
        f'python3 "{big_chunker}" '
        f'--pb "{pb_path}" '
        f'--input_root "{cfg_paths["input_dir"]}" '
        f'--rss_cap_gb {rss_cap_gb} '
        f'--db_path "{cfg_paths["db_path"]}" '
        f'--retry-run-id "{run_id}" '
        + " ".join(split_flags)
    ).strip()

    job_name = f'{job_name_prefix}big_retry.{run_id}'
    bsub_parts = ["bsub"]
    if job_group: bsub_parts += ["-g", job_group]
    bsub_parts += ["-J", job_name, "-o", log_file, "-q", queue, run_cmd]
    bsub_cmd = " ".join(f'"{p}"' if (" " in p and not p.startswith("-")) else p for p in bsub_parts)

    print(f"[bsub big]   {bsub_cmd}")
    if dry_run: return None
    r = subprocess.run(bsub_cmd, shell=True, capture_output=True, text=True)
    if r.returncode != 0:
        sys.stderr.write(f"[WARN] big submit rc={r.returncode}\n{r.stdout}\n{r.stderr}\n")
        return None
    return _extract_job_id((r.stdout or "") + (r.stderr or ""))

# -------------------- Merge orchestrator (wait → 03a → 03b) --------------------

def _submit_merge_orchestrator(output_base: str,
                               merge_chunks_script: str,
                               merge_date_script: str,
                               queue: str,
                               job_group: Optional[str],
                               job_name_prefix: str,
                               log_parent: str,
                               run_id: str,
                               delete_chunks: bool,
                               delete_after_date: bool,
                               dry_run: bool) -> Optional[str]:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(log_parent, run_id)
    os.makedirs(run_dir, exist_ok=True)
    log_file = os.path.join(run_dir, f"{run_id}_merge_orchestrator_{ts}.log")

    # Build commands
    merge_cmds = []
    cmd1 = f'python3 "{merge_chunks_script}" --input_dir "{output_base}"'
    if delete_chunks: cmd1 += " --delete_chunks"
    merge_cmds.append(cmd1)

    cmd2 = f'python3 "{merge_date_script}" --input_dir "{output_base}"'
    if delete_after_date: cmd2 += " --delete_after_date"
    merge_cmds.append(cmd2)

    # grep pattern for this run's jobs
    import re as _re
    pattern = f'^(?:{_re.escape(job_name_prefix)}small_retry|{_re.escape(job_name_prefix)}big_retry)\\.{_re.escape(run_id)}$'

    wait_and_merge = (
        f'while bjobs -noheader -o job_name | grep -E -q "{pattern}"; do '
        f'sleep 60; '
        f'done; '
        + " ; ".join(merge_cmds)
    )

    job_name = f'{job_name_prefix}merge_retry.{run_id}'
    bsub_parts = ["bsub"]
    if job_group: bsub_parts += ["-g", job_group]
    bsub_parts += ["-J", job_name, "-o", log_file, "-q", queue, "bash", "-lc", wait_and_merge]
    bsub_cmd = " ".join(f'"{p}"' if (" " in p and not p.startswith("-")) else p for p in bsub_parts)

    print(f"[bsub merge] {bsub_cmd}")
    if dry_run: return None
    r = subprocess.run(bsub_cmd, shell=True, capture_output=True, text=True)
    if r.returncode != 0:
        sys.stderr.write(f"[WARN] merge orchestrator submit rc={r.returncode}\n{r.stdout}\n{r.stderr}\n")
        return None
    return _extract_job_id((r.stdout or "") + (r.stderr or ""))

# -------------------- run.json --------------------

def _write_run_json(log_parent: str, run_id: str, planned: int, small: int, big: int):
    js = {
        "run_id": run_id,
        "planned_total": planned,
        "planned_small": small,
        "planned_big": big,
        "started_ts": _now_iso(),
        "notes": "Snapshot; live progress in DB.",
    }
    try:
        os.makedirs(os.path.join(log_parent, run_id), exist_ok=True)
        with open(os.path.join(log_parent, run_id, "run.json"), "w", encoding="utf-8") as f:
            json.dump(js, f, ensure_ascii=False, indent=2)
    except Exception as e:
        sys.stderr.write(f"[WARN] could not write run.json: {e}\n")

# -------------------- main --------------------

def _parse_failed_list(path: str) -> List[str]:
    out, seen = [], set()
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            s = raw.strip()
            if not s: continue
            if "—" in s: p = s.split("—", 1)[0].strip()
            elif " - " in s: p = s.split(" - ", 1)[0].strip()
            else: p = s
            if p and (p not in seen) and os.path.isfile(p):
                seen.add(p); out.append(p)
    return out

def main():
    ap = argparse.ArgumentParser(description="Retry failed PB conversions with campaign tracking + merges.")
    ap.add_argument("--config", default=DEFAULT_CONFIG, help="Path to config.ini")
    ap.add_argument("--source", choices=["db", "file"], default="db")
    ap.add_argument("--failed_list", default="/home/belle2/amubarak/failed_pb_files.txt")
    ap.add_argument("--via-bsub", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--include_submit_failed", action="store_true")
    ap.add_argument("--include_too_large", action="store_true")
    ap.add_argument("--count_all_jobs", action="store_true")
    ap.add_argument("--chunk_size", type=int, default=None)
    ap.add_argument("--big_threshold_gb", type=float, default=None)
    ap.add_argument("--run-tag", default=None)
    args = ap.parse_args()

    paths, lsf, submit, merge, _repo_root = _read_cfg(args.config)

    input_dir   = paths["input_dir"]
    output_base = paths["output_base"]
    logs_root   = paths["logs_root"]
    logs_subdir = paths["logs_subdir_pb2parquet"] or "pb2parquet"
    db_path     = paths["db_path"]
    small_script= paths["small_script"]
    big_script  = paths["big_chunker_script"]
    merge_chunks_script = paths["merge_chunks_script"]
    merge_date_script   = paths["merge_date_script"]

    queue            = lsf["queue"]
    max_concurrent   = int(lsf["max_concurrent_bsubs"])
    job_group        = lsf.get("job_group")
    job_name_prefix  = lsf.get("job_name_prefix", "pvpipe_")
    # normalize to avoid accidental double dots
    if not job_name_prefix.endswith("_"):
        job_name_prefix = job_name_prefix + "_"

    chunk_size       = int(args.chunk_size or submit["chunk_size"])
    big_threshold_gb = float(args.big_threshold_gb or submit["big_threshold_gb"])
    rss_cap_gb       = float(submit["rss_cap_gb"])
    split_mode       = submit["split_mode"]
    split_granularity = submit["split_granularity"]
    tz_offset_hours  = submit["tz_offset_hours"]
    emit_date_view   = bool(submit["emit_date_view"])
    date_view_root   = submit["date_view_root"]

    delete_chunks_after_pv = bool(merge["delete_chunks_after_pv"])
    delete_after_date      = bool(merge["delete_parquet_after_date"])

    retry_logs_parent = os.path.join(logs_root.rstrip("/"), logs_subdir, "retry")
    os.makedirs(retry_logs_parent, exist_ok=True)

    _ensure_tables_and_columns(db_path)

    if args.source == "db":
        failed_list = _get_failed_from_db(
            db_path=db_path,
            include_submit_failed=args.include_submit_failed,
            include_too_large=args.include_too_large,
            max_retries=2
        )
    else:
        if not os.path.exists(args.failed_list):
            sys.stderr.write(f"[ERROR] Missing failed list file: {args.failed_list}\n")
            sys.exit(2)
        failed_list = _parse_failed_list(args.failed_list)

    if not failed_list:
        print("Nothing to retry.")
        return

    small_pbs, big_pbs = _classify_small_big(failed_list, big_threshold_gb)
    print(f"[plan] small={len(small_pbs)}, big={len(big_pbs)}, chunk_size={chunk_size}, big_threshold_gb={big_threshold_gb}")

    small_split_on, small_split_off = [], []
    for p in small_pbs:
        (small_split_on if _want_split_by_date(split_mode, p) else small_split_off).append(p)

    def _make_bundles(paths: List[str], k: int) -> List[List[str]]:
        return [paths[i:i+k] for i in range(0, len(paths), k)]

    bundles_on  = _make_bundles(small_split_on,  chunk_size)
    bundles_off = _make_bundles(small_split_off, chunk_size)

    base_run = _new_run_id()
    clean_tag = re.sub(r"[^A-Za-z0-9_-]+", "_", args.run_tag) if args.run_tag else None
    run_id = base_run if not clean_tag else (base_run + "_" + clean_tag)
    run_dir = os.path.join(retry_logs_parent, run_id); os.makedirs(run_dir, exist_ok=True)

    planned_all = list(big_pbs) + small_split_on + small_split_off
    _create_retry_campaign(
        db_path=db_path, run_id=run_id, logs_dir=run_dir, planned=planned_all,
        args={
            "source": args.source,
            "include_submit_failed": args.include_submit_failed,
            "include_too_large": args.include_too_large,
            "chunk_size": chunk_size,
            "big_threshold_gb": big_threshold_gb,
            "queue": queue,
            "split_mode": split_mode,
            "split_granularity": split_granularity,
            "tz_offset_hours": tz_offset_hours,
            "emit_date_view": emit_date_view,
            "date_view_root": date_view_root,
            "delete_chunks_after_pv": delete_chunks_after_pv,
            "delete_after_date": delete_after_date
        }
    )
    _set_initial_statuses(db_path, run_id, planned_all)
    _write_run_json(retry_logs_parent, run_id, planned=len(planned_all), small=len(small_pbs), big=len(big_pbs))

    current = _current_job_count(
        (f"{job_name_prefix}small_retry", f"{job_name_prefix}big_retry"),
        count_all_jobs_if_unknown=args.count_all_jobs
    )
    room = max(0, max_concurrent - current)
    print(f"[ceiling] current_jobs={current}, ceiling={max_concurrent}, room={room}")

    if not args.via_bsub:
        sys.stderr.write("[ERROR] --via-bsub is required for retry submission.\n")
        sys.exit(2)

    submitted_jobs = 0
    cfg_paths = {
        "input_dir": input_dir,
        "db_path": db_path,
        "small_script": small_script,
        "big_chunker_script": big_script,
    }

    # BIG first (ensures chunking path is used for large PBs)
    for pb in big_pbs:
        if room <= 0: break
        split_flags = _build_split_flags(split_mode, pb, split_granularity, tz_offset_hours, emit_date_view, date_view_root)
        job_id = _submit_big_job(pb, split_flags, cfg_paths, rss_cap_gb, queue, job_group, job_name_prefix,
                                 retry_logs_parent, args.dry_run, run_id)
        if job_id:
            if not args.dry_run:
                _increment_retries_and_mark_queued(db_path, [pb], "retry: big")
                _record_job_ids(db_path, [pb], job_id)
                _mark_retry_queued(db_path, run_id, [pb], job_id)
        else:
            if not args.dry_run:
                _mark_submit_failed(db_path, [pb], "retry submit failed (big)")
        submitted_jobs += 1; room -= 1

    # SMALL bundles
    def _submit_bundle_group(bundles: List[List[str]]):
        nonlocal submitted_jobs, room
        for bundle in bundles:
            if room <= 0: break
            rep = bundle[0]
            split_flags = _build_split_flags(split_mode, rep, split_granularity, tz_offset_hours, emit_date_view, date_view_root)
            job_id = _submit_small_bundle(bundle, split_flags, cfg_paths, queue, job_group, job_name_prefix,
                                          retry_logs_parent, args.dry_run, run_id)
            if job_id:
                if not args.dry_run:
                    _increment_retries_and_mark_queued(db_path, bundle, "retry: small-bundle")
                    _record_job_ids(db_path, bundle, job_id)
                    _mark_retry_queued(db_path, run_id, bundle, job_id)
            else:
                if not args.dry_run:
                    _mark_submit_failed(db_path, bundle, "retry submit failed (small bundle)")
            submitted_jobs += 1; room -= 1

    _submit_bundle_group(bundles_on)
    _submit_bundle_group(bundles_off)

    # Orchestrate merges after conversion
    _submit_merge_orchestrator(
        output_base=output_base,
        merge_chunks_script=merge_chunks_script,
        merge_date_script=merge_date_script,
        queue=queue,
        job_group=job_group,
        job_name_prefix=job_name_prefix,
        log_parent=retry_logs_parent,
        run_id=run_id,
        delete_chunks=delete_chunks_after_pv,
        delete_after_date=delete_after_date,
        dry_run=args.dry_run
    )

    print(f"[summary] planned small_bundles={len(bundles_on)+len(bundles_off)}, big_jobs={len(big_pbs)}")
    print(f"[summary] actually submitted jobs={submitted_jobs} (respecting ceiling)")
    print(f"[summary] run_id={run_id} logs_dir={run_dir}")
    if args.dry_run:
        print("[note] dry-run: no DB updates or LSF submissions were performed.")

if __name__ == "__main__":
    main()