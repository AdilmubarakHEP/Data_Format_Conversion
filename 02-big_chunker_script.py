#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
02-big_chunker_script.py

BIG .pb chunker with date-splitting, OOO tolerance, and parallel Parquet writes.

Reads config from Data_Format_Conversion/config.ini (or $PVPIPE_CONFIG).
Defaults pulled from:
  [paths]     output_base, db_path, input_dir
  [submitter] thread_count, rss_cap_gb, allow_out_of_order, ooo_max_open_buckets,
              split_mode, split_granularity, tz_offset_hours
  [java]      pb2parquet_classpath

CLI flags still override config defaults.
"""

import os
import sys
import re
import argparse
import sqlite3
import subprocess
import threading
import configparser
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict

import numpy as np
import numpy.ma as ma
import pyarrow as pa
import pyarrow.parquet as pq

# Try psutil; fall back to /proc/self/status
try:
    import psutil
except Exception:
    psutil = None

# ------------------------- config loader -------------------------
def _load_ini():
    """
    Load INI from:
      1) $PVPIPE_CONFIG
      2) <repo>/Data_Format_Conversion/config.ini (relative to this file)
      3) ~/Data_Format_Conversion/config.ini
    """
    candidates = []
    env_path = os.environ.get("PVPIPE_CONFIG")
    if env_path:
        candidates.append(env_path)

    # repo-relative
    here = os.path.abspath(os.path.dirname(__file__))
    repo_guess = os.path.normpath(os.path.join(here, "..", "Data_Format_Conversion", "config.ini"))
    candidates.append(repo_guess)

    # home fallback
    home_guess = os.path.expanduser("~/Data_Format_Conversion/config.ini")
    candidates.append(home_guess)

    parser = configparser.ConfigParser()
    for p in candidates:
        if p and os.path.isfile(p):
            if parser.read(p):
                return parser, p
    raise SystemExit(f"Config not found. Tried: {candidates}")

def _get(parser, sect, key, default=None):
    return parser.get(sect, key, fallback=default)

def _getint(parser, sect, key, default=None):
    try:
        return parser.getint(sect, key, fallback=default)
    except Exception:
        return default

def _getfloat(parser, sect, key, default=None):
    try:
        return parser.getfloat(sect, key, fallback=default)
    except Exception:
        return default

def _getbool(parser, sect, key, default=None):
    try:
        return parser.getboolean(sect, key, fallback=default)
    except Exception:
        # treat typical strings
        v = parser.get(sect, key, fallback=None)
        if v is None:
            return default
        s = str(v).strip().lower()
        if s in ("1","true","yes","on"): return True
        if s in ("0","false","no","off"): return False
        return default

PARSER, CFG_PATH = _load_ini()

# paths
OUTPUT_BASE_DEFAULT = _get(PARSER, "paths", "output_base", "/tmp/converted_flat2")
DB_PATH_DEFAULT     = _get(PARSER, "paths", "db_path", None)
INPUT_DIR_DEFAULT   = _get(PARSER, "paths", "input_dir", None)

# submitter defaults
THREADS_DEFAULT      = max(1, _getint(PARSER, "submitter", "thread_count", 4) or 4)
RSS_CAP_GB_DEFAULT   = float(_getfloat(PARSER, "submitter", "rss_cap_gb", 0.5) or 0.5)
ALLOW_OOO_DEFAULT    = bool(_getbool(PARSER, "submitter", "allow_out_of_order", False))
OOO_MAX_OPEN_DEFAULT = int(_getint(PARSER, "submitter", "ooo_max_open_buckets", 6) or 6)
SPLIT_MODE_DEFAULT   = _get(PARSER, "submitter", "split_mode", "auto")  # "none", "auto"
SPLIT_GRAN_DEFAULT   = _get(PARSER, "submitter", "split_granularity", "day")  # day|week|month
TZ_OFFSET_DEFAULT    = int(_get(PARSER, "submitter", "tz_offset_hours", "0") or 0)

# java
JAVA_CLASS_PATH      = _get(PARSER, "java", "pb2parquet_classpath", "/home/belle2/amubarak/Data_Format_Conversion/java")
PB2PARQUET_MAIN      = "pb2parquet"

# env override for threads if provided
THREADS_DEFAULT = int(os.environ.get("PVPIPE_WRITER_THREADS", THREADS_DEFAULT))

# ------------------------- helpers -------------------------
_CHUNK_RE = re.compile(r"_chunk(\d{4})\.parquet$")

def safe_base_from_pb(pb_path: str) -> str:
    base = os.path.splitext(os.path.basename(pb_path))[0]
    return base.replace(":", "_")

def date_token_from_pb(pb_path: str) -> str:
    base = os.path.splitext(os.path.basename(pb_path))[0]
    parts = base.split(":")
    return parts[-1] if parts else base

def derive_rel_dir(pb_path: str, input_root: Optional[str]) -> str:
    pb_dir = os.path.dirname(pb_path)
    if input_root:
        try:
            rel = os.path.relpath(pb_dir, input_root)
            if not rel.startswith(".."):
                return "." if rel == "." else rel
        except Exception:
            pass
    return os.path.basename(pb_dir)

def get_rss_gb() -> float:
    if psutil is not None:
        try:
            return psutil.Process().memory_info().rss / (1024 ** 3)
        except Exception:
            pass
    try:
        with open("/proc/self/status", "r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    kb = float(line.split()[1])
                    return kb / (1024 ** 2)
    except Exception:
        pass
    return 0.0

# ----- stats & timing (numeric only) -----
def convert_to_fractional_seconds(times: np.ndarray) -> np.ndarray:
    if times.size == 0:
        return times
    return times - times[0]

def _nan_stats_1d(x: np.ndarray):
    x = np.asarray(x, dtype=float)
    nan_count = int(np.isnan(x).sum())
    xv = x[~np.isnan(x)]
    if xv.size == 0:
        return {
            "min": None, "max": None, "mean": None, "std": None,
            "skewness": None, "kurtosis": None, "snr": None, "mad": None, "cv": None,
            "rate_of_change": None, "rate_of_change_variance": None, "peaks": 0,
            "nan_count": nan_count
        }
    mn = float(np.nanmin(xv)); mx = float(np.nanmax(xv))
    mu = float(np.nanmean(xv)); sd = float(np.nanstd(xv))
    if xv.size >= 3 and sd > 0:
        z = (xv - mu) / sd
        sk = float(np.nanmean(z**3))
        ku = float(np.nanmean(z**4))
    else:
        sk, ku = None, None
    snr = None if sd == 0 else float(mu / sd)
    cv  = None if mu == 0 else float(sd / abs(mu))
    diffs = np.diff(xv)
    roc = float(np.nanmean(diffs)) if diffs.size > 0 else None
    roc_var = float(np.nanvar(diffs)) if diffs.size > 0 else None
    peaks = 0
    if xv.size >= 3:
        peaks = int(np.sum((xv[1:-1] > xv[:-2]) & (xv[1:-1] > xv[2:])))
    mad = float(np.nanmedian(np.abs(xv - np.nanmedian(xv))))
    return {
        "min": mn, "max": mx, "mean": mu, "std": sd,
        "skewness": sk, "kurtosis": ku, "snr": snr, "mad": mad, "cv": cv,
        "rate_of_change": roc, "rate_of_change_variance": roc_var,
        "peaks": peaks, "nan_count": nan_count
    }

def timing_stats(times: List[float]):
    t = np.asarray(times, dtype=float)
    if t.size <= 1:
        return {
            "sampling_frequency": None, "avg_interval": None, "median_interval": None,
            "jitter": None, "interval_variance": None, "max_interval": None,
            "duration": 0.0, "start_time": 0.0, "end_time": 0.0
        }
    frac = convert_to_fractional_seconds(t)
    intervals = np.diff(frac)
    mean_int = float(np.mean(intervals))
    sfreq = float(1.0 / mean_int) if mean_int > 0.0 else None
    return {
        "sampling_frequency": sfreq,
        "avg_interval": mean_int,
        "median_interval": float(np.median(intervals)),
        "jitter": float(np.std(intervals)),
        "interval_variance": float(np.var(intervals)),
        "max_interval": float(np.max(intervals)),
        "duration": float(frac[-1] - frac[0]),
        "start_time": float(frac[0]),
        "end_time": float(frac[-1]),
    }

def vector_stats_2d(mat: np.ndarray):
    if mat.ndim != 2:
        mat = np.reshape(mat, (-1, 1))
    T, N = mat.shape
    if T == 0 or N == 0:
        return {
            "min": [None]*N, "max": [None]*N, "mean": [None]*N, "std": [None]*N,
            "skewness": [None]*N, "kurtosis": [None]*N, "snr": [None]*N, "mad": [None]*N, "cv": [None]*N,
            "rate_of_change": [None]*N, "rate_of_change_variance": [None]*N,
            "peaks": [0]*N, "nan_counts": [0]*N
        }
    m = ma.masked_invalid(mat)
    mins  = m.min(axis=0).filled(np.nan).astype(float)
    maxs  = m.max(axis=0).filled(np.nan).astype(float)
    means = m.mean(axis=0).filled(np.nan).astype(float)
    stds  = m.std(axis=0).filled(np.nan).astype(float)
    sd_nz = np.where(stds == 0, np.nan, stds)
    z = (mat - means) / sd_nz
    valid_z = np.any(np.isfinite(z), axis=0)
    skews = np.full(N, np.nan, dtype=float)
    kurts = np.full(N, np.nan, dtype=float)
    if np.any(valid_z):
        zv = z[:, valid_z]
        skews[valid_z] = np.nanmean(zv**3, axis=0)
        kurts[valid_z] = np.nanmean(zv**4, axis=0)
    snrs = np.where(stds == 0, np.nan, means / stds)
    cvs  = np.where(np.abs(means) == 0, np.nan, stds / np.abs(means))
    diffs = np.diff(mat, axis=0)
    valid_d = np.any(np.isfinite(diffs), axis=0)
    rocs     = np.full(N, np.nan, dtype=float)
    roc_vars = np.full(N, np.nan, dtype=float)
    if np.any(valid_d):
        dv = diffs[:, valid_d]
        rocs[valid_d] = np.nanmean(dv, axis=0)
        roc_vars[valid_d] = np.nanvar(dv, axis=0)
    med  = ma.median(m, axis=0).filled(np.nan).astype(float)
    mads = ma.median(ma.abs(m - med), axis=0).filled(np.nan).astype(float)
    peaks = np.zeros(N, dtype=np.int64)
    for j in range(N):
        col = mat[:, j]
        col = col[~np.isnan(col)]
        if col.size >= 3:
            peaks[j] = int(np.sum((col[1:-1] > col[:-2]) & (col[1:-1] > col[2:])))
        else:
            peaks[j] = 0
    nan_counts = np.isnan(mat).sum(axis=0).astype(np.int64)
    def _tolist(a):
        out = []
        for x in a:
            if isinstance(x, (np.floating, float)):
                out.append(None if (isinstance(x, float) and np.isnan(x)) else float(x))
            else:
                out.append(x)
        return out
    return {
        "min": _tolist(mins), "max": _tolist(maxs), "mean": _tolist(means), "std": _tolist(stds),
        "skewness": _tolist(skews), "kurtosis": _tolist(kurts), "snr": _tolist(snrs), "mad": _tolist(mads), "cv": _tolist(cvs),
        "rate_of_change": _tolist(rocs), "rate_of_change_variance": _tolist(roc_vars),
        "peaks": peaks.tolist(), "nan_counts": nan_counts.tolist()
    }

# ----- Parquet writers -----
def write_chunk_numeric_vector(out_path: str, pv_name: str, date_token: str, t0_ms: int,
                               times: List[float], comp_lists: List[List[float]]):
    N = len(comp_lists) if comp_lists else 0
    T = len(times)
    if N == 0:
        N = 1
        comp_lists = [[]]
    mat = np.full((T, N), np.nan, dtype=np.float64)
    for j, col in enumerate(comp_lists):
        if col:
            m = min(len(col), T)
            mat[:m, j] = np.asarray(col[:m], dtype=np.float64)
    stats = vector_stats_2d(mat)
    tstats = timing_stats(times)
    values_matrix = mat.tolist()
    values_by_component = comp_lists
    names = [
        "value_type","pv","date","t0","Nval","times","length",
        "values_matrix","values_by_component","component_names",
        "min","max","mean","std","skewness","kurtosis","snr","mad","cv",
        "rate_of_change","rate_of_change_variance","peaks","nan_counts",
        "sampling_frequency","avg_interval","median_interval","jitter",
        "interval_variance","max_interval","duration","start_time","end_time"
    ]
    arrays = [
        pa.array(["numeric"]),
        pa.array([pv_name]),
        pa.array([date_token]),
        pa.array([int(t0_ms)], type=pa.int64()),
        pa.array([int(N)], type=pa.int64()),
        pa.array([times], type=pa.list_(pa.float64())),
        pa.array([T], type=pa.int64()),
        pa.array([values_matrix], type=pa.list_(pa.list_(pa.float64()))),
        pa.array([values_by_component], type=pa.list_(pa.list_(pa.float64()))),
        pa.array([[f"{pv_name}[{i}]" for i in range(N)]], type=pa.list_(pa.string())),
        pa.array([stats["min"]], type=pa.list_(pa.float64())),
        pa.array([stats["max"]], type=pa.list_(pa.float64())),
        pa.array([stats["mean"]], type=pa.list_(pa.float64())),
        pa.array([stats["std"]], type=pa.list_(pa.float64())),
        pa.array([stats["skewness"]], type=pa.list_(pa.float64())),
        pa.array([stats["kurtosis"]], type=pa.list_(pa.float64())),
        pa.array([stats["snr"]], type=pa.list_(pa.float64())),
        pa.array([stats["mad"]], type=pa.list_(pa.float64())),
        pa.array([stats["cv"]], type=pa.list_(pa.float64())),
        pa.array([stats["rate_of_change"]], type=pa.list_(pa.float64())),
        pa.array([stats["rate_of_change_variance"]], type=pa.list_(pa.float64())),
        pa.array([stats["peaks"]], type=pa.list_(pa.int64())),
        pa.array([stats["nan_counts"]], type=pa.list_(pa.int64())),
        pa.array([tstats["sampling_frequency"]], type=pa.float64()),
        pa.array([tstats["avg_interval"]], type=pa.float64()),
        pa.array([tstats["median_interval"]], type=pa.float64()),
        pa.array([tstats["jitter"]], type=pa.float64()),
        pa.array([tstats["interval_variance"]], type=pa.float64()),
        pa.array([tstats["max_interval"]], type=pa.float64()),
        pa.array([tstats["duration"]], type=pa.float64()),
        pa.array([tstats["start_time"]], type=pa.float64()),
        pa.array([tstats["end_time"]], type=pa.float64()),
    ]
    table = pa.Table.from_arrays(arrays, names=names)
    pq.write_table(table, out_path, compression="snappy", use_dictionary=False)

def write_chunk_numeric_scalar(out_path: str, pv_name: str, date_token: str, t0_ms: int,
                               times: List[float], values: List[float]):
    s = _nan_stats_1d(np.asarray(values, dtype=np.float64))
    tstats = timing_stats(times)
    names = [
        "value_type","pv","date","t0","Nval","times","length","values",
        "min","max","mean","std","skewness","kurtosis","snr","mad","cv",
        "rate_of_change","rate_of_change_variance","peaks",
        "sampling_frequency","avg_interval","median_interval","jitter",
        "interval_variance","max_interval","duration","start_time","end_time"
    ]
    arrays = [
        pa.array(["numeric"]),
        pa.array([pv_name]),
        pa.array([date_token]),
        pa.array([int(t0_ms)], type=pa.int64()),
        pa.array([1], type=pa.int64()),
        pa.array([times], type=pa.list_(pa.float64())),
        pa.array([len(times)], type=pa.int64()),
        pa.array([values], type=pa.list_(pa.float64())),
        pa.array([s["min"]], type=pa.float64()),
        pa.array([s["max"]], type=pa.float64()),
        pa.array([s["mean"]], type=pa.float64()),
        pa.array([s["std"]], type=pa.float64()),
        pa.array([s["skewness"]], type=pa.float64()),
        pa.array([s["kurtosis"]], type=pa.float64()),
        pa.array([s["snr"]], type=pa.float64()),
        pa.array([s["mad"]], type=pa.float64()),
        pa.array([s["cv"]], type=pa.float64()),
        pa.array([s["rate_of_change"]], type=pa.float64()),
        pa.array([s["rate_of_change_variance"]], type=pa.float64()),
        pa.array([s["peaks"]], type=pa.int64()),
        pa.array([tstats["sampling_frequency"]], type=pa.float64()),
        pa.array([tstats["avg_interval"]], type=pa.float64()),
        pa.array([tstats["median_interval"]], type=pa.float64()),
        pa.array([tstats["jitter"]], type=pa.float64()),
        pa.array([tstats["interval_variance"]], type=pa.float64()),
        pa.array([tstats["max_interval"]], type=pa.float64()),
        pa.array([tstats["duration"]], type=pa.float64()),
        pa.array([tstats["start_time"]], type=pa.float64()),
        pa.array([tstats["end_time"]], type=pa.float64()),
    ]
    table = pa.Table.from_arrays(arrays, names=names)
    pq.write_table(table, out_path, compression="snappy", use_dictionary=False)

def write_chunk_text_vector(out_path: str, pv_name: str, date_token: str, t0_ms: int,
                            times: List[float], comp_lists: List[List[str]]):
    N = len(comp_lists)
    T = len(times)
    values_text_matrix = []
    for i in range(T):
        row = [comp_lists[j][i] if i < len(comp_lists[j]) else "" for j in range(N)]
        values_text_matrix.append(row)
    names = [
        "value_type","pv","date","t0","Nval","times","length",
        "values_text_matrix","values_text_by_component","component_names"
    ]
    arrays = [
        pa.array(["text"]),
        pa.array([pv_name]),
        pa.array([date_token]),
        pa.array([int(t0_ms)], type=pa.int64()),
        pa.array([int(N)], type=pa.int64()),
        pa.array([times], type=pa.list_(pa.float64())),
        pa.array([len(times)], type=pa.int64()),
        pa.array([values_text_matrix], type=pa.list_(pa.list_(pa.string()))),
        pa.array([comp_lists], type=pa.list_(pa.list_(pa.string()))),
        pa.array([[f"{pv_name}[{i}]" for i in range(N)]], type=pa.list_(pa.string())),
    ]
    table = pa.Table.from_arrays(arrays, names=names)
    pq.write_table(table, out_path, compression="snappy")

def write_chunk_text_scalar(out_path: str, pv_name: str, date_token: str, t0_ms: int,
                            times: List[float], values: List[str]):
    names = ["value_type","pv","date","t0","Nval","times","length","values_text"]
    arrays = [
        pa.array(["text"]),
        pa.array([pv_name]),
        pa.array([date_token]),
        pa.array([int(t0_ms)], type=pa.int64()),
        pa.array([1], type=pa.int64()),
        pa.array([times], type=pa.list_(pa.float64())),
        pa.array([len(times)], type=pa.int64()),
        pa.array([values], type=pa.list_(pa.string())),
    ]
    table = pa.Table.from_arrays(arrays, names=names)
    pq.write_table(table, out_path, compression="snappy")

# ---------------- Date helpers ----------------
def make_tz(offset_hours: int) -> timezone:
    return timezone(timedelta(hours=int(offset_hours or 0)))

def date_token_from_timestamp_ms(ts_ms: int, tz: timezone, granularity: str) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(tz)
    if granularity == "month":
        return dt.strftime("%Y_%m")
    elif granularity == "week":
        iso = dt.isocalendar()
        return f"{iso.year}_W{iso.week:02d}"
    else:
        return dt.strftime("%Y_%m_%d")

# ---------------- SQLite helpers (status + retry) ----------------
def _db_connect(db_path: str):
    conn = sqlite3.connect(db_path, timeout=30)
    c = conn.cursor()
    c.execute("PRAGMA journal_mode=WAL;")
    c.execute("PRAGMA busy_timeout=5000;")
    c.execute("PRAGMA synchronous=NORMAL;")
    conn.commit()
    return conn

def _ensure_conversions_table(conn: sqlite3.Connection):
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS conversions (
        pb_path   TEXT PRIMARY KEY,
        timestamp TEXT,
        status    TEXT,
        message   TEXT
    )""")
    conn.commit()

def _ensure_retry_items_table(conn: sqlite3.Connection):
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS retry_items (
            run_id         TEXT,
            pb_path        TEXT,
            initial_status TEXT,
            queued_ts      TEXT,
            job_id         TEXT,
            finished_ts    TEXT,
            outcome        TEXT,
            PRIMARY KEY (run_id, pb_path)
        )
    """)
    conn.commit()

def _ensure_db(db_path: str):
    conn = _db_connect(db_path)
    try:
        _ensure_conversions_table(conn)
        _ensure_retry_items_table(conn)
    finally:
        conn.close()

def _upsert_status(db_path: Optional[str], pb_path: str, status: str, message: str = ""):
    if not db_path:
        return
    try:
        _ensure_db(db_path)
    except Exception:
        pass
    ts = datetime.now().isoformat(timespec="seconds")
    delay = 0.1
    for _ in range(6):
        try:
            conn = _db_connect(db_path)
            c = conn.cursor()
            c.execute(
                "INSERT OR REPLACE INTO conversions (pb_path, timestamp, status, message) VALUES (?, ?, ?, ?)",
                (pb_path, ts, status, message[:512]),
            )
            conn.commit()
            conn.close()
            break
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                import time as _t; _t.sleep(delay); delay = min(1.0, delay * 2)
                continue
            else:
                sys.stderr.write(f"[DB] operational error: {e}\n")
                break
        except Exception as e:
            sys.stderr.write(f"[DB] error: {e}\n")
            break
    print(f"PV_STATUS: {status} :: {pb_path}")

def _retry_mark_start(db_path: Optional[str], run_id: str, pb_path: str):
    if not db_path or not run_id:
        return
    try:
        conn = _db_connect(db_path)
        _ensure_retry_items_table(conn)
        now = datetime.now().isoformat(timespec="seconds")
        c = conn.cursor()
        c.execute("""
            UPDATE retry_items
               SET queued_ts = COALESCE(queued_ts, ?)
             WHERE run_id = ? AND pb_path = ?
        """, (now, run_id, pb_path))
        conn.commit(); conn.close()
    except Exception as e:
        sys.stderr.write(f"[DB] retry_mark_start error: {e}\n")

def _retry_mark_done(db_path: Optional[str], run_id: str, pb_path: str, outcome: str):
    if not db_path or not run_id:
        return
    try:
        conn = _db_connect(db_path)
        _ensure_retry_items_table(conn)
        now = datetime.now().isoformat(timespec="seconds")
        c = conn.cursor()
        c.execute("""
            UPDATE retry_items
               SET finished_ts = ?,
                   outcome     = ?
             WHERE run_id = ? AND pb_path = ?
        """, (now, outcome, run_id, pb_path))
        conn.commit(); conn.close()
    except Exception as e:
        sys.stderr.write(f"[DB] retry_mark_done error: {e}\n")

# ---------------- Core (OOO-aware with parallel writers) ----------------
def stream_and_chunk(pb_path: str, output_base: str, rss_cap_gb: float, input_root: Optional[str],
                     check_every: int, split_by_date: bool, split_granularity: str,
                     tz_offset_hours: int, allow_ooo: bool, ooo_max_open: int,
                     db_path: Optional[str], retry_run_id: str,
                     writer_threads: int):

    if retry_run_id:
        _retry_mark_start(db_path, retry_run_id, pb_path)

    tz = make_tz(tz_offset_hours)

    # Launch Java with a larger pipe buffer and explicit encoding
    cmd = ["java", "-cp", JAVA_CLASS_PATH, PB2PARQUET_MAIN, pb_path]
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="ignore",
        bufsize=1 << 20  # 1 MiB pipe buffer
    )

    pv_name: Optional[str] = None
    t0_ms: Optional[int] = None
    nval_header: Optional[int] = None

    value_mode: Optional[str] = None  # "numeric" | "text"
    ncols: Optional[int] = None

    safe_base_global = safe_base_from_pb(pb_path)
    fallback_date_tok = date_token_from_pb(pb_path)

    rel_dir = derive_rel_dir(pb_path, input_root)
    out_dir = os.path.join(output_base, "" if rel_dir == "." else rel_dir)
    os.makedirs(out_dir, exist_ok=True)

    # Per-date chunk index, protected by a lock
    chunk_counters: Dict[str, int] = {}
    counter_lock = threading.Lock()

    def _seed_counter_from_fs(date_key: str):
        if date_key in chunk_counters:
            return
        if split_by_date:
            base = f"{safe_base_global}_{date_key}"
        else:
            base = safe_base_global
        max_idx = 0
        try:
            for name in os.listdir(out_dir):
                if not (name.startswith(base) and name.endswith(".parquet")):
                    continue
                m = _CHUNK_RE.search(name)
                if m:
                    idx = int(m.group(1))
                    if idx > max_idx:
                        max_idx = idx
        except Exception:
            pass
        chunk_counters[date_key] = max_idx

    def next_chunk_path(date_key: str) -> str:
        with counter_lock:
            _seed_counter_from_fs(date_key)
            base = f"{safe_base_global}_{date_key}" if split_by_date else safe_base_global
            idx = chunk_counters.get(date_key, 0) + 1
            chunk_counters[date_key] = idx
            return os.path.join(out_dir, f"{base}_chunk{idx:04d}.parquet")

    # Buckets keyed by date token (or "GLOBAL")
    class Bucket:
        __slots__ = ("key","times","num_scalar","num_vec","txt_scalar","txt_vec","last_touch")
        def __init__(self, key: str):
            self.key = key
            self.times: List[float] = []
            self.num_scalar: Optional[List[float]] = None
            self.num_vec: Optional[List[List[float]]] = None
            self.txt_scalar: Optional[List[str]] = None
            self.txt_vec: Optional[List[List[str]]] = None
            self.last_touch = 0  # monotonic counter

    buckets: Dict[str, Bucket] = {}
    touch_counter = 0
    current_key: Optional[str] = None

    # Parallel writer pool
    writer_pool = ThreadPoolExecutor(max_workers=max(1, writer_threads))
    pending = []
    max_pending = max(2, writer_threads * 2)

    def date_for_row(date_key: str) -> str:
        return date_key if split_by_date else fallback_date_tok

    def flush_bucket_async(date_key: str):
        b = buckets.get(date_key)
        if not b or not b.times:
            return

        # Snapshot and clear to free memory quickly
        local_times = b.times; b.times = []
        local_num_scalar = b.num_scalar; b.num_scalar = [] if local_num_scalar is not None else None
        local_num_vec = b.num_vec; b.num_vec = [[] for _ in range(len(local_num_vec))] if local_num_vec is not None else None
        local_txt_scalar = b.txt_scalar; b.txt_scalar = [] if local_txt_scalar is not None else None
        local_txt_vec = b.txt_vec; b.txt_vec = [[] for _ in range(len(local_txt_vec))] if local_txt_vec is not None else None

        out_path = next_chunk_path(date_key)
        row_date = date_for_row(date_key)

        def _writer():
            if value_mode == "numeric":
                if ncols == 1:
                    write_chunk_numeric_scalar(out_path, pv_name, row_date, int(t0_ms), local_times, local_num_scalar or [])
                else:
                    write_chunk_numeric_vector(out_path, pv_name, row_date, int(t0_ms), local_times, local_num_vec or [[] for _ in range(ncols or 1)])
            else:
                if ncols == 1:
                    write_chunk_text_scalar(out_path, pv_name, row_date, int(t0_ms), local_times, local_txt_scalar or [])
                else:
                    write_chunk_text_vector(out_path, pv_name, row_date, int(t0_ms), local_times, local_txt_vec or [[] for _ in range(ncols or 1)])
            print(f"SAVED CHUNK: {out_path} (rows={len(local_times)}, rss={get_rss_gb():.3f} GB)")

        fut = writer_pool.submit(_writer)
        pending.append(fut)
        # Back-pressure if too many writes queued
        while len(pending) > max_pending:
            done = pending.pop(0)
            done.result()

    total_rows = 0
    chunks_enqueued = 0
    line_count = 0

    try:
        for raw in proc.stdout:
            if not raw:
                break
            if raw.startswith("#"):
                s = raw.strip()
                if s.startswith("# PV name"):
                    pv_name = s.split(None, 3)[-1]
                elif s.startswith("# t0"):
                    try:
                        t0_ms = int(s.split()[-1])
                    except Exception:
                        pass
                elif s.startswith("# Nval"):
                    try:
                        nval_header = int(s.split()[-1])
                    except Exception:
                        pass
                continue
            if not raw.strip():
                continue

            # Fast numeric parse
            arr = np.fromstring(raw, sep=' ')
            parsed_as_numeric = arr.size >= 1
            if parsed_as_numeric:
                off_ms = float(arr[0]); off_s = off_ms / 1000.0
                vals_num = arr[1:]
                if value_mode is None:
                    if nval_header and nval_header > 0:
                        ncols = int(nval_header)
                    else:
                        ncols = max(1, int(vals_num.size))
                    value_mode = "numeric" if vals_num.size >= (ncols or 1) else "text"

                if value_mode == "numeric":
                    key = "GLOBAL"
                    if split_by_date:
                        ts_ms = int((t0_ms or 0) + off_ms)
                        key = date_token_from_timestamp_ms(ts_ms, tz, split_granularity)
                    if not allow_ooo:
                        if current_key is None:
                            current_key = key
                        elif key != current_key:
                            flush_bucket_async(current_key); chunks_enqueued += 1
                            current_key = key
                    b = buckets.get(key)
                    if b is None:
                        b = Bucket(key); buckets[key] = b
                    if b.num_scalar is None and b.num_vec is None:
                        if ncols == 1:
                            b.num_scalar = []
                        else:
                            b.num_vec = [[] for _ in range(ncols)]
                    b.times.append(off_s)
                    if ncols == 1:
                        v = float(vals_num[0]) if vals_num.size >= 1 else float("nan")
                        b.num_scalar.append(v)
                    else:
                        for j in range(ncols):
                            b.num_vec[j].append(float(vals_num[j]) if j < vals_num.size else float("nan"))
                    total_rows += 1
                    line_count += 1
                    touch_counter += 1
                    b.last_touch = touch_counter

                    if line_count % max(1, check_every) == 0:
                        if get_rss_gb() >= rss_cap_gb and buckets:
                            # Evict least-recently-touched
                            victim_key = min(buckets.keys(), key=lambda k: buckets[k].last_touch)
                            flush_bucket_async(victim_key); chunks_enqueued += 1

                    if allow_ooo and len(buckets) > max(1, ooo_max_open):
                        victim_key = min(buckets.keys(), key=lambda k: buckets[k].last_touch)
                        flush_bucket_async(victim_key); chunks_enqueued += 1

                    continue  # handled numeric

            # TEXT path
            toks = raw.strip().split()
            if not toks:
                continue
            try:
                off_ms = float(toks[0]); off_s = off_ms / 1000.0
            except Exception:
                continue
            vals_txt = toks[1:]
            if value_mode is None:
                ncols = int(nval_header) if (nval_header and nval_header > 0) else max(1, len(vals_txt))
                value_mode = "text"
            key = "GLOBAL"
            if split_by_date:
                ts_ms = int((t0_ms or 0) + off_ms)
                key = date_token_from_timestamp_ms(ts_ms, tz, split_granularity)
            if not allow_ooo:
                if current_key is None:
                    current_key = key
                elif key != current_key:
                    flush_bucket_async(current_key); chunks_enqueued += 1
                    current_key = key
            b = buckets.get(key)
            if b is None:
                b = Bucket(key); buckets[key] = b
            if b.txt_scalar is None and b.txt_vec is None:
                if ncols == 1:
                    b.txt_scalar = []
                else:
                    b.txt_vec = [[] for _ in range(ncols)]
            b.times.append(off_s)
            if ncols == 1:
                b.txt_scalar.append(vals_txt[0] if len(vals_txt) >= 1 else "")
            else:
                for j in range(ncols):
                    b.txt_vec[j].append(vals_txt[j] if j < len(vals_txt) else "")
            total_rows += 1
            line_count += 1
            touch_counter += 1
            b.last_touch = touch_counter

            if line_count % max(1, check_every) == 0:
                if get_rss_gb() >= rss_cap_gb and buckets:
                    victim_key = min(buckets.keys(), key=lambda k: buckets[k].last_touch)
                    flush_bucket_async(victim_key); chunks_enqueued += 1

            if allow_ooo and len(buckets) > max(1, ooo_max_open):
                victim_key = min(buckets.keys(), key=lambda k: buckets[k].last_touch)
                flush_bucket_async(victim_key); chunks_enqueued += 1

        proc.wait()
        if proc.returncode != 0:
            err = proc.stderr.read() if proc.stderr else ""
            raise RuntimeError(f"pb2parquet failed for {pb_path}:\n{err}")

        # Final flush of all remaining buckets
        for k in list(buckets.keys()):
            if buckets[k].times:
                flush_bucket_async(k); chunks_enqueued += 1

        # Wait for all writes to complete
        for fut in pending:
            fut.result()

        # PV status → DB
        if chunks_enqueued > 0:
            _upsert_status(db_path, pb_path, "chunked", f"{chunks_enqueued} chunk(s), {total_rows} rows")
            if retry_run_id:
                _retry_mark_done(db_path, retry_run_id, pb_path, "converted_ok")
        else:
            _upsert_status(db_path, pb_path, "skipped", "no rows / no output produced")
            if retry_run_id:
                _retry_mark_done(db_path, retry_run_id, pb_path, "skipped")

        # Summary
        print("\n------ Chunker Summary ------")
        print(f"PB file            : {pb_path}")
        print(f"PV name            : {pv_name}")
        print(f"t0 (ms)            : {t0_ms}")
        print(f"Nval (header)      : {nval_header}")
        print(f"value_type         : {value_mode}")
        print(f"split_by_date      : {split_by_date} (granularity={split_granularity}, tz_offset_hours={tz_offset_hours})")
        print(f"Chunks written     : {chunks_enqueued}")
        print(f"Total rows         : {total_rows}")
        print(f"Output directory   : {out_dir}")
        print(f"Finished at        : {datetime.now().isoformat()}")

    except Exception as e:
        msg = f"{type(e).__name__}: {str(e).strip()}"
        print(f"FAIL: {pb_path} :: {msg}")  # ADD THIS LINE
        sys.stderr.write(f"ERROR: {msg}\n")  # AND THIS FOR GOOD MEASURE
        _upsert_status(db_path, pb_path, "failed", msg)
        if retry_run_id:
            _retry_mark_done(db_path, retry_run_id, pb_path, "failed_again")
        raise
    finally:
        try:
            if proc.stdout: proc.stdout.close()
        except Exception:
            pass
        try:
            if proc.stderr:
                tail = proc.stderr.read()
                if tail:
                    sys.stderr.write(tail + "\n")
                proc.stderr.close()
        except Exception:
            pass
        writer_pool.shutdown(wait=True)

# ---------------- Main ----------------
def main():
    # Derive default split_by_date from config:
    # split_mode=none  -> default False
    # split_mode=auto  -> default True with split_granularity
    split_by_date_default = False if str(SPLIT_MODE_DEFAULT).lower() == "none" else True

    ap = argparse.ArgumentParser(description="Chunk a BIG .pb into Parquet parts (split-by-date, OOO-tolerant, parallel writes).")
    ap.add_argument("--pb", required=True, help="Path to a single .pb file")

    ap.add_argument("--output_base", default=OUTPUT_BASE_DEFAULT, help="Output base for Parquet chunks (from config paths.output_base)")
    ap.add_argument("--input_root", default=INPUT_DIR_DEFAULT, help="Mirror output subdirs relative to this input root (from config paths.input_dir)")

    ap.add_argument("--rss_cap_gb", type=float, default=RSS_CAP_GB_DEFAULT, help="Flush a chunk when RSS >= this many GB (config submitter.rss_cap_gb)")
    ap.add_argument("--check_every", type=int, default=500, help="Check RSS every N data lines")

    ap.add_argument("--db_path", default=DB_PATH_DEFAULT, help="SQLite DB for status tables (from config paths.db_path)")
    ap.add_argument("--retry-run-id", default="", help="If set, update retry_items for this campaign.")

    # Date split
    ap.add_argument("--split_by_date", action="store_true", default=split_by_date_default, help="Split by calendar boundary")
    ap.add_argument("--split_granularity", choices=["day", "week", "month"], default=SPLIT_GRAN_DEFAULT, help="Calendar granularity")
    ap.add_argument("--tz_offset_hours", type=int, default=TZ_OFFSET_DEFAULT, help="Local time offset relative to UTC")

    # Out-of-order handling
    ap.add_argument("--allow_out_of_order", action="store_true", default=ALLOW_OOO_DEFAULT, help="Route late samples to correct date buckets")
    ap.add_argument("--ooo_max_open_buckets", type=int, default=OOO_MAX_OPEN_DEFAULT, help="Max number of open buckets in memory")

    # Parallel writers
    ap.add_argument("--writer_threads", type=int, default=THREADS_DEFAULT, help="Number of parallel writer threads (config submitter.thread_count or $PVPIPE_WRITER_THREADS)")

    # Compatibility flags
    ap.add_argument("--emit_date_view", action="store_true", help="Accepted and ignored")
    ap.add_argument("--date_view_root", default=None, help="Accepted and ignored")

    args = ap.parse_args()

    if not os.path.isfile(args.pb):
        sys.stderr.write(f"ERROR: --pb file not found: {args.pb}\n")
        sys.exit(2)
    os.makedirs(args.output_base, exist_ok=True)
    if not os.path.isdir(JAVA_CLASS_PATH):
        sys.stderr.write(f"WARNING: JAVA_CLASS_PATH does not exist: {JAVA_CLASS_PATH}\n")

    stream_and_chunk(
        pb_path=args.pb,
        output_base=args.output_base,
        rss_cap_gb=float(args.rss_cap_gb),
        input_root=args.input_root,
        check_every=int(args.check_every),
        split_by_date=bool(args.split_by_date),
        split_granularity=args.split_granularity,
        tz_offset_hours=int(args.tz_offset_hours),
        allow_ooo=bool(args.allow_out_of_order),
        ooo_max_open=max(1, int(args.ooo_max_open_buckets)),
        db_path=args.db_path,
        retry_run_id=args.retry_run_id.strip(),
        writer_threads=max(1, int(args.writer_threads)),
    )

if __name__ == "__main__":
    main()