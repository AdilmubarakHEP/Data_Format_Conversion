#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
01-small_script.py
Small-file converter with split-by-date and out-of-order handling.
Single source of truth: Data_Format_Conversion/config.ini

Rules
-----
- No hardcoded defaults. All knobs come from config.ini.
- CLI flags are allowed only for orchestration (file list, retry run id) or when
  00 passes through decisions. If a CLI value conflicts with config.ini, we exit.

Config keys used
----------------
[paths]
  input_dir, output_base, db_path
[java]
  pb2parquet_classpath
[submitter]
  chunk_size, split_mode, split_granularity, tz_offset_hours,
  allow_out_of_order, ooo_max_open_buckets
"""

import os
import sys
import argparse
import glob
import time
import subprocess
import re
import sqlite3
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Tuple, Union

import numpy as np
import numpy.ma as ma
import pyarrow as pa
import pyarrow.parquet as pq

try:
    import psutil  # optional
except Exception:
    psutil = None

# ---------------- INI loader (no defaults) ----------------
import configparser

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(HERE))  # script lives inside the repo
CONFIG_PATH = os.environ.get("PVPIPE_CONFIG", os.path.join(REPO_ROOT, "config.ini"))

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

def _get_str(section: str, key: str, example_line: str, allow_blank: bool = False) -> str:
    if not cfg.has_option(section, key):
        _err_missing(section, [example_line])
    val = cfg.get(section, key)
    if not allow_blank and (val is None or val.strip() == ""):
        _err_missing(section, [example_line])
    return (val or "").strip()

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

def _split_tokens(s: str) -> List[str]:
    raw = re.split(r"[,\n;\s]+", s.strip())
    return [t for t in raw if t]

# -------- Required config (strict) --------
# [paths]
CFG_INPUT_DIR  = _get_str("paths", "input_dir", "input_dir = /gpfs/.../pv/2024/AA/")
CFG_OUTPUT_BASE = _get_str("paths", "output_base", "output_base = /gpfs/.../AA_parquet/converted_flat2")
CFG_DB_PATH    = _get_str("paths", "db_path", "db_path = /home/belle2/amubarak/conversion_log.db")

# [java]
JAVA_CLASSPATH = _get_str("java", "pb2parquet_classpath", "pb2parquet_classpath = /home/belle2/amubarak/Data_Format_Conversion/java")

# [submitter]
SUB_CHUNK_SIZE    = _get_int("submitter", "chunk_size", "chunk_size = 100")
SUB_SPLIT_MODE    = _get_str("submitter", "split_mode", "split_mode = auto")
SUB_SPLIT_GRAN    = _get_str("submitter", "split_granularity", "split_granularity = day")
SUB_TZ_OFFSET_RAW = _get_str("submitter", "tz_offset_hours", "tz_offset_hours = ", allow_blank=True)
SUB_ALLOW_OOO     = _get_bool("submitter", "allow_out_of_order", "allow_out_of_order = false")
SUB_OOO_MAX_OPEN  = _get_int("submitter", "ooo_max_open_buckets", "ooo_max_open_buckets = 8")

# Class name stays stable unless you rename the Java entry point
PB2TSV_CLASS = "pb2parquet"

# ---------------- Utilities ----------------
DATE_TOK_RE = re.compile(r"(19|20)\d{2}[_-]\d{2}[_-]\d{2}$")  # YYYY_MM_DD or YYYY-MM-DD at end

def _fatal(msg: str):
    raise SystemExit(f"[FATAL] {msg}")

def _require_match(name: str, cli_val, cfg_val):
    """Fail if a provided CLI value conflicts with config."""
    if cli_val is None:
        return
    if isinstance(cfg_val, str):
        a = (cli_val or "").strip()
        b = (cfg_val or "").strip()
        if os.path.normpath(a) == os.path.normpath(b):
            return
        if a == b:
            return
        _fatal(f"{name} from CLI conflicts with config.ini. CLI={cli_val!r} vs INI={cfg_val!r}. Fix config or drop the CLI arg.")
    else:
        if cli_val != cfg_val:
            _fatal(f"{name} from CLI conflicts with config.ini. CLI={cli_val!r} vs INI={cfg_val!r}. Fix config or drop the CLI arg.")

def format_size(bytes_val: int) -> str:
    mb = bytes_val / 1e6
    return f"{mb:.2f} MB" if mb < 1000 else f"{mb/1000:.2f} GB"

def rel_mirror(input_dir: str, fpath: str) -> str:
    rel_path = os.path.relpath(fpath, input_dir)
    base_no_ext, _ = os.path.splitext(rel_path)
    parts = base_no_ext.split(os.sep)
    if parts:
        parts[-1] = parts[-1].replace(":", "-")
    return os.path.join(*parts)

def make_out_path(input_dir: str, fpath: str, date_token: Optional[str], split_on: bool) -> str:
    base_rel = rel_mirror(input_dir, fpath)
    dir_part, fn = os.path.split(base_rel)
    if not split_on:
        out_fn = fn + ".parquet"
    else:
        stem = fn
        if DATE_TOK_RE.search(stem):
            stem = DATE_TOK_RE.sub("", stem).rstrip("-_")
        if not date_token:
            _fatal("split_by_date is enabled but date_token is empty")
        out_fn = f"{stem}-{date_token}.parquet"
    return os.path.join(CFG_OUTPUT_BASE, dir_part, out_fn)

def _try_parse_numeric(line: str) -> np.ndarray:
    return np.fromstring(line, sep=' ')

def _parse_text_row(line: str):
    parts = line.strip().split()
    if not parts:
        return None, None
    try:
        off_ms = float(parts[0])
    except Exception:
        return None, None
    return off_ms, parts[1:]

# ---------------- Time bucketing ----------------
def day_key(ts_utc: float, tz_offset_h: int) -> str:
    dt = datetime.utcfromtimestamp(ts_utc) + timedelta(hours=tz_offset_h or 0)
    return f"{dt.year:04d}_{dt.month:02d}_{dt.day:02d}"

def week_key(ts_utc: float, tz_offset_h: int) -> str:
    dt = datetime.utcfromtimestamp(ts_utc) + timedelta(hours=tz_offset_h or 0)
    y, w, _ = dt.isocalendar()
    return f"{y:04d}_W{w:02d}"

def month_key(ts_utc: float, tz_offset_h: int) -> str:
    dt = datetime.utcfromtimestamp(ts_utc) + timedelta(hours=tz_offset_h or 0)
    return f"{dt.year:04d}_{dt.month:02d}"

def pick_bucket_key_fn(granularity: str):
    return {"week": week_key, "month": month_key}.get(granularity, day_key)

# ---------------- Timing + stats ----------------
def convert_to_fractional_seconds(times: np.ndarray) -> np.ndarray:
    if times.size == 0:
        return times
    return times - times[0]

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

def _nan_stats_1d(x: np.ndarray):
    x = np.asarray(x, dtype=float)
    nan_count = int(np.isnan(x).sum())
    xv = x[~np.isnan(x)]
    if xv.size == 0:
        return {
            "min": None, "max": None, "mean": None, "std": None,
            "skewness": None, "kurtosis": None, "snr": None, "mad": None, "cv": None,
            "rate_of_change": None, "rate_of_change_variance": None,
            "peaks": 0, "nan_count": nan_count
        }
    mn = float(np.nanmin(xv)); mx = float(np.nanmax(xv))
    mu = float(np.nanmean(xv)); sd = float(np.nanstd(xv))
    sk = None if xv.size < 3 or sd == 0 else float(np.nanmean(((xv - mu)/sd)**3))
    ku = None if xv.size < 3 or sd == 0 else float(np.nanmean(((xv - mu)/sd)**4))
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

def vector_stats_2d(mat: np.ndarray):
    T = mat.shape[0]
    if mat.ndim != 2:
        mat = mat.reshape(T, -1)
    N = mat.shape[1]
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

# ---------------- Build tables ----------------
def make_numeric_scalar_table(pv_name, date_token, t0_ms, times, values) -> pa.Table:
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
    return pa.Table.from_arrays(arrays, names=names)

def make_numeric_vector_table(pv_name, date_token, t0_ms, times, comp_lists) -> pa.Table:
    N = len(comp_lists); T = len(times)
    mat = np.full((T, N), np.nan, dtype=np.float64)
    for j, col in enumerate(comp_lists):
        if col:
            m = min(len(col), T); mat[:m, j] = np.asarray(col[:m], dtype=np.float64)
    stats = vector_stats_2d(mat)
    tstats = timing_stats(times)
    names = [
        "value_type","pv","date","t0","Nval","times","length",
        "values_by_component","component_names",
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
        pa.array([comp_lists], type=pa.list_(pa.list_(pa.float64()))),
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
    return pa.Table.from_arrays(arrays, names=names)

def make_text_scalar_table(pv_name, date_token, t0_ms, times, values) -> pa.Table:
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
    return pa.Table.from_arrays(arrays, names=names)

def make_text_vector_table(pv_name, date_token, t0_ms, times, comp_lists) -> pa.Table:
    N = len(comp_lists)
    names = ["value_type","pv","date","t0","Nval","times","length","values_text_by_component","component_names"]
    arrays = [
        pa.array(["text"]),
        pa.array([pv_name]),
        pa.array([date_token]),
        pa.array([int(t0_ms)], type=pa.int64()),
        pa.array([int(N)], type=pa.int64()),
        pa.array([times], type=pa.list_(pa.float64())),
        pa.array([len(times)], type=pa.int64()),
        pa.array([comp_lists], type=pa.list_(pa.list_(pa.string()))),
        pa.array([[f"{pv_name}[{i}]" for i in range(N)]], type=pa.list_(pa.string())),
    ]
    return pa.Table.from_arrays(arrays, names=names)

def append_table_atomic(path: str, new_tbl: pa.Table, compression="snappy", use_dictionary=False):
    if os.path.exists(path):
        old = pq.read_table(path)
        combined = pa.concat_tables([old, new_tbl], promote=True)
    else:
        combined = new_tbl
    tmp = path + ".tmp"
    pq.write_table(combined, tmp, compression=compression, use_dictionary=use_dictionary)
    os.replace(tmp, path)

# ---------------- SQLite helpers ----------------
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
            run_id        TEXT,
            pb_path       TEXT,
            initial_status TEXT,
            queued_ts     TEXT,
            job_id        TEXT,
            finished_ts   TEXT,
            outcome       TEXT,
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
                time.sleep(delay); delay = min(1.0, delay * 2)
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

# ---------------- Java header + first row ----------------
def stream_header_and_first_mode(proc) -> Tuple[str, int, Optional[int], Optional[str], Optional[Union[np.ndarray, Tuple[float, List[str]]]]]:
    pv_name, t0_ms, nval_header = None, None, None
    mode = None; first_row = None
    for line in proc.stdout:
        if not line:
            break
        if line.startswith("#"):
            s = line.strip()
            if s.startswith("# PV name"):
                pv_name = s.split(None, 3)[-1]
            elif s.startswith("# t0"):
                try: t0_ms = int(s.split()[-1])
                except Exception: pass
            elif s.startswith("# Nval"):
                try: nval_header = int(s.split()[-1])
                except Exception: pass
            continue
        if not line.strip():
            continue
        arr = _try_parse_numeric(line)
        if arr.size >= 1 and np.isfinite(arr[0]) and line.strip().find(" ") != -1:
            mode = "numeric"; first_row = arr
        else:
            off_ms, toks = _parse_text_row(line)
            mode = "text"; first_row = (off_ms, toks)
        break
    if pv_name is None or t0_ms is None:
        _fatal("Missing '# PV name' or '# t0' in pb2tsv header")
    return pv_name, t0_ms, nval_header, mode, first_row

# ---------------- Streaming split ----------------
def process_one_file_streaming_split(
    fpath: str,
    input_dir: str,
    split_granularity: str,
    tz_offset_hours: int,
    allow_ooo: bool,
    ooo_max_open: int
) -> List[str]:

    cmd = ["java", "-cp", JAVA_CLASSPATH, PB2TSV_CLASS, fpath]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, bufsize=1)

    out_paths: List[str] = []
    bucket_key_fn = pick_bucket_key_fn(split_granularity)

    class Buf:
        __slots__ = ("times","vals_scalar","vals_vector","txt_scalar","txt_vector","last_ts")
        def __init__(self, ncols:int, mode:str):
            self.times: List[float] = []
            if mode == "numeric":
                self.vals_scalar: Optional[List[float]] = [] if ncols == 1 else None
                self.vals_vector: Optional[List[List[float]]] = [[] for _ in range(ncols)] if ncols > 1 else None
                self.txt_scalar = None; self.txt_vector = None
            else:
                self.txt_scalar: Optional[List[str]] = [] if ncols == 1 else None
                self.txt_vector: Optional[List[List[str]]] = [[] for _ in range(ncols)] if ncols > 1 else None
                self.vals_scalar = None; self.vals_vector = None
            self.last_ts: float = float("-inf")

    def flush_bucket(key: str, buf: Buf, mode: str, ncols: int):
        out_path = make_out_path(input_dir, fpath, date_token=key, split_on=True)
        os.makedirs(os.path.dirname(out_path), exist_ok=True)

        if mode == "text":
            if ncols == 1:
                tbl = make_text_scalar_table(pv_name, key, t0_ms, buf.times, buf.txt_scalar or [])
                append_table_atomic(out_path, tbl)
            else:
                tbl = make_text_vector_table(pv_name, key, t0_ms, buf.times, buf.txt_vector or [[] for _ in range(ncols)])
                append_table_atomic(out_path, tbl)
        else:
            if ncols == 1:
                tbl = make_numeric_scalar_table(pv_name, key, t0_ms, buf.times, buf.vals_scalar or [])
                append_table_atomic(out_path, tbl, use_dictionary=False)
            else:
                tbl = make_numeric_vector_table(pv_name, key, t0_ms, buf.times, buf.vals_vector or [[] for _ in range(ncols)])
                append_table_atomic(out_path, tbl, use_dictionary=False)
        print(f"SAVED: {out_path}")
        out_paths.append(out_path)

    try:
        pv_name, t0_ms, nval_header, mode, first_row = stream_header_and_first_mode(proc)
        if mode is None:
            return out_paths

        ncols = None
        if nval_header and nval_header > 0:
            ncols = int(nval_header)

        buffers: Dict[str, Buf] = {}

        def ensure_buf(key: str) -> Buf:
            nonlocal buffers
            b = buffers.get(key)
            if b is None:
                b = Buf(ncols or 1, mode)
                buffers[key] = b
            return b

        def evict_if_needed(cur_key: str):
            if not allow_ooo:
                return
            if len(buffers) <= max(1, ooo_max_open):
                return
            victim_key = min(buffers.keys(), key=lambda k: buffers[k].last_ts if buffers[k].times else float("-inf"))
            if victim_key in buffers:
                flush_bucket(victim_key, buffers[victim_key], mode, ncols or 1)
                del buffers[victim_key]

        def push_point(off_ms: float, payload_vals: Union[np.ndarray, List[str]]):
            nonlocal buffers
            ts_utc = (t0_ms or 0) / 1000.0 + float(off_ms) / 1000.0
            key = bucket_key_fn(ts_utc, tz_offset_hours or 0)
            buf = ensure_buf(key)

            rel_time = float(off_ms) / 1000.0
            buf.times.append(rel_time)
            buf.last_ts = ts_utc

            if mode == "text":
                if (ncols or 1) == 1:
                    v = payload_vals[0] if payload_vals and len(payload_vals) > 0 else ""
                    buf.txt_scalar.append(str(v))
                else:
                    row = payload_vals
                    for j in range(ncols or 1):
                        buf.txt_vector[j].append(str(row[j]) if j < len(row) else "")
            else:
                if (ncols or 1) == 1:
                    if isinstance(payload_vals, np.ndarray):
                        buf.vals_scalar.append(float(payload_vals[0]) if payload_vals.size > 0 else np.nan)
                    else:
                        try: buf.vals_scalar.append(float(payload_vals[0]))
                        except Exception: buf.vals_scalar.append(np.nan)
                else:
                    if isinstance(payload_vals, np.ndarray):
                        for j in range(ncols or 1):
                            buf.vals_vector[j].append(float(payload_vals[j]) if payload_vals.size > j else np.nan)
                    else:
                        for j in range(ncols or 1):
                            try: buf.vals_vector[j].append(float(payload_vals[j]))
                            except Exception: buf.vals_vector[j].append(np.nan)

            evict_if_needed(key)

        # Seed with first row
        if mode == "numeric":
            arr = first_row  # type: ignore
            if ncols is None:
                ncols = max(1, int(arr.size - 1))
            push_point(arr[0], arr[1:])
        else:
            off_ms, toks = first_row  # type: ignore
            if ncols is None:
                ncols = max(1, len(toks) if toks else 1)
            push_point(off_ms, toks or [])

        # Stream remaining lines
        last_key_seen: Optional[str] = None
        for line in proc.stdout:
            if not line or not line.strip():
                continue
            if mode == "numeric":
                arr = _try_parse_numeric(line)
                if arr.size >= 1 and np.isfinite(arr[0]):
                    if not allow_ooo:
                        ts_utc = (t0_ms or 0) / 1000.0 + float(arr[0]) / 1000.0
                        key = pick_bucket_key_fn(split_granularity)(ts_utc, tz_offset_hours or 0)
                        if last_key_seen is None:
                            last_key_seen = key
                        if key != last_key_seen:
                            if last_key_seen in buffers:
                                flush_bucket(last_key_seen, buffers[last_key_seen], mode, ncols or 1)
                                del buffers[last_key_seen]
                            last_key_seen = key
                    push_point(arr[0], arr[1:])
                else:
                    for k, b in list(buffers.items()):
                        flush_bucket(k, b, mode, ncols or 1)
                        del buffers[k]
                    mode = "text"
                    off_ms, toks = _parse_text_row(line)
                    if off_ms is not None:
                        push_point(off_ms, toks or [])
            else:
                off_ms, toks = _parse_text_row(line)
                if off_ms is not None:
                    if not allow_ooo:
                        ts_utc = (t0_ms or 0) / 1000.0 + float(off_ms) / 1000.0
                        key = pick_bucket_key_fn(split_granularity)(ts_utc, tz_offset_hours or 0)
                        if last_key_seen is None:
                            last_key_seen = key
                        if key != last_key_seen:
                            if last_key_seen in buffers:
                                flush_bucket(last_key_seen, buffers[last_key_seen], mode, ncols or 1)
                                del buffers[last_key_seen]
                            last_key_seen = key
                    push_point(off_ms, toks or [])

        # Flush remaining
        for k, b in list(buffers.items()):
            flush_bucket(k, b, mode, ncols or 1)
        buffers.clear()

        return out_paths

    finally:
        try:
            if proc.stdout: proc.stdout.close()
        except Exception:
            pass
        proc.wait()
        if proc.returncode != 0:
            err = proc.stderr.read() if proc.stderr else ""
            _fatal(f"pb2tsv failed for {fpath}:\n{err}")

# ---------------- Non-splitting path ----------------
def process_one_file_nosplit(fpath: str, input_dir: str) -> List[str]:
    cmd = ["java", "-cp", JAVA_CLASSPATH, PB2TSV_CLASS, fpath]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, bufsize=1)

    pv_name = None; t0_ms = None; nval_header = None; mode = None
    offsets_numeric: List[float] = []
    numeric_rows: List[np.ndarray] = []
    offsets_text: List[float] = []
    text_rows: List[List[str]] = []
    max_cols_seen = 0

    try:
        for line in proc.stdout:
            if not line:
                break
            if line.startswith("#"):
                s = line.strip()
                if s.startswith("# PV name"): pv_name = s.split(None, 3)[-1]
                elif s.startswith("# t0"):
                    try: t0_ms = int(s.split()[-1])
                    except Exception: pass
                elif s.startswith("# Nval"):
                    try: nval_header = int(s.split()[-1])
                    except Exception: pass
                continue
            if not line.strip():
                continue
            if mode in (None, "numeric"):
                arr = _try_parse_numeric(line)
                if arr.size >= 1 and np.isfinite(arr[0]):
                    if mode is None: mode = "numeric"
                    offsets_numeric.append(arr[0]); vals = arr[1:]; numeric_rows.append(vals)
                    if vals.size > max_cols_seen: max_cols_seen = int(vals.size)
                    continue
                else:
                    if mode == "numeric" and (offsets_numeric or numeric_rows):
                        for off, vals in zip(offsets_numeric, numeric_rows):
                            offsets_text.append(off)
                            text_rows.append([str(x) for x in vals.tolist()])
                        offsets_numeric.clear(); numeric_rows.clear()
                    mode = "text"
            off_ms, toks = _parse_text_row(line)
            if off_ms is None: continue
            offsets_text.append(off_ms); text_rows.append(toks)
            if toks and len(toks) > max_cols_seen: max_cols_seen = len(toks)
    finally:
        try:
            if proc.stdout: proc.stdout.close()
        except Exception:
            pass
        proc.wait()
        if proc.returncode != 0:
            err = proc.stderr.read() if proc.stderr else ""
            _fatal(f"pb2tsv failed for {fpath}:\n{err}")

    if pv_name is None or t0_ms is None:
        _fatal("Missing '# PV name' or '# t0' in pb2tsv header")

    ncols = int(nval_header) if (nval_header and nval_header > 0) else max(1, max_cols_seen)
    out_path = make_out_path(input_dir, fpath, date_token=None, split_on=False)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    if mode == "text":
        padded = []
        for r in text_rows:
            if len(r) < ncols: r = r + [""] * (ncols - len(r))
            elif len(r) > ncols: r = r[:ncols]
            padded.append(r)
        times = (np.asarray(offsets_text, dtype=np.float64) / 1000.0).tolist()
        if ncols == 1:
            vals = [ (r[0] if r else "") for r in padded ]
            tbl = make_text_scalar_table(pv_name, None, t0_ms, times, vals)
        else:
            comp_lists = [[] for _ in range(ncols)]
            for r in padded:
                for j in range(ncols):
                    comp_lists[j].append(r[j] if j < len(r) else "")
            tbl = make_text_vector_table(pv_name, None, t0_ms, times, comp_lists)
    else:
        times = (np.asarray(offsets_numeric, dtype=np.float64) / 1000.0).tolist()
        if ncols == 1:
            vals = [ float(v[0]) if isinstance(v, np.ndarray) and v.size > 0 else np.nan for v in numeric_rows ]
            tbl = make_numeric_scalar_table(pv_name, None, t0_ms, times, vals)
        else:
            comp_lists = [[] for _ in range(ncols)]
            for v in numeric_rows:
                for j in range(ncols):
                    comp_lists[j].append(float(v[j]) if isinstance(v, np.ndarray) and v.size > j else np.nan)
            tbl = make_numeric_vector_table(pv_name, None, t0_ms, times, comp_lists)

    pq.write_table(tbl, out_path, compression="snappy", use_dictionary=False)
    print(f"SAVED: {out_path}")
    return [out_path]

# ---------------- Split decision helpers (for auto) ----------------
DATE_IN_NAME_RE = re.compile(r"(19|20)\d{2}[-_/]?\d{2}[-_/]?\d{2}")

def filenames_look_dated(sample_paths: List[str]) -> bool:
    if not sample_paths:
        return False
    hits = 0
    for p in sample_paths:
        base = os.path.basename(p)
        if DATE_IN_NAME_RE.search(base):
            hits += 1
    return (hits / len(sample_paths)) >= 0.50

def decide_effective_split(input_dir: str, files: List[str]) -> bool:
    if SUB_SPLIT_MODE == "on":
        return True
    if SUB_SPLIT_MODE == "off":
        return False
    # auto
    if "/2024c/" in input_dir or "/2024c/" in os.path.normpath(input_dir):
        return False
    sample = files[:200] if files else []
    return not filenames_look_dated(sample)

# ---------------- Main ----------------
def main():
    p = argparse.ArgumentParser(description="Convert SMALL .pb files to Parquet.")
    # Orchestration inputs
    p.add_argument("--input_dir", help="Must match [paths].input_dir; provided by 00")
    p.add_argument("--db_path", help="Must match [paths].db_path; provided by 00")
    p.add_argument("--retry-run-id", default="", help="Retry campaign ID from 05, if any")
    p.add_argument("--chunk_file", help="Text file containing a list of .pb files")
    p.add_argument("--chunk_index", type=int, help="Chunk index (used with --chunk_size)")
    p.add_argument("--chunk_size", type=int, help="Files per chunk (must match config if provided)")
    # Passthrough split knobs from 00 (must not contradict config)
    p.add_argument("--split_by_date", action="store_true")
    p.add_argument("--split_granularity", choices=["day","week","month"])
    p.add_argument("--tz_offset_hours", type=int)
    p.add_argument("--allow_out_of_order", action="store_true")
    p.add_argument("--ooo_max_open_buckets", type=int)
    # Compatibility flags (ignored)
    p.add_argument("--emit_date_view", action="store_true")
    p.add_argument("--date_view_root", default=None)
    args = p.parse_args()

    # Strict matching for core paths
    if args.input_dir:
        _require_match("input_dir", args.input_dir, CFG_INPUT_DIR)
    if args.db_path:
        _require_match("db_path", args.db_path, CFG_DB_PATH)
    if args.chunk_size is not None:
        _require_match("chunk_size", args.chunk_size, SUB_CHUNK_SIZE)

    # Resolve classpath
    if not JAVA_CLASSPATH:
        _err_missing("java", ["pb2parquet_classpath = /home/belle2/amubarak/Data_Format_Conversion/java"])
    # No fragile filesystem checks here; classpath can be a colon-joined string of jars/dirs.

    # Resolve file list
    effective_input_dir = CFG_INPUT_DIR
    effective_db_path = CFG_DB_PATH

    if args.chunk_file:
        with open(args.chunk_file, "r") as f:
            files = [ln.strip() for ln in f if ln.strip()]
    else:
        # Fallback planning inside 01 (rare; usually 00 supplies a bundle)
        csize = args.chunk_size if args.chunk_size is not None else SUB_CHUNK_SIZE
        all_files = sorted(glob.glob(os.path.join(effective_input_dir, "**", "*.pb"), recursive=True))
        start = (args.chunk_index or 0) * csize
        end = start + csize
        files = all_files[start:end]

    # Decide splitting: prefer passthrough from 00, else compute from config
    if args.split_by_date:
        effective_split = True
    else:
        effective_split = decide_effective_split(effective_input_dir, files)

    # Resolve split parameters
    gran_cli = args.split_granularity
    tz_cli   = args.tz_offset_hours
    ooo_cli  = True if args.allow_out_of_order else None
    omax_cli = args.ooo_max_open_buckets

    # Enforce no conflicts with config
    # split_granularity
    if gran_cli is not None:
        _require_match("split_granularity", gran_cli, SUB_SPLIT_GRAN)
    effective_gran = gran_cli if gran_cli is not None else SUB_SPLIT_GRAN

    # tz_offset_hours: required if splitting
    if effective_split:
        if tz_cli is None:
            if SUB_TZ_OFFSET_RAW.strip() == "":
                _err_missing("submitter", ["tz_offset_hours = 9   # required when split_mode leads to splitting"])
            try:
                effective_tz = int(SUB_TZ_OFFSET_RAW)
            except Exception:
                _err_missing("submitter", ["tz_offset_hours = 9   # must be an integer when splitting"])
        else:
            # CLI provided; compare to config if config not blank
            if SUB_TZ_OFFSET_RAW.strip() != "":
                _require_match("tz_offset_hours", tz_cli, int(SUB_TZ_OFFSET_RAW))
            effective_tz = tz_cli
    else:
        # Not splitting; tz not used
        effective_tz = 0

    # allow_out_of_order
    if ooo_cli is not None:
        _require_match("allow_out_of_order", True, SUB_ALLOW_OOO)
    effective_allow_ooo = SUB_ALLOW_OOO if ooo_cli is None else True

    # ooo_max_open_buckets
    if omax_cli is not None:
        _require_match("ooo_max_open_buckets", omax_cli, SUB_OOO_MAX_OPEN)
    effective_ooo_max = omax_cli if omax_cli is not None else SUB_OOO_MAX_OPEN

    # Summary of resolved knobs
    print(f"[cfg] input_dir        : {effective_input_dir}")
    print(f"[cfg] output_base      : {CFG_OUTPUT_BASE}")
    print(f"[cfg] db_path          : {effective_db_path}")
    print(f"[cfg] split_mode       : {SUB_SPLIT_MODE}  -> effective_split={effective_split}")
    if effective_split:
        print(f"[cfg] split_granularity: {effective_gran}")
        print(f"[cfg] tz_offset_hours  : {effective_tz}")
        print(f"[cfg] allow_out_of_order: {effective_allow_ooo}")
        print(f"[cfg] ooo_max_open_buckets: {effective_ooo_max}")
    print(f"[cfg] chunk_size       : {SUB_CHUNK_SIZE}")
    print(f"[cfg] java_classpath   : {JAVA_CLASSPATH}")

    # Sanity
    os.makedirs(CFG_OUTPUT_BASE, exist_ok=True)

    n_files = len(files)
    in_total = sum((os.path.getsize(pth) for pth in files if os.path.exists(pth)), 0)

    start_dt = datetime.now()
    t0 = time.time()
    ok, fail, skipped = 0, 0, 0
    peak_rss = 0
    ps = psutil.Process() if psutil else None
    out_total = 0
    out_count = 0

    # Pre-create tables to reduce per-file overhead when db_path is set
    if effective_db_path:
        _ensure_db(effective_db_path)

    for fpath in files:
        if args.retry_run_id:
            _retry_mark_start(effective_db_path, args.retry_run_id, fpath)

        try:
            if effective_split:
                out_paths = process_one_file_streaming_split(
                    fpath=fpath,
                    input_dir=effective_input_dir,
                    split_granularity=effective_gran,
                    tz_offset_hours=effective_tz,
                    allow_ooo=bool(effective_allow_ooo),
                    ooo_max_open=max(1, int(effective_ooo_max))
                )
            else:
                out_paths = process_one_file_nosplit(fpath=fpath, input_dir=effective_input_dir)

            if out_paths:
                _upsert_status(effective_db_path, fpath, "success", f"{len(out_paths)} parquet file(s) saved")
                ok += 1
                if args.retry_run_id:
                    _retry_mark_done(effective_db_path, args.retry_run_id, fpath, "converted_ok")
            else:
                _upsert_status(effective_db_path, fpath, "skipped", "no samples or no output produced")
                skipped += 1
                if args.retry_run_id:
                    _retry_mark_done(effective_db_path, args.retry_run_id, fpath, "skipped")

            for out_path in out_paths:
                if os.path.exists(out_path):
                    out_total += os.path.getsize(out_path)
                    out_count += 1

        except Exception as e:
            fail += 1
            msg = f"{type(e).__name__}: {str(e).strip()}"
            print(f"FAIL: {fpath} :: {msg}")
            _upsert_status(effective_db_path, fpath, "failed", msg)
            if args.retry_run_id:
                _retry_mark_done(effective_db_path, args.retry_run_id, fpath, "failed_again")

        if ps:
            try:
                rss = ps.memory_info().rss
                if rss > peak_rss: peak_rss = rss
            except Exception:
                pass

    elapsed = time.time() - t0
    end_dt = datetime.now()

    print("\n------ Chunk Summary ------")
    print(f"Input files            : {n_files}")
    print(f"Succeeded / Failed     : {ok} / {fail}")
    print(f"Skipped                : {skipped}")
    print(f"Total .pb input size   : {format_size(in_total)}")
    print(f"Total .parquet files   : {out_count}")
    print(f"Total .parquet size    : {format_size(out_total)}")
    print(f"Elapsed time (s)       : {elapsed:.2f}")
    print(f"Start time             : {start_dt.isoformat()}")
    print(f"End time               : {end_dt.isoformat()}")
    if ps:
        print(f"Peak RSS               : {format_size(peak_rss)}")

if __name__ == "__main__":
    # Strict check: classpath must be set in config
    if not JAVA_CLASSPATH:
        _err_missing("java", ["pb2parquet_classpath = /home/belle2/amubarak/Data_Format_Conversion/java"])
    main()