#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
04-recompute_submission_summaries.py

Reads config from Data_Format_Conversion/config.ini (or $PVPIPE_CONFIG).
- paths.logs_root + paths.logs_subdir_pb2parquet -> LOG_ROOT
- paths.output_base -> OUTPUT_ROOT
- paths.db_path -> DB_PATH
- submitter.thread_count -> THREADS (for parallel schema classification)

Improvements:
- Fast Parquet classification via schema-only reads (no data I/O).
- Optional threaded classification to speed up large runs.
"""

import os
import re
import json
import sqlite3
import configparser
from datetime import datetime
from collections import defaultdict
from typing import Optional, Tuple, Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.types as pat


# ---------------------- config loader ----------------------
def _load_cfg():
    """
    Minimal INI loader.
    Priority: $PVPIPE_CONFIG else ~/Data_Format_Conversion/config.ini
    """
    default_path = os.path.expanduser("~/Data_Format_Conversion/config.ini")
    cfg_path = os.environ.get("PVPIPE_CONFIG", default_path)

    parser = configparser.ConfigParser()
    if not parser.read(cfg_path):
        raise SystemExit(f"Config not found or unreadable: {cfg_path}")

    def get(section, key, default=None):
        if parser.has_option(section, key):
            return parser.get(section, key)
        return default

    def getint(section, key, default=None):
        if parser.has_option(section, key):
            try:
                return parser.getint(section, key)
            except Exception:
                return default
        return default

    paths_logs_root = get("paths", "logs_root", "/home/belle2/amubarak/Data_Format_Conversion/logs")
    paths_logs_sub = get("paths", "logs_subdir_pb2parquet", "pb2parquet")
    log_root = os.path.join(paths_logs_root, paths_logs_sub)

    output_root = get("paths", "output_base", "/home/belle2/amubarak/PV_Output/converted_flat2")
    db_path = get("paths", "db_path", "/home/belle2/amubarak/conversion_log.db")

    threads = getint("submitter", "thread_count", 4)

    return {
        "CFG_PATH": cfg_path,
        "LOG_ROOT": log_root,
        "OUTPUT_ROOT": output_root,
        "DB_PATH": db_path,
        "THREADS": max(1, int(threads or 1)),
    }


CFG = _load_cfg()
LOG_ROOT = CFG["LOG_ROOT"]
OUTPUT_ROOT = CFG["OUTPUT_ROOT"]
DB_PATH = CFG["DB_PATH"]
THREADS = CFG["THREADS"]

FAILED_SUMMARY_DIR = LOG_ROOT
FAILED_TXT_TEMPLATE = "failed_files_{timestamp}.txt"

# ---------- patterns ----------
PARQUET_PATH_RE = re.compile(r"(/[^()\s\"']+\.parquet)")
DATE_RE = re.compile(r"(\d{4}_\d{2}_\d{2})")  # YYYY_MM_DD
CHUNK_RE = re.compile(r"^(?P<base>.+)_chunk(?P<idx>\d{4})\.parquet$")  # capture PV base from chunk filename


# ---------- helpers ----------
def bytes_from_size_string(size_str: str) -> float:
    s = size_str.strip().upper()
    for token in ["GB", "MB", "KB", "B"]:
        if s.endswith(token):
            try:
                val = float(s[: -len(token)].strip())
            except ValueError:
                return 0.0
            if token == "GB": return val * 1024**3
            if token == "MB": return val * 1024**2
            if token == "KB": return val * 1024
            return val
    try:
        return float(s) * 1024**2
    except ValueError:
        return 0.0


def extract_first_parquet_path(text: str) -> Optional[str]:
    m = PARQUET_PATH_RE.search(text)
    return m.group(1) if m else None


def extract_failures(lines: List[str]) -> List[str]:
    fails = []
    for raw in lines:
        ls = raw.strip()
        if (
            "ERROR" in ls
            or ls.startswith("❌")
            or ls.startswith("FAIL:")
            or ls.startswith("FAILED")
            or "Failed to merge" in ls
            or "pb2tsv failed" in ls
            or "FAILED (pb2tsv)" in ls
            or "FAILED (convert/save)" in ls
        ):
            fails.append(ls)
    return fails


def find_parquet_bytes_for_line(line: str) -> int:
    p = extract_first_parquet_path(line)
    if p and os.path.exists(p):
        try:
            return os.path.getsize(p)
        except OSError:
            return 0
    # legacy: try to resolve from OUTPUT_ROOT by filename only
    if "Saved:" in line or "SAVED:" in line:
        tail = line.split(":", 1)[-1].strip().strip('"').strip("'")
        base = os.path.basename(tail)
        if base.endswith(".pb"):
            base = base[:-3]
        base = base.replace(":", "-")
        candidate = base + ".parquet"
        for root, _, files in os.walk(OUTPUT_ROOT):
            if candidate in files:
                try:
                    return os.path.getsize(os.path.join(root, candidate))
                except OSError:
                    return 0
    return 0


def parse_rss_gb_from_line(line: str) -> Optional[float]:
    # matches: 'rss=1.23 GB', 'mem≈1.23 GB', 'mem=1.23 GB'
    m = re.search(r"(?:rss\s*=\s*|mem[≈=]\s*)([0-9]+(?:\.[0-9]+)?)\s*GB", line, re.IGNORECASE)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return None
    return None


def extract_date_ymd_from_filename(filename: str) -> Optional[str]:
    m = DATE_RE.search(filename)
    return m.group(1) if m else None


# ---------- Parquet classification (schema-only, fast) ----------
def _classify_parquet_fast(path: str) -> Tuple[str, str]:
    """
    Returns (value_kind, shape_kind)
      value_kind: 'numeric' | 'text' | 'unknown'
      shape_kind: 'scalar'  | 'vector' | 'unknown'
    Uses schema only to avoid reading data.
    """
    try:
        pf = pq.ParquetFile(path)
        schema: pa.Schema = pf.schema_arrow
    except Exception:
        return ("unknown", "unknown")

    cols = set(schema.names)

    # shape via Nval when present
    shape_kind = "unknown"
    try:
        if "Nval" in cols:
            # read only first row of the Nval column metadata if possible is costly; rely on presence of vector fields
            # prefer structural hint first
            if "values_by_component" in cols or "values_text_by_component" in cols or "values_matrix" in cols or "values_text_matrix" in cols:
                shape_kind = "vector"
            else:
                # fall back: treat missing vectors as scalar
                shape_kind = "scalar"
    except Exception:
        pass

    # type via value_type when present
    value_kind = "unknown"
    if "value_type" in cols:
        # cannot read the value cheaply without data; infer from other fields
        pass

    if value_kind == "unknown":
        if "values" in cols or "values_matrix" in cols or "values_by_component" in cols:
            value_kind = "numeric"
        elif "values_text" in cols or "values_text_matrix" in cols or "values_text_by_component" in cols:
            value_kind = "text"

    return (value_kind, shape_kind)


def _chunk_base_from_path(p: str) -> Optional[str]:
    fn = os.path.basename(p)
    m = CHUNK_RE.match(fn)
    return m.group("base") if m else None


# ---------- DB helpers for progress snapshot ----------
def _progress_snapshot(as_of: datetime) -> Dict[str, int]:
    """
    Returns a dict:
      { 'as_of': ISO, 'total_pb': int, 'converted': int, 'remaining': int, 'pct': int }
    Using:
      - total_pb: COUNT(*) FROM pb_catalog (if table exists, else 0)
      - converted: COUNT(*) FROM conversions WHERE status IN ('success','skipped') AND timestamp <= as_of
    """
    total = 0
    converted = 0
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        # total known .pb files
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pb_catalog'")
        if c.fetchone():
            c.execute("SELECT COUNT(*) FROM pb_catalog")
            row = c.fetchone()
            total = int(row[0]) if row and row[0] is not None else 0

        # converted up to snapshot time
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='conversions'")
        if c.fetchone():
            asof = as_of.isoformat(timespec="seconds")
            c.execute(
                "SELECT COUNT(*) FROM conversions "
                "WHERE status IN ('success','skipped') AND timestamp <= ?",
                (asof,)
            )
            row = c.fetchone()
            converted = int(row[0]) if row and row[0] is not None else 0
        conn.close()
    except Exception:
        pass

    remaining = max(0, total - converted)
    pct = int(round(100 * (converted / total), 0)) if total else 0
    return {
        "as_of": as_of.isoformat(timespec="seconds"),
        "total_pb": total,
        "converted": converted,
        "remaining": remaining,
        "pct": pct
    }


# ---------- core ----------
def generate_summary_for_log_dir(log_dir: str):
    # list files with scandir for speed
    files = []
    with os.scandir(log_dir) as it:
        for e in it:
            if e.is_file():
                files.append(e.path)

    small_logs = [f for f in files if os.path.basename(f).startswith("small_bundle_")]
    big_logs   = [f for f in files if os.path.basename(f).startswith("big_chunk_")]
    old_logs   = [f for f in files if os.path.basename(f).startswith("log_chunk_")]
    other_logs = [f for f in files if f not in small_logs + big_logs + old_logs]

    # Totals
    total_pb_bytes = 0
    total_parquet_bytes = 0
    peak_rss_gb_overall = 0.0
    sum_of_job_peaks_gb = 0.0

    # Per-category bytes
    small_saved_bytes = 0
    big_chunk_bytes = 0
    pv_merged_bytes = 0
    date_merged_bytes = 0
    unsplit_merged_bytes = 0

    # Counters
    saved_files_count = 0          # "SAVED:" from small path
    chunk_files_count = 0          # "SAVED CHUNK:" / "SAVED FINAL CHUNK:"
    pv_merges_count = 0            # "✅ Merged ..."
    date_merges_count = 0          # "✅ Date YYYY_MM_DD ..."
    unsplit_merges_count = 0       # "✅ Unsplit PV ..."

    # PB classification by route
    small_pb_count = 0
    big_pb_count = 0
    small_pv_examples: List[str] = []
    big_pv_examples: List[str] = []

    # Track parquet paths for classification
    small_saved_paths = set()   # final PVs produced by small path
    big_chunk_paths = set()     # chunk files from big path
    big_final_paths = set()     # final PV files from PV-chunk merge

    # timing
    start_times: List[datetime] = []
    end_times: List[datetime] = []

    # failures
    failures_all: Dict[str, List[str]] = defaultdict(list)

    def maybe_add_example(lst: List[str], val: str, cap: int = 10):
        if len(lst) < cap and val and val not in lst:
            lst.append(val)

    def pv_id_from_saved_path(path: str) -> str:
        try:
            return os.path.splitext(os.path.basename(path))[0]
        except Exception:
            return path

    def process_small_log(path: str):
        nonlocal total_pb_bytes, total_parquet_bytes
        nonlocal peak_rss_gb_overall, sum_of_job_peaks_gb
        nonlocal small_pb_count, small_saved_bytes, saved_files_count

        per_log_peak = 0.0
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()

        fails = extract_failures(lines)
        if fails:
            failures_all[os.path.basename(path)].extend(fails)

        for line in lines:
            ls = line.strip()

            if "Total .pb input size" in ls:
                part = ls.split(":", 1)[1] if ":" in ls else ""
                total_pb_bytes += bytes_from_size_string(part)

            if ls.startswith("Peak RSS"):
                part = ls.split(":", 1)[1] if ":" in ls else ""
                gb = bytes_from_size_string(part) / (1024**3)
                peak_rss_gb_overall = max(peak_rss_gb_overall, gb)
                per_log_peak = max(per_log_peak, gb)

            if ls.startswith("Start time"):
                try:
                    ts = ls.split(":", 1)[1].strip()
                    start_times.append(datetime.fromisoformat(ts))
                except Exception:
                    pass
            if ls.startswith("End time"):
                try:
                    ts = ls.split(":", 1)[1].strip()
                    end_times.append(datetime.fromisoformat(ts))
                except Exception:
                    pass

            if ls.startswith("SAVED:"):
                sz = find_parquet_bytes_for_line(ls)
                if sz > 0:
                    total_parquet_bytes += sz
                    small_saved_bytes += sz
                p = extract_first_parquet_path(ls)
                if p:
                    small_saved_paths.add(p)
                    maybe_add_example(small_pv_examples, pv_id_from_saved_path(p))
                saved_files_count += 1
                small_pb_count += 1

        sum_of_job_peaks_gb += per_log_peak

    def process_big_log(path: str):
        nonlocal total_parquet_bytes
        nonlocal peak_rss_gb_overall, sum_of_job_peaks_gb
        nonlocal big_pb_count, big_chunk_bytes, chunk_files_count

        per_log_peak = 0.0
        seen_pb_path = None

        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()

        fails = extract_failures(lines)
        if fails:
            failures_all[os.path.basename(path)].extend(fails)

        for line in lines:
            ls = line.strip()

            if "SAVED CHUNK:" in ls or "SAVED FINAL CHUNK:" in ls:
                rss_gb = parse_rss_gb_from_line(ls)
                if rss_gb is not None:
                    peak_rss_gb_overall = max(peak_rss_gb_overall, rss_gb)
                    per_log_peak = max(per_log_peak, rss_gb)

                sz = find_parquet_bytes_for_line(ls)
                if sz > 0:
                    total_parquet_bytes += sz
                    big_chunk_bytes += sz
                p = extract_first_parquet_path(ls)
                if p:
                    big_chunk_paths.add(p)
                chunk_files_count += 1

            if ls.startswith("Finished at"):
                try:
                    ts = ls.split(":", 1)[1].strip()
                    end_times.append(datetime.fromisoformat(ts))
                except Exception:
                    pass
            if ls.startswith("Started at"):
                try:
                    ts = ls.split(":", 1)[1].strip()
                    start_times.append(datetime.fromisoformat(ts))
                except Exception:
                    pass

            if ls.startswith("PB file"):
                try:
                    seen_pb_path = ls.split(":", 1)[1].strip()
                except Exception:
                    pass

        big_pb_count += 1
        if seen_pb_path:
            base = os.path.splitext(os.path.basename(seen_pb_path))[0].replace(":", "_")
            maybe_add_example(big_pv_examples, base)
        sum_of_job_peaks_gb += per_log_peak

    def process_other_log(path: str):
        nonlocal total_parquet_bytes, pv_merged_bytes, date_merged_bytes, unsplit_merged_bytes
        nonlocal pv_merges_count, date_merges_count, unsplit_merges_count

        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()

        fails = extract_failures(lines)
        if fails:
            failures_all[os.path.basename(path)].extend(fails)

        for line in lines:
            ls = line.strip()

            # PV chunk merge → final PV parquet
            if ls.startswith("✅ Merged "):
                sz = find_parquet_bytes_for_line(ls)
                if sz > 0:
                    total_parquet_bytes += sz
                    pv_merged_bytes += sz
                p = extract_first_parquet_path(ls)
                if p:
                    big_final_paths.add(p)
                pv_merges_count += 1

            # Date-level merge
            if ls.startswith("✅ Date "):
                sz = find_parquet_bytes_for_line(ls)
                if sz > 0:
                    total_parquet_bytes += sz
                    date_merged_bytes += sz
                date_merges_count += 1

            # Unsplit PV (merge per-day back to one PV)
            if ls.startswith("✅ Unsplit PV"):
                sz = find_parquet_bytes_for_line(ls)
                if sz > 0:
                    total_parquet_bytes += sz
                    unsplit_merged_bytes += sz
                unsplit_merges_count += 1

    # Walk all logs
    for p in small_logs:
        process_small_log(p)
    for p in big_logs:
        process_big_log(p)
    for p in old_logs + other_logs:
        process_other_log(p)

    # ---------- classification counts ----------
    def _inc(counter: dict, vk: str, sk: str):
        key = None
        if vk == "numeric" and sk == "scalar": key = "numeric_scalar"
        elif vk == "numeric" and sk == "vector": key = "numeric_vector"
        elif vk == "text" and sk == "scalar": key = "text_scalar"
        elif vk == "text" and sk == "vector": key = "text_vector"
        else: key = "unknown"
        counter[key] = counter.get(key, 0) + 1

    # Small path classification
    small_type_counts = {"numeric_scalar":0,"numeric_vector":0,"text_scalar":0,"text_vector":0,"unknown":0}

    # Big path classification: prefer final PVs when present; else sample a chunk for that base
    big_type_counts = {"numeric_scalar":0,"numeric_vector":0,"text_scalar":0,"text_vector":0,"unknown":0}
    base_to_final: Dict[str, Tuple[str, str]] = {}
    for fp in big_final_paths:
        base = os.path.splitext(os.path.basename(fp))[0]
        base_to_final[base] = ("final", fp)
    for cp in sorted(big_chunk_paths):
        fn = os.path.basename(cp)
        m = CHUNK_RE.match(fn)
        if not m:
            continue
        base = m.group("base")
        if base not in base_to_final:
            base_to_final[base] = ("chunk", cp)

    # parallel classification to speed up many files
    def classify_many(paths: List[str]) -> Dict[str, Tuple[str, str]]:
        out: Dict[str, Tuple[str, str]] = {}
        if not paths:
            return out
        with ThreadPoolExecutor(max_workers=THREADS) as ex:
            fut_map = {ex.submit(_classify_parquet_fast, p): p for p in paths}
            for fut in as_completed(fut_map):
                p = fut_map[fut]
                try:
                    vk, sk = fut.result()
                except Exception:
                    vk, sk = "unknown", "unknown"
                out[p] = (vk, sk)
        return out

    small_class = classify_many(sorted(small_saved_paths))
    for p, (vk, sk) in small_class.items():
        _inc(small_type_counts, vk, sk)

    big_class = classify_many([path for (_, path) in base_to_final.values()])
    for p, (vk, sk) in big_class.items():
        _inc(big_type_counts, vk, sk)

    # Summary object
    ts_dirname = os.path.basename(log_dir)
    summary = {
        "timestamp": datetime.now().isoformat(),
        "config_path": CFG["CFG_PATH"],
        "log_dir": log_dir,

        # Job sets
        "small_bundle_logs": len(small_logs),
        "big_chunk_logs": len(big_logs),
        "legacy_chunk_logs": len(old_logs),
        "other_logs": len(other_logs),

        # Counters across categories
        "counters": {
            "saved_files_count": saved_files_count,
            "chunk_files_count": chunk_files_count,
            "pv_merges_count": pv_merges_count,
            "date_merges_count": date_merges_count,
            "unsplit_merges_count": unsplit_merges_count,
        },

        # PB classification by path (how many PBs took each route)
        "pb_classification": {
            "small_pb_count": small_pb_count,
            "big_pb_count": big_pb_count,
            "small_pv_examples": small_pv_examples,
            "big_pv_examples": big_pv_examples,
        },

        # PV type breakdowns (inferred by Parquet schema)
        "pv_type_breakdown": {
            "small": small_type_counts,
            "big":   big_type_counts,
        },

        # Sizes (overall + per category)
        "sizes": {
            "total_pb_size_MB": round(total_pb_bytes / (1024 * 1024), 2),
            "total_parquet_size_MB": round(total_parquet_bytes / (1024 * 1024), 2),
            "small_saved_MB": round(small_saved_bytes / (1024 * 1024), 2),
            "big_chunk_MB": round(big_chunk_bytes / (1024 * 1024), 2),
            "pv_merged_MB": round(pv_merged_bytes / (1024 * 1024), 2),
            "date_merged_MB": round(date_merged_bytes / (1024 * 1024), 2),
            "unsplit_merged_MB": round(unsplit_merged_bytes / (1024 * 1024), 2),
        },

        # Memory
        "peak_RSS_GB_overall": round(peak_rss_gb_overall, 3),
        "sum_of_job_peaks_GB": round(sum_of_job_peaks_gb, 3),
    }

    # Wall clock from any start/end we could parse
    wall_end_for_snapshot: Optional[datetime] = None
    if start_times and end_times:
        wall_start = min(start_times)
        wall_end = max(end_times)
        wall_time_sec = int((wall_end - wall_start).total_seconds())
        minutes = wall_time_sec // 60
        seconds = wall_time_sec % 60
        summary.update({
            "wall_start": wall_start.isoformat(),
            "wall_end": wall_end.isoformat(),
            "wall_time_sec": wall_time_sec,
            "wall_time_hms": f"{minutes}m {seconds}s" if minutes else f"{seconds}s",
        })
        wall_end_for_snapshot = wall_end

    # Progress snapshot (as of wall_end if available; else now)
    as_of = wall_end_for_snapshot or datetime.now()
    summary["progress_snapshot"] = _progress_snapshot(as_of)

    # Write JSON summary
    summary_path = os.path.join(LOG_ROOT, f"submission_summary_{ts_dirname}.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    # Write failure report
    failed_txt_path = os.path.join(FAILED_SUMMARY_DIR, FAILED_TXT_TEMPLATE.format(timestamp=ts_dirname))
    with open(failed_txt_path, "w", encoding="utf-8") as f:
        any_fail = False
        for fname, msgs in sorted(failures_all.items()):
            if not msgs:
                continue
            any_fail = True
            f.write(f"===== {fname} =====\n")
            for m in msgs:
                f.write(m + "\n")
            f.write("\n")
        if not any_fail:
            f.write("(no failures detected)\n")

    return summary_path, failed_txt_path


# === MAIN ===================================================================

if __name__ == "__main__":
    if not os.path.isdir(LOG_ROOT):
        print(f"Log root not found: {LOG_ROOT}")
        raise SystemExit(2)

    # each subdir under LOG_ROOT is a run folder
    log_dirs = []
    with os.scandir(LOG_ROOT) as it:
        for e in it:
            if e.is_dir():
                log_dirs.append(e.path)

    generated = []
    for log_dir in sorted(log_dirs):
        summary, failed_txt = generate_summary_for_log_dir(log_dir)
        print(f"✅ Created: {summary}")
        print(f"📄 Failed file report: {failed_txt}")
        generated.append(summary)

    print(f"\nDone. Regenerated {len(generated)} summary file(s).")