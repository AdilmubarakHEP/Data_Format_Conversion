#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
03c-submit-merge-jobs.py

Master Merge Orchestrator
Submits bsub jobs for both PV chunk merging (03a) and date merging (03b).

Workflow:
1. Scan for PV chunk groups and submit one bsub job per PV → calls 03a
2. Scan for dates and submit one bsub job per date → calls 03b

Config (configparser)
- [paths].output_base
- [merge].output_dir
- [lsf].queue
- [merge].delete_chunks_after_pv_merge
- [merge].merge_delete_parquet_after_date
- [merge].merge_sort_by_time
- Optional env: PVPIPE_CONFIG
"""

import os
import re
import sys
import argparse
import configparser
import subprocess
from collections import defaultdict
from typing import Dict, List

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

OUTPUT_BASE = _cfg_str("paths", "output_base", "/home/belle2/amubarak/PV_Output/converted_flat2")

MERGE_OUTPUT_DIR = _cfg_str("merge", "output_dir", None)
if not MERGE_OUTPUT_DIR:
    MERGE_OUTPUT_DIR = _cfg_str("merge", "merge_output_dir", "/home/belle2/amubarak/PV_Output/merged")

LSF_QUEUE = _cfg_str("lsf", "queue", "h")
DELETE_CHUNKS = _cfg_bool("merge", "delete_chunks_after_pv_merge", False)
DELETE_AFTER_DATE = _cfg_bool("merge", "merge_delete_parquet_after_date", False)
SORT_BY_TIME = _cfg_bool("merge", "merge_sort_by_time", True)

# ---- chunk detection ----
CHUNK_RE = re.compile(r"^(?P<base>.+)_chunk(?P<idx>\d{4})\.parquet$")

def is_chunk_file(filename: str) -> bool:
    return bool(CHUNK_RE.match(filename))

def chunk_base_and_index(filename: str):
    m = CHUNK_RE.match(filename)
    if not m:
        return None, None
    return m.group("base"), int(m.group("idx"))

def _single_walk(input_dir: str):
    """One walk over input_dir. Returns (chunk_groups, date_to_files)."""
    date_re = re.compile(r"(\d{4}_\d{2}_\d{2})")
    chunk_groups = defaultdict(list)
    date_to_files = defaultdict(list)

    for root, _, files in os.walk(input_dir):
        for fn in files:
            if not fn.endswith(".parquet"):
                continue
            full_path = os.path.join(root, fn)
            if is_chunk_file(fn):
                base, idx = chunk_base_and_index(fn)
                if base is not None:
                    final_path = os.path.join(root, f"{base}.parquet")
                    chunk_groups[final_path].append((idx, full_path))
            else:
                m = date_re.search(fn)
                if m:
                    date_to_files[m.group(1)].append(full_path)

    # Sort chunk groups by index
    sorted_groups = {}
    for final_path, items in chunk_groups.items():
        items.sort(key=lambda t: t[0])
        sorted_groups[final_path] = [p for _, p in items]

    return sorted_groups, dict(date_to_files)

# ---- job submission ----
def submit_pv_merge_jobs(script_03a: str, input_dir: str, delete_chunks: bool,
                         sort_by_time: bool, queue: str, dry_run: bool,
                         precomputed_groups: Dict = None) -> tuple:
    """Submit bsub jobs for PV chunk merging (one job per PV)."""
    groups = precomputed_groups if precomputed_groups is not None else _single_walk(input_dir)[0]
    if not groups:
        print("No PV chunk groups found.")
        return 0, 0
    
    print(f"Found {len(groups)} PV chunk groups to merge.")
    submitted = 0
    skipped = 0
    failed = 0
    
    for pv_final_path, chunk_paths in sorted(groups.items()):
        if os.path.exists(pv_final_path):
            print(f"Skip (already exists): {pv_final_path}")
            skipped += 1
            continue
        
        cmd = [
            "bsub",
            "-q", queue,
            "-J", f"pvmerge_{os.path.basename(pv_final_path)}",
            sys.executable, script_03a,
            "--input_dir", input_dir,
            "--pv_final_path", pv_final_path
        ]
        
        if delete_chunks:
            cmd.append("--delete_chunks")
        if sort_by_time:
            cmd.append("--sort_by_time")
        
        if dry_run:
            print(f"[DRY RUN] Would submit: {pv_final_path} ({len(chunk_paths)} chunks)")
            submitted += 1
        else:
            try:
                result = subprocess.run(cmd, check=False, capture_output=True, text=True)
                if result.returncode == 0:
                    print(f"✅ Submitted PV merge: {pv_final_path} ({len(chunk_paths)} chunks)")
                    submitted += 1
                else:
                    print(f"❌ bsub failed for {pv_final_path}: rc={result.returncode}")
                    print(f"   stdout: {result.stdout.strip()}")
                    print(f"   stderr: {result.stderr.strip()}")
                    failed += 1
            except Exception as e:
                print(f"❌ bsub exception for {pv_final_path}: {e}")
                failed += 1
    
    print(f"\nPV merge jobs: submitted={submitted}, skipped={skipped}, failed={failed}")
    return submitted, skipped

def submit_date_merge_jobs(script_03b: str, input_dir: str, output_dir: str,
                           delete_after_date: bool, queue: str, dry_run: bool,
                           precomputed_dates: dict = None) -> int:
    """Write per-date manifests, submit one bsub per date."""
    date_to_files = precomputed_dates if precomputed_dates is not None else _single_walk(input_dir)[1]

    if not date_to_files:
        print("No dates found for date-level merge.")
        return 0

    print(f"Found {sum(len(v) for v in date_to_files.values())} files across {len(date_to_files)} dates.")

    # Write manifests to a temp directory that persists until jobs finish
    manifest_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "manifests")
    os.makedirs(manifest_dir, exist_ok=True)

    submitted = 0
    skipped = 0
    for date_str in sorted(date_to_files):
        file_list = date_to_files[date_str]

        # Skip if merged output already exists
        merged_path = os.path.join(output_dir, f"{date_str}.parquet")
        if os.path.exists(merged_path):
            print(f"Skip {date_str}: {merged_path} already exists.")
            skipped += 1
            continue

        # Write manifest
        manifest_path = os.path.join(manifest_dir, f"{date_str}.txt")
        with open(manifest_path, "w", encoding="utf-8") as mf:
            mf.write("\n".join(sorted(file_list)))

        if dry_run:
            print(f"[DRY RUN] {date_str}: {len(file_list)} files → {manifest_path}")
            submitted += 1
            continue

        # Submit bsub with manifest
        argv = [
            "bsub", "-q", queue,
            "-J", f"datemerge_{date_str}",
            sys.executable, script_03b,
            "--internal", "--date", date_str,
            "--input_dir", input_dir,
            "--output_dir", output_dir,
            "--manifest", manifest_path,
        ]
        if delete_after_date:
            argv.append("--delete_after_date")

        try:
            r = subprocess.run(argv, check=False, capture_output=True, text=True)
            if r.returncode == 0:
                print(f"Submitted merge for {date_str} ({len(file_list)} files)")
                submitted += 1
            else:
                print(f"❌ bsub failed for {date_str}: rc={r.returncode} {r.stderr.strip()}")
        except Exception as e:
            print(f"❌ bsub exception for {date_str}: {e}")

    print(f"\nDate merge: submitted={submitted}, skipped={skipped}")
    return 0

def main():
    ap = argparse.ArgumentParser(description="Master merge orchestrator - submits jobs for PV and date merges.")
    ap.add_argument("--input_dir", default=OUTPUT_BASE, help="Where PV Parquets and chunks live")
    ap.add_argument("--output_dir", default=MERGE_OUTPUT_DIR, help="Where to write date-level merged Parquets")
    ap.add_argument("--queue", default=LSF_QUEUE, help="LSF queue")
    ap.add_argument("--delete_chunks", action="store_true", default=DELETE_CHUNKS,
                    help="Delete chunks after PV merge")
    ap.add_argument("--delete_after_date", action="store_true", default=DELETE_AFTER_DATE,
                    help="Delete per-PV Parquets after date merge")
    ap.add_argument("--sort_by_time", action="store_true", default=SORT_BY_TIME,
                    help="Sort by time during PV merge")
    ap.add_argument("--pv_only", action="store_true", help="Only submit PV merge jobs")
    ap.add_argument("--date_only", action="store_true", help="Only submit date merge jobs")
    ap.add_argument("--dry_run", action="store_true", help="Show what would be submitted without submitting")
    args = ap.parse_args()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    script_03a = os.path.join(script_dir, "03a-pv-chunks.py")
    script_03b = os.path.join(script_dir, "03b-merge-date.py")
    
    # Validate scripts exist
    missing = []
    if not os.path.exists(script_03a):
        missing.append(script_03a)
    if not os.path.exists(script_03b):
        missing.append(script_03b)
    if missing:
        print(f"❌ Missing merge scripts:")
        for m in missing:
            print(f"   {m}")
        sys.exit(1)

    # Single walk for both stages
    print(f"Scanning {args.input_dir} for parquet files...")
    chunk_groups, date_to_files = _single_walk(args.input_dir)
    print(f"Found {sum(len(v) for v in chunk_groups.values())} chunk files in {len(chunk_groups)} groups, "
          f"{sum(len(v) for v in date_to_files.values())} date files across {len(date_to_files)} dates.")

    # Submit PV merge jobs unless --date_only
    if not args.date_only:
        print("=" * 70)
        print("STAGE 1: PV Chunk Merge (03a)")
        print("=" * 70)
        submit_pv_merge_jobs(script_03a, args.input_dir, args.delete_chunks,
                             args.sort_by_time, args.queue, args.dry_run,
                             precomputed_groups=chunk_groups)

    # Submit date merge jobs unless --pv_only
    if not args.pv_only:
        print("\n" + "=" * 70)
        print("STAGE 2: Date Merge (03b)")
        print("=" * 70)
        rc = submit_date_merge_jobs(script_03b, args.input_dir, args.output_dir,
                                     args.delete_after_date, args.queue, args.dry_run,
                                     precomputed_dates=date_to_files)
        if rc != 0:
            print(f"❌ Date merge submission failed with exit code {rc}")
            sys.exit(rc)
    
    print("\n" + "=" * 70)
    print("Merge job submission complete!")
    print("=" * 70)

if __name__ == "__main__":
    main()