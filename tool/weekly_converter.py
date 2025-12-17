#!/usr/bin/env python3
"""
Weekly Converter - Memory-Efficient Large File Converter
=========================================================

This script converts large .pb files by processing ONE WEEK at a time.
Each week is a separate pass through the file, keeping memory usage minimal.

Key features:
- Works within 4GB memory limit
- Can resume from partial conversions (skips completed weeks)
- Processes weeks in parallel via LSF jobs

Usage:
    # Convert a single file (one week)
    python3 weekly_converter.py --pb /path/to/file.pb --week 2024-W01

    # Submit all weeks for a file
    python3 weekly_converter.py --pb /path/to/file.pb --submit-all

    # Submit jobs for all large files
    python3 weekly_converter.py --submit-large-files
"""

import os
import sys
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
import json

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# Configuration
PB_DIR = "/gpfs/group/belle2/group/accelerator/pv/2024/AA/"
PARQUET_DIR = "/gpfs/group/belle2/group/accelerator/pv/2024/AA_parquet/converted_flat2"
LOG_DIR = "/home/belle2/amubarak/Data_Format_Conversion/logs/weekly"
JAVA_CLASSPATH = "/home/belle2/amubarak/Data_Format_Conversion/java"
AUDIT_FILE = "/home/belle2/amubarak/fast_audit_results.json"

# All weeks in 2024
ALL_WEEKS = [f"2024-W{i:02d}" for i in range(1, 54)]

# Time constants
WEEK_MS = 7 * 24 * 60 * 60 * 1000  # milliseconds per week
YEAR_START_MS = 0  # 2024-01-01 00:00:00 as offset


def get_week_time_range(week_label: str) -> tuple:
    """Get start and end time offsets for a week label."""
    # Parse week number
    year, week_str = week_label.split('-W')
    week_num = int(week_str)

    if year == "2024":
        start_ms = (week_num - 1) * WEEK_MS
        end_ms = week_num * WEEK_MS
    elif year == "2023":
        # Handle spillover from previous year
        start_ms = -WEEK_MS
        end_ms = 0
    else:  # 2025
        start_ms = 53 * WEEK_MS + (week_num - 1) * WEEK_MS
        end_ms = start_ms + WEEK_MS

    return start_ms, end_ms


def get_output_dir(pb_path: str) -> Path:
    """Get output directory for a .pb file."""
    try:
        rel_path = Path(pb_path).relative_to(PB_DIR)
        return Path(PARQUET_DIR) / rel_path.parent
    except ValueError:
        return Path(PARQUET_DIR) / "other"


def get_missing_weeks(pb_path: str) -> list:
    """Get list of weeks that haven't been converted yet."""
    pb_name = Path(pb_path).stem
    pv_name = pb_name.split(':')[0] if ':' in pb_name else pb_name
    out_dir = get_output_dir(pb_path)

    existing_weeks = set()
    if out_dir.exists():
        for pf in out_dir.glob(f'{pv_name}-2024-W*.parquet'):
            # Extract week from filename like "TEMP60-2024-W01.parquet"
            week = pf.stem.split('-')[-1]
            week_label = f"2024-{week}"
            existing_weeks.add(week_label)

    missing = [w for w in ALL_WEEKS if w not in existing_weeks]
    return missing


def convert_single_week(pb_path: str, week_label: str) -> bool:
    """Convert a single week from a .pb file."""

    pb_name = Path(pb_path).stem
    pv_name = pb_name.split(':')[0] if ':' in pb_name else pb_name
    out_dir = get_output_dir(pb_path)
    out_path = out_dir / f"{pv_name}-{week_label}.parquet"

    # Skip if already exists
    if out_path.exists():
        print(f"SKIP: {week_label} already exists")
        return True

    os.makedirs(out_dir, exist_ok=True)

    # Get time range for this week
    start_ms, end_ms = get_week_time_range(week_label)

    file_size_gb = os.path.getsize(pb_path) / (1024**3)
    print(f"Converting: {pb_path}")
    print(f"Week: {week_label} (offset {start_ms} to {end_ms})")
    print(f"File size: {file_size_gb:.1f} GB")
    print(f"Output: {out_path}")
    print(f"Started: {datetime.now()}")
    sys.stdout.flush()

    # Run Java reader with minimal memory
    cmd = ["java", "-Xmx512m", "-cp", JAVA_CLASSPATH, "pb2parquet", pb_path]

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1024*1024
    )

    # Collect data for this week only
    times = []
    values = []
    nval = None
    is_vector = False
    is_text = False

    lines_read = 0
    lines_matched = 0
    last_progress = 0

    for line_bytes in proc.stdout:
        try:
            line = line_bytes.decode('utf-8', errors='replace').strip()
        except:
            continue

        if not line:
            continue

        # Parse metadata
        if line.startswith('#'):
            if '# Nval ' in line:
                try:
                    nval = int(line.split()[-1])
                    is_vector = nval > 1
                except:
                    pass
            continue

        lines_read += 1

        # Progress every 10M lines
        if lines_read // 10000000 > last_progress:
            last_progress = lines_read // 10000000
            print(f"  Read {lines_read/1e6:.0f}M lines, matched {lines_matched:,} for {week_label}")
            sys.stdout.flush()

        # Parse data line
        parts = line.split()
        if len(parts) < 2:
            continue

        try:
            ts_offset = int(parts[0])
        except ValueError:
            continue

        # Filter to this week only
        if ts_offset < start_ms or ts_offset >= end_ms:
            continue

        lines_matched += 1
        times.append(ts_offset)

        # Parse value
        if is_vector and nval and nval > 1:
            row_vals = []
            for v in parts[1:]:
                if v.startswith('"') and v.endswith('"'):
                    is_text = True
                    row_vals.append(v[1:-1])
                else:
                    try:
                        row_vals.append(float(v))
                    except ValueError:
                        is_text = True
                        row_vals.append(v)
            values.append(row_vals)
        else:
            val_str = ' '.join(parts[1:])
            if val_str.startswith('"') and val_str.endswith('"'):
                is_text = True
                values.append(val_str[1:-1])
            else:
                try:
                    values.append(float(val_str))
                except ValueError:
                    is_text = True
                    values.append(val_str)

    # Wait for process
    _, stderr = proc.communicate()
    if stderr:
        stderr_str = stderr.decode('utf-8', errors='replace')
        if stderr_str.strip() and 'INFO' not in stderr_str:
            print(f"Java stderr: {stderr_str[:200]}")

    print(f"Read complete: {lines_read:,} total, {lines_matched:,} for {week_label}")

    # Write parquet if we have data
    if not times:
        print(f"No data for {week_label}")
        return True  # Not an error, just no data for this week

    times_arr = np.array(times, dtype=np.int64)

    if is_vector:
        if is_text:
            list_type = pa.list_(pa.string())
        else:
            list_type = pa.list_(pa.float64())
        schema = pa.schema([
            ('time_offset_ms', pa.int64()),
            ('values', list_type)
        ])
        table = pa.table({
            'time_offset_ms': times_arr,
            'values': pa.array(values, type=list_type)
        }, schema=schema)
    else:
        if is_text:
            schema = pa.schema([
                ('time_offset_ms', pa.int64()),
                ('value', pa.string())
            ])
            table = pa.table({
                'time_offset_ms': times_arr,
                'value': pa.array([str(v) for v in values], type=pa.string())
            }, schema=schema)
        else:
            schema = pa.schema([
                ('time_offset_ms', pa.int64()),
                ('value', pa.float64())
            ])
            table = pa.table({
                'time_offset_ms': times_arr,
                'value': pa.array(values, type=pa.float64())
            }, schema=schema)

    pq.write_table(table, str(out_path), compression='zstd')

    print(f"COMPLETE: {week_label} - {lines_matched:,} rows written")
    print(f"Finished: {datetime.now()}")

    return True


def submit_week_job(pb_path: str, week_label: str, job_group: str) -> bool:
    """Submit LSF job for a single week."""
    pb_name = Path(pb_path).stem.replace(':', '_')
    job_name = f"wk_{pb_name}_{week_label}"[:100]
    log_file = f"{LOG_DIR}/{pb_name}_{week_label}.log"

    cmd = [
        'bsub',
        '-q', 'h',
        '-g', job_group,
        '-J', job_name,
        '-o', log_file,
        '-e', log_file,
        'python3', __file__,
        '--pb', pb_path,
        '--week', week_label
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode == 0


def submit_all_weeks(pb_path: str, job_group: str = '/pvpipe_weekly') -> int:
    """Submit jobs for all missing weeks of a file."""
    missing = get_missing_weeks(pb_path)

    if not missing:
        print(f"All weeks complete for {Path(pb_path).name}")
        return 0

    print(f"Submitting {len(missing)} week jobs for {Path(pb_path).name}")

    os.makedirs(LOG_DIR, exist_ok=True)
    subprocess.run(['bgadd', '-L', '500', job_group], capture_output=True)

    submitted = 0
    for week in missing:
        if submit_week_job(pb_path, week, job_group):
            submitted += 1

    print(f"  Submitted {submitted}/{len(missing)} jobs")
    return submitted


def get_large_files_needing_conversion(min_size_gb: float = 5.0) -> list:
    """Get list of large files that need conversion."""
    if not os.path.exists(AUDIT_FILE):
        print(f"ERROR: Audit file not found: {AUDIT_FILE}")
        return []

    with open(AUDIT_FILE) as f:
        audit = json.load(f)

    all_files = list(set(
        audit.get('files_not_in_db', []) +
        audit.get('queued_files', [])
    ))

    large_files = []
    for pb_path in all_files:
        if not os.path.exists(pb_path):
            continue
        size_gb = os.path.getsize(pb_path) / (1024**3)
        if size_gb < min_size_gb:
            continue

        missing = get_missing_weeks(pb_path)
        if not missing:
            continue

        large_files.append({
            'path': pb_path,
            'size_gb': size_gb,
            'missing_weeks': len(missing)
        })

    return sorted(large_files, key=lambda x: -x['size_gb'])


def submit_large_files(min_size_gb: float = 5.0, max_jobs: int = 1000):
    """Submit weekly jobs for all large files."""
    files = get_large_files_needing_conversion(min_size_gb)

    if not files:
        print("No large files need conversion!")
        return

    total_weeks = sum(f['missing_weeks'] for f in files)
    print(f"Found {len(files)} large files needing {total_weeks} week jobs")
    print(f"Total size: {sum(f['size_gb'] for f in files):.1f} GB")
    print()

    job_group = '/pvpipe_weekly'
    os.makedirs(LOG_DIR, exist_ok=True)
    subprocess.run(['bgadd', '-L', '1000', job_group], capture_output=True)

    total_submitted = 0
    for f in files:
        if total_submitted >= max_jobs:
            print(f"Reached max jobs ({max_jobs}), stopping")
            break

        remaining = max_jobs - total_submitted
        missing = get_missing_weeks(f['path'])[:remaining]

        print(f"Submitting {len(missing)} weeks for {Path(f['path']).name} ({f['size_gb']:.1f} GB)")

        for week in missing:
            if submit_week_job(f['path'], week, job_group):
                total_submitted += 1

    print()
    print("=" * 60)
    print(f"SUBMITTED {total_submitted} JOBS")
    print("=" * 60)
    print(f"Monitor: bjobs -g {job_group}")
    print(f"Logs: {LOG_DIR}/")


def main():
    parser = argparse.ArgumentParser(description='Weekly memory-efficient converter')
    parser.add_argument('--pb', type=str, help='Path to .pb file')
    parser.add_argument('--week', type=str, help='Week to convert (e.g., 2024-W01)')
    parser.add_argument('--submit-all', action='store_true',
                        help='Submit jobs for all missing weeks of a file')
    parser.add_argument('--submit-large-files', action='store_true',
                        help='Submit jobs for all large files')
    parser.add_argument('--min-size-gb', type=float, default=5.0,
                        help='Minimum file size for large file processing')
    parser.add_argument('--max-jobs', type=int, default=1000,
                        help='Maximum number of jobs to submit')
    parser.add_argument('--list-missing', action='store_true',
                        help='List missing weeks for a file')

    args = parser.parse_args()

    if args.submit_large_files:
        submit_large_files(args.min_size_gb, args.max_jobs)
        return

    if not args.pb:
        parser.print_help()
        return

    if args.list_missing:
        missing = get_missing_weeks(args.pb)
        print(f"Missing weeks for {Path(args.pb).name}: {len(missing)}")
        for w in missing:
            print(f"  {w}")
        return

    if args.submit_all:
        submit_all_weeks(args.pb)
        return

    if args.week:
        success = convert_single_week(args.pb, args.week)
        sys.exit(0 if success else 1)
    else:
        # Default: show missing weeks
        missing = get_missing_weeks(args.pb)
        print(f"Missing {len(missing)} weeks for {Path(args.pb).name}")
        print("Use --week WEEK to convert a specific week")
        print("Use --submit-all to submit jobs for all missing weeks")


if __name__ == "__main__":
    main()
