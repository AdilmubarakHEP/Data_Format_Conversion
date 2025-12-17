#!/usr/bin/env python3
"""
Submit All Files for Chunked Conversion
=======================================

Submits all remaining .pb files for conversion using the chunked approach:
- Small files (<5GB): Submit via LSF using chunked_pb_converter.py
- Large files (>=5GB): Run sequentially on login node (more memory available)

Usage:
    python3 submit_all_chunked.py              # Audit and submit all
    python3 submit_all_chunked.py --audit-only # Just show what needs work
    python3 submit_all_chunked.py --small-only # Only submit small files via LSF
    python3 submit_all_chunked.py --large-only # Only process large files on login node
"""

import os
import sys
import json
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Configuration
PB_DIR = "/gpfs/group/belle2/group/accelerator/pv/2024/AA/"
PARQUET_DIR = "/gpfs/group/belle2/group/accelerator/pv/2024/AA_parquet/converted_flat2"
LOG_DIR = "/home/belle2/amubarak/Data_Format_Conversion/logs/chunked"
AUDIT_FILE = "/home/belle2/amubarak/fast_audit_results.json"
CONVERTER = "/home/belle2/amubarak/Data_Format_Conversion/tool/chunked_pb_converter.py"

SMALL_THRESHOLD_GB = 5.0
MIN_WEEKS_COMPLETE = 45


def audit_files():
    """Find all files needing conversion."""
    print("Loading audit data...")

    if not os.path.exists(AUDIT_FILE):
        print(f"ERROR: Audit file not found: {AUDIT_FILE}")
        sys.exit(1)

    with open(AUDIT_FILE) as f:
        audit_data = json.load(f)

    all_files = list(set(
        audit_data.get('files_not_in_db', []) +
        audit_data.get('queued_files', [])
    ))

    needs_work = []
    complete = []

    for pb_path in all_files:
        if not os.path.exists(pb_path):
            continue

        size_gb = os.path.getsize(pb_path) / (1024**3)
        pb_name = Path(pb_path).stem
        pv_name = pb_name.split(':')[0] if ':' in pb_name else pb_name

        try:
            rel_path = Path(pb_path).relative_to(PB_DIR)
            out_dir = Path(PARQUET_DIR) / rel_path.parent
        except ValueError:
            out_dir = Path(PARQUET_DIR) / "other"

        weeks_found = 0
        if out_dir.exists():
            weeks_found = len(list(out_dir.glob(f'{pv_name}-2024-W*.parquet')))

        if weeks_found >= MIN_WEEKS_COMPLETE:
            complete.append(pb_path)
        else:
            needs_work.append({
                'path': pb_path,
                'size_gb': size_gb,
                'weeks_found': weeks_found,
                'pv_name': pv_name
            })

    # Sort by size (smallest first for faster initial results)
    needs_work.sort(key=lambda x: x['size_gb'])

    return needs_work, complete


def print_summary(needs_work, complete):
    """Print audit summary."""
    print("\n" + "=" * 60)
    print("CONVERSION STATUS")
    print("=" * 60)
    print(f"Complete: {len(complete)} files")
    print(f"Needs work: {len(needs_work)} files")

    if needs_work:
        small = [f for f in needs_work if f['size_gb'] < SMALL_THRESHOLD_GB]
        large = [f for f in needs_work if f['size_gb'] >= SMALL_THRESHOLD_GB]

        print(f"\nBy size category:")
        print(f"  Small (<{SMALL_THRESHOLD_GB}GB): {len(small)} files, {sum(f['size_gb'] for f in small):.1f} GB")
        print(f"  Large (>={SMALL_THRESHOLD_GB}GB): {len(large)} files, {sum(f['size_gb'] for f in large):.1f} GB")

        print(f"\nTop 10 largest files needing work:")
        for f in sorted(needs_work, key=lambda x: -x['size_gb'])[:10]:
            print(f"  {f['size_gb']:8.1f} GB  {os.path.basename(f['path'])} ({f['weeks_found']} weeks)")


def submit_small_files(files, max_jobs=500):
    """Submit small files via LSF using chunked converter."""
    os.makedirs(LOG_DIR, exist_ok=True)

    job_group = '/pvpipe_chunked'
    subprocess.run(['bgadd', '-L', '500', job_group], capture_output=True)

    submitted = 0
    for f in files[:max_jobs]:
        pb_path = f['path']
        pb_name = Path(pb_path).stem.replace(':', '_')
        job_name = f"ch_{pb_name}"[:100]
        log_file = f"{LOG_DIR}/{pb_name}.log"

        cmd = [
            'bsub',
            '-q', 'h',
            '-g', job_group,
            '-J', job_name,
            '-o', log_file,
            '-e', log_file,
            'python3', CONVERTER,
            '--pb', pb_path
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            submitted += 1
            if submitted <= 10:
                print(f"  Submitted: {os.path.basename(pb_path)} ({f['size_gb']:.1f} GB)")
        else:
            print(f"  FAILED: {os.path.basename(pb_path)}")

    if submitted > 10:
        print(f"  ... and {submitted - 10} more")

    print(f"\nSubmitted {submitted} small file jobs to LSF")
    print(f"Monitor with: bjobs -g {job_group}")
    return submitted


def process_large_files_on_login_node(files):
    """Process large files directly on login node (sequential)."""
    print(f"\nProcessing {len(files)} large files on login node...")
    print("This will run sequentially - can take many hours for huge files.")

    os.makedirs(LOG_DIR, exist_ok=True)
    main_log = f"{LOG_DIR}/large_files_main.log"

    completed = 0
    failed = 0

    for i, f in enumerate(files):
        pb_path = f['path']
        pb_name = Path(pb_path).stem.replace(':', '_')

        print(f"\n{'='*60}")
        print(f"[{i+1}/{len(files)}] Processing: {os.path.basename(pb_path)} ({f['size_gb']:.1f} GB)")
        print(f"{'='*60}")

        log_file = f"{LOG_DIR}/{pb_name}.log"

        cmd = ['python3', CONVERTER, '--pb', pb_path]

        with open(log_file, 'w') as log:
            result = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT)

        if result.returncode == 0:
            print(f"  SUCCESS: {os.path.basename(pb_path)}")
            completed += 1
        else:
            print(f"  FAILED: {os.path.basename(pb_path)} - see {log_file}")
            failed += 1

        # Log to main log
        with open(main_log, 'a') as log:
            status = "SUCCESS" if result.returncode == 0 else "FAILED"
            log.write(f"{datetime.now()} | {status} | {pb_path}\n")

    print(f"\nLarge files complete: {completed} succeeded, {failed} failed")
    return completed, failed


def main():
    parser = argparse.ArgumentParser(description='Submit all files for chunked conversion')
    parser.add_argument('--audit-only', action='store_true', help='Just show status')
    parser.add_argument('--small-only', action='store_true', help='Only submit small files via LSF')
    parser.add_argument('--large-only', action='store_true', help='Only process large files on login node')
    parser.add_argument('--max-jobs', type=int, default=500, help='Max LSF jobs to submit')

    args = parser.parse_args()

    print(f"Submit All Chunked - {datetime.now()}")
    print("=" * 60)

    needs_work, complete = audit_files()
    print_summary(needs_work, complete)

    if args.audit_only:
        return

    if not needs_work:
        print("\nAll files are complete!")
        return

    small_files = [f for f in needs_work if f['size_gb'] < SMALL_THRESHOLD_GB]
    large_files = [f for f in needs_work if f['size_gb'] >= SMALL_THRESHOLD_GB]

    if not args.large_only and small_files:
        print(f"\n{'='*60}")
        print("SUBMITTING SMALL FILES VIA LSF")
        print(f"{'='*60}")
        submit_small_files(small_files, args.max_jobs)

    if not args.small_only and large_files:
        print(f"\n{'='*60}")
        print("PROCESSING LARGE FILES ON LOGIN NODE")
        print(f"{'='*60}")
        process_large_files_on_login_node(large_files)

    print(f"\n{'='*60}")
    print("ALL SUBMISSIONS COMPLETE")
    print(f"{'='*60}")
    print(f"Monitor small files: bjobs -g /pvpipe_chunked")
    print(f"Logs: {LOG_DIR}/")


if __name__ == "__main__":
    main()
