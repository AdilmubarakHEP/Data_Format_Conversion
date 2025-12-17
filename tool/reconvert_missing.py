#!/usr/bin/env python3
"""
Reconvert Missing/Failed .pb Files
===================================

This tool audits the conversion status and reconverts any .pb files that:
- Were never converted
- Failed during conversion
- Have incomplete weekly parquet files

Usage:
    python3 reconvert_missing.py --audit           # Show status only
    python3 reconvert_missing.py --submit          # Submit jobs for missing files
    python3 reconvert_missing.py --submit --small-only  # Only files < 5GB
    python3 reconvert_missing.py --submit --large-only  # Only files >= 5GB
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
LOG_DIR = "/home/belle2/amubarak/Data_Format_Conversion/logs/reconvert"
AUDIT_FILE = "/home/belle2/amubarak/fast_audit_results.json"
JAVA_CLASSPATH = "/home/belle2/amubarak/Data_Format_Conversion/java"

# For small files, use existing conversion scripts
SMALL_SCRIPT = "/home/belle2/amubarak/Data_Format_Conversion/01-small_script.py"
# For large files, use chunked approach
BIG_CHUNKER = "/home/belle2/amubarak/Data_Format_Conversion/02-big_chunker_script.py"

# Thresholds
SMALL_THRESHOLD_GB = 5.0
MIN_WEEKS_COMPLETE = 45  # Consider file complete if 45+ weekly files exist


def get_week_label(ts_offset_ms: int) -> str:
    """Get week label for a timestamp offset."""
    WEEK_MS = 7 * 24 * 60 * 60 * 1000
    if ts_offset_ms < 0:
        return "2023-W52"
    week_num = (ts_offset_ms // WEEK_MS) + 1
    if week_num > 53:
        return f"2025-W{week_num - 53:02d}"
    return f"2024-W{week_num:02d}"


def get_output_dir(pb_path: str) -> Path:
    """Get output directory for a .pb file."""
    try:
        rel_path = Path(pb_path).relative_to(PB_DIR)
        return Path(PARQUET_DIR) / rel_path.parent
    except ValueError:
        return Path(PARQUET_DIR) / "other"


def audit_file(pb_path: str) -> dict:
    """Audit a single .pb file's conversion status."""
    result = {
        'path': pb_path,
        'exists': os.path.exists(pb_path),
        'size_gb': 0,
        'status': 'unknown',
        'weeks_found': 0,
        'parquet_files': []
    }

    if not result['exists']:
        result['status'] = 'missing_pb'
        return result

    result['size_gb'] = os.path.getsize(pb_path) / (1024**3)

    pb_name = Path(pb_path).stem
    pv_name = pb_name.split(':')[0] if ':' in pb_name else pb_name
    out_dir = get_output_dir(pb_path)

    # Find parquet files
    if out_dir.exists():
        parquet_files = list(out_dir.glob(f'{pv_name}-2024-W*.parquet'))
        result['parquet_files'] = [str(f) for f in parquet_files]
        result['weeks_found'] = len(parquet_files)

    # Determine status
    if result['weeks_found'] >= MIN_WEEKS_COMPLETE:
        result['status'] = 'complete'
    elif result['weeks_found'] > 0:
        result['status'] = 'partial'
    else:
        result['status'] = 'not_converted'

    return result


def audit_all() -> dict:
    """Audit all .pb files that should be converted."""
    print("Loading audit data...")

    if not os.path.exists(AUDIT_FILE):
        print(f"ERROR: Audit file not found: {AUDIT_FILE}")
        print("Run: python3 /home/belle2/amubarak/fast_audit.py")
        sys.exit(1)

    with open(AUDIT_FILE) as f:
        audit_data = json.load(f)

    all_files = list(set(
        audit_data.get('files_not_in_db', []) +
        audit_data.get('queued_files', [])
    ))

    print(f"Auditing {len(all_files)} files...")

    results = {
        'complete': [],
        'partial': [],
        'not_converted': [],
        'missing_pb': []
    }

    for i, pb_path in enumerate(all_files):
        if (i + 1) % 100 == 0:
            print(f"  Progress: {i+1}/{len(all_files)}")

        info = audit_file(pb_path)
        results[info['status']].append(info)

    return results


def print_audit_summary(results: dict):
    """Print audit summary."""
    print("\n" + "="*60)
    print("CONVERSION STATUS AUDIT")
    print("="*60)

    for status in ['complete', 'partial', 'not_converted', 'missing_pb']:
        files = results[status]
        total_size = sum(f['size_gb'] for f in files)
        print(f"\n{status.upper()}: {len(files)} files ({total_size:.1f} GB)")

        if status in ['partial', 'not_converted'] and files:
            print("  Largest files:")
            for f in sorted(files, key=lambda x: -x['size_gb'])[:10]:
                weeks_info = f" ({f['weeks_found']} weeks)" if f['weeks_found'] > 0 else ""
                print(f"    {f['size_gb']:7.1f} GB  {os.path.basename(f['path'])}{weeks_info}")

    # Summary of work needed
    needs_work = results['partial'] + results['not_converted']
    total_size = sum(f['size_gb'] for f in needs_work)
    small_files = [f for f in needs_work if f['size_gb'] < SMALL_THRESHOLD_GB]
    large_files = [f for f in needs_work if f['size_gb'] >= SMALL_THRESHOLD_GB]

    print("\n" + "="*60)
    print("WORK NEEDED")
    print("="*60)
    print(f"Total files needing conversion: {len(needs_work)}")
    print(f"Total size: {total_size:.1f} GB")
    print(f"  Small files (<{SMALL_THRESHOLD_GB}GB): {len(small_files)} ({sum(f['size_gb'] for f in small_files):.1f} GB)")
    print(f"  Large files (>={SMALL_THRESHOLD_GB}GB): {len(large_files)} ({sum(f['size_gb'] for f in large_files):.1f} GB)")


def clean_partial_outputs(pb_path: str):
    """Remove partial parquet files for a .pb file."""
    pb_name = Path(pb_path).stem
    pv_name = pb_name.split(':')[0] if ':' in pb_name else pb_name
    out_dir = get_output_dir(pb_path)

    if out_dir.exists():
        for pf in out_dir.glob(f'{pv_name}-2024-W*.parquet'):
            os.remove(pf)
            print(f"  Removed: {pf.name}")


def submit_small_file(pb_path: str, job_group: str) -> bool:
    """Submit job for a small file using the singlepass converter."""
    pb_name = Path(pb_path).stem.replace(':', '_')
    job_name = f"rc_sm_{pb_name}"[:100]
    log_file = f"{LOG_DIR}/{pb_name}.log"

    # Use singlepass converter for small files - it works well under 4GB limit
    cmd = [
        'bsub',
        '-q', 'h',
        '-g', job_group,
        '-J', job_name,
        '-o', log_file,
        '-e', log_file,
        'python3', '/home/belle2/amubarak/singlepass_converter.py',
        '--pb', pb_path
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode == 0


def submit_large_file(pb_path: str, job_group: str) -> bool:
    """Submit weekly jobs for a large file using weekly_converter.py."""
    # Use the new weekly converter which processes one week at a time
    WEEKLY_CONVERTER = "/home/belle2/amubarak/Data_Format_Conversion/tool/weekly_converter.py"

    cmd = [
        'python3', WEEKLY_CONVERTER,
        '--pb', pb_path,
        '--submit-all'
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode == 0


def submit_jobs(results: dict, small_only: bool = False, large_only: bool = False,
                max_jobs: int = 500, clean_partial: bool = True):
    """Submit jobs for files needing conversion."""

    needs_work = results['partial'] + results['not_converted']

    if small_only:
        needs_work = [f for f in needs_work if f['size_gb'] < SMALL_THRESHOLD_GB]
    elif large_only:
        needs_work = [f for f in needs_work if f['size_gb'] >= SMALL_THRESHOLD_GB]

    if not needs_work:
        print("No files need conversion!")
        return

    # Sort by size (smaller first for faster initial results)
    needs_work.sort(key=lambda x: x['size_gb'])

    # Limit jobs
    to_submit = needs_work[:max_jobs]

    print(f"\nSubmitting {len(to_submit)} jobs...")

    # Create log directory and job group
    os.makedirs(LOG_DIR, exist_ok=True)
    job_group = '/pvpipe_reconvert'
    subprocess.run(['bgadd', '-L', '200', job_group], capture_output=True)

    submitted = 0
    failed = 0

    for f in to_submit:
        pb_path = f['path']

        # Clean partial outputs if requested
        if clean_partial and f['status'] == 'partial':
            clean_partial_outputs(pb_path)

        # Submit based on size
        if f['size_gb'] < SMALL_THRESHOLD_GB:
            success = submit_small_file(pb_path, job_group)
        else:
            success = submit_large_file(pb_path, job_group)

        if success:
            submitted += 1
            if submitted <= 20:
                print(f"  Submitted: {os.path.basename(pb_path)} ({f['size_gb']:.1f} GB)")
        else:
            failed += 1
            print(f"  FAILED: {os.path.basename(pb_path)}")

    if submitted > 20:
        print(f"  ... and {submitted - 20} more")

    print(f"\n{'='*60}")
    print(f"SUBMISSION COMPLETE")
    print(f"{'='*60}")
    print(f"Submitted: {submitted}")
    print(f"Failed: {failed}")
    print(f"\nMonitor with:")
    print(f"  bjobs -g {job_group}")
    print(f"  watch -n 60 'bjobs -g {job_group} | wc -l'")
    print(f"\nLogs in: {LOG_DIR}/")


def main():
    parser = argparse.ArgumentParser(description='Reconvert missing/failed .pb files')
    parser.add_argument('--audit', action='store_true', help='Show audit status only')
    parser.add_argument('--submit', action='store_true', help='Submit jobs for missing files')
    parser.add_argument('--small-only', action='store_true', help='Only process small files (<5GB)')
    parser.add_argument('--large-only', action='store_true', help='Only process large files (>=5GB)')
    parser.add_argument('--max-jobs', type=int, default=500, help='Maximum jobs to submit')
    parser.add_argument('--no-clean', action='store_true', help='Do not clean partial outputs')

    args = parser.parse_args()

    if not args.audit and not args.submit:
        args.audit = True  # Default to audit

    print(f"Reconvert Missing Tool - {datetime.now()}")
    print("="*60)

    results = audit_all()
    print_audit_summary(results)

    if args.submit:
        submit_jobs(
            results,
            small_only=args.small_only,
            large_only=args.large_only,
            max_jobs=args.max_jobs,
            clean_partial=not args.no_clean
        )


if __name__ == "__main__":
    main()
