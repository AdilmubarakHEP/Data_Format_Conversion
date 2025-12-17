#!/usr/bin/env python3
"""
Chunked PB to Parquet Converter
===============================

Splits large .pb files into chunks before processing to stay within memory limits.
This works because .pb files are line-delimited (one sample per line).

Strategy:
1. Read first line (header) from .pb file
2. Split remaining lines into chunks
3. Process each chunk with Java (header + data lines)
4. Accumulate data and write final weekly parquet files

Usage:
    python3 chunked_pb_converter.py --pb /path/to/huge_file.pb
    python3 chunked_pb_converter.py --pb /path/to/huge_file.pb --lines-per-chunk 300000
"""

import os
import sys
import argparse
import subprocess
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq

# Configuration
PB_DIR = "/gpfs/group/belle2/group/accelerator/pv/2024/AA/"
PARQUET_DIR = "/gpfs/group/belle2/group/accelerator/pv/2024/AA_parquet/converted_flat2"
JAVA_CLASSPATH = "/home/belle2/amubarak/Data_Format_Conversion/java"

# Chunk settings - tuned for 4GB memory limit on compute nodes
# For login node, we can use larger chunks
DEFAULT_LINES_PER_CHUNK = 300000

# Fixed anchor: 2024-01-01 00:00:00 JST
T2024_MS = 1704034800 * 1000
WEEK_MS = 7 * 24 * 60 * 60 * 1000


def get_available_memory_gb():
    """Get available memory in GB."""
    try:
        with open('/proc/meminfo') as f:
            for line in f:
                if line.startswith('MemAvailable:'):
                    return int(line.split()[1]) / (1024 * 1024)
    except:
        pass
    return 4  # Default to 4GB


def get_week_label(ts_offset_ms: int) -> str:
    """Get week label for a timestamp offset from 2024-01-01."""
    if ts_offset_ms < 0:
        return "2023-W52"
    week_num = (ts_offset_ms // WEEK_MS) + 1
    if week_num > 53:
        return f"2025-W{week_num - 53:02d}"
    return f"2024-W{week_num:02d}"


def split_and_process_pb_file(pb_path: str, lines_per_chunk: int, java_heap: str):
    """Split .pb file into chunks and process each with Java.

    Yields (tsv_output, chunk_num) for each chunk.
    """
    print(f"Processing {pb_path} in chunks of {lines_per_chunk} lines...")

    with open(pb_path, 'rb') as f:
        # Read header line (first line)
        header = f.readline()

        chunk_num = 0
        lines_buffer = []

        for line in f:
            lines_buffer.append(line)

            if len(lines_buffer) >= lines_per_chunk:
                # Create temp chunk file
                with tempfile.NamedTemporaryFile(mode='wb', suffix='.pb', delete=False) as tmp:
                    tmp.write(header)
                    tmp.writelines(lines_buffer)
                    tmp_path = tmp.name

                # Process chunk with Java
                print(f"  Processing chunk {chunk_num}: {len(lines_buffer)} lines...")
                cmd = ["java", f"-Xmx{java_heap}", "-cp", JAVA_CLASSPATH, "pb2parquet", tmp_path]
                result = subprocess.run(cmd, capture_output=True, text=True)

                # Clean up temp file
                os.unlink(tmp_path)

                if result.returncode == 0:
                    yield result.stdout, chunk_num
                else:
                    print(f"  Warning: Java error on chunk {chunk_num}: {result.stderr[:200]}")

                lines_buffer = []
                chunk_num += 1

        # Process remaining lines
        if lines_buffer:
            with tempfile.NamedTemporaryFile(mode='wb', suffix='.pb', delete=False) as tmp:
                tmp.write(header)
                tmp.writelines(lines_buffer)
                tmp_path = tmp.name

            print(f"  Processing final chunk {chunk_num}: {len(lines_buffer)} lines...")
            cmd = ["java", f"-Xmx{java_heap}", "-cp", JAVA_CLASSPATH, "pb2parquet", tmp_path]
            result = subprocess.run(cmd, capture_output=True, text=True)
            os.unlink(tmp_path)

            if result.returncode == 0:
                yield result.stdout, chunk_num


def parse_tsv_to_records(tsv_output: str):
    """Parse Java TSV output and return records with metadata.

    Returns: (records_by_week, pv_name, is_vector)
    """
    weekly_data = defaultdict(list)
    pv_name = None
    nval = 1
    is_vector = False

    for line in tsv_output.split('\n'):
        line = line.strip()
        if not line:
            continue

        # Parse header comments
        if line.startswith('#'):
            if '# PV name' in line:
                pv_name = line.split()[-1]
            elif '# Nval' in line:
                nval = int(line.split()[-1])
                is_vector = nval > 1
            continue

        # Parse data line: timestamp_offset value1 [value2 ...]
        parts = line.split()
        if len(parts) < 2:
            continue

        try:
            ts_offset = int(parts[0])
            week = get_week_label(ts_offset)

            if is_vector:
                values = []
                for v in parts[1:]:
                    v = v.strip('"')
                    try:
                        values.append(float(v))
                    except ValueError:
                        values.append(v)
                weekly_data[week].append((ts_offset, values))
            else:
                val = parts[1].strip('"')
                try:
                    val = float(val)
                except ValueError:
                    pass
                weekly_data[week].append((ts_offset, val))
        except (ValueError, IndexError):
            continue

    return weekly_data, pv_name, is_vector


def write_weekly_parquet(all_weekly_data: dict, pv_name: str, is_vector: bool, output_dir: Path):
    """Write final weekly parquet files."""
    output_dir.mkdir(parents=True, exist_ok=True)

    files_written = 0
    total_rows = 0

    for week, records in sorted(all_weekly_data.items()):
        if not records:
            continue

        # Sort by timestamp
        records.sort(key=lambda x: x[0])

        timestamps = [r[0] for r in records]

        if is_vector:
            first_val = records[0][1][0] if records[0][1] else 0.0
            if isinstance(first_val, str):
                values = [r[1] for r in records]
                table = pa.table({
                    'time_offset_ms': pa.array(timestamps, type=pa.int64()),
                    'values': pa.array(values, type=pa.list_(pa.string()))
                })
            else:
                values = [r[1] for r in records]
                table = pa.table({
                    'time_offset_ms': pa.array(timestamps, type=pa.int64()),
                    'values': pa.array(values, type=pa.list_(pa.float64()))
                })
        else:
            first_val = records[0][1] if records else 0.0
            if isinstance(first_val, str):
                values = [r[1] for r in records]
                table = pa.table({
                    'time_offset_ms': pa.array(timestamps, type=pa.int64()),
                    'value': pa.array(values, type=pa.string())
                })
            else:
                values = [r[1] for r in records]
                table = pa.table({
                    'time_offset_ms': pa.array(timestamps, type=pa.int64()),
                    'value': pa.array(values, type=pa.float64())
                })

        out_path = output_dir / f"{pv_name}-{week}.parquet"
        pq.write_table(table, out_path, compression='zstd')
        files_written += 1
        total_rows += len(records)

    return files_written, total_rows


def process_pb_file(pb_path: str, lines_per_chunk: int = None):
    """Main entry point: process a .pb file using chunked approach."""
    pb_path = os.path.abspath(pb_path)
    pb_name = Path(pb_path).stem
    pv_name = pb_name.split(':')[0] if ':' in pb_name else pb_name

    # Auto-detect memory and set parameters
    mem_gb = get_available_memory_gb()
    if mem_gb > 100:  # Login node
        java_heap = "16g"
        if lines_per_chunk is None:
            lines_per_chunk = 2000000  # 2M lines per chunk
    else:  # Compute node
        java_heap = "2g"
        if lines_per_chunk is None:
            lines_per_chunk = 300000  # 300K lines per chunk

    # Determine output directory
    try:
        rel_path = Path(pb_path).relative_to(PB_DIR)
        output_dir = Path(PARQUET_DIR) / rel_path.parent
    except ValueError:
        output_dir = Path(PARQUET_DIR) / "other"

    file_size_gb = os.path.getsize(pb_path) / (1024**3)

    print("=" * 60)
    print("Chunked PB Converter")
    print("=" * 60)
    print(f"Input: {pb_path}")
    print(f"Size: {file_size_gb:.2f} GB")
    print(f"Output: {output_dir}")
    print(f"Memory available: {mem_gb:.0f} GB")
    print(f"Java heap: {java_heap}")
    print(f"Lines per chunk: {lines_per_chunk}")
    print(f"Started: {datetime.now()}")
    print()

    # Process chunks and accumulate data
    all_weekly_data = defaultdict(list)
    detected_pv_name = None
    is_vector = False
    total_chunks = 0

    for tsv_output, chunk_num in split_and_process_pb_file(pb_path, lines_per_chunk, java_heap):
        weekly_data, pv, vec = parse_tsv_to_records(tsv_output)
        if pv:
            detected_pv_name = pv
        is_vector = vec

        for week, records in weekly_data.items():
            all_weekly_data[week].extend(records)

        lines_processed = sum(len(v) for v in weekly_data.values())
        print(f"    -> {lines_processed} records across {len(weekly_data)} weeks")
        total_chunks += 1

    # Write final parquet files
    print(f"\nWriting {len(all_weekly_data)} weekly parquet files...")
    final_pv_name = detected_pv_name or pv_name
    files_written, total_rows = write_weekly_parquet(
        all_weekly_data, final_pv_name, is_vector, output_dir
    )

    print()
    print("=" * 60)
    print(f"COMPLETE: {final_pv_name}")
    print("=" * 60)
    print(f"Chunks processed: {total_chunks}")
    print(f"Files written: {files_written}")
    print(f"Total rows: {total_rows:,}")
    print(f"Output: {output_dir}")
    print(f"Finished: {datetime.now()}")


def main():
    parser = argparse.ArgumentParser(description='Chunked PB to Parquet converter')
    parser.add_argument('--pb', required=True, help='Path to .pb file')
    parser.add_argument('--lines-per-chunk', type=int, default=None,
                        help='Lines per chunk (auto-detected based on memory)')

    args = parser.parse_args()

    if not os.path.exists(args.pb):
        print(f"Error: File not found: {args.pb}", file=sys.stderr)
        sys.exit(1)

    process_pb_file(args.pb, args.lines_per_chunk)


if __name__ == "__main__":
    main()
