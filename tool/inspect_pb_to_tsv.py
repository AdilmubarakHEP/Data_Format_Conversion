#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
07-inspect_pb_to_tsv.py

Inspect a .pb by streaming it through the Java decoder and emitting a small,
human-friendly TSV. Robust to:
  - numeric vs text payloads
  - scalar vs vector PVs
  - variable column counts (pads ragged rows)

Output columns:
  numeric scalar -> timestamp_s, value
  numeric vector -> timestamp_s, value_0, value_1, ...
  text scalar    -> timestamp_s, text
  text vector    -> timestamp_s, text_0, text_1, ...

Usage:
  python3 07-inspect_pb_to_tsv.py /path/to/MEAN:2024.pb
  # optional:
  python3 07-inspect_pb_to_tsv.py /path/to/file.pb --no-save
  python3 07-inspect_pb_to_tsv.py /path/to/file.pb --out_dir ./pb_inspection_output2
  python3 07-inspect_pb_to_tsv.py /path/to/file.pb --java_class_path /path/to/java --class_name pb2parquet
"""

import os
import sys
import argparse
import subprocess
from io import StringIO
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

# ====== CONFIG (defaults; can be overridden via CLI) ======
JAVA_CLASS_PATH_DEFAULT = "/home/belle2/amubarak/Data_Format_Conversion/java"
PB2TSV_CLASS_DEFAULT    = "pb2parquet"
OUTPUT_DIR_DEFAULT      = "./pb_inspection_output"

# ====== Decoder ======
def decode_pb_file(pb_path: str, java_class_path: str, class_name: str) -> List[str]:
    proc = subprocess.Popen(
        ["java", "-cp", java_class_path, class_name, pb_path],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1
    )
    out_lines = []
    try:
        for line in proc.stdout:  # stream to avoid holding stderr until the end
            out_lines.append(line.rstrip("\n"))
    finally:
        try:
            if proc.stdout:
                proc.stdout.close()
        except Exception:
            pass
        proc.wait()
        if proc.returncode != 0:
            err = ""
            try:
                if proc.stderr:
                    err = proc.stderr.read()
                    proc.stderr.close()
            except Exception:
                pass
            raise RuntimeError(f"pb2tsv failed for {pb_path}:\n{err}")

    return out_lines

# ====== Parsing helpers ======
def parse_header(lines: List[str]) -> Tuple[Optional[str], Optional[int], Optional[int], int]:
    """
    Returns (pv_name, t0_ms, nval_header, header_len)
    header_len is the number of lines consumed (to slice data region).
    """
    pv_name = None
    t0_ms = None
    nval_header = None
    header_len = 0
    for i, ln in enumerate(lines):
        if not ln.startswith("#"):
            header_len = i
            break
        s = ln.strip()
        if s.lower().startswith("# pv name"):
            # e.g. "# PV name   BM:BLM:BTCBT:ADC"
            pv_name = s.split(None, 3)[-1]
        elif s.lower().startswith("# t0"):
            # e.g. "# t0        1733184000000"
            try:
                t0_ms = int(s.split()[-1])
            except Exception:
                pass
        elif s.lower().startswith("# nval"):
            try:
                nval_header = int(s.split()[-1])
            except Exception:
                pass
    else:
        # All lines were headers (unlikely)
        header_len = len(lines)
    return pv_name, t0_ms, nval_header, header_len

def _try_numeric_row(ln: str) -> Optional[np.ndarray]:
    arr = np.fromstring(ln, sep=" ")
    # must at least have offset_ms
    if arr.size >= 1 and np.isfinite(arr[0]):
        return arr
    return None

def parse_body(lines: List[str], nval_header: Optional[int]):
    """
    Decide numeric/text mode by first data line that parses cleanly.
    Build rows as lists; also compute max column count.
    Returns: mode ("numeric"|"text"), rows (list), max_cols_seen (int)
    """
    mode = None
    rows_numeric = []
    rows_text = []
    max_cols_seen = 0

    for ln in lines:
        if not ln.strip():
            continue

        if mode in (None, "numeric"):
            arr = _try_numeric_row(ln)
            if arr is not None:
                if mode is None:
                    mode = "numeric"
                rows_numeric.append(arr)
                # payload columns = total - 1 (offset)
                vals_n = max(0, arr.size - 1)
                if vals_n > max_cols_seen:
                    max_cols_seen = int(vals_n)
                continue
            else:
                # switch to text (spill any collected numeric into text as strings)
                if mode == "numeric" and rows_numeric:
                    for a in rows_numeric:
                        toks = [str(x) for x in a[1:].tolist()]
                        rows_text.append([str(a[0])] + toks)
                    rows_numeric.clear()
                mode = "text"

        # text mode
        parts = ln.strip().split()
        if not parts:
            continue
        rows_text.append(parts)
        val_cnt = max(0, len(parts) - 1)
        if val_cnt > max_cols_seen:
            max_cols_seen = val_cnt

    # Normalize to a single representation: (offset_ms, [values...]) rows
    if mode == "text":
        rows = []
        for parts in rows_text:
            try:
                off_ms = float(parts[0])
            except Exception:
                # skip malformed line
                continue
            rows.append((off_ms, parts[1:]))
        return "text", rows, max_cols_seen

    # numeric
    rows = []
    for arr in rows_numeric:
        off_ms = float(arr[0])
        vals = arr[1:].tolist()
        rows.append((off_ms, vals))
    return "numeric", rows, max_cols_seen

def build_dataframe(mode: str, rows, nval_header: Optional[int]) -> Tuple[pd.DataFrame, int]:
    """
    Build a DataFrame with timestamp_s and value columns.
    Returns (df, ncols_payload).
    """
    # Decide number of value columns: header wins if present; else max seen
    max_seen = 0 if not rows else max(len(vs) for _, vs in rows)
    ncols = int(nval_header) if (nval_header and nval_header > 0) else int(max_seen)
    ncols = max(1, ncols)

    timestamps_s = [off_ms / 1000.0 for (off_ms, _) in rows]
    if mode == "numeric":
        # pad to ncols with NaN
        data_cols = []
        for j in range(ncols):
            col = []
            for _, vals in rows:
                if j < len(vals):
                    try:
                        col.append(float(vals[j]))
                    except Exception:
                        col.append(np.nan)
                else:
                    col.append(np.nan)
            data_cols.append(col)
        df = pd.DataFrame({"timestamp_s": timestamps_s})
        if ncols == 1:
            df["value"] = data_cols[0]
        else:
            for j, col in enumerate(data_cols):
                df[f"value_{j}"] = col
        return df, ncols

    # text mode: pad with ""
    data_cols = []
    for j in range(ncols):
        col = []
        for _, vals in rows:
            col.append(vals[j] if j < len(vals) else "")
        data_cols.append(col)
    df = pd.DataFrame({"timestamp_s": timestamps_s})
    if ncols == 1:
        df["text"] = data_cols[0]
    else:
        for j, col in enumerate(data_cols):
            df[f"text_{j}"] = col
    return df, ncols

# ====== Entry Point ======
def main():
    ap = argparse.ArgumentParser(description="Inspect a .pb file by decoding to a tidy TSV.")
    ap.add_argument("pb_path", help="Path to .pb file")
    ap.add_argument("--java_class_path", default=JAVA_CLASS_PATH_DEFAULT, help="Java classpath for the decoder")
    ap.add_argument("--class_name", default=PB2TSV_CLASS_DEFAULT, help="Java main class name")
    ap.add_argument("--out_dir", default=OUTPUT_DIR_DEFAULT, help="Directory to write the TSV")
    ap.add_argument("--no-save", action="store_true", help="Do not write TSV to disk (print summary only)")
    args = ap.parse_args()

    pb_path = args.pb_path
    if not os.path.isfile(pb_path):
        sys.stderr.write(f"ERROR: PB not found: {pb_path}\n")
        sys.exit(2)

    os.makedirs(args.out_dir, exist_ok=True)

    print(f"\n📦 Inspecting: {pb_path}")

    try:
        lines = decode_pb_file(pb_path, args.java_class_path, args.class_name)
    except Exception as e:
        print(f"❌ Failed to decode {pb_path}: {e}")
        sys.exit(1)

    pv_name, t0_ms, nval_header, header_len = parse_header(lines)
    if header_len >= len(lines):
        print("❌ No data lines after header; empty or malformed file.")
        sys.exit(1)

    mode, rows, max_cols_seen = parse_body(lines[header_len:], nval_header)
    if pv_name is None or t0_ms is None:
        print("⚠️  Missing PV name or t0 in header; proceeding with parsed data.")

    df, ncols = build_dataframe(mode, rows, nval_header)

    # ----- Summary -----
    shape = "scalar" if ncols == 1 else "vector"
    print(f"\n🔎 PV Name         : {pv_name or '(unknown)'}")
    print(f"🕒 Start Time t0   : {t0_ms}")
    print(f"🔢 Value type      : {mode}")
    print(f"📐 Shape           : {shape} (Nval={ncols}, max_seen={max_cols_seen})")
    print(f"🧮 Rows            : {len(df)}")
    print("\nHead:")
    print(df.head())

    # ----- Save -----
    if not args.no_save:
        # Make a readable filename: prefer PV name; fall back to pb base
        base = (pv_name or os.path.splitext(os.path.basename(pb_path))[0]).replace(":", "_")
        out_path = os.path.join(args.out_dir, base + ".tsv")
        df.to_csv(out_path, sep="\t", index=False)
        print(f"\n📁 TSV saved to: {out_path}")

if __name__ == "__main__":
    main()