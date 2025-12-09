#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
03a-pv-chunks.py

PV Chunk Merge Worker
Merges chunk parts (*_chunkNNNN.parquet) back into single PV parquet files.

Designed to be called:
- Via bsub with --pv_final_path for single PV (typical use)
- Directly for all PVs (testing/debugging only)

Uses per-PV locks to prevent concurrent merges of same PV.
NO THREADING - each bsub job processes one PV.

Config (configparser)
- [paths].output_base
- [merge].delete_chunks_after_pv_merge
- [merge].merge_sort_by_time
- Optional env: PVPIPE_CONFIG
"""

import os
import re
import sys
import fcntl
import hashlib
import argparse
import configparser
from collections import defaultdict
from typing import List, Dict, Optional

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# ---- config ----
CONFIG_PATH = os.environ.get(
    "PVPIPE_CONFIG",
    "/home/belle2/amubarak/Data_Format_Conversion/config.ini",
)
_cfg = configparser.ConfigParser()
if not _cfg.read(CONFIG_PATH):
    raise SystemExit(f"[FATAL] config.ini not found or unreadable at {CONFIG_PATH}")

def _cfg_bool(sec, key, default=False):
    try: return _cfg.getboolean(sec, key)
    except Exception: return bool(default)

def _cfg_str(sec, key, default=None):
    try:
        v = _cfg.get(sec, key)
        return v.strip() if v is not None else default
    except Exception:
        return default

OUTPUT_BASE = _cfg_str("paths", "output_base", "/home/belle2/amubarak/PV_Output/converted_flat2")
DELETE_CHUNKS_AFTER_PV = _cfg_bool("merge", "delete_chunks_after_pv_merge", False)
SORT_BY_TIME_DEFAULT   = _cfg_bool("merge", "merge_sort_by_time", True)

# ---------------- locks ----------------
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_LOCKS_ROOT = os.path.join(_SCRIPT_DIR, "locks")
_PV_LOCK_DIR = os.path.join(_LOCKS_ROOT, "pvchunks")
os.makedirs(_PV_LOCK_DIR, exist_ok=True)

def _pv_lock_path(final_path: str) -> str:
    h = hashlib.sha1(final_path.encode("utf-8")).hexdigest()
    return os.path.join(_PV_LOCK_DIR, f"pv-{h}.lock")

def _acquire_pv_lock(final_path: str):
    lock_path = _pv_lock_path(final_path)
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return fd
    except BlockingIOError:
        try: os.close(fd)
        except Exception: pass
        return None

def _release_pv_lock(fd: Optional[int]):
    if fd is None:
        return
    try: fcntl.flock(fd, fcntl.LOCK_UN)
    except Exception: pass
    try: os.close(fd)
    except Exception: pass

def _unlink_lock_file(final_path: str):
    try:
        lp = _pv_lock_path(final_path)
        if os.path.exists(lp):
            os.unlink(lp)
    except Exception:
        pass

def _rmdir_if_empty(path: str):
    try:
        if os.path.isdir(path) and not os.listdir(path):
            os.rmdir(path)
    except Exception:
        pass

def _cleanup_lock_dirs():
    _rmdir_if_empty(_PV_LOCK_DIR)
    _rmdir_if_empty(_LOCKS_ROOT)

# ---------------- utils ----------------
CHUNK_RE = re.compile(r"^(?P<base>.+)_chunk(?P<idx>\d{4})\.parquet$")

def _ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def is_chunk_file(filename: str) -> bool:
    return bool(CHUNK_RE.match(filename))

def chunk_base_and_index(filename: str):
    m = CHUNK_RE.match(filename)
    if not m:
        return None, None
    return m.group("base"), int(m.group("idx"))

def format_size(bytes_val: int) -> str:
    mb = bytes_val / 1e6
    return f"{mb:.2f} MB" if mb < 1000 else f"{mb/1000:.2f} GB"

def read_single_row(table: pa.Table) -> Dict:
    """
    Read single-row table but keep large array columns as PyArrow scalars
    to avoid memory explosion from nested list conversion.
    """
    if table.num_rows != 1:
        raise ValueError(f"Expected single-row table, got {table.num_rows} rows")
    
    # Metadata fields that are safe/needed as Python objects (small)
    metadata_fields = {"pv", "date", "t0", "Nval", "value_type", "length", "component_names"}
    
    rec = {}
    for col in table.column_names:
        scalar = table[col][0]
        if col in metadata_fields:
            rec[col] = scalar.as_py()
        else:
            # Keep as PyArrow scalar - extract on-demand to allow GC
            rec[col] = scalar
    return rec

def _append_numeric_matrix_to_columns(mat_rows: List[List[float]], cols: List[List[float]], nval: int):
    for row in mat_rows:
        if len(row) < nval:
            row = row + [float("nan")] * (nval - len(row))
        for j in range(nval):
            cols[j].append(float(row[j]))

def _append_text_matrix_to_columns(mat_rows: List[List[str]], cols: List[List[str]], nval: int):
    for row in mat_rows:
        if len(row) < nval:
            row = row + [""] * (nval - len(row))
        for j in range(nval):
            cols[j].append(row[j])

def _ensure_comp_names(pv: str, nval: int, comp_names: Optional[List[str]]) -> List[str]:
    if comp_names and len(comp_names) == nval:
        return comp_names
    return [f"{pv}[{i}]" for i in range(nval)]

def _is_monotonic(t: List[float]) -> bool:
    for i in range(len(t) - 1):
        if t[i] > t[i + 1]:
            return False
    return True

def _reorder_by_time(times: List[float], payload_mode: str, nval: int,
                     vals_num=None, comp_cols_num=None,
                     vals_text=None, comp_cols_text=None):
    if len(times) <= 1 or _is_monotonic(times):
        return times, (vals_num or comp_cols_num or vals_text or comp_cols_text)
    order = np.argsort(np.asarray(times, dtype=float))
    times_sorted = [times[i] for i in order]
    if payload_mode == "num_scalar":
        vals_num_sorted = [vals_num[i] for i in order]
        return times_sorted, vals_num_sorted
    elif payload_mode == "num_vector":
        comp_sorted = []
        for j in range(nval):
            col = comp_cols_num[j]
            comp_sorted.append([col[i] for i in order])
        return times_sorted, comp_sorted
    elif payload_mode == "text_scalar":
        vals_text_sorted = [vals_text[i] for i in order]
        return times_sorted, vals_text_sorted
    else:
        comp_sorted = []
        for j in range(nval):
            col = comp_cols_text[j]
            comp_sorted.append([col[i] for i in order])
        return times_sorted, comp_sorted

# ---------------- PV chunk scan and merge ----------------
def find_chunk_groups(input_dir: str, pv_final_path: Optional[str] = None) -> Dict[str, List[str]]:
    """
    Find all chunk groups in input_dir.
    If pv_final_path is specified, only return that specific PV's chunks.
    """
    groups = defaultdict(list)
    for root, _, files in os.walk(input_dir):
        for fn in files:
            if not fn.endswith(".parquet"):
                continue
            if not is_chunk_file(fn):
                continue
            base, idx = chunk_base_and_index(fn)
            if base is None:
                continue
            final_path = os.path.join(root, f"{base}.parquet")
            
            # Filter to specific PV if requested
            if pv_final_path and final_path != pv_final_path:
                continue
                
            chunk_path = os.path.join(root, fn)
            groups[final_path].append((idx, chunk_path))
    
    sorted_groups = {}
    for final_path, items in groups.items():
        items.sort(key=lambda t: t[0])
        sorted_groups[final_path] = [p for _, p in items]
    return sorted_groups

def merge_one_pv_chunkset(chunk_paths: List[str],
                          out_final_path: str,
                          delete_chunks: bool=False,
                          sort_by_time: bool=True) -> bool:
    if not chunk_paths:
        return False

    first = pq.read_table(chunk_paths[0])
    first_rec = read_single_row(first)
    for k in ("pv", "date", "t0", "Nval", "times", "length"):
        if k not in first_rec:
            raise ValueError(f"Chunk missing required field '{k}': {chunk_paths[0]}")

    pv   = first_rec["pv"]
    date = first_rec["date"]
    t0   = int(first_rec["t0"])
    nval = int(first_rec["Nval"])
    value_type = first_rec.get("value_type", "numeric")

    is_vector = nval > 1 or \
                ("values_matrix" in first_rec) or \
                ("values_by_component" in first_rec) or \
                ("values_text_matrix" in first_rec) or \
                ("values_text_by_component" in first_rec)

    times_all: List[float] = []
    length_sum = 0
    comp_names_ref = first_rec.get("component_names", None)

    if value_type == "text":
        if is_vector:
            comp_cols_text: List[List[str]] = [[] for _ in range(max(1, nval))]
            payload_mode = "text_vector"
        else:
            vals_text: List[str] = []
            payload_mode = "text_scalar"
    else:
        if is_vector:
            comp_cols_num: List[List[float]] = [[] for _ in range(max(1, nval))]
            payload_mode = "num_vector"
        else:
            vals_num: List[float] = []
            payload_mode = "num_scalar"

    for p in chunk_paths:
        t = pq.read_table(p)
        r = read_single_row(t)

        if r.get("pv") != pv or r.get("date") != date or int(r.get("Nval", nval)) != nval or int(r.get("t0")) != t0:
            raise ValueError(f"Inconsistent header across chunks for {out_final_path} (file: {p})")

        # Extract times - convert PyArrow list scalar to Python list only for this field
        times_scalar = r["times"]
        times = times_scalar.as_py() if hasattr(times_scalar, 'as_py') else times_scalar or []
        times_all.extend(times)
        length_sum += int(r.get("length", len(times)))

        if payload_mode == "num_scalar":
            if "values" not in r:
                raise ValueError(f"Numeric scalar chunk missing 'values': {p}")
            values_scalar = r["values"]
            values_py = values_scalar.as_py() if hasattr(values_scalar, 'as_py') else values_scalar or []
            vals_num.extend([float(x) for x in values_py])

        elif payload_mode == "num_vector":
            if "values_by_component" in r:
                vbc_scalar = r["values_by_component"]
                vbc = vbc_scalar.as_py() if hasattr(vbc_scalar, 'as_py') else vbc_scalar or [[] for _ in range(nval)]
                if len(vbc) < nval:
                    vbc = vbc + [[] for _ in range(nval - len(vbc))]
                for j in range(nval):
                    comp_cols_num[j].extend([float(x) for x in vbc[j]])
            elif "values_matrix" in r:
                vm_scalar = r["values_matrix"]
                vm = vm_scalar.as_py() if hasattr(vm_scalar, 'as_py') else vm_scalar or []
                _append_numeric_matrix_to_columns(vm, comp_cols_num, nval)
            else:
                raise ValueError(f"Numeric vector chunk missing values_by_component/values_matrix: {p}")
            c = r.get("component_names")
            if comp_names_ref and c and c != comp_names_ref:
                comp_names_ref = None

        elif payload_mode == "text_scalar":
            if "values_text" not in r:
                raise ValueError(f"Text scalar chunk missing 'values_text': {p}")
            vt_scalar = r["values_text"]
            vt = vt_scalar.as_py() if hasattr(vt_scalar, 'as_py') else vt_scalar or []
            vals_text.extend([str(x) for x in vt])

        else:
            if "values_text_by_component" in r:
                vtbc_scalar = r["values_text_by_component"]
                vtbc = vtbc_scalar.as_py() if hasattr(vtbc_scalar, 'as_py') else vtbc_scalar or [[] for _ in range(nval)]
                if len(vtbc) < nval:
                    vtbc = vtbc + [[] for _ in range(nval - len(vtbc))]
                for j in range(nval):
                    comp_cols_text[j].extend([str(x) for x in vtbc[j]])
            elif "values_text_matrix" in r:
                vtm_scalar = r["values_text_matrix"]
                vtm = vtm_scalar.as_py() if hasattr(vtm_scalar, 'as_py') else vtm_scalar or []
                _append_text_matrix_to_columns(vtm, comp_cols_text, nval)
            else:
                raise ValueError(f"Text vector chunk missing values_text_by_component/values_text_matrix: {p}")
            c = r.get("component_names")
            if comp_names_ref and c and c != comp_names_ref:
                comp_names_ref = None

    comp_names = _ensure_comp_names(pv, nval, comp_names_ref) if is_vector else None

    if sort_by_time and length_sum > 1:
        if payload_mode == "num_scalar":
            times_all, vals_num = _reorder_by_time(times_all, payload_mode, nval, vals_num=vals_num)
        elif payload_mode == "num_vector":
            times_all, comp_cols_num = _reorder_by_time(times_all, payload_mode, nval, comp_cols_num=comp_cols_num)
        elif payload_mode == "text_scalar":
            times_all, vals_text = _reorder_by_time(times_all, payload_mode, nval, vals_text=vals_text)
        else:
            times_all, comp_cols_text = _reorder_by_time(times_all, payload_mode, nval, comp_cols_text=comp_cols_text)

    arrays = [
        pa.array([pv]),
        pa.array([date]),
        pa.array([t0],   type=pa.int64()),
        pa.array([nval], type=pa.int64()),
        pa.array([times_all], type=pa.list_(pa.float64())),
        pa.array([length_sum], type=pa.int64()),
        pa.array([value_type]),
    ]
    names = ["pv", "date", "t0", "Nval", "times", "length", "value_type"]

    if value_type == "text":
        if is_vector:
            arrays.append(pa.array([comp_cols_text], type=pa.list_(pa.list_(pa.string()))))
            names.append("values_text_by_component")
            arrays.append(pa.array([comp_names], type=pa.list_(pa.string())))
            names.append("component_names")
        else:
            arrays.append(pa.array([vals_text], type=pa.list_(pa.string())))
            names.append("values_text")
    else:
        if is_vector:
            arrays.append(pa.array([comp_cols_num], type=pa.list_(pa.list_(pa.float64()))))
            names.append("values_by_component")
            arrays.append(pa.array([comp_names], type=pa.list_(pa.string())))
            names.append("component_names")
        else:
            arrays.append(pa.array([vals_num], type=pa.list_(pa.float64())))
            names.append("values")

    final_tbl = pa.Table.from_arrays(arrays, names=names)
    _ensure_dir(os.path.dirname(out_final_path))
    pq.write_table(final_tbl, out_final_path, compression="snappy")
    out_sz = os.path.getsize(out_final_path) if os.path.exists(out_final_path) else 0
    print(f"✅ Merged {len(chunk_paths)} chunks -> {out_final_path} (rows={length_sum}, size≈{format_size(out_sz)})")

    if delete_chunks:
        for p in chunk_paths:
            try:
                os.remove(p)
            except FileNotFoundError:
                pass
            except Exception as e:
                print(f"Could not delete chunk {p}: {e}")

    return True

def merge_all_pv_chunks(input_dir: str, force: bool, delete_chunks: bool,
                        sort_by_time: bool, pv_final_path: Optional[str] = None):
    """
    Merge PV chunks. No threading - processes sequentially.
    If pv_final_path is specified, only process that one PV.
    """
    groups = find_chunk_groups(input_dir, pv_final_path=pv_final_path)
    if not groups:
        print("No PV chunk sets found.")
        _cleanup_lock_dirs()
        return 0, 0

    merged = 0
    skipped = 0
    busy = 0

    for final_path, chunk_paths in sorted(groups.items()):
        if os.path.exists(final_path) and not force:
            print(f"Skip, final exists: {final_path}")
            skipped += 1
            continue

        fd = _acquire_pv_lock(final_path)
        if fd is None:
            print(f"Skip, lock held: {final_path}")
            busy += 1
            continue

        try:
            ok = merge_one_pv_chunkset(chunk_paths, final_path,
                                       delete_chunks=delete_chunks,
                                       sort_by_time=sort_by_time)
            if ok:
                merged += 1
            else:
                print(f"Failed to merge chunks for {final_path}")
        finally:
            _release_pv_lock(fd)
            _unlink_lock_file(final_path)

    print(f"\nPV-chunk merge summary: merged={merged}, skipped={skipped}, busy={busy}")
    _cleanup_lock_dirs()
    return merged, skipped

def main():
    ap = argparse.ArgumentParser(description="Merge PV chunk parts into final PV Parquets.")
    ap.add_argument("--input_dir", default=OUTPUT_BASE, help="Where PV Parquets and chunks live")
    ap.add_argument("--pv_final_path", help="Process only this specific PV final parquet path")
    ap.add_argument("--force", action="store_true", help="Re-merge even if final exists")
    ap.add_argument("--delete_chunks", action="store_true", default=DELETE_CHUNKS_AFTER_PV,
                    help="Delete chunk parts after PV merge")
    ap.add_argument("--sort_by_time", action="store_true", default=SORT_BY_TIME_DEFAULT,
                    help="Sort rows by time inside each merged PV")
    args = ap.parse_args()

    merge_all_pv_chunks(args.input_dir, force=args.force,
                        delete_chunks=args.delete_chunks,
                        sort_by_time=bool(args.sort_by_time),
                        pv_final_path=args.pv_final_path)

if __name__ == "__main__":
    main()