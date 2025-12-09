#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
03c-unsplit.py

Task
----
UNSPLIT per-day/week/month PV Parquets back into a single PV-wide Parquet.

Config (from conf/pvpipe_config.py)
-----------------------------------
- output_base                  -> default input directory
- merge_output_dir (or submit_date_view_root) -> default output directory
"""

import os
import re
import sys
import argparse
from collections import defaultdict
from typing import List, Dict, Tuple, Optional

import pyarrow as pa
import pyarrow.parquet as pq

# ---- REQUIRED CONFIG ----
sys.path.insert(0, "/home/belle2/amubarak/Data_Format_Conversion")
from conf.pvpipe_config import load_cfg

CFG = load_cfg()
OUTPUT_BASE = CFG.output_base
DEFAULT_OUT = CFG.merge_output_dir or (CFG.submit_date_view_root or "/home/belle2/amubarak/PV_Output/merged")

CHUNK_RE    = re.compile(r"^(?P<base>.+)_chunk(?P<idx>\d{4})\.parquet$")
DAY_TOK_RE   = re.compile(r"^(?P<pv>.+)_(?P<tok>\d{4}_\d{2}_\d{2})$")
WEEK_TOK_RE  = re.compile(r"^(?P<pv>.+)_(?P<tok>\d{4}_W\d{2})$")
MONTH_TOK_RE = re.compile(r"^(?P<pv>.+)_(?P<tok>\d{4}_\d{2})$")

def _ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def is_chunk_file(filename: str) -> bool:
    return bool(CHUNK_RE.match(filename))

def read_single_row(table: pa.Table) -> Dict:
    if table.num_rows != 1:
        raise ValueError(f"Expected single-row table, got {table.num_rows} rows")
    rec = {}
    for col in table.column_names:
        rec[col] = table[col][0].as_py()
    return rec

def split_base_and_token(base_no_ext: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    m = DAY_TOK_RE.match(base_no_ext)
    if m:
        return m.group("pv"), m.group("tok"), "day"
    m = WEEK_TOK_RE.match(base_no_ext)
    if m:
        return m.group("pv"), m.group("tok"), "week"
    m = MONTH_TOK_RE.match(base_no_ext)
    if m:
        return m.group("pv"), m.group("tok"), "month"
    return None, None, None

def normalize_pvbase_after_strip(pvbase: str, token: str) -> str:
    try:
        year = token.split("_")[0]
        if pvbase.endswith("_" + year):
            return pvbase[:-(len(year) + 1)]
    except Exception:
        pass
    return pvbase

def token_sort_key(tok: str, kind: str) -> Tuple[int, int, int]:
    if kind == "day":
        y, m, d = [int(x) for x in tok.split("_")]
        return (y, m, d)
    if kind == "month":
        y, m = [int(x) for x in tok.split("_")]
        return (y, m, 1)
    y, w = tok.split("_W")
    y = int(y); w = int(w)
    return (y, w, 0)

def find_unsplit_groups(input_dir: str) -> Dict[Tuple[str, str], List[Tuple[str, str, str]]]:
    groups: Dict[Tuple[str, str], List[Tuple[str, str, str]]] = defaultdict(list)
    for root, _, files in os.walk(input_dir):
        for fn in files:
            if not fn.endswith(".parquet"):
                continue
            if is_chunk_file(fn):
                continue
            base_no_ext = os.path.splitext(fn)[0]
            pvbase_raw, tok, kind = split_base_and_token(base_no_ext)
            if not pvbase_raw:
                continue
            pvbase_clean = normalize_pvbase_after_strip(pvbase_raw, tok)
            full_path = os.path.join(root, fn)
            groups[(root, pvbase_clean)].append((full_path, tok, kind))
    return groups

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

def merge_one_pv_unsplit(in_files: List[Tuple[str, str, str]], out_path: str, delete_inputs: bool=False) -> bool:
    if not in_files:
        return False

    in_files_sorted = sorted(in_files, key=lambda x: token_sort_key(x[1], x[2]))

    first_tbl = pq.read_table(in_files_sorted[0][0])
    fr = read_single_row(first_tbl)

    for k in ("pv", "t0", "Nval", "times", "length", "value_type"):
        if k not in fr:
            raise ValueError(f"Input missing field '{k}': {in_files_sorted[0][0]}")

    pv   = fr["pv"]
    nval = int(fr["Nval"])
    value_type = fr.get("value_type", "numeric")
    is_vector = (nval > 1) or \
                ("values_by_component" in fr) or \
                ("values_matrix" in fr) or \
                ("values_text_by_component" in fr) or \
                ("values_text_matrix" in fr)

    abs_times_all: List[float] = []
    if value_type == "numeric" and not is_vector:
        vals_num_all: List[float] = []
        payload_mode = "num_scalar"
    elif value_type == "numeric" and is_vector:
        rows_num: List[List[float]] = []
        payload_mode = "num_vector"
    elif value_type == "text" and not is_vector:
        vals_txt_all: List[str] = []
        payload_mode = "text_scalar"
    else:
        rows_txt: List[List[str]] = []
        payload_mode = "text_vector"

    comp_names_ref = fr.get("component_names")

    for path, tok, kind in in_files_sorted:
        t = pq.read_table(path)
        r = read_single_row(t)
        if r.get("pv") != pv or int(r.get("Nval", nval)) != nval:
            raise ValueError(f"Inconsistent PV/Nval across parts for {out_path} (file: {path})")

        t0_ms = int(r["t0"])
        times_rel = r["times"] or []
        abs_times = [(t0_ms / 1000.0) + float(s) for s in times_rel]
        abs_times_all.extend(abs_times)

        if payload_mode == "num_scalar":
            vals = [float(x) for x in (r.get("values") or [])]
            vals_num_all.extend(vals)
        elif payload_mode == "text_scalar":
            vals = [str(x) for x in (r.get("values_text") or [])]
            vals_txt_all.extend(vals)
        elif payload_mode == "num_vector":
            if "values_by_component" in r:
                vbc = r["values_by_component"] or [[] for _ in range(max(1, nval))]
                length_here = max((len(col) for col in vbc), default=0)
                for i in range(length_here):
                    row = [float(vbc[j][i]) if i < len(vbc[j]) else float("nan") for j in range(max(1, nval))]
                    rows_num.append(row)
            elif "values_matrix" in r:
                rows_num.extend([[float(x) for x in row] for row in (r["values_matrix"] or [])])
            else:
                raise ValueError(f"Numeric vector PV part missing payload: {path}")
            c = r.get("component_names")
            if comp_names_ref and c and c != comp_names_ref:
                comp_names_ref = None
        else:
            if "values_text_by_component" in r:
                vtbc = r["values_text_by_component"] or [[] for _ in range(max(1, nval))]
                length_here = max((len(col) for col in vtbc), default=0)
                for i in range(length_here):
                    row = [vtbc[j][i] if i < len(vtbc[j]) else "" for j in range(max(1, nval))]
                    rows_txt.append(row)
            elif "values_text_matrix" in r:
                rows_txt.extend([[str(x) for x in row] for row in (r["values_text_matrix"] or [])])
            else:
                raise ValueError(f"Text vector PV part missing payload: {path}")
            c = r.get("component_names")
            if comp_names_ref and c and c != comp_names_ref:
                comp_names_ref = None

    if not abs_times_all:
        return False

    first_abs = min(abs_times_all)
    t0_ms_out = int(round(first_abs * 1000.0))
    idx = list(range(len(abs_times_all)))
    idx.sort(key=lambda k: abs_times_all[k])
    times_out = [abs_times_all[k] - (t0_ms_out / 1000.0) for k in idx]

    names = ["pv", "date", "t0", "Nval", "times", "length", "value_type"]
    arrays = [
        pa.array([pv]),
        pa.array(["unsplit"]),
        pa.array([t0_ms_out],   type=pa.int64()),
        pa.array([nval],        type=pa.int64()),
        pa.array([times_out],   type=pa.list_(pa.float64())),
        pa.array([len(times_out)], type=pa.int64()),
        pa.array([value_type]),
    ]

    if payload_mode == "num_scalar":
        vals_sorted = [vals_num_all[k] for k in idx]
        arrays.append(pa.array([vals_sorted], type=pa.list_(pa.float64())))
        names.append("values")
    elif payload_mode == "text_scalar":
        vals_sorted = [vals_txt_all[k] for k in idx]
        arrays.append(pa.array([vals_sorted], type=pa.list_(pa.string())))
        names.append("values_text")
    elif payload_mode == "num_vector":
        rows_sorted = [rows_num[k] for k in idx]
        nval_eff = max(1, nval)
        cols = [[] for _ in range(nval_eff)]
        _append_numeric_matrix_to_columns(rows_sorted, cols, nval_eff)
        arrays.append(pa.array([cols], type=pa.list_(pa.list_(pa.float64()))))
        names.append("values_by_component")
        comp_names = _ensure_comp_names(pv, nval_eff, comp_names_ref)
        arrays.append(pa.array([comp_names], type=pa.list_(pa.string())))
        names.append("component_names")
    else:
        rows_sorted = [rows_txt[k] for k in idx]
        nval_eff = max(1, nval)
        cols = [[] for _ in range(nval_eff)]
        _append_text_matrix_to_columns(rows_sorted, cols, nval_eff)
        arrays.append(pa.array([cols], type=pa.list_(pa.list_(pa.string()))))
        names.append("values_text_by_component")
        comp_names = _ensure_comp_names(pv, nval_eff, comp_names_ref)
        arrays.append(pa.array([comp_names], type=pa.list_(pa.string())))
        names.append("component_names")

    out_tbl = pa.Table.from_arrays(arrays, names=names)
    _ensure_dir(os.path.dirname(out_path))
    pq.write_table(out_tbl, out_path, compression="snappy")

    print(f"✅ PV unsplit: merged {len(in_files_sorted)} day/week/month files → {out_path} "
          f"(rows={len(times_out)})")

    if delete_inputs:
        for p, _, _ in in_files_sorted:
            try:
                os.remove(p)
            except Exception as e:
                print(f"⚠️ Could not delete {p}: {e}")

    return True

def run_unsplit(input_dir: str, output_dir: Optional[str], delete_inputs: bool=False):
    groups = find_unsplit_groups(input_dir)
    if not groups:
        print("No per-day/week/month PV files found to unsplit.")
        return 0, 0

    merged, skipped = 0, 0
    for (dir_path, pvbase), items in sorted(groups.items()):
        out_dir = (output_dir or dir_path)
        out_path = os.path.join(out_dir, f"{pvbase}.parquet")
        if os.path.exists(out_path):
            print(f"⏭️  Unsplit target exists, skipping: {out_path}")
            skipped += 1
            continue
        try:
            if merge_one_pv_unsplit(items, out_path, delete_inputs=delete_inputs):
                merged += 1
        except Exception as e:
            print(f"❌ Failed to unsplit PV for {pvbase} in {dir_path}: {e}")

    print(f"\nPV unsplit summary: merged={merged}, skipped={skipped}")
    return merged, skipped

def main():
    ap = argparse.ArgumentParser(description="Unsplit per-day/week/month PV Parquets back to a single PV file.")
    ap.add_argument("--input_dir",  default=OUTPUT_BASE, help="Where PV Parquets live")
    ap.add_argument("--output_dir", default=DEFAULT_OUT, help="Where to write unsplit outputs (defaults to input dir siblings)")
    ap.add_argument("--delete_after_unsplit", action="store_true", help="Delete per-day/week/month inputs after unsplitting")
    args = ap.parse_args()

    run_unsplit(args.input_dir, output_dir=args.output_dir, delete_inputs=args.delete_after_unsplit)

if __name__ == "__main__":
    main()