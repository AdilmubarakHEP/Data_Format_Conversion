#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
06-init_conversion_db.py

Initialize (and optionally seed) the conversion tracking DB used by the submitter.

Reads defaults from Data_Format_Conversion/config.ini:
  [paths] db_path, input_dir, output_base

CLI flags override config values.
"""

import os
import argparse
import sqlite3
import configparser
from datetime import datetime
from typing import Optional, Tuple

# -------- config --------

CONFIG_DEFAULT = "/home/belle2/amubarak/Data_Format_Conversion/config.ini"

def _load_cfg(path: Optional[str]) -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    cfg.read(path or CONFIG_DEFAULT)
    return cfg

def _cfg_get(cfg: configparser.ConfigParser, section: str, key: str, default: Optional[str] = None) -> Optional[str]:
    try:
        v = cfg.get(section, key, fallback=default)
        if v is None:
            return default
        v = v.strip()
        return v if v != "" else default
    except Exception:
        return default

# -------- core --------

def init_db(db_path: str):
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS conversions (
            pb_path   TEXT PRIMARY KEY,
            timestamp TEXT,
            status    TEXT,
            message   TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS pb_sizes (
            pb_path    TEXT PRIMARY KEY,
            size_bytes INTEGER
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_conversions_status ON conversions(status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_conversions_timestamp ON conversions(timestamp)")
    conn.commit()
    conn.close()
    print("OK: initialized tables and indexes")

def safe_parquet_path_for_pb(pb_path: str, input_base: str, output_base: str) -> Optional[str]:
    try:
        rel = os.path.relpath(pb_path, input_base)
        if rel.startswith(".."):
            return None
    except Exception:
        rel = os.path.basename(pb_path)
    base_no_ext, _ = os.path.splitext(rel)
    parts = base_no_ext.split(os.sep)
    if parts:
        parts[-1] = parts[-1].replace(":", "-")
    safe_rel = os.path.join(*parts) + ".parquet"
    return os.path.join(output_base, safe_rel)

def seed_from_existing_outputs(db_path: str, input_base: str, output_base: str) -> Tuple[int, int]:
    if not os.path.isdir(input_base):
        print(f"Warn: input_base not found: {input_base}")
        return (0, 0)
    seeded, checked = 0, 0
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    now = datetime.now().isoformat(timespec="seconds")

    for root, _, files in os.walk(input_base):
        for fn in files:
            if not fn.endswith(".pb"):
                continue
            pb_path = os.path.join(root, fn)
            checked += 1
            out_path = safe_parquet_path_for_pb(pb_path, input_base, output_base)
            if not out_path or not os.path.exists(out_path):
                continue

            c.execute("""
                INSERT INTO conversions (pb_path, timestamp, status, message)
                VALUES (?, ?, 'success', 'seeded_by_init')
                ON CONFLICT(pb_path) DO UPDATE SET
                    timestamp=excluded.timestamp,
                    status=excluded.status,
                    message=excluded.message
            """, (pb_path, now))

            try:
                sz = os.path.getsize(pb_path)
            except OSError:
                sz = None
            if sz is not None:
                c.execute("""
                    INSERT INTO pb_sizes (pb_path, size_bytes)
                    VALUES (?, ?)
                    ON CONFLICT(pb_path) DO UPDATE SET size_bytes=excluded.size_bytes
                """, (pb_path, int(sz)))

            seeded += 1
            if seeded % 500 == 0:
                print(f"  ...seeded {seeded} of {checked}")

    conn.commit()
    conn.close()
    print(f"Done: seeded {seeded} PB files (checked {checked})")
    return (seeded, checked)

def main():
    ap = argparse.ArgumentParser(description="Initialize and optionally seed the conversion DB (reads config.ini).")
    ap.add_argument("--config", default=CONFIG_DEFAULT, help="Path to config.ini")
    ap.add_argument("--db_path", help="Override DB path")
    ap.add_argument("--seed", action="store_true", help="Seed DB by scanning for already-converted PBs")
    ap.add_argument("--input_base", help="Override input PB root for seeding")
    ap.add_argument("--output_base", help="Override Parquet output root for seeding")
    args = ap.parse_args()

    cfg = _load_cfg(args.config)
    db_path     = args.db_path     or _cfg_get(cfg, "paths", "db_path", "/home/belle2/amubarak/conversion_log.db")
    input_base  = args.input_base  or _cfg_get(cfg, "paths", "input_dir", "/gpfs/group/belle2/group/accelerator/pv/2024/AA/")
    output_base = args.output_base or _cfg_get(cfg, "paths", "output_base", "/gpfs/group/belle2/group/accelerator/pv/2024/AA_parquet/converted_flat2")

    init_db(db_path)
    if args.seed:
        seed_from_existing_outputs(db_path, input_base, output_base)

if __name__ == "__main__":
    main()