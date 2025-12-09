#!/usr/bin/env python3
# 06-progress.py
#
# Prints conversion progress using db_path from config.ini by default.

import sqlite3
import configparser
import argparse

CONFIG_DEFAULT = "/home/belle2/amubarak/Data_Format_Conversion/config.ini"

def _load_cfg(path: str) -> configparser.ConfigParser:
    cfg = configparser.ConfigParser()
    cfg.read(path or CONFIG_DEFAULT)
    return cfg

def _cfg_get(cfg: configparser.ConfigParser, section: str, key: str, default: str) -> str:
    try:
        v = cfg.get(section, key, fallback=default)
        v = v.strip() if v is not None else default
        return v or default
    except Exception:
        return default

def _count_or_zero(c, sql: str, params=()):
    try:
        c.execute(sql, params)
        row = c.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except Exception:
        return 0

def main():
    ap = argparse.ArgumentParser(description="Show PB conversion progress (reads config.ini).")
    ap.add_argument("--config", default=CONFIG_DEFAULT, help="Path to config.ini")
    ap.add_argument("--db_path", help="Override DB path")
    args = ap.parse_args()

    cfg = _load_cfg(args.config)
    db_path = args.db_path or _cfg_get(cfg, "paths", "db_path", "/home/belle2/amubarak/conversion_log.db")

    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    total = _count_or_zero(c, "SELECT COUNT(*) FROM pb_catalog")
    done  = _count_or_zero(c, "SELECT COUNT(*) FROM conversions WHERE status IN ('success','skipped')")
    conn.close()

    left = max(0, total - done)
    pct = 100.0 * done / total if total else 0.0
    bar_len = 40
    filled = int((pct / 100.0) * bar_len)
    bar = "[" + "#" * filled + "-" * (bar_len - filled) + "]"
    # no em dash
    print(f"{bar} {pct:.1f}% ({done:,}/{total:,})  left {left:,}")

if __name__ == "__main__":
    main()