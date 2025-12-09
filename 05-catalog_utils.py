#!/usr/bin/env python3
# 05-catalog_utils.py
#
# Catalog rebuild/load utilities.
# Reads defaults from Data_Format_Conversion/config.ini:
#   [paths] db_path, input_dir
#   [exclude] subpaths   (comma or newline separated)
#
# Provides:
#   ensure_pb_catalog_table(db_path)
#   rebuild_catalog(db_path, root_dir, exclude_tokens=None, echo_cmd=False)
#   load_catalog_map(db_path)
#   load_catalog_paths(db_path, root_dir=None, exclude_tokens=None)
#   rebuild_from_config(echo_cmd=False)  -> uses config defaults
#
# CLI:
#   --config /path/to/config.ini
#   --rebuild            run rebuild using config defaults
#   --db PATH            override db_path
#   --root DIR           override input root
#   --no-exclude         ignore config exclude list
#   --echo               print the find command

import os
import shlex
import sqlite3
import subprocess
import time
import configparser
import argparse
from typing import Dict, List, Tuple, Optional

# ---------- config ----------

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

def _cfg_list(cfg: configparser.ConfigParser, section: str, key: str) -> List[str]:
    raw = _cfg_get(cfg, section, key, "") or ""
    # split on commas or newlines
    parts = []
    for token in raw.replace("\r", "\n").split("\n"):
        for t in token.split(","):
            s = t.strip()
            if s:
                parts.append(s)
    return parts

# ---------- SQLite helpers ----------

def _db_connect(db_path: str, timeout_s: int = 120) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=timeout_s)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=60000;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

def _cleanup_old_lockfile(db_path: str) -> None:
    """Remove legacy .wlock file if it exists. We no longer use it."""
    try:
        os.remove(db_path + ".wlock")
    except FileNotFoundError:
        pass
    except OSError:
        # Ignore races or permissions; file is obsolete.
        pass

# ---------- Public API ----------

def ensure_pb_catalog_table(db_path: str) -> None:
    conn = _db_connect(db_path)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS pb_catalog (
        pb_path    TEXT PRIMARY KEY,
        size_bytes INTEGER,
        mtime      REAL,
        inode      INTEGER
    )""")
    conn.commit()
    conn.close()

def _normalize_excludes(exclude_tokens) -> List[str]:
    toks: List[str] = []
    for t in (exclude_tokens or []):
        if not t:
            continue
        n = os.path.normpath(str(t))
        toks.append(n)
    return toks

def rebuild_catalog(db_path: str, root_dir: str, exclude_tokens=None, echo_cmd: bool = False) -> int:
    ensure_pb_catalog_table(db_path)
    root_dir = os.path.normpath(root_dir)
    toks = _normalize_excludes(exclude_tokens)

    cmd = ["find", root_dir, "-type", "f", "-name", "*.pb"]
    for t in toks:
        pat = f"*{t.strip().strip('*')}*"
        cmd += ["!", "-path", pat]
    cmd += ["-printf", r"%s\t%T@\t%i\t%p\0"]

    if echo_cmd:
        printable = " ".join(shlex.quote(x) for x in cmd)
        print(f"[CATALOG] find cmd: {printable}")

    out = subprocess.run(cmd, check=True, stdout=subprocess.PIPE).stdout
    rows = out.split(b"\0")

    batch: List[Tuple[str, int, float, int]] = []
    for row in rows:
        if not row:
            continue
        size_s, mtime_s, inode_s, path = row.decode("utf-8", "ignore").split("\t", 4)
        batch.append((path, int(size_s), float(mtime_s), int(inode_s)))

    # Serialize writers using SQLite reservation and exponential backoff.
    for attempt in range(8):
        try:
            conn = _db_connect(db_path, timeout_s=120)
            c = conn.cursor()
            c.execute("BEGIN IMMEDIATE")
            c.execute("""CREATE TABLE IF NOT EXISTS pb_catalog (
                pb_path    TEXT PRIMARY KEY,
                size_bytes INTEGER,
                mtime      REAL,
                inode      INTEGER
            )""")
            c.execute("DELETE FROM pb_catalog")
            if batch:
                c.executemany(
                    "INSERT OR REPLACE INTO pb_catalog(pb_path,size_bytes,mtime,inode) VALUES (?,?,?,?)",
                    batch
                )
            conn.commit()
            conn.close()
            break
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                time.sleep(0.5 * (2 ** attempt))
                continue
            raise

    print(f"[CATALOG] Rebuilt {len(batch)} entries from root {root_dir}")

    # Remove legacy lock file if present (no longer used).
    _cleanup_old_lockfile(db_path)

    return len(batch)

def load_catalog_map(db_path: str) -> Dict[str, Tuple[int, float, int]]:
    conn = _db_connect(db_path, timeout_s=60)
    c = conn.cursor()
    try:
        c.execute("SELECT pb_path,size_bytes,mtime,inode FROM pb_catalog")
        out = {row[0]: (row[1], row[2], row[3]) for row in c.fetchall()}
    except sqlite3.OperationalError:
        out = {}
    conn.close()
    return out

def load_catalog_paths(db_path: str, root_dir: Optional[str] = None, exclude_tokens=None) -> List[str]:
    m = load_catalog_map(db_path)
    if root_dir:
        root = os.path.normpath(root_dir)
        m = {p: v for p, v in m.items() if os.path.normpath(p).startswith(root)}
    toks = _normalize_excludes(exclude_tokens)
    if toks:
        def ok(p: str) -> bool:
            n = os.path.normpath(p)
            return not any(t in n for t in toks)
        return [p for p in m.keys() if ok(p)]
    return list(m.keys())

# -------- convenience from config --------

def rebuild_from_config(config_path: Optional[str] = None, echo_cmd: bool = False) -> int:
    cfg = _load_cfg(config_path)
    db_path  = _cfg_get(cfg, "paths", "db_path", "/home/belle2/amubarak/conversion_log.db")
    root_dir = _cfg_get(cfg, "paths", "input_dir", "/gpfs/group/belle2/group/accelerator/pv/2024/AA/")
    excludes = _cfg_list(cfg, "exclude", "subpaths")
    return rebuild_catalog(db_path=db_path, root_dir=root_dir, exclude_tokens=excludes, echo_cmd=echo_cmd)

# -------- CLI --------

def _main():
    ap = argparse.ArgumentParser(description="PB catalog utilities (config.ini aware).")
    ap.add_argument("--config", default=CONFIG_DEFAULT, help="Path to config.ini")
    ap.add_argument("--rebuild", action="store_true", help="Rebuild catalog using config defaults")
    ap.add_argument("--db", help="Override DB path")
    ap.add_argument("--root", help="Override input root")
    ap.add_argument("--no-exclude", action="store_true", help="Ignore config exclude list")
    ap.add_argument("--echo", action="store_true", help="Echo the find command")
    args = ap.parse_args()

    cfg = _load_cfg(args.config)
    db_path  = args.db or _cfg_get(cfg, "paths", "db_path", "/home/belle2/amubarak/conversion_log.db")
    root_dir = args.root or _cfg_get(cfg, "paths", "input_dir", "/gpfs/group/belle2/group/accelerator/pv/2024/AA/")
    excludes = [] if args.no-exclude else _cfg_list(cfg, "exclude", "subpaths")

    if args.rebuild:
        rebuild_catalog(db_path=db_path, root_dir=root_dir, exclude_tokens=excludes, echo_cmd=args.echo)
    else:
        # simple dump counts
        paths = load_catalog_paths(db_path, root_dir=root_dir, exclude_tokens=excludes)
        print(f"[CATALOG] entries={len(paths)}")

if __name__ == "__main__":
    _main()