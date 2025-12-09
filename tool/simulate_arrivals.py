#!/usr/bin/env python3
import os
import time
import shutil
import random
import glob
import argparse
import sys

# ========== CONFIGURATION ==========
BASE_SOURCE_ROOT = "/gpfs/group/belle2/group/accelerator/pv"
DEFAULT_TREE     = "2025c"   # change to "2024" if you want that as default

TARGET_BASE      = "/home/belle2/amubarak"

MAX_FILES        = 50        # Max number of files to simulate
DELAY_BETWEEN    = 60        # Delay between each file (seconds)
RANDOMIZE        = True      # Randomize the files selected
# ===================================

def main():
    # Optional CLI/env overrides so you don't have to edit the file
    ap = argparse.ArgumentParser(description="Simulate .pb arrivals by copying from a source tree.")
    ap.add_argument("--tree", choices=["2024", "2024c"], help="Which source tree to simulate from")
    ap.add_argument("--max", type=int, help="Max files to copy (overrides MAX_FILES)")
    ap.add_argument("--delay", type=int, help="Seconds between copies (overrides DELAY_BETWEEN)")
    ap.add_argument("--no-random", action="store_true", help="Disable randomization (keep sorted order)")
    args = ap.parse_args()

    tree = args.tree or os.environ.get("SIM_TREE") or DEFAULT_TREE
    source_dir = os.path.join(BASE_SOURCE_ROOT, tree)
    mirrored_root = os.path.join(TARGET_BASE, tree)

    max_files = args.max if args.max is not None else MAX_FILES
    delay = args.delay if args.delay is not None else DELAY_BETWEEN
    randomized = RANDOMIZE and not args.no_random

    if not os.path.isdir(source_dir):
        print(f"[ERROR] Source directory not found: {source_dir}")
        sys.exit(2)

    os.makedirs(mirrored_root, exist_ok=True)

    pb_files = glob.glob(os.path.join(source_dir, "**", "*.pb"), recursive=True)
    if not pb_files:
        print(f"[ERROR] No .pb files found under {source_dir}")
        sys.exit(1)

    if randomized:
        random.shuffle(pb_files)
    else:
        pb_files.sort()

    if max_files is not None and max_files >= 0:
        pb_files = pb_files[:max_files]

    print(f"[INFO] Simulating push of {len(pb_files)} files...")
    print(f"[INFO] From: {source_dir}")
    print(f"[INFO] Into: {mirrored_root}")

    for i, src_path in enumerate(pb_files, start=1):
        rel_path = os.path.relpath(src_path, source_dir)
        dest_path = os.path.join(mirrored_root, rel_path)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        try:
            shutil.copy2(src_path, dest_path)
            print(f"[{i:03d}/{len(pb_files)}] Copied: {rel_path}")
        except Exception as e:
            print(f"[ERROR] Failed to copy {src_path}: {e}")
        time.sleep(delay)

    print("[DONE] Simulation complete.")

if __name__ == "__main__":
    main()