#!/bin/bash
#
# Process Large Files on Login Node
# ==================================
# Run this in tmux on the login node (no memory limits)
#
# Usage:
#   tmux new -s large_convert
#   /home/belle2/amubarak/Data_Format_Conversion/tool/process_large_files.sh
#
# To detach: Ctrl+B, then D
# To reattach: tmux attach -t large_convert

TOOL_DIR="/home/belle2/amubarak/Data_Format_Conversion/tool"
LOG_DIR="/home/belle2/amubarak/Data_Format_Conversion/logs/large_direct"
CONVERTER="/home/belle2/amubarak/singlepass_converter.py"

mkdir -p "$LOG_DIR"

echo "=============================================="
echo "  Large File Processor (Login Node)"
echo "  Started: $(date)"
echo "  Memory: $(free -h | grep Mem | awk '{print $7}') available"
echo "=============================================="

# Get list of large files needing conversion
get_large_files() {
    python3 -c "
import json
import os
from pathlib import Path

PB_DIR = '/gpfs/group/belle2/group/accelerator/pv/2024/AA/'
PARQUET_DIR = '/gpfs/group/belle2/group/accelerator/pv/2024/AA_parquet/converted_flat2'

with open('/home/belle2/amubarak/fast_audit_results.json') as f:
    audit = json.load(f)

all_files = list(set(audit.get('files_not_in_db', []) + audit.get('queued_files', [])))

for pb_path in sorted(all_files):
    if not os.path.exists(pb_path):
        continue
    size_gb = os.path.getsize(pb_path) / (1024**3)
    if size_gb < 5.0:
        continue

    # Check if already converted (45+ weeks)
    pb_name = Path(pb_path).stem
    pv_name = pb_name.split(':')[0] if ':' in pb_name else pb_name
    try:
        rel_path = Path(pb_path).relative_to(PB_DIR)
        out_dir = Path(PARQUET_DIR) / rel_path.parent
    except:
        out_dir = Path(PARQUET_DIR) / 'other'

    if out_dir.exists():
        parquet_files = list(out_dir.glob(f'{pv_name}-2024-W*.parquet'))
        if len(parquet_files) >= 45:
            continue

    print(f'{size_gb:.1f}|{pb_path}')
" | sort -t'|' -k1 -n
}

# Process files one by one
TOTAL=0
DONE=0
FAILED=0

echo ""
echo "Finding large files to process..."
LARGE_FILES=$(get_large_files)
TOTAL=$(echo "$LARGE_FILES" | grep -c "|" || echo 0)

echo "Found $TOTAL large files to process"
echo ""

if [[ $TOTAL -eq 0 ]]; then
    echo "No large files need conversion!"
    exit 0
fi

echo "$LARGE_FILES" | while IFS='|' read -r SIZE_GB PB_PATH; do
    if [[ -z "$PB_PATH" ]]; then
        continue
    fi

    BASENAME=$(basename "$PB_PATH")
    LOGNAME=$(echo "$BASENAME" | tr ':' '_')

    echo ""
    echo "=============================================="
    echo "Processing: $BASENAME ($SIZE_GB GB)"
    echo "Started: $(date)"
    echo "Progress: $((DONE+1))/$TOTAL"
    echo "=============================================="

    # Run the singlepass converter
    python3 "$CONVERTER" --pb "$PB_PATH" 2>&1 | tee "$LOG_DIR/${LOGNAME}.log"

    if [[ ${PIPESTATUS[0]} -eq 0 ]]; then
        echo "SUCCESS: $BASENAME"
        DONE=$((DONE+1))
    else
        echo "FAILED: $BASENAME"
        FAILED=$((FAILED+1))
    fi

    echo ""
    echo "Status: $DONE completed, $FAILED failed, $((TOTAL-DONE-FAILED)) remaining"
done

echo ""
echo "=============================================="
echo "  PROCESSING COMPLETE"
echo "  Finished: $(date)"
echo "  Total: $TOTAL, Done: $DONE, Failed: $FAILED"
echo "=============================================="
