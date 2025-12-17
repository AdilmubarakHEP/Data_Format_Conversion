#!/bin/bash
#
# Master Conversion Script - Run in tmux
# =======================================
# This script monitors and manages the .pb to .parquet conversion
#
# Usage:
#   tmux new -s convert
#   /home/belle2/amubarak/Data_Format_Conversion/tool/run_all_conversions.sh
#
# To detach: Ctrl+B, then D
# To reattach: tmux attach -t convert

TOOL_DIR="/home/belle2/amubarak/Data_Format_Conversion/tool"
LOG_DIR="/home/belle2/amubarak/Data_Format_Conversion/logs/reconvert"

echo "=============================================="
echo "  PB to Parquet Conversion Manager"
echo "  Started: $(date)"
echo "=============================================="

# Function to show status
show_status() {
    echo ""
    echo "=== STATUS at $(date '+%H:%M:%S') ==="
    JOBS=$(bjobs -g /pvpipe_reconvert 2>/dev/null | grep -v JOBID | wc -l)
    RUNNING=$(bjobs -g /pvpipe_reconvert 2>/dev/null | grep -c RUN)
    PENDING=$(bjobs -g /pvpipe_reconvert 2>/dev/null | grep -c PEND)
    COMPLETED=$(grep -l "COMPLETE\|Files written" $LOG_DIR/*.log 2>/dev/null | wc -l)
    KILLED=$(grep -l "Killed\|TERM_MEMLIMIT" $LOG_DIR/*.log 2>/dev/null | wc -l)

    echo "Jobs: $JOBS (Running: $RUNNING, Pending: $PENDING)"
    echo "Completed: $COMPLETED | Killed: $KILLED"
}

# Function to run audit and submit missing
submit_missing() {
    echo ""
    echo "=== Checking for missing files... ==="
    python3 $TOOL_DIR/reconvert_missing.py --audit 2>&1 | tail -20

    echo ""
    read -p "Submit jobs for missing files? (y/n): " answer
    if [[ "$answer" == "y" || "$answer" == "Y" ]]; then
        echo "Submitting jobs..."
        python3 $TOOL_DIR/reconvert_missing.py --submit --max-jobs 500 2>&1
    fi
}

# Main loop
ROUND=0
while true; do
    ROUND=$((ROUND + 1))
    echo ""
    echo "=============================================="
    echo "  ROUND $ROUND - $(date)"
    echo "=============================================="

    show_status

    # Check if any jobs are running
    JOBS=$(bjobs -g /pvpipe_reconvert 2>/dev/null | grep -v JOBID | wc -l)

    if [[ $JOBS -eq 0 ]]; then
        echo ""
        echo "No jobs currently running."

        # Run audit to check what's missing
        echo ""
        echo "Running audit..."
        python3 $TOOL_DIR/reconvert_missing.py --audit 2>&1 | grep -A20 "WORK NEEDED"

        # Check if there's work to do
        NEEDS_WORK=$(python3 -c "
import json
import os
from pathlib import Path

PB_DIR = '/gpfs/group/belle2/group/accelerator/pv/2024/AA/'
PARQUET_DIR = '/gpfs/group/belle2/group/accelerator/pv/2024/AA_parquet/converted_flat2'

with open('/home/belle2/amubarak/fast_audit_results.json') as f:
    audit = json.load(f)

all_files = list(set(audit.get('files_not_in_db', []) + audit.get('queued_files', [])))
needs_work = 0

for pb_path in all_files:
    if not os.path.exists(pb_path):
        continue
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
    needs_work += 1

print(needs_work)
" 2>/dev/null)

        if [[ $NEEDS_WORK -gt 0 ]]; then
            echo ""
            echo "Found $NEEDS_WORK files still need conversion."
            echo ""
            read -p "Submit jobs? (y/n/q to quit): " answer

            if [[ "$answer" == "q" || "$answer" == "Q" ]]; then
                echo "Exiting..."
                break
            elif [[ "$answer" == "y" || "$answer" == "Y" ]]; then
                echo "Submitting jobs..."
                python3 $TOOL_DIR/reconvert_missing.py --submit --max-jobs 500 2>&1
            fi
        else
            echo ""
            echo "=============================================="
            echo "  ALL CONVERSIONS COMPLETE!"
            echo "  Finished: $(date)"
            echo "=============================================="
            break
        fi
    else
        echo ""
        echo "Waiting for $JOBS jobs to complete..."
        echo "Next check in 5 minutes. Press Ctrl+C to interrupt."
        sleep 300
    fi
done

echo ""
echo "Final status:"
show_status
echo ""
echo "Script ended at $(date)"
