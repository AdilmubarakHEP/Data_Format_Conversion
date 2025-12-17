#!/bin/bash
#
# Automatic Conversion Script
# ===========================
# Monitors jobs and resubmits failed files automatically
#
# Usage:
#   tmux new -s convert
#   /home/belle2/amubarak/Data_Format_Conversion/tool/auto_convert.sh

TOOL_DIR="/home/belle2/amubarak/Data_Format_Conversion/tool"
MAX_ROUNDS=100
WAIT_SECONDS=300  # 5 minutes

echo "=============================================="
echo "  Automatic PB to Parquet Conversion"
echo "  Started: $(date)"
echo "=============================================="

show_status() {
    echo ""
    echo "=== STATUS $(date '+%H:%M:%S') ==="

    # Weekly jobs (large files)
    WEEKLY_TOTAL=$(bjobs -g /pvpipe_weekly 2>/dev/null | grep -v JOBID | wc -l)
    WEEKLY_RUN=$(bjobs -g /pvpipe_weekly 2>/dev/null | grep -c RUN)
    WEEKLY_PEND=$(bjobs -g /pvpipe_weekly 2>/dev/null | grep -c PEND)

    # Small file jobs
    SMALL_TOTAL=$(bjobs -g /pvpipe_reconvert 2>/dev/null | grep -v JOBID | wc -l)
    SMALL_RUN=$(bjobs -g /pvpipe_reconvert 2>/dev/null | grep -c RUN)
    SMALL_PEND=$(bjobs -g /pvpipe_reconvert 2>/dev/null | grep -c PEND)

    echo "Weekly jobs (large files): $WEEKLY_TOTAL ($WEEKLY_RUN running, $WEEKLY_PEND pending)"
    echo "Small file jobs: $SMALL_TOTAL ($SMALL_RUN running, $SMALL_PEND pending)"
    echo "Total: $((WEEKLY_TOTAL + SMALL_TOTAL)) jobs"
}

for ROUND in $(seq 1 $MAX_ROUNDS); do
    echo ""
    echo "=============================================="
    echo "  ROUND $ROUND/$MAX_ROUNDS - $(date)"
    echo "=============================================="

    show_status

    # Count total jobs
    WEEKLY=$(bjobs -g /pvpipe_weekly 2>/dev/null | grep -v JOBID | wc -l)
    SMALL=$(bjobs -g /pvpipe_reconvert 2>/dev/null | grep -v JOBID | wc -l)
    TOTAL=$((WEEKLY + SMALL))

    if [[ $TOTAL -gt 0 ]]; then
        echo ""
        echo "Waiting for $TOTAL jobs... (next check in $((WAIT_SECONDS/60)) min)"
        sleep $WAIT_SECONDS
        continue
    fi

    # No jobs - check what's left
    echo ""
    echo "All jobs completed. Running audit..."

    python3 $TOOL_DIR/reconvert_missing.py --audit 2>&1 | grep -A10 "WORK NEEDED"

    # Check for remaining work
    NEEDS_WORK=$(python3 $TOOL_DIR/reconvert_missing.py --audit 2>&1 | grep "Total files needing" | grep -oP '\d+')

    if [[ -z "$NEEDS_WORK" || "$NEEDS_WORK" -eq 0 ]]; then
        echo ""
        echo "=============================================="
        echo "  ALL CONVERSIONS COMPLETE!"
        echo "  Finished: $(date)"
        echo "=============================================="
        break
    fi

    echo ""
    echo "Resubmitting $NEEDS_WORK files..."

    # Submit large files with weekly converter
    echo "Submitting large files..."
    python3 $TOOL_DIR/weekly_converter.py --submit-large-files --max-jobs 1000 2>&1 | tail -5

    # Submit small files
    echo "Submitting small files..."
    python3 $TOOL_DIR/reconvert_missing.py --submit --small-only --max-jobs 500 2>&1 | tail -5

    sleep $WAIT_SECONDS
done

echo ""
echo "=== FINAL STATUS ==="
show_status
python3 $TOOL_DIR/reconvert_missing.py --audit 2>&1 | grep -A20 "CONVERSION STATUS"
echo ""
echo "Script ended at $(date)"
