#!/bin/bash
# Quick status check for all conversion jobs

echo "=== JOB STATUS ==="
WEEKLY=$(bjobs -g /pvpipe_weekly 2>/dev/null | grep -v JOBID | wc -l)
WEEKLY_RUN=$(bjobs -g /pvpipe_weekly 2>/dev/null | grep -c RUN)
SMALL=$(bjobs -g /pvpipe_reconvert 2>/dev/null | grep -v JOBID | wc -l)
SMALL_RUN=$(bjobs -g /pvpipe_reconvert 2>/dev/null | grep -c RUN)

echo "Weekly (large files): $WEEKLY jobs ($WEEKLY_RUN running)"
echo "Small files: $SMALL jobs ($SMALL_RUN running)"
echo "Total: $((WEEKLY + SMALL)) jobs"

echo ""
echo "=== LOG STATUS ==="
WEEKLY_LOG="/home/belle2/amubarak/Data_Format_Conversion/logs/weekly"
SMALL_LOG="/home/belle2/amubarak/Data_Format_Conversion/logs/reconvert"

WEEKLY_DONE=$(grep -l "COMPLETE" $WEEKLY_LOG/*.log 2>/dev/null | wc -l)
WEEKLY_FAIL=$(grep -l "Killed" $WEEKLY_LOG/*.log 2>/dev/null | wc -l)
SMALL_DONE=$(grep -l "COMPLETE\|Files written" $SMALL_LOG/*.log 2>/dev/null | wc -l)
SMALL_FAIL=$(grep -l "Killed" $SMALL_LOG/*.log 2>/dev/null | wc -l)

echo "Weekly completed: $WEEKLY_DONE, killed: $WEEKLY_FAIL"
echo "Small completed: $SMALL_DONE, killed: $SMALL_FAIL"

echo ""
echo "=== COMMANDS ==="
echo "Monitor:    watch -n 60 '$0'"
echo "Auto run:   /home/belle2/amubarak/Data_Format_Conversion/tool/auto_convert.sh"
echo "Full audit: python3 /home/belle2/amubarak/Data_Format_Conversion/tool/reconvert_missing.py --audit"
