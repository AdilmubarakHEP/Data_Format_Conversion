# PB to Parquet Conversion Project - Status & Implementation

**Last Updated:** 2025-12-16
**Location:** KEKCC (KEK Computing Center)

## Overview

Converting EPICS Archiver Appliance `.pb` (protobuf) files to `.parquet` format for 2024 accelerator data at Belle II.

### Data Locations
- **Source:** `/gpfs/group/belle2/group/accelerator/pv/2024/AA/`
- **Output:** `/gpfs/group/belle2/group/accelerator/pv/2024/AA_parquet/converted_flat2/`
- **Tools:** `/home/belle2/amubarak/Data_Format_Conversion/`
- **Database:** `/home/belle2/amubarak/Data_Format_Conversion/database/conversion_log_2024.db`

## Problem Summary

### Initial State
- ~101,173 total .pb files for 2024
- ~99.6% already converted successfully
- ~408 files needed reconversion (failed, partial, or missing)
- 81 "big" files (>5GB) totaling ~8TB
- 4 massive files: 948GB, 923GB, 878GB, 699GB

### Key Constraint
**All KEKCC batch queues have a 4GB memory limit!**
- Queue 'h', 'l', 's', 'p' all have `MEMLIMIT: 4G`
- The Java `pb2parquet` reader uses >4GB for files larger than ~5GB
- This prevented processing large files via LSF batch jobs

## Solution Architecture

### Two-Track Approach

#### Track 1: Small Files (<5GB) via LSF Batch
- Use `singlepass_converter.py` with LSF queue 'h'
- Java heap: 1GB (`-Xmx1g`)
- Flush threshold: 100K rows per week
- Works within 4GB limit

#### Track 2: Large Files (>=5GB) on Login Node
- Run directly on login node (cw05) with 600GB+ memory
- Java heap: 32GB (`-Xmx32g`)
- Flush threshold: 2M rows per week
- Process files sequentially (one at a time)

## Key Scripts

### 1. `/home/belle2/amubarak/singlepass_converter.py`
**Purpose:** Single-pass streaming converter that reads .pb once and writes all weekly parquet files.

**Key Features:**
- Auto-detects memory (login node vs compute node)
- Streams data, flushes to parquet periodically
- Writes weekly files: `{PV_NAME}-2024-W{01-53}.parquet`

**Memory Detection Logic:**
```python
# Reads /proc/meminfo to detect available memory
if mem_gb > 100:  # Login node
    java_heap = "32g"
    FLUSH_THRESHOLD = 2000000
else:  # Compute node (4GB limit)
    java_heap = "1g"
    FLUSH_THRESHOLD = 100000
```

**Usage:**
```bash
# Single file
python3 singlepass_converter.py --pb /path/to/file.pb

# Submit all large files as LSF jobs (won't work due to memory)
python3 singlepass_converter.py --submit-all
```

### 2. `/home/belle2/amubarak/Data_Format_Conversion/tool/reconvert_missing.py`
**Purpose:** Audit conversion status and submit jobs for missing/failed files.

**Usage:**
```bash
# Audit only
python3 reconvert_missing.py --audit

# Submit small files
python3 reconvert_missing.py --submit --small-only

# Submit large files (uses weekly_converter which won't work due to memory)
python3 reconvert_missing.py --submit --large-only
```

### 3. `/home/belle2/amubarak/Data_Format_Conversion/tool/process_large_files.sh`
**Purpose:** Process large files sequentially on login node (no memory limit).

**Usage:**
```bash
# Run in tmux on login node
tmux new -s convert
/home/belle2/amubarak/Data_Format_Conversion/tool/process_large_files.sh

# Or with nohup
nohup ./process_large_files.sh >> logs/large_direct/main.log 2>&1 &
```

### 4. `/home/belle2/amubarak/Data_Format_Conversion/tool/check_status.sh`
**Purpose:** Quick status check of all running jobs.

### 5. `/home/belle2/amubarak/Data_Format_Conversion/tool/auto_convert.sh`
**Purpose:** Automatic monitoring and resubmission script.

## Output Format

Each .pb file produces multiple weekly parquet files:
```
{PV_NAME}-2024-W01.parquet
{PV_NAME}-2024-W02.parquet
...
{PV_NAME}-2024-W53.parquet
```

Schema:
```
- time_offset_ms: INT64 (milliseconds since 2024-01-01 00:00:00 JST)
- value: FLOAT64 or STRING (for scalar PVs)
- values: LIST<FLOAT64> or LIST<STRING> (for vector PVs)
```

## Job Groups

- `/pvpipe_reconvert` - Small file LSF jobs
- `/pvpipe_weekly` - Weekly converter LSF jobs (deprecated - doesn't work)
- `/pvpipe_singlepass` - Old singlepass LSF jobs (deprecated)

## Current Status (as of 2025-12-16 17:51 JST)

### Small Files
- **236 files** (<5GB, 151 GB total)
- **~48 jobs running** via LSF queue 'h'
- **Expected completion:** 1-2 hours

### Large Files
- **75 files** (>=5GB, 8TB total)
- **Running on login node** (PID tracked in logs)
- **Java heap:** 32GB
- **Expected completion:** 24-48 hours (sequential processing)

### Monitoring
```bash
# Quick status
/home/belle2/amubarak/Data_Format_Conversion/tool/check_status.sh

# Large file progress
tail -f /home/belle2/amubarak/Data_Format_Conversion/logs/large_direct/main.log

# Small file jobs
bjobs -g /pvpipe_reconvert

# Full audit
python3 /home/belle2/amubarak/Data_Format_Conversion/tool/reconvert_missing.py --audit
```

## Log Locations

- Small files: `/home/belle2/amubarak/Data_Format_Conversion/logs/reconvert/`
- Large files: `/home/belle2/amubarak/Data_Format_Conversion/logs/large_direct/`
- Weekly (deprecated): `/home/belle2/amubarak/Data_Format_Conversion/logs/weekly/`
- Singlepass (deprecated): `/home/belle2/amubarak/Data_Format_Conversion/logs/singlepass/`

## Failed Approaches (for reference)

### 1. Weekly Converter (`weekly_converter.py`)
- Idea: Process one week at a time (53 jobs per file)
- Problem: Java still needs to read entire file, uses >4GB
- Status: Abandoned

### 2. Parallel Weekly Chunks (`parallel_pb_converter.py`)
- Idea: 53 parallel jobs per file, each filtering one week
- Problem: Reads file 53 times = 53x I/O overhead
- Status: Abandoned

### 3. Big Chunker with RSS Cap (`02-big_chunker_script.py`)
- Idea: Use RSS monitoring to flush before hitting limit
- Problem: Java uses memory before Python can monitor
- Status: Works for small files, fails for large

## Resuming Work

If conversion needs to be resumed:

1. **Check current status:**
   ```bash
   /home/belle2/amubarak/Data_Format_Conversion/tool/check_status.sh
   ```

2. **Run audit to see what's missing:**
   ```bash
   python3 /home/belle2/amubarak/Data_Format_Conversion/tool/reconvert_missing.py --audit
   ```

3. **For small files (<5GB):**
   ```bash
   python3 /home/belle2/amubarak/Data_Format_Conversion/tool/reconvert_missing.py --submit --small-only
   ```

4. **For large files (>=5GB):**
   ```bash
   tmux new -s convert
   /home/belle2/amubarak/Data_Format_Conversion/tool/process_large_files.sh
   # Ctrl+B, D to detach
   ```

## Key Learnings

1. **KEKCC has 4GB memory limit on ALL batch queues** - must run large jobs on login node
2. **Java pb2parquet reader buffers aggressively** - can't control memory from Python
3. **Login node (cw05) has 600GB+ memory** - use for large file processing
4. **Single-pass streaming is most efficient** - reads file once, writes all weeks
5. **Weekly output format** allows efficient time-range queries downstream

## Files Reference

```
/home/belle2/amubarak/
├── singlepass_converter.py          # Main converter (auto-detects memory)
├── fast_audit_results.json          # Cached audit of files needing work
├── Data_Format_Conversion/
│   ├── tool/
│   │   ├── reconvert_missing.py     # Audit & job submission
│   │   ├── process_large_files.sh   # Login node large file processor
│   │   ├── check_status.sh          # Quick status check
│   │   ├── auto_convert.sh          # Automatic monitor & resubmit
│   │   └── weekly_converter.py      # (deprecated) Weekly chunk approach
│   ├── logs/
│   │   ├── reconvert/               # Small file job logs
│   │   ├── large_direct/            # Login node processor logs
│   │   ├── weekly/                  # (deprecated)
│   │   └── singlepass/              # (deprecated)
│   ├── java/                        # pb2parquet Java classes
│   ├── database/
│   │   └── conversion_log_2024.db   # SQLite tracking database
│   ├── 01-small_script.py           # Original small file converter
│   ├── 02-big_chunker_script.py     # Original big file chunker
│   └── config.ini                   # Configuration file
```

## Contact

For questions about this conversion:
- Check this document first
- Review logs in the locations above
- The singlepass_converter.py has inline comments explaining the logic
