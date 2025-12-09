# Data Format Conversion

## Overview

Automated pipeline to convert EPICS Archiver `.pb` files to `.parquet`, with batching, LSF `bsub` submission, status tracking in SQLite, a background daemon, and a small monitoring dashboard.

---

## Scripts and roles

| Path | What it does |
|---|---|
| `00-submit_pb2parquet_jobs.py` | Scans for `.pb` files, skips ones already in the DB, builds bundles, and submits jobs with `bsub`. Marks files as `queued` in `conversion_log.db` at submit time so they are not resubmitted while in flight. |
| `01-small_script.py` | Worker for small `.pb` bundles. Converts each `.pb` to `.parquet`. Emits log lines like `SAVED: /.../name.parquet`. |
| `02-big_chunker_script.py` | Worker for large `.pb` files. Streams and writes chunked parquet files. Emits `SAVED CHUNK:` lines. |
| `03-merge_parquet.py` | Merges per-PV chunks then daily files. Supports date split and unsplit modes. |
| `04-recompute_submission_summaries.py` | Rebuilds per submission summary JSONs. Reads logs and recomputes sizes, counts, wall time, RAM. |
| `daemon/pb_conversion_daemon.py` | Background loop. Calls the submitter, parses logs, updates DB, detects job completion, triggers merges, and tracks `.pb` size changes. |
| `daemon/monitor_dashboard.py` | Flask + Plotly dashboard that shows recent submissions, 7-day bars, all-time pie, and type breakdowns. |
| `logs/` | Timestamped log folders per submission. Files like `small_bundle_0.log`. The daemon also writes `submission_summary_<TS>.json` in `logs/pb2parquet`. |

If you still keep older helpers like `03-retry_failed_pb_files.py` or `06-inspect_pb_to_tsv.py`, they continue to work, but are optional.

---

## Directories and defaults

- Input roots commonly used  
  - Yearly layout: `/home/belle2/amubarak/2024`  
  - Dated layout: `/home/belle2/amubarak/2024c`

- Output parquet (per file or chunk)  
  - `/home/belle2/amubarak/PV_Output/converted_flat2`

- Merged outputs  
  - Daily: `/home/belle2/amubarak/PV_Output/merged/YYYY_MM_DD.parquet`

- Logs and DB  
  - Logs: `/home/belle2/amubarak/Data_Format_Conversion/logs/pb2parquet/<timestamp>/`  
  - Submission summaries: `/home/belle2/amubarak/Data_Format_Conversion/logs/pb2parquet/submission_summary_<timestamp>.json`  
  - DB: `/home/belle2/amubarak/conversion_log.db`

---

## Requirements

- Python 3.8+
- LSF `bsub`
- `sqlite3`
- `pyarrow`
- `Flask`, `plotly` for the dashboard

Install minimal dependencies:
```bash
pip install pyarrow flask plotly
```

---

## Quick start

### Option A. Run the daemon
Watches for `.pb` arrivals, submits conversion jobs, parses logs, and triggers merges.
```bash
nohup python3 daemon/pb_conversion_daemon.py > daemon.log 2>&1 &
```

### Option B. One manual submission
Useful for testing on a directory.
```bash
python3 00-submit_pb2parquet_jobs.py   --input_dir "/home/belle2/amubarak/2024"   --chunk_size 100   --big_threshold_gb 5   --db_path "/home/belle2/amubarak/conversion_log.db"   --queue h   --submit
```

---

## Key behavior

### Queue marking to prevent re-submission
- `00-submit_pb2parquet_jobs.py` inserts or updates rows in `conversions` as `queued` for every `.pb` it submits.
- Subsequent runs skip any `.pb` already present in the DB.
- The daemon later parses logs and flips `queued` to `success`, `failed`, or `skipped`.

Minimal schema used:
```sql
CREATE TABLE IF NOT EXISTS conversions (
  pb_path   TEXT PRIMARY KEY,
  timestamp TEXT,
  status    TEXT,    -- queued, success, failed, skipped
  message   TEXT
);
```

### Split modes
- `--split_mode auto` decides based on path and filenames.  
  - Paths with `2024c` are treated as dated PBs (no split).  
  - Plain `2024` is treated as yearly PBs (date split).
- You can force with `--split_mode on|off` or the legacy `--split_by_date` flag.
- Date granularity: `--split_granularity day|week|month`.
- Extras that forward when splitting: `--tz_offset_hours`, `--allow_out_of_order`, `--ooo_max_open_buckets`, `--emit_date_view`, `--date_view_root`.

### Routing by size or path
- Files ≥ `--big_threshold_gb` go to the big chunker.
- `--force_big_subpath` can route specific subpaths to the big chunker.
- `--exclude_subpath` and `--exclude_filelist` skip irrelevant subtrees.

---

## Useful commands

Count how many were saved in a given submission:
```bash
grep -c '^SAVED:' /home/belle2/amubarak/Data_Format_Conversion/logs/pb2parquet/20250901_183340/small_bundle_0.log
```

Check DB status counts:
```bash
sqlite3 /home/belle2/amubarak/conversion_log.db 'select status, count(*) from conversions group by status;'
```

Spot one PB:
```bash
sqlite3 /home/belle2/amubarak/conversion_log.db "select pb_path, status, timestamp from conversions
 where pb_path like '%BT_D08_1/CH27:2024.pb%';"
```

Recompute a summary after jobs finish:
```bash
python3 04-recompute_submission_summaries.py
```

Trigger merges explicitly if needed:
```bash
python3 03-merge_parquet.py --mode auto
```

---

## Monitoring dashboard

Start it:
```bash
python3 daemon/monitor_dashboard.py
# then open http://localhost:7860
# or tunnel: ssh -L 7860:localhost:7860 <host>
```

What the dashboard shows:
- Recent submission summaries
- 7-day bar chart of successes and failures
- All-time pie chart
- Type breakdown  
  - `numeric_scalar`, `numeric_vector`, `text_scalar`, `text_vector`, `unknown`  
  - This is a per-file classification based on the converter’s output schema  
  - Large counts can occur when a single PV is date-split into many daily parquet files

---

## Output layout

Per PB or per chunk parquet under:
```
/home/belle2/amubarak/PV_Output/converted_flat2/AA/BT/.../PV-2024-YYYY_MM_DD.parquet
```

Merged outputs:
```
/home/belle2/amubarak/PV_Output/merged/YYYY_MM_DD.parquet
```

---

## Housekeeping flags

Inside the daemon you can control:
```python
DELETE_CHUNKS_AFTER_PV_MERGE = False
DELETE_PARQUET_AFTER_DATE   = False
```
Set to `True` if you want to delete intermediates after successful merges.

---

## Troubleshooting

- File keeps getting reconverted  
  - Ensure `00-submit_pb2parquet_jobs.py` marks it as `queued` at submission time.  
  - Confirm your DB has the PB row after submit, before workers finish.

- Dashboard shows a very large `numeric_scalar` count  
  - It is counting parquet files saved in that submission, not unique PVs.  
  - Date split will produce many small files.

- Some logs not mapped back to PBs  
  - Ensure workers print canonical lines:  
    - small: `SAVED: /abs/path/out.parquet`  
    - big: `SAVED CHUNK: /abs/path/out_chunk0001.parquet`

---

## Notes

- Use `--submit` only after a quick dry run.
- The daemon runs submissions repeatedly, so the queue-mark in DB prevents duplicates between cycles.
- For retries you can filter by `status='failed'` and resubmit those PBs if you keep a helper script.

---

## Acknowledgements

> Original `.pb → .tsv` and `.tsv → .parquet` scripts were developed by Daniel and Philipp.  
> Source: https://gitlab.desy.de/philipp.horak/epics-python-conversion/-/tree/main?ref_type=heads

---

## Author

Adil Mubarak  
Ozaki Program @ KEK, 2025