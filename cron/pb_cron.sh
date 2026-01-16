#!/usr/bin/env bash
# Data_Format_Conversion/cron/pb_cron.sh

# If executed (not sourced), enable strict mode
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  set -Eeuo pipefail
fi

# Resolve repo root from this script path
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RUN_ONCE_PY="$REPO_ROOT/daemon/pb_conversion_once.py"

# Config path: prefer env override, else repo config
DEFAULT_CFG="$REPO_ROOT/config.ini"

# Small helper: parse needed values from config.ini
# Usage: py_cfg <config_path>
# Prints 4 lines:
#   1) schedule_mode
#   2) live_poll_seconds
#   3) daily_time
#   4) daemon_logs (may be empty)
py_cfg() {
  local cfg_path="${1:-$DEFAULT_CFG}"
  python3 - "$cfg_path" <<'PY'
import sys, configparser

cfg_path = sys.argv[1]
# Enable inline comment stripping
cp = configparser.ConfigParser(inline_comment_prefixes=("#",";"))
cp.read(cfg_path)

def get(sec, key, default=""):
    v = cp.get(sec, key, fallback=str(default))
    return (v or str(default)).strip()

mode   = get("daemon", "schedule_mode", "interval").lower()
poll   = get("daemon", "live_poll_seconds", "600")
daily  = get("daemon", "daily_time", "13:00")
dlogs  = get("paths",  "daemon_logs", "")

print(mode)   # 1
print(poll)   # 2
print(daily)  # 3
print(dlogs)  # 4
PY
}

_need() {
  command -v "$1" >/dev/null 2>&1 || { echo "Missing required command: $1"; _finish 1; }
}

_finish() {
  local rc="${1:-0}"
  if [[ "${BASH_SOURCE[0]}" != "$0" ]]; then
    return "$rc"
  else
    exit "$rc"
  fi
}

install() {
  _need crontab
  _need flock
  _need python3

  # Resolve config path now
  local CONFIG_PATH="${PVPIPE_CONFIG:-$DEFAULT_CFG}"
  [[ -f "$CONFIG_PATH" ]] || { echo "config.ini not found: $CONFIG_PATH"; _finish 1; }

  mapfile -t LINES < <(py_cfg "$CONFIG_PATH")
  local mode="${LINES[0]:-interval}"
  local poll_s="${LINES[1]:-600}"
  local daily_time="${LINES[2]:-13:00}"
  local cfg_daemon_logs="${LINES[3]:-}"

  # Daemon log dir: config wins, else env, else repo default
  local DAEMON_LOG_DIR
  if [[ -n "$cfg_daemon_logs" ]]; then
    DAEMON_LOG_DIR="$cfg_daemon_logs"
  elif [[ -n "${DAEMON_LOG_DIR:-}" ]]; then
    DAEMON_LOG_DIR="$DAEMON_LOG_DIR"
  else
    DAEMON_LOG_DIR="$REPO_ROOT/daemon/logs"
  fi

  local LOCK_DIR="$REPO_ROOT/cron/locks"
  local LOCK_FILE="$LOCK_DIR/pb_daemon.lock"
  local SHELL_BIN="/bin/bash"

  mkdir -p "$LOCK_DIR" "$DAEMON_LOG_DIR"

  # Bake absolute paths into the cron command
  local RUN_SELF="$SHELL_BIN $REPO_ROOT/cron/pb_cron.sh __run"
  local CRON_CMD="$SHELL_BIN -lc 'export PVPIPE_CONFIG=\"$CONFIG_PATH\"; export DAEMON_LOG_DIR=\"$DAEMON_LOG_DIR\"; mkdir -p \"$LOCK_DIR\"; touch \"$LOCK_FILE\"; flock -n \"$LOCK_FILE\" -c \"$RUN_SELF\"; rc=\$?; rm -f \"$LOCK_FILE\"; exit \$rc'"

  local tmp
  tmp="$(mktemp)"
  crontab -l 2>/dev/null | grep -v "PBPIPE_CRON" > "$tmp" || true

  if [[ "$mode" == "daily" ]]; then
    # Support multiple comma-separated times (e.g., "09:12,21:12")
    local idx=0
    IFS=',' read -ra TIMES <<< "$daily_time"
    for t in "${TIMES[@]}"; do
      t="${t// /}"  # trim spaces
      IFS=: read -r HH MM <<<"$t"
      [[ -z "$HH" || -z "$MM" ]] && { echo "Bad daily_time '$t' in config"; _finish 2; }
      local spec="$MM $HH * * *"
      if (( idx == 0 )); then
        echo "$spec $CRON_CMD # PBPIPE_CRON" >> "$tmp"
      else
        echo "$spec $CRON_CMD # PBPIPE_CRON_${idx}" >> "$tmp"
      fi
      echo "Installed cron: $spec"
      ((idx++))
    done
  else
    local poll="${poll_s%%.*}"
    [[ "$poll" =~ ^[0-9]+$ ]] || poll=600
    local minutes=$(( (poll + 59) / 60 ))
    (( minutes < 1 )) && minutes=1
    if (( minutes == 1 )); then
      local spec="* * * * *"
    else
      local spec="*/${minutes} * * * *"
    fi
    echo "$spec $CRON_CMD # PBPIPE_CRON" >> "$tmp"
    echo "Installed cron: $spec"
  fi

  crontab "$tmp"
  rm -f "$tmp"

  echo "Daemon logs: $DAEMON_LOG_DIR"
}

delete() {
  _need crontab
  local tmp
  tmp="$(mktemp)"
  crontab -l 2>/dev/null | grep -v "PBPIPE_CRON" > "$tmp" || true
  crontab "$tmp" || true
  rm -f "$tmp"
  echo "Removed cron entry PBPIPE_CRON"
  _finish 0
}

__run() {
  # Resolve paths again inside the cron tick
  local CONFIG_PATH="${PVPIPE_CONFIG:-$DEFAULT_CFG}"
  mapfile -t LINES < <(py_cfg "$CONFIG_PATH")
  local cfg_daemon_logs="${LINES[3]:-}"

  local DAEMON_LOG_DIR
  if [[ -n "$cfg_daemon_logs" ]]; then
    DAEMON_LOG_DIR="$cfg_daemon_logs"
  elif [[ -n "${DAEMON_LOG_DIR:-}" ]]; then
    DAEMON_LOG_DIR="$DAEMON_LOG_DIR"
  else
    DAEMON_LOG_DIR="$REPO_ROOT/daemon/logs"
  fi
  mkdir -p "$DAEMON_LOG_DIR"

  local ts logfile
  ts="$(date +%F_%H%M%S)"
  logfile="$DAEMON_LOG_DIR/daemon_${ts}.log"

  {
    echo "[CRON] start $(date -Is)"
    echo "[CRON] repo=$REPO_ROOT"
    echo "[CRON] cfg=$CONFIG_PATH"
    echo "[CRON] log=$logfile"

    export PVPIPE_CONFIG="$CONFIG_PATH"
    export PYTHONUNBUFFERED=1
    export PYTHONPATH="$REPO_ROOT:${PYTHONPATH:-}"
    export PATH="/usr/share/Modules/bin:/usr/local/bin:/usr/bin:/bin:$PATH"

    if [[ -f /etc/profile.d/lsf.sh ]]; then source /etc/profile.d/lsf.sh || true; fi
    for d in /opt/ibm/lsf /opt/lsf /usr/local/lsf /usr/share/lsf; do
      [[ -f "$d/conf/profile.lsf" ]] && { source "$d/conf/profile.lsf" || true; break; }
    done

    echo "[CRON] which bsub: $(command -v bsub || echo 'NOT FOUND')"
    echo "[CRON] running once: $RUN_ONCE_PY"
    
    # Use virtual environment Python if available
    VENV_PYTHON="$REPO_ROOT/../env/bin/python3"
    if [[ -x "$VENV_PYTHON" ]]; then
      echo "[CRON] Using venv Python: $VENV_PYTHON"
      "$VENV_PYTHON" -u "$RUN_ONCE_PY"
    else
      echo "[CRON] Venv not found, using system python3"
      python3 -u "$RUN_ONCE_PY"
    fi
    rc=$?
    echo "[CRON] end $(date -Is) rc=$rc"

    # NEW: Push to GitLab if export succeeded
    if [[ $rc -eq 0 ]]; then
      PUBLIC_DIR="$REPO_ROOT/public"
      if [[ -d "$PUBLIC_DIR" ]] && [[ -f "$PUBLIC_DIR/index.html" ]]; then
        echo "[CRON] Committing dashboard changes"
        cd "$PUBLIC_DIR" || exit 1
        git add -A
        if git diff --staged --quiet; then
          echo "[CRON] No changes to commit"
        else
          git commit -m "Update dashboard $(date -Iseconds)"
          git push -uf origin main || echo "[CRON] Git push failed"
        fi
      fi
    fi

    # Best-effort cleanup of cron lock dir (the per-run lock file is removed by the caller)
    rmdir "$REPO_ROOT/cron/locks" 2>/dev/null || true
    exit "$rc"
  } >>"$logfile" 2>&1
}

case "${1:-}" in
  install) install ;;
  delete)  delete ;;
  __run)   __run  ;;
  *) echo "Usage: $(basename "$0") {install|delete}"; _finish 1 ;;
esac