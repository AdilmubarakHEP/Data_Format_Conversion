#!/usr/bin/env bash
# Data_Format_Conversion/cron/pb_cron.sh
#
# Multi-config cron manager for pb conversion pipeline.
#
# Usage:
#   pb_cron.sh install <name|all>   Install cron entries for the given config(s)
#   pb_cron.sh delete  <name|all>   Remove cron entries for the given config(s)
#   pb_cron.sh list                 List available config files
#   pb_cron.sh status               Show active cron entries
#
# Config files live in  <repo>/configs/config_<name>.ini
# Each config gets its own lock file and cron tag (PBPIPE_CRON_<NAME>).

# If executed (not sourced), enable strict mode
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  set -Eeuo pipefail
fi

# Resolve repo root from this script path
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RUN_ONCE_PY="$REPO_ROOT/daemon/pb_conversion_once.py"

CONFIGS_DIR="$REPO_ROOT/configs"

# ─── helpers ───────────────────────────────────────────────────────────

# py_cfg <config_path>  →  prints 4 lines: schedule_mode, poll_s, daily_time, daemon_logs
py_cfg() {
  local cfg_path="$1"
  python3 - "$cfg_path" <<'PY'
import sys, configparser

cfg_path = sys.argv[1]
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

# Return the config path for a given name (e.g. "2025c" → configs/config_2025c.ini)
_config_path() {
  local name="$1"
  echo "$CONFIGS_DIR/config_${name}.ini"
}

# Return cron tag for a given name
_cron_tag() {
  local name="$1"
  echo "PBPIPE_CRON_${name}"
}

# List available config names (strip prefix/suffix from filenames)
_available_configs() {
  local names=()
  for f in "$CONFIGS_DIR"/config_*.ini; do
    [[ -f "$f" ]] || continue
    local base
    base="$(basename "$f")"
    base="${base#config_}"
    base="${base%.ini}"
    names+=("$base")
  done
  echo "${names[@]}"
}

# ─── install one config ───────────────────────────────────────────────

_install_one() {
  local name="$1"
  local CONFIG_PATH
  CONFIG_PATH="$(_config_path "$name")"
  [[ -f "$CONFIG_PATH" ]] || { echo "Config not found: $CONFIG_PATH"; _finish 1; }

  mapfile -t LINES < <(py_cfg "$CONFIG_PATH")
  local mode="${LINES[0]:-interval}"
  local poll_s="${LINES[1]:-600}"
  local daily_time="${LINES[2]:-13:00}"
  local cfg_daemon_logs="${LINES[3]:-}"

  # Daemon log dir
  local DAEMON_LOG_DIR
  if [[ -n "$cfg_daemon_logs" ]]; then
    DAEMON_LOG_DIR="$cfg_daemon_logs"
  else
    DAEMON_LOG_DIR="$REPO_ROOT/daemon/logs"
  fi

  local LOCK_DIR="$REPO_ROOT/cron/locks"
  local LOCK_FILE="$LOCK_DIR/pb_daemon_${name}.lock"
  local SHELL_BIN="/bin/bash"
  local TAG
  TAG="$(_cron_tag "$name")"

  mkdir -p "$LOCK_DIR" "$DAEMON_LOG_DIR"

  local RUN_SELF="$SHELL_BIN $REPO_ROOT/cron/pb_cron.sh __run $name"
  local CRON_CMD="$SHELL_BIN -lc 'export PVPIPE_CONFIG=\"$CONFIG_PATH\"; export DAEMON_LOG_DIR=\"$DAEMON_LOG_DIR\"; mkdir -p \"$LOCK_DIR\"; touch \"$LOCK_FILE\"; flock -n \"$LOCK_FILE\" -c \"$RUN_SELF\"; rc=\$?; rm -f \"$LOCK_FILE\"; exit \$rc'"

  # Remove existing entries for this config
  local tmp
  tmp="$(mktemp)"
  crontab -l 2>/dev/null | grep -v "$TAG" > "$tmp" || true

  if [[ "$mode" == "daily" ]]; then
    local idx=0
    IFS=',' read -ra TIMES <<< "$daily_time"
    for t in "${TIMES[@]}"; do
      t="${t// /}"
      IFS=: read -r HH MM <<<"$t"
      [[ -z "$HH" || -z "$MM" ]] && { echo "Bad daily_time '$t' in config for $name"; rm -f "$tmp"; _finish 2; }
      local spec="$MM $HH * * *"
      if (( idx == 0 )); then
        echo "$spec $CRON_CMD # ${TAG}" >> "$tmp"
      else
        echo "$spec $CRON_CMD # ${TAG}_${idx}" >> "$tmp"
      fi
      echo "[$name] Installed cron: $spec"
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
    echo "$spec $CRON_CMD # ${TAG}" >> "$tmp"
    echo "[$name] Installed cron: $spec"
  fi

  crontab "$tmp"
  rm -f "$tmp"
  echo "[$name] Daemon logs: $DAEMON_LOG_DIR"
}

# ─── delete one config ────────────────────────────────────────────────

_delete_one() {
  local name="$1"
  local TAG
  TAG="$(_cron_tag "$name")"
  local tmp
  tmp="$(mktemp)"
  crontab -l 2>/dev/null | grep -v "$TAG" > "$tmp" || true
  crontab "$tmp" || true
  rm -f "$tmp"
  echo "[$name] Removed cron entries"
}

# ─── __run (called by cron) ──────────────────────────────────────────

__run() {
  local name="${1:-}"
  local CONFIG_PATH="${PVPIPE_CONFIG:-}"

  # If name given, resolve config path from it
  if [[ -n "$name" ]]; then
    CONFIG_PATH="$(_config_path "$name")"
  fi

  # Fallback to PVPIPE_CONFIG or root config.ini
  if [[ -z "$CONFIG_PATH" || ! -f "$CONFIG_PATH" ]]; then
    CONFIG_PATH="$REPO_ROOT/config.ini"
  fi

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

  local label="${name:-unknown}"
  local ts logfile
  ts="$(date +%F_%H%M%S)"
  logfile="$DAEMON_LOG_DIR/daemon_${label}_${ts}.log"

  {
    echo "[CRON] start $(date -Is)"
    echo "[CRON] config=$label"
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

    # Push to GitLab if export succeeded
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

    # Best-effort cleanup
    rmdir "$REPO_ROOT/cron/locks" 2>/dev/null || true
    exit "$rc"
  } >>"$logfile" 2>&1
}

# ─── main dispatch ────────────────────────────────────────────────────

_usage() {
  cat <<EOF
Usage: $(basename "$0") <command> [name]

Commands:
  install <name|all>   Install cron entries for config(s)
  delete  <name|all>   Remove cron entries for config(s)
  list                 List available config files in configs/
  status               Show active PBPIPE cron entries

Available configs: $(_available_configs)

Examples:
  $(basename "$0") install 2025c      # install just 2025c
  $(basename "$0") install 2026ab     # install just 2026ab
  $(basename "$0") install all        # install both
  $(basename "$0") delete  2026ab     # remove just 2026ab
  $(basename "$0") delete  all        # remove all
EOF
  _finish 1
}

cmd="${1:-}"
target="${2:-}"

case "$cmd" in
  install)
    _need crontab
    _need flock
    _need python3
    [[ -z "$target" ]] && { echo "Error: specify a config name or 'all'"; _usage; }
    if [[ "$target" == "all" ]]; then
      for name in $(_available_configs); do
        _install_one "$name"
      done
    else
      _install_one "$target"
    fi
    echo ""
    echo "Done. Verify with: crontab -l"
    ;;
  delete)
    _need crontab
    [[ -z "$target" ]] && { echo "Error: specify a config name or 'all'"; _usage; }
    if [[ "$target" == "all" ]]; then
      for name in $(_available_configs); do
        _delete_one "$name"
      done
    else
      _delete_one "$target"
    fi
    echo ""
    echo "Done. Verify with: crontab -l"
    ;;
  list)
    echo "Available configs in $CONFIGS_DIR:"
    for name in $(_available_configs); do
      echo "  $name  →  $(_config_path "$name")"
    done
    ;;
  status)
    echo "Active PBPIPE cron entries:"
    crontab -l 2>/dev/null | grep "PBPIPE_CRON" || echo "  (none)"
    ;;
  __run)
    __run "$target"
    ;;
  *)
    _usage
    ;;
esac
