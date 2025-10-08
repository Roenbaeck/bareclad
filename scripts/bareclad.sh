#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Bareclad convenience script (macOS / Linux)
# Similar intent as PowerShell bareclad.ps1: start / stop / restart the server
# with controlled logging. Can be sourced to expose functions or executed
# directly with subcommands.
# ---------------------------------------------------------------------------
# Usage examples:
#   ./scripts/bareclad.sh start                      # start with default (normal) profile
#   ./scripts/bareclad.sh start --profile verbose    # verbose logging
#   ./scripts/bareclad.sh start --log 'warn,bareclad=info' --tail
#   ./scripts/bareclad.sh restart --profile trace
#   ./scripts/bareclad.sh stop
#   source ./scripts/bareclad.sh && bareclad_start --profile verbose
#
# Profiles:
#   quiet -> RUST_LOG=error
#   normal -> RUST_LOG=info
#   verbose -> RUST_LOG=debug,bareclad=info
#   trace -> RUST_LOG=trace
# If --log is supplied it overrides the profile mapping.
#
# Flags:
#   --profile <quiet|normal|verbose|trace>
#   --log <env_filter>
#   --release            (use cargo --release)
#   --force-rebuild      (cargo clean before starting)
#   --tail               (run in foreground, do not daemonize)
#   --help
# ---------------------------------------------------------------------------
set -euo pipefail

SCRIPT_DIR="$( cd "${BASH_SOURCE[0]%/*}" && pwd )"
REPO_ROOT="$( cd "${SCRIPT_DIR}/.." && pwd )"
PID_FILE="${REPO_ROOT}/.bareclad.pid"
LOG_FILE="${REPO_ROOT}/bareclad.out"
DEFAULT_PROFILE="normal"

color() { local c="$1"; shift || true; if [[ -t 1 ]]; then case "$c" in
  red) printf '\033[31m%s\033[0m' "$*";; green) printf '\033[32m%s\033[0m' "$*";;
  yellow) printf '\033[33m%s\033[0m' "$*";; cyan) printf '\033[36m%s\033[0m' "$*";;
  *) printf '%s' "$*";; esac; else printf '%s' "$*"; fi }

set_log_env() {
  local profile="$1"; local log_override="${2:-}"; local val
  if [[ -n "${log_override}" ]]; then
    val="${log_override}"
  else
    case "$profile" in
      quiet)   val="error";;
      normal)  val="info";;
      verbose) val="debug,bareclad=info";;
      trace)   val="trace";;
      *)       val="info";;
    esac
  fi
  export RUST_LOG="$val"
  echo "[bareclad] RUST_LOG=${RUST_LOG}" >&2
}

is_running() {
  [[ -f "$PID_FILE" ]] || return 1
  local pid; pid="$(cat "$PID_FILE" 2>/dev/null || true)"; [[ -n "$pid" ]] || return 1
  if kill -0 "$pid" 2>/dev/null; then return 0; fi
  return 1
}

bareclad_start() {
  local profile="$DEFAULT_PROFILE" log_override="" release=0 force=0 tail=0
  while [[ $# -gt 0 ]]; do case "$1" in
    --profile) profile="$2"; shift 2;;
    --log) log_override="$2"; shift 2;;
    --release) release=1; shift;;
    --force-rebuild) force=1; shift;;
    --tail) tail=1; shift;;
    --help) bareclad_help; return 0;;
    *) echo "Unknown option: $1" >&2; return 1;;
  esac; done
  if is_running; then echo "$(color yellow '[bareclad] Already running') (PID $(cat "$PID_FILE"))"; return 0; fi
  set_log_env "$profile" "$log_override"
  local cargo_cmd=(cargo run --quiet)
  (( release )) && cargo_cmd=(cargo run --release --quiet)
  (( force )) && { echo "$(color yellow '[bareclad] Forcing clean build…')"; (cd "$REPO_ROOT" && cargo clean); }
  echo "$(color green '[bareclad] Starting') args: ${cargo_cmd[*]}" >&2
  if (( tail )); then
    (cd "$REPO_ROOT" && exec "${cargo_cmd[@]}")
  else
    (cd "$REPO_ROOT" && "${cargo_cmd[@]}" >"$LOG_FILE" 2>&1 & echo $! >"$PID_FILE")
    sleep 0.6
    if is_running; then
      echo "$(color green '[bareclad] Running') PID $(cat "$PID_FILE") (logs: $LOG_FILE)" >&2
    else
      echo "$(color red '[bareclad] Failed to start (see logs)')" >&2
      return 1
    fi
  fi
}

bareclad_stop() {
  if ! is_running; then echo "[bareclad] Not running"; return 0; fi
  local pid; pid="$(cat "$PID_FILE")"
  echo "$(color yellow '[bareclad] Stopping') PID $pid" >&2
  if kill "$pid" 2>/dev/null; then
    wait "$pid" 2>/dev/null || true
    rm -f "$PID_FILE"
    echo "$(color green '[bareclad] Stopped')" >&2
  else
    echo "$(color red '[bareclad] Failed to signal process')" >&2
    return 1
  fi
}

bareclad_restart() {
  local args=("$@")
  bareclad_stop || true
  bareclad_start "${args[@]}"
}

bareclad_status() {
  if is_running; then echo "[bareclad] Running (PID $(cat "$PID_FILE"))"; else echo "[bareclad] Not running"; fi
}

bareclad_tail() {
  if ! is_running; then echo "[bareclad] Not running"; return 1; fi
  tail -f "$LOG_FILE"
}

bareclad_help() {
  cat <<EOF
Bareclad helper (bash)
Commands:
  start [--profile P] [--log FILTER] [--release] [--force-rebuild] [--tail]
  stop
  restart [same flags as start]
  status
  tail            Follow log file (background mode only)
  help
Profiles: quiet | normal | verbose | trace (default: normal)
Examples:
  ./scripts/bareclad.sh start --profile verbose
  ./scripts/bareclad.sh start --log 'warn,bareclad=info'
  ./scripts/bareclad.sh restart --profile trace --force-rebuild
EOF
}

# When sourced, do not execute CLI dispatch
if [[ "${BASH_SOURCE[0]}" != "$0" ]]; then
  return 0
fi

cmd="${1:-help}"; shift || true
case "$cmd" in
  start)   bareclad_start "$@";;
  stop)    bareclad_stop;;
  restart) bareclad_restart "$@";;
  status)  bareclad_status;;
  tail)    bareclad_tail;;
  help|*)  bareclad_help;;
 esac
