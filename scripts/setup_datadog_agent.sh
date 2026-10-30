#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

# Configure the local Datadog Agent for use with bazel_conduit.
#
# The Agent must already have a valid API key (and site) whose org can ingest
# OTLP traces and logs. This script never sets or changes `api_key:`.
#
# What it does, in order:
#   1. Locates datadog.yaml (macOS or Linux).
#   2. Ensures an OTLP receiver is configured (gRPC + HTTP, traces + logs).
#      - If `otlp_config:` already exists, its gRPC port is reused.
#      - Otherwise a `# >>> conduit-managed >>>` block is appended, using
#        the first free port from {4317, 14317, 24317, ...}.
#   3. Restarts the agent (skip with --no-restart).
#   4. Prints what to add to user.bazelrc.
#   5. Execs `bazel run //rust/conduit:conduit` so the terminal becomes
#      the conduit process (skip with --no-conduit).
#
# Tested on macOS (launchctl) and Linux (systemctl). No extra deps beyond
# `bash`, `awk`, `lsof` (or /dev/tcp), `bazel`.

set -euo pipefail

# ---------- defaults ---------------------------------------------------------
NO_RESTART=0
NO_CONDUIT=0
DRY_RUN=0
CONDUIT_BES_PORT="${CONDUIT_BES_PORT:-8080}"
CONDUIT_EXEC_LOG="${CONDUIT_EXEC_LOG:-}"
CONDUIT_TARGET="${CONDUIT_TARGET:-//rust/conduit:conduit}"
PORT_CANDIDATES=(4317 14317 24317 34317 44317 54317)
REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

# ---------- helpers ----------------------------------------------------------
err()  { printf 'error: %s\n' "$*" >&2; exit 1; }
log()  { printf '==> %s\n' "$*" >&2; }
note() { printf '    %s\n' "$*" >&2; }

usage() {
  sed -n '5,22p' "$0" | sed 's/^# \{0,1\}//'
  cat <<'EOF'

Usage: setup_datadog_agent.sh [--no-restart] [--no-conduit] [--dry-run]

  --no-restart      Skip restarting the agent
  --no-conduit      Don't exec into `bazel run` for conduit at the end
  --dry-run         Print actions without modifying files, restarting, or running conduit
  -h, --help        Show this help

Env overrides:
  CONDUIT_BES_PORT       Port conduit listens on for BES        (default: 8080)
  CONDUIT_EXEC_LOG       Opt into per-spawn detail: path for --execution_log_compact_file=
                         and run conduit with --exec-log (default: unset, off)
  CONDUIT_TARGET         Bazel target for the conduit binary    (default: //rust/conduit:conduit)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-restart)    NO_RESTART=1; shift ;;
    --no-conduit)    NO_CONDUIT=1; shift ;;
    --dry-run)       DRY_RUN=1; shift ;;
    -h|--help)       usage; exit 0 ;;
    *)               err "unknown flag: $1 (try --help)" ;;
  esac
done

# ---------- platform detection ----------------------------------------------
case "$(uname -s)" in
  Darwin)
    DD_YAML="/opt/datadog-agent/etc/datadog.yaml"
    # The installer registers either a system LaunchDaemon or a per-user
    # LaunchAgent; `kickstart -k` needs the label already loaded.
    if launchctl print system/com.datadoghq.agent >/dev/null 2>&1; then
      RESTART_CMD=(sudo launchctl kickstart -k system/com.datadoghq.agent)
    else
      RESTART_CMD=(launchctl kickstart -k "gui/$(id -u)/com.datadoghq.agent")
    fi
    ;;
  Linux)
    DD_YAML="/etc/datadog-agent/datadog.yaml"
    RESTART_CMD=(sudo systemctl restart datadog-agent)
    ;;
  *)
    err "unsupported OS: $(uname -s)"
    ;;
esac

[[ -f "$DD_YAML" ]] || err "$DD_YAML not found -- is the Datadog Agent installed?"

# Linux config is owned by dd-agent; we'll need sudo for the in-place edit.
WRITE_AS_SUDO=0
if [[ ! -w "$DD_YAML" ]]; then
  command -v sudo >/dev/null || err "$DD_YAML is not writable and sudo is unavailable"
  WRITE_AS_SUDO=1
fi

run_priv() {
  if (( WRITE_AS_SUDO )); then sudo "$@"; else "$@"; fi
}

# ---------- step 1: discover/pick OTLP gRPC port -----------------------------
extract_existing_grpc_port() {
  # Look for the first `endpoint: <host>:<port>` under an `otlp_config:` block.
  # KISS: assume the first `endpoint:` after `otlp_config:` is the gRPC one
  # (Datadog's own example yaml lists grpc before http).
  awk '
    /^otlp_config:/        { in_otlp=1; next }
    in_otlp && /^[^[:space:]#]/ { in_otlp=0 }
    in_otlp && /^[[:space:]]+endpoint:[[:space:]]*/ {
      sub(/^[[:space:]]+endpoint:[[:space:]]*/, "")
      gsub(/["'"'"']/, "")
      n=split($0, a, ":"); print a[n]; exit
    }
  ' "$DD_YAML"
}

port_in_use() {
  local p="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -iTCP:"$p" -sTCP:LISTEN -P -n 2>/dev/null | awk 'NR>1{found=1} END{exit !found}'
  else
    (exec 3<>/dev/tcp/127.0.0.1/"$p") 2>/dev/null && { exec 3<&-; exec 3>&-; return 0; } || return 1
  fi
}

EXISTING_PORT="$(extract_existing_grpc_port || true)"
if [[ -n "$EXISTING_PORT" ]]; then
  OTLP_GRPC_PORT="$EXISTING_PORT"
  HAS_OTLP=1
  log "reusing existing otlp_config gRPC port: $OTLP_GRPC_PORT"
else
  HAS_OTLP=0
  OTLP_GRPC_PORT=""
  for p in "${PORT_CANDIDATES[@]}"; do
    if ! port_in_use "$p"; then OTLP_GRPC_PORT="$p"; break; fi
  done
  [[ -n "$OTLP_GRPC_PORT" ]] || err "no free port among ${PORT_CANDIDATES[*]}"
  log "no otlp_config in $DD_YAML; will add one on port $OTLP_GRPC_PORT"
fi
OTLP_HTTP_PORT=$((OTLP_GRPC_PORT + 1))

# ---------- step 2: append otlp_config to datadog.yaml -----------------------
# The leading blank line also terminates a last line that lacks a newline.
OTLP_BLOCK="
# >>> conduit-managed >>>
# Added by scripts/setup_datadog_agent.sh -- safe to edit or delete.
otlp_config:
  receiver:
    protocols:
      grpc:
        endpoint: 0.0.0.0:${OTLP_GRPC_PORT}
      http:
        endpoint: 0.0.0.0:${OTLP_HTTP_PORT}
  traces:
    enabled: true
  logs:
    enabled: true
logs_enabled: true
# <<< conduit-managed <<<"

SKIP_WRITE="$HAS_OTLP"
if (( SKIP_WRITE )); then
  log "datadog.yaml already has otlp_config; skipping rewrite and restart"
elif (( DRY_RUN )); then
  log "dry-run: would append to $DD_YAML:"
  printf '%s\n' "$OTLP_BLOCK"
else
  BACKUP="${DD_YAML}.bak.$(date +%Y%m%d-%H%M%S)"
  log "backing up $DD_YAML -> $BACKUP"
  run_priv cp -p "$DD_YAML" "$BACKUP"
  log "appending otlp_config to $DD_YAML"
  # Appending in place keeps the owner, mode and ACLs of a file that holds
  # the API key.
  printf '%s\n' "$OTLP_BLOCK" | run_priv tee -a "$DD_YAML" >/dev/null
fi

# ---------- step 3: restart agent --------------------------------------------
if (( SKIP_WRITE )) || (( DRY_RUN )) || (( NO_RESTART )); then
  (( SKIP_WRITE )) && note "skipping restart (config unchanged)"
  (( DRY_RUN ))    && note "skipping restart (--dry-run)"
  (( NO_RESTART )) && note "skipping restart (--no-restart)"
else
  log "restarting agent: ${RESTART_CMD[*]}"
  if ! "${RESTART_CMD[@]}"; then
    err "agent restart failed; revert with: sudo mv $BACKUP $DD_YAML"
  fi
fi

# ---------- step 4: print bazel snippet --------------------------------------
EXEC_LOG_RC=""
if [[ -n "${CONDUIT_EXEC_LOG}" ]]; then
  EXEC_LOG_RC="common --execution_log_compact_file=${CONDUIT_EXEC_LOG}"$'\n'
fi
cat <<EOF

==================================================================
Add the following to your user.bazelrc (or .bazelrc):
------------------------------------------------------------------
common --bes_backend=grpc://127.0.0.1:${CONDUIT_BES_PORT}
${EXEC_LOG_RC}# Optional: emit BEP for every action, not just the failing/test ones.
build  --build_event_publish_all_actions

Then run any bazel build/test as usual; traces and logs will land in
your Datadog org via the local agent.
==================================================================
EOF

# ---------- step 5: launch conduit -------------------------------------------
CONDUIT_ARGS=(serve --port "${CONDUIT_BES_PORT}"
              --export otlp --otlp-endpoint "http://localhost:${OTLP_GRPC_PORT}")
if [[ -n "${CONDUIT_EXEC_LOG}" ]]; then
  CONDUIT_ARGS+=(--exec-log)
fi

if (( NO_CONDUIT )) || (( DRY_RUN )); then
  cat <<EOF

To launch conduit yourself, run:
  cd ${REPO_ROOT} && bazel run ${CONDUIT_TARGET} -c opt -- ${CONDUIT_ARGS[*]}
EOF
  exit 0
fi

command -v bazel >/dev/null \
  || err "bazel not found in PATH; install bazelisk or rerun with --no-conduit"

log "launching conduit: bazel run ${CONDUIT_TARGET} -c opt -- ${CONDUIT_ARGS[*]}"
note "press Ctrl-C to stop"
cd "$REPO_ROOT"
exec bazel run "$CONDUIT_TARGET" -c opt -- "${CONDUIT_ARGS[@]}"
