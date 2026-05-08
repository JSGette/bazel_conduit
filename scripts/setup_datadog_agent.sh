#!/usr/bin/env bash
# Configure the local Datadog Agent for use with bazel_conduit.
#
# What it does, in order:
#   1. Runs `dd-auth` to fetch a personal API key (and site).
#   2. Locates datadog.yaml (macOS or Linux).
#   3. Writes the API key into the file's top-level `api_key:` (idempotent).
#   4. Ensures an OTLP receiver is configured (gRPC + HTTP, traces + logs).
#      - If `otlp_config:` already exists, its gRPC port is reused.
#      - Otherwise a `# >>> conduit-managed >>>` block is appended, using
#        the first free port from {4317, 14317, 24317, ...}.
#   5. Restarts the agent (skip with --no-restart).
#   6. Prints what to add to user.bazelrc.
#   7. Execs `bazel run //rust/conduit:conduit` so the terminal becomes
#      the conduit process (skip with --no-conduit).
#
# Tested on macOS (launchctl) and Linux (systemctl). No extra deps beyond
# `bash`, `awk`, `grep`, `sed`, `lsof` (or /dev/tcp), `dd-auth`, `bazel`.

set -euo pipefail

# ---------- defaults ---------------------------------------------------------
DOMAIN="app.datadoghq.com"
NO_RESTART=0
NO_CONDUIT=0
DRY_RUN=0
CONDUIT_BES_PORT="${CONDUIT_BES_PORT:-8080}"
CONDUIT_EXEC_LOG="${CONDUIT_EXEC_LOG:-/tmp/conduit-exec.log}"
CONDUIT_TARGET="${CONDUIT_TARGET:-//rust/conduit:conduit}"
PORT_CANDIDATES=(4317 14317 24317 34317 44317 54317)
REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

# ---------- helpers ----------------------------------------------------------
err()  { printf 'error: %s\n' "$*" >&2; exit 1; }
log()  { printf '==> %s\n' "$*" >&2; }
note() { printf '    %s\n' "$*" >&2; }

usage() {
  sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'
  cat <<'EOF'

Usage: setup_datadog_agent.sh [--domain DOMAIN] [--no-restart] [--no-conduit] [--dry-run]

  --domain DOMAIN   Datadog org domain passed to dd-auth (default: app.datadoghq.com)
  --no-restart      Skip restarting the agent
  --no-conduit      Don't exec into `bazel run` for conduit at the end
  --dry-run         Print actions without modifying files, restarting, or running conduit
  -h, --help        Show this help

Env overrides:
  CONDUIT_BES_PORT       Port conduit listens on for BES        (default: 8080)
  CONDUIT_EXEC_LOG       Path for --execution_log_compact_file= (default: /tmp/conduit-exec.log)
  CONDUIT_TARGET         Bazel target for the conduit binary    (default: //rust/conduit:conduit)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --domain)     DOMAIN="$2"; shift 2 ;;
    --no-restart) NO_RESTART=1; shift ;;
    --no-conduit) NO_CONDUIT=1; shift ;;
    --dry-run)    DRY_RUN=1; shift ;;
    -h|--help)    usage; exit 0 ;;
    *)            err "unknown flag: $1 (try --help)" ;;
  esac
done

# ---------- platform detection ----------------------------------------------
case "$(uname -s)" in
  Darwin)
    DD_YAML="/opt/datadog-agent/etc/datadog.yaml"
    RESTART_CMD=(datadog-agent restart-service)
    NEEDS_SUDO=0
    ;;
  Linux)
    DD_YAML="/etc/datadog-agent/datadog.yaml"
    RESTART_CMD=(sudo systemctl restart datadog-agent)
    NEEDS_SUDO=1
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

# ---------- step 1: dd-auth --------------------------------------------------
command -v dd-auth >/dev/null || err "dd-auth not found in PATH"

log "fetching credentials via dd-auth (--domain $DOMAIN)"
# `dd-auth -o` prints DD_API_KEY=..., DD_APP_KEY=..., DD_SITE=... -- one per line.
DD_AUTH_OUT="$(dd-auth --domain "$DOMAIN" -o)" \
  || err "dd-auth failed; run 'dd-auth --domain $DOMAIN -o' manually to debug"

# Parse without exporting DD_APP_KEY into our env (we don't need it).
DD_API_KEY="$(printf '%s\n' "$DD_AUTH_OUT" | awk -F= '$1=="DD_API_KEY"{print $2; exit}')"
DD_SITE="$(  printf '%s\n' "$DD_AUTH_OUT" | awk -F= '$1=="DD_SITE"  {print $2; exit}')"
[[ -n "$DD_API_KEY" ]] || err "dd-auth did not return DD_API_KEY"
[[ -n "$DD_SITE"    ]] || DD_SITE="datadoghq.com"

# ---------- step 2: discover/pick OTLP gRPC port -----------------------------
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

# ---------- step 3: edit datadog.yaml (atomic) -------------------------------
TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT

# Replace top-level `api_key:` value (or append if missing).
# Then ensure an otlp_config block.
awk -v key="$DD_API_KEY" -v site="$DD_SITE" -v has_otlp="$HAS_OTLP" \
    -v grpc_port="$OTLP_GRPC_PORT" -v http_port="$OTLP_HTTP_PORT" '
  BEGIN { api_done=0; site_done=0 }
  /^api_key:/ && !api_done { print "api_key: " key; api_done=1; next }
  /^site:/    && !site_done { print "site: " site; site_done=1; next }
  { print }
  END {
    if (!api_done) print "api_key: " key
    if (!site_done) print "site: " site
    if (has_otlp == "0") {
      print ""
      print "# >>> conduit-managed >>>"
      print "# Added by scripts/setup_datadog_agent.sh -- safe to edit or delete."
      print "otlp_config:"
      print "  receiver:"
      print "    protocols:"
      print "      grpc:"
      print "        endpoint: 0.0.0.0:" grpc_port
      print "      http:"
      print "        endpoint: 0.0.0.0:" http_port
      print "  traces:"
      print "    enabled: true"
      print "  logs:"
      print "    enabled: true"
      print "logs_enabled: true"
      print "# <<< conduit-managed <<<"
    }
  }
' "$DD_YAML" > "$TMP"

if (( DRY_RUN )); then
  log "dry-run: would write the following diff to $DD_YAML"
  diff -u "$DD_YAML" "$TMP" || true
  log "dry-run: skipping restart and exit"
else
  BACKUP="${DD_YAML}.bak.$(date +%Y%m%d-%H%M%S)"
  log "backing up $DD_YAML -> $BACKUP"
  run_priv cp -p "$DD_YAML" "$BACKUP"
  log "writing $DD_YAML"
  run_priv install -m 0644 "$TMP" "$DD_YAML"
fi

# ---------- step 4: restart agent --------------------------------------------
if (( NO_RESTART )) || (( DRY_RUN )); then
  note "skipping restart (--no-restart or --dry-run)"
else
  log "restarting agent: ${RESTART_CMD[*]}"
  if ! "${RESTART_CMD[@]}"; then
    err "agent restart failed; revert with: run_priv mv $BACKUP $DD_YAML"
  fi
fi

# ---------- step 5: print bazel snippet --------------------------------------
cat <<EOF

==================================================================
Add the following to your user.bazelrc (or .bazelrc):
------------------------------------------------------------------
common --bes_backend=grpc://localhost:${CONDUIT_BES_PORT}
common --execution_log_compact_file=${CONDUIT_EXEC_LOG}
# Optional: emit BEP for every action, not just the failing/test ones.
build  --build_event_publish_all_actions

Then run any bazel build/test as usual; traces and logs will land in
your Datadog org via the local agent (site: ${DD_SITE}).
==================================================================
EOF

# ---------- step 6: launch conduit -------------------------------------------
CONDUIT_ARGS=(--serve --port "${CONDUIT_BES_PORT}"
              --export otlp --otlp-endpoint "http://localhost:${OTLP_GRPC_PORT}")

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
