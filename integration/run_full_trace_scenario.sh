#!/usr/bin/env bash
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

# End-to-end scenario: run a real bazel build through the LIVE gRPC BES path
# (the production path), capture the emitted trace, and assert structural
# invariants with analyze_trace.py.
#
# Live serve mode is used rather than `conduit replay` because it is the
# production path and also exercises the compact exec-log tailer.
#
# Usage: run_full_trace_scenario.sh [target] [profile] [port]
#   target  : bazel target to build   (default: //rust/conduit:conduit_lib)
#   profile : lightweight|full   (default: full)
#   port    : BES gRPC port           (default: 8130)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TARGET="${1:-//rust/conduit:conduit_lib}"
PROFILE="${2:-full}"
PORT="${3:-8130}"
EXEC_LOG="${REPO_ROOT}/exec.compact"
TRACE_OUT="${REPO_ROOT}/trace_stdout.txt"

cd "${REPO_ROOT}"

echo "Building conduit..."
bazel build //rust/conduit:conduit
# Stash the binary: a --disk_cache= build below re-executes actions but the
# binary itself is read from a separate output path, so no restash is needed.
CONDUIT_BIN="${REPO_ROOT}/bazel-bin/rust/conduit/conduit"

echo "Starting conduit serve on :${PORT} (profile=${PROFILE})..."
"${CONDUIT_BIN}" serve --port "${PORT}" --export stdout --exec-log \
  --trace-profile "${PROFILE}" --log-level error > "${TRACE_OUT}" 2>/dev/null &
CONDUIT_PID=$!
trap 'kill "${CONDUIT_PID}" 2>/dev/null || true' EXIT
sleep 2

echo "Running bazel build ${TARGET} against the live BES endpoint..."
bazel build "${TARGET}" \
  --bes_backend="grpc://127.0.0.1:${PORT}" \
  --build_event_publish_all_actions \
  --execution_log_compact_file="${EXEC_LOG}" \
  --bes_timeout=120s || true

# Give the batch span processor time to flush the finished invocation.
sleep 4
kill "${CONDUIT_PID}" 2>/dev/null || true
wait "${CONDUIT_PID}" 2>/dev/null || true
trap - EXIT

echo "Analyzing trace..."
python3 "${SCRIPT_DIR}/analyze_trace.py" "${TRACE_OUT}"

echo "Done. exec log: ${EXEC_LOG}, trace: ${TRACE_OUT}"
