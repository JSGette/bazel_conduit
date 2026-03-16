#!/usr/bin/env bash
# Full scenario: run bazel build for conduit with BEP + exec log, then produce and analyze trace.
# Use for manual/CI validation. Not a Bazel test (runs bazel build from inside repo).
#
# Usage: run_full_trace_scenario.sh [conduit_bin]
#   conduit_bin: optional path to conduit (default: bazel-bin/rust/conduit/conduit)
#
# Produces: bep.ndjson, exec.bin, trace_stdout.txt in current dir; runs analyzer.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONDUIT_BIN="${1:-${REPO_ROOT}/bazel-bin/rust/conduit/conduit}"
BEP_FILE="${REPO_ROOT}/bep.ndjson"
EXEC_LOG="${REPO_ROOT}/exec.bin"
TRACE_OUT="${REPO_ROOT}/trace_stdout.txt"

cd "${REPO_ROOT}"

# Build conduit if needed
if [[ ! -x "${CONDUIT_BIN}" ]]; then
  echo "Building conduit..."
  bazel build //rust/conduit:conduit
  CONDUIT_BIN="${REPO_ROOT}/bazel-bin/rust/conduit/conduit"
fi

# Run bazel build with BEP and exec log
echo "Running: bazel build //rust/conduit:conduit --build_event_json_file=... --execution_log_binary_file=..."
bazel build //rust/conduit:conduit \
  --build_event_json_file="${BEP_FILE}" \
  --execution_log_binary_file="${EXEC_LOG}"

# Produce trace from BEP
echo "Producing trace from BEP..."
"${CONDUIT_BIN}" --input "${BEP_FILE}" --export stdout --log-level error 2>/dev/null \
  | tee "${TRACE_OUT}" >/dev/null

# Analyze
echo "Analyzing trace..."
python3 "${SCRIPT_DIR}/analyze_trace.py" "${TRACE_OUT}"

echo "Done. BEP: ${BEP_FILE}, exec log: ${EXEC_LOG}, trace: ${TRACE_OUT}"
