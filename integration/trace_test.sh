#!/usr/bin/env bash
# Integration test: run conduit on BEP input, export trace to stdout, analyze for regressions.
# Usage: trace_test.sh <conduit_bin> <bep_file> <analyzer_py> [trace_out]
#   conduit_bin: path to conduit binary
#   bep_file: path to BEP NDJSON file
#   analyzer_py: path to analyze_trace.py
#   trace_out: optional path to write trace stdout (default: $TEST_TMPDIR/trace_stdout.txt or ./trace_stdout.txt)

set -euo pipefail

CONDUIT_BIN="${1:?conduit binary path required}"
BEP_FILE="${2:?BEP file path required}"
ANALYZER="${3:?analyzer script path required}"
TRACE_OUT="${4:-${TEST_TMPDIR:-.}/trace_stdout.txt}"

# Run conduit: BEP file -> stdout (trace), capture to file
"${CONDUIT_BIN}" --input "${BEP_FILE}" --export stdout --log-level error 2>/dev/null \
  | tee "${TRACE_OUT}" >/dev/null

# Analyze trace for structure / regression
python3 "${ANALYZER}" "${TRACE_OUT}"
