#!/usr/bin/env python3
# Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
# This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

"""Structural invariant checker for conduit `--export stdout` traces.

Parses the OTLP-JSON lines (one export request per line) and asserts the
properties a valid Datadog trace must hold:

  * exactly one root span, named `bazel ...`, single trace id;
  * no dangling parents (every ParentSpanId resolves);
  * temporal containment (child fully inside its parent's window);
  * non-negative and non-absurd durations, no pre-2001 (epoch) starts;
  * type-specific attributes present (target label, action/spawn mnemonic,
    root invocation id + command).

Exit code is non-zero if any hard invariant is violated, so this doubles as a
CI gate. Missing attributes are warnings, not failures.
"""
import json
import sys
from datetime import datetime, timezone

ZERO = "0000000000000000"
DAY_MS = 24 * 3600 * 1000
STATUS = {0: "Unset", 1: "Ok", 2: "Error"}


def _ts(nanos):
    return datetime.fromtimestamp(int(nanos) / 1e9, timezone.utc) if nanos else None


def _attrs(kvs):
    return {kv["key"]: str(next(iter(kv["value"].values()), "")) for kv in kvs}


def parse_spans(text):
    """Collect every span from the `resourceSpans` lines; log lines are skipped."""
    spans = []
    for line in text.splitlines():
        if not line.startswith("{"):
            continue
        for rs in json.loads(line).get("resourceSpans", []):
            for ss in rs.get("scopeSpans", []):
                for s in ss.get("spans", []):
                    status = s.get("status", {})
                    spans.append({
                        "name": s["name"],
                        "trace_id": s["traceId"],
                        "span_id": s["spanId"],
                        "parent": s.get("parentSpanId"),
                        "start": _ts(s.get("startTimeUnixNano")),
                        "end": _ts(s.get("endTimeUnixNano")),
                        "status": STATUS.get(status.get("code", 0)),
                        "attrs": _attrs(s.get("attributes", [])),
                    })
    return spans


def kind(name):
    if name.startswith(("bazel build", "bazel test", "bazel ")):
        return "root"
    for k in ("target", "action", "spawn", "skipped targets"):
        if name.startswith(k):
            return k
    return "other"


def check(path):
    spans = parse_spans(open(path).read())
    errors, warns = [], []
    by_id = {s["span_id"]: s for s in spans}

    roots = [s for s in spans if not s["parent"] or s["parent"] == ZERO]
    if len(roots) != 1:
        errors.append(f"expected exactly 1 root, got {len(roots)}: "
                      f"{[r['name'] for r in roots]}")
    root = roots[0] if roots else None
    if root and not root["name"].startswith("bazel"):
        errors.append(f"root name not 'bazel ...': {root['name']!r}")

    trace_ids = {s["trace_id"] for s in spans if s["trace_id"]}
    if len(trace_ids) > 1:
        errors.append(f"multiple trace ids: {trace_ids}")

    for s in spans:
        n = s["name"]
        if s["start"] is None or s["end"] is None:
            errors.append(f"[{n}] missing start/end")
            continue
        dur_ms = (s["end"] - s["start"]).total_seconds() * 1000
        if dur_ms < 0:
            errors.append(f"[{n}] negative duration {dur_ms:.3f}ms")
        if s["start"].year < 2001:
            errors.append(f"[{n}] start before 2001 (epoch bug): {s['start']}")
        if dur_ms > DAY_MS:
            errors.append(f"[{n}] absurd duration {dur_ms / 3600000:.1f}h")
        if s["parent"] and s["parent"] != ZERO:
            p = by_id.get(s["parent"])
            if p is None:
                errors.append(f"[{n}] dangling parent {s['parent']}")
            elif p["start"] and p["end"]:
                if s["start"] < p["start"]:
                    errors.append(f"[{n}] starts "
                                  f"{(p['start'] - s['start']).total_seconds() * 1000:.3f}ms "
                                  f"before parent [{p['name']}]")
                if s["end"] > p["end"]:
                    errors.append(f"[{n}] ends "
                                  f"{(s['end'] - p['end']).total_seconds() * 1000:.3f}ms "
                                  f"after parent [{p['name']}]")
        req = {"root": ["bazel.invocation_id", "bazel.command"],
               "target": ["bazel.target.label"],
               "action": ["bazel.action.mnemonic"],
               "spawn": ["bazel.spawn.mnemonic"]}.get(kind(n), [])
        for a in req:
            if a not in s["attrs"]:
                warns.append(f"[{n}] missing attr {a}")

    counts = {}
    for s in spans:
        counts[kind(s["name"])] = counts.get(kind(s["name"]), 0) + 1
    supp = {k: root["attrs"].get(k) for k in
            ("bazel.trace.profile", "bazel.trace.suppressed_actions",
             "bazel.trace.suppressed_spawns", "bazel.trace.suppressed_targets",
             "bazel.fetch.count", "bazel.fetch.failed")
            } if root else {}

    print(f"\n=== {path} ===")
    print(f"spans={len(spans)} kinds={counts}")
    print(f"root={root['name'] if root else None} status={root['status'] if root else None}")
    print(f"suppressed={supp}")
    if warns:
        print(f"WARN ({len(warns)}):")
        for w in warns[:20]:
            print("  -", w)
        if len(warns) > 20:
            print(f"  ... and {len(warns) - 20} more")
    if errors:
        print(f"FAIL ({len(errors)}):")
        for e in errors[:40]:
            print("  -", e)
        if len(errors) > 40:
            print(f"  ... and {len(errors) - 40} more")
    else:
        print("OK: all structural invariants hold")
    return len(errors)


def main():
    if len(sys.argv) < 2:
        print("Usage: analyze_trace.py <trace_stdout.txt> [more...]", file=sys.stderr)
        return 2
    return 1 if sum(check(p) for p in sys.argv[1:]) else 0


if __name__ == "__main__":
    sys.exit(main())
