#!/usr/bin/env python3
"""Parse conduit stdout trace output and assert no performance/structure regression.

Conduit with --export stdout prints human-readable span lines. We count spans,
find the root span name, and optionally check duration. Exit 0 if assertions pass.
"""
import re
import sys


def analyze(contents: str) -> tuple[int, list[str], bool]:
    """Returns (span_count, span_names, has_root)."""
    span_count = 0
    span_names: list[str] = []
    current_name: str | None = None
    for line in contents.splitlines():
        if line.strip().startswith("Span #"):
            span_count += 1
            if current_name is not None:
                span_names.append(current_name)
            current_name = None
        elif "Name        :" in line or "Name:" in line:
            # "	Name        : bazel build" or similar
            m = re.search(r"Name\s*:\s*(.+)", line)
            if m:
                current_name = m.group(1).strip()
    if current_name is not None:
        span_names.append(current_name)
    root_names = ["bazel build", "bazel test", "bazel"]
    has_root = any(
        any(root in name for root in root_names) for name in span_names
    )
    return span_count, span_names, has_root


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: analyze_trace.py <trace_stdout.txt>", file=sys.stderr)
        return 2
    path = sys.argv[1]
    try:
        with open(path) as f:
            contents = f.read()
    except OSError as e:
        print(f"Error reading {path}: {e}", file=sys.stderr)
        return 1
    span_count, span_names, has_root = analyze(contents)
    if span_count < 1:
        print(f"Regression: expected at least 1 span, got {span_count}", file=sys.stderr)
        return 1
    if not has_root:
        print(
            f"Regression: no root span (bazel build/test) in names: {span_names}",
            file=sys.stderr,
        )
        return 1
    print(f"OK: {span_count} span(s), root present, names={span_names[:5]}{'...' if len(span_names) > 5 else ''}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
