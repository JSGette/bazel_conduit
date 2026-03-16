# Integration tests for conduit

## Trace test (no performance regression)

Asserts that conduit turns BEP into a valid OTel trace: at least one span, root span named like `bazel build` / `bazel test`.

### Hermetic test (Bazel)

Uses a minimal BEP sample (`bep_sample.ndjson`: Started + BuildFinished). No live Bazel build.

```bash
bazel test //integration:trace_test
```

### Full scenario (manual / CI)

One-command helper (from repo root):

```bash
./integration/run_full_trace_scenario.sh
```

This builds conduit, runs `bazel build //rust/conduit:conduit` with `--build_event_json_file=bep.ndjson` and `--execution_log_binary_file=exec.bin`, runs conduit on the BEP to produce a trace, and runs the analyzer. Outputs: `bep.ndjson`, `exec.bin`, `trace_stdout.txt`.

Or step by step:

1. **Record BEP and exec log**
   ```bash
   bazel build //rust/conduit:conduit \
     --build_event_json_file=bep.ndjson \
     --execution_log_binary_file=exec.bin
   ```

2. **Produce trace from BEP**
   ```bash
   ./bazel-bin/rust/conduit/conduit --input bep.ndjson --export stdout 2>/dev/null | tee trace_stdout.txt
   ```

3. **Analyze trace**
   ```bash
   python3 integration/analyze_trace.py trace_stdout.txt
   ```

4. **Optional: export to JSON for tooling**  
   Use `--export otlp` and point at an OTLP collector that writes to file (e.g. Jaeger all-in-one with file storage), or use a local collector with file exporter to get a JSON trace for deeper analysis.

## Files

| File | Purpose |
|------|--------|
| `bep_sample.ndjson` | Minimal BEP (Started, BuildFinished) for hermetic test |
| `trace_test.sh` | Runs conduit on BEP, captures stdout, runs analyzer |
| `analyze_trace.py` | Parses conduit stdout trace; asserts span count ≥ 1 and root span present |
| `run_full_trace_scenario.sh` | Full scenario: bazel build + BEP + exec log + conduit + analyze (manual/CI) |
| `BUILD.bazel` | Bazel test target |
