# Bazel Conduit

A Rust tool that converts Bazel [Build Event Protocol](https://bazel.build/remote/bep) (BEP) streams into [OpenTelemetry](https://opentelemetry.io/) traces, enabling observability of Bazel builds in Datadog, Jaeger, and any OTLP-compatible backend.

## What It Does

Conduit intercepts Bazel's build event stream — either as a live gRPC [Build Event Service](https://bazel.build/remote/bep#build-event-service) (BES) backend or from saved JSON files — and produces a structured OTel trace where:

- **1 trace = 1 Bazel invocation** (trace ID derived from Bazel's invocation UUID)
- **Root span** covers `BuildStarted` → `BuildFinished` with build metadata
- **Target spans** represent each configured target with timing, output files, tags, and success/failure
- **Action spans** represent build actions (compile, link, etc.) nested under their target
- **Test spans** capture test attempts with status, timing, caching, and execution strategy
- **Spawn spans** (from execution log) provide process-level detail: command lines, I/O metrics, timing breakdowns, cache hit/miss

```
bazel.invocation (root)
├── target //pkg:lib (TargetConfigured → TargetCompleted)
│   ├── action CppCompile //pkg:lib
│   │   └── spawn CppCompile lib/foo.cc (from exec log)
│   └── test //pkg:lib_test
│       └── test attempt 1 (PASSED)
├── target //external:zlib (synthetic, from exec log)
│   └── spawn CppCompile zlib/adler32.c
├── fetches
│   └── fetch https://...
└── skipped targets
    └── target //pkg:skipped (ANALYSIS_FAILURE)
```

## Architecture

```
                   ┌─────────────────────────────┐
                   │        Bazel Build           │
                   └──────────┬──────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼                               ▼
     BES gRPC stream                  JSON file (NDJSON)
     (--bes_backend)            (--build_event_json_file)
              │                               │
              ▼                               ▼
     ┌────────────────┐              ┌────────────────┐
     │  gRPC Server   │              │  BEP Decoder   │
     │  (BES proto)   │              │  (JSON parser)  │
     └───────┬────────┘              └───────┬────────┘
             │  Proto-direct routing          │  JSON routing
             └───────────┬───────────────────┘
                         ▼
                  ┌──────────────┐
                  │ EventRouter  │
                  │  (dispatch)  │
                  └──────┬───────┘
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
        BuildState   OtelMapper   ExecLog
        (tracking)   (spans)      (enrichment)
                         │
                         ▼
                  ┌──────────────┐
                  │ OTLP Export  │──▶ Datadog / Jaeger / etc.
                  └──────────────┘
```

### Key Components

| Module | Purpose |
|--------|---------|
| `bep/decoder.rs` | Parses NDJSON BEP files into `BepJsonEvent` structs |
| `bep/router.rs` | Dispatches BEP events to the mapper; dual-path (JSON + proto-direct) |
| `otel/mapper.rs` | Creates and manages OTel spans for the build trace |
| `otel/trace_context.rs` | UUID→TraceID conversion, TracerProvider/LoggerProvider init |
| `otel/attributes.rs` | ~100 typed attribute key constants (`bazel.*` namespace) |
| `otel/redact.rs` | In-process scrubber for `--client_env=NAME=VALUE` style flags |
| `grpc/server.rs` | BES gRPC server implementation (`PublishBuildEvent` service) |
| `exec_log.rs` | Parses `--execution_log_binary_file` and creates spawn child spans |
| `state/build_state.rs` | Build lifecycle tracking, action buffering, named set cache |
| `state/action_mode.rs` | Lightweight vs full action processing mode detection |

## Usage

### Build

```bash
bazel build //rust/conduit:conduit
```

### Run as BES Backend (primary mode)

```bash
# Start conduit
./bazel-bin/rust/conduit/conduit --serve --port 8080 --export otlp --otlp-endpoint http://localhost:4317

# Run Bazel with conduit as BES backend
bazel build //your:target \
  --bes_backend=grpc://localhost:8080 \
  --build_event_publish_all_actions
```

### Process a JSON BEP File (offline/debug mode)

```bash
# Record BEP during a build
bazel build //your:target --build_event_json_file=bep.ndjson

# Process offline
./bazel-bin/rust/conduit/conduit --input bep.ndjson --export otlp
```

### CLI Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--input <FILE>` | Read BEP from NDJSON file | - |
| `--serve` | Start BES gRPC server | false |
| `--port <PORT>` | gRPC server port | 8080 |
| `--export <MODE>` | Export mode: `none`, `stdout`, `otlp` | `none` |
| `--otlp-endpoint <URL>` | OTLP endpoint | `http://localhost:4317` |
| `--log-level <LEVEL>` | Log level (trace/debug/info/warn/error) | `info` |
| `--no-redact` | Disable in-process scrubbing of `--client_env=NAME=VALUE` style flags | off (scrubbing on) |
| `--redact-name-pattern <SUBSTR>` | Replace the default sensitive-name list (repeatable) | built-in defaults |

### Secret Redaction

Bazel surfaces environment variables on the command line via flags like
`--client_env=NAME=VALUE`, `--action_env=`, `--test_env=`, `--repo_env=`, and
`--host_action_env=`. Without intervention, the *value* half of those flags
(e.g. `--client_env=GITLAB_TOKEN=glpat-...`) ends up verbatim in the
`bazel.command_line`, `bazel.explicit_cmd_line`, `bazel.startup_options`, and
`bazel.action.command_line` span attributes — and from there into whatever
backend the OTLP exporter is wired to.

Conduit ships with an in-process scrubber (`rust/conduit/src/otel/redact.rs`)
that runs **before** any attribute is set on a span. When the variable name
matches a case-insensitive substring deny list, the value is replaced with
`***`. The pass-through form `--client_env=NAME` (no `=VALUE`, inherits from
the parent process env) is left untouched because no value is on the wire.

Default sensitive-name substrings: `TOKEN`, `SECRET`, `PASSWORD`, `PASSWD`,
`CREDENTIAL`, `COOKIE`, `APIKEY`, `API_KEY`, `ACCESS_KEY`, `PRIVATE_KEY`,
`AUTH`. The list is intentionally narrow (e.g. plain `KEY` is excluded
because it would match `MONKEY`). Override or extend it via
`--redact-name-pattern` (repeatable, supplied list fully replaces the
default). Disable entirely with `--no-redact` — only safe when the receiving
backend is fully trusted or has its own scrubbing layer.

```bash
# Default list
./conduit --serve --port 8080 --export otlp --otlp-endpoint http://localhost:4317

# Custom list (replaces default)
./conduit --serve \
  --redact-name-pattern TOKEN --redact-name-pattern JIRA --redact-name-pattern AWS_

# Disabled
./conduit --serve --no-redact
```

#### Defense in depth: Datadog Agent server-side scrubbing

For belt-and-braces protection, also configure the Datadog Agent's
`apm_config.replace_tags` so a future call-site that forgets to route through
the scrubber still cannot leak. Example `datadog.yaml`:

```yaml
apm_config:
  replace_tags:
    - name: "bazel.command_line"
      pattern: "--client_env=([A-Z0-9_]*?(TOKEN|SECRET|PASSWORD|KEY|CREDENTIAL)[A-Z0-9_]*)=[^ ]+"
      repl:    "--client_env=$1=***"
    - name: "bazel.explicit_cmd_line"
      pattern: "--client_env=([A-Z0-9_]*?(TOKEN|SECRET|PASSWORD|KEY|CREDENTIAL)[A-Z0-9_]*)=[^ ]+"
      repl:    "--client_env=$1=***"
```

Equivalent functionality exists in the OpenTelemetry Collector's
[`redactionprocessor`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/processor/redactionprocessor)
when traces transit a collector hop.

### Action Processing Modes

| Mode | Trigger | Behavior |
|------|---------|----------|
| **Lightweight** | Default (no flag) | Only failed actions create spans |
| **Full** | `--build_event_publish_all_actions` | Every action gets a span with accurate start/end timestamps |

### Execution Log Enrichment

When Bazel is run with `--execution_log_binary_file=exec.bin`, conduit automatically detects the flag in `OptionsParsed` and after the build finishes, parses the binary exec log to create spawn child spans with:

- Full command lines (capped at 4 KB)
- Runner, cache hit/miss, remotable/cacheable flags
- Timing breakdown: wall time, queue time, network time, setup, fetch, upload, parse, retry
- I/O metrics: input bytes/files, output count, memory estimate
- Digest hash and size

Spawn matching uses a multi-tier strategy: exact `(label, mnemonic, output)` key → output-only match → `(label, mnemonic)` fuzzy match → output directory match.

## Integration Tests

```bash
# Hermetic test (minimal BEP sample, no live Bazel)
bazel test //integration:trace_test

# Full scenario (builds conduit, records BEP + exec log, analyzes trace)
./integration/run_full_trace_scenario.sh
```

## Project History & Lessons Learned

This project was developed iteratively over ~13 sessions. The journey from initial prototype to the current state surfaced several non-obvious insights about Bazel's BEP, OpenTelemetry SDKs, and trace backend differences.

### Lesson 1: Bazel's BEP Does Not Report Transitive Dependency Actions

BEP only emits `TargetConfigured`/`TargetCompleted`/`ActionCompleted` events for **directly requested targets**. If you build `//my:app` and it depends on `@zlib//:zlib`, the zlib actions won't appear in BEP at all. The execution log (`--execution_log_binary_file`) is the only way to get full coverage of what Bazel actually spawned.

This is why conduit has the exec log enrichment pass: it catches all the "invisible" work and groups unmatched spawns under synthetic target spans.

### Lesson 2: Datadog and Jaeger Interpret Traces Differently

Datadog uses the span `name` field for operation grouping in its flame graph, while Jaeger uses `operationName` (which maps to the OTel span's display name). When all spans had `SpanKind::Internal`, Datadog showed them all as "Internal" — flattening the hierarchy visually even though parent-child relationships were correct.

Datadog also uses **temporal containment** for flame graph nesting: a child span must start after and end before its parent. This required adding `clamp_time_range()` to ensure spawn child spans never escape their parent action's time bounds.

### Lesson 3: Bazel's `start_time` in `BuildStarted` Can Be Stale

The `start_time` / `start_time_millis` in `BuildStarted` can hold the **Bazel server start time** (potentially weeks old on long-lived daemons), not the current invocation start. Conduit prefers the BES-level `event_time` (when the event was actually emitted) as a more reliable fallback for root span start time in gRPC mode.

### Lesson 4: Timestamp Validation Is Essential

Bazel sometimes sends `start_time_nanos = 0` on action spans with `end_time_nanos` set to a duration value rather than an absolute timestamp. Without validation, this produces spans starting at Unix epoch (1970) with multi-day durations. Conduit rejects timestamps below a `MIN_ABSOLUTE_NANOS` threshold (~year 2001) to catch these.

### Lesson 5: BatchSpanProcessor Can Silently Drop Spans

The default OpenTelemetry `BatchSpanProcessor` queue size is 2048 spans. Large Bazel builds (thousands of actions + spawns) overflow this easily. Conduit sets the queue to **65,536** with a batch size of **4,096** to avoid silent span loss.

### Lesson 6: Proto-Generated Duration Types Vary

Prost-generated code for `google.protobuf.Duration` may produce different Rust types depending on the proto path (`prost_types::Duration` vs a generated `duration_proto::Duration`). The solution is to access raw fields (`d.seconds`, `d.nanos`) rather than relying on typed parameters.

### Lesson 7: BEP Label Formats Are Inconsistent

BEP events use `@@repo//:target` (double-at canonical form) while the execution log uses `@repo//:target` (single-at). Label normalization (`trim_start_matches('@')`) is essential for cross-format matching.

### Lesson 8: LoggerProvider Flush Timing in Server Mode

In gRPC serve mode, `router.finish()` emits OTel log records to the batch processor, but if `shutdown_providers()` is never called (e.g., the server loops for the next connection), the batch processor may never flush. Explicit shutdown or force-flush is required after each build stream completes.

### Lesson 9: Not All BEP Actions Appear in the Execution Log

The binary execution log only records **spawned processes** (actual `execve` calls). Internal Bazel actions like `FileWrite`, `TemplateExpand`, and `SymlinkTree` are not spawns and won't appear. This means some BEP actions will never have matching exec log entries — by design.

### Lesson 10: BES Progress Event `children` Are Misleading

The `children` field on Progress events is for BEP DAG ordering (announcing which events will follow), **not** content correlation. The stderr/stdout text in a Progress event is not necessarily related to the child events it declares.

## Pitfalls & Caveats

### Build System
- **Must use `bazel build`, not `cargo build`**: The project uses Bazel as its build system with `rules_rust` and `rules_rs` (crate_universe). Cargo is only present for IDE support (`Cargo.toml`).
- **Rust edition 2024 / rustc 1.93+**: OpenTelemetry 0.28 crates require a recent Rust toolchain. The Bazel toolchain is pinned to 1.93.0.
- **Protobuf compilation**: BEP proto has deep import chains (`failure_details.proto` → `descriptor.proto`). Proto targets use `rules_rust_prost` with `rules_rs` for crate resolution.

### Runtime
- **Co-location requirement**: Exec log enrichment requires conduit to run on the same machine as Bazel (it reads a local file path from `OptionsParsed`).
- **Non-standard OTLP ports**: The Datadog Agent often uses `14317`/`14318` instead of the OTel-default `4317`/`4318`. Configure `--otlp-endpoint` accordingly.
- **API key vs environment variable**: `dd-auth` sets `DD_API_KEY` as an env var, but the Datadog Agent reads its `api_key` from its config file at startup. These are separate mechanisms.

### Data Quality
- **Action-level durations may be unreliable**: Some BEP events report zero-length or negative durations due to clock skew. Target-level durations (derived from action buffering) are more reliable.
- **Proto3 JSON defaults**: In proto3 JSON, `success: true` is often omitted (it's the default). Conduit defaults `success` to `true` when absent from `TargetCompleted` to avoid false negatives.
- **Progress stderr/stdout can be empty**: Bazel sends many Progress events with no content. Conduit skips these.

## Attribute Reference

All OTel span attributes follow the `bazel.<component>.<field>` naming convention. See `rust/conduit/src/otel/attributes.rs` for the full list (~100 constants), organized by:

- **Trace-level**: `bazel.invocation_id`, `bazel.command`, `bazel.exit_code`, `bazel.patterns`, ...
- **Target spans**: `bazel.target.label`, `bazel.target.kind`, `bazel.target.success`, `bazel.target.output_files`, ...
- **Action spans**: `bazel.action.mnemonic`, `bazel.action.exit_code`, `bazel.action.cached`, `bazel.action.runner`, ...
- **Spawn spans**: `bazel.spawn.runner`, `bazel.spawn.cache_hit`, `bazel.spawn.command`, `bazel.spawn.execution_wall_time_ms`, ...
- **Test spans**: `bazel.test.status`, `bazel.test.attempt`, `bazel.test.cached_locally`, `bazel.test.strategy`, ...
- **Build metrics**: `bazel.metrics.wall_time_ms`, `bazel.metrics.cpu_time_ms`, `bazel.metrics.cache_hits`, ...

## Limitations

1. **No distributed / remote build support**: Conduit is a local sidecar. It does not support remote BES endpoints or multi-machine builds.
2. **No phase spans**: Loading, analysis, and execution phases are not modeled as separate spans (metrics for these are captured as attributes on the root span via `BuildMetrics`).
3. **No span links / DAG representation**: BEP's DAG structure (secondary parents) is not represented as OTel span links. All relationships are parent-child.
4. **No sampling policies**: All qualifying events produce spans. There is no configurable top-N-slowest or probabilistic sampling.
5. **Single invocation at a time**: The gRPC server processes one build stream at a time (sequential invocations via `Arc<Mutex<EventRouter>>`).
6. **Exec log is post-hoc**: Spawn enrichment happens after `BuildFinished`, not during streaming. This means spawn data isn't available until the build completes.
7. **`TestProgress` and `ExecRequest` are no-ops**: These event types are received but not mapped to any OTel construct.

## Directory Structure

```
bazel_conduit/
├── MODULE.bazel                  # Bazel module (rules_rust, rules_rs, prost)
├── BUILD.bazel                   # Top-level build targets
├── BEP_TO_OTEL_DESIGN.md        # Original design document
├── RUST_CONDUIT_PLAN.md          # Development plan
├── NOTES.md                      # BEP event analysis notes
├── proto/
│   ├── build_event_stream/       # Bazel BEP proto (vendored)
│   ├── spawn/                    # SpawnExec proto (execution log)
│   └── google/                   # Google API protos (BES, well-known types)
├── rust/conduit/
│   ├── BUILD.bazel               # rust_binary, rust_library, rust_test targets
│   ├── Cargo.toml / Cargo.lock   # IDE support
│   ├── src/
│   │   ├── main.rs               # CLI entry (clap)
│   │   ├── lib.rs                # Crate root, proto re-exports
│   │   ├── bep/                  # BEP decoder + event router
│   │   ├── otel/                 # OTel mapper, trace context, attributes
│   │   ├── grpc/                 # BES gRPC server
│   │   ├── exec_log.rs           # Execution log parser + enrichment
│   │   └── state/                # Build state tracking
│   └── tests/                    # Unit tests (decoder, action_mode, router)
├── integration/                  # Integration tests + full scenario script
├── toolchain/                    # Prost toolchain config
└── docs/                         # Additional documentation
```
