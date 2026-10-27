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
     BES gRPC stream                 binary BEP file
     (--bes_backend)          (--build_event_binary_file)
              │                               │
              ▼                               ▼
     ┌────────────────┐              ┌────────────────┐
     │ conduit serve  │              │ conduit replay │
     │  (BES proto)   │              │ (varint proto) │
     └───────┬────────┘              └───────┬────────┘
             │         BuildEvent protos      │
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
| `bep/replay.rs` | Reads binary BEP files (`--build_event_binary_file`) |
| `bep/router.rs` | Dispatches BEP events to the mapper |
| `otel/mapper.rs` | Creates and manages OTel spans for the build trace |
| `otel/span.rs` | Minimal span/log model (UUID→trace ID, parenting, attributes) |
| `otel/export.rs` | Batching OTLP gRPC / OTLP-JSON exporter with bounded retry and drop counting |
| `otel/attributes.rs` | ~100 typed attribute key constants (`bazel.*` namespace) |
| `otel/redact.rs` | In-process scrubber for `--client_env=NAME=VALUE` style flags |
| `grpc/server.rs` | BES gRPC server implementation (`PublishBuildEvent` service) |
| `exec_log/tailer.rs` | Live tail-follows `--execution_log_compact_file`, decodes zstd, ships `SpawnExec`s to the mapper via mpsc |
| `exec_log/compact.rs` | Incremental decoder state (dedup table + `SpawnExec` reconstruction) shared by tailer and tests |
| `state/trace_profile.rs` | Trace emission policy: full vs lightweight, applied to actions, spawns, and targets |

## Usage

### Build

```bash
bazel build //rust/conduit:conduit
```

### Run as BES Backend (primary mode)

```bash
# Start conduit
./bazel-bin/rust/conduit/conduit serve --port 8080 --export otlp --otlp-endpoint http://localhost:4317

# Run Bazel with conduit as BES backend
bazel build //your:target \
  --bes_backend=grpc://127.0.0.1:8080 \
  --build_event_publish_all_actions
```

### Replay a binary BEP file (offline/debug mode)

```bash
# Record BEP during a build
bazel build //your:target --build_event_binary_file=bep.bin

# Process offline
./bazel-bin/rust/conduit/conduit replay bep.bin --export otlp
```

A BEP file carries no BES event times, so untimed events are placed on the
build's own clock (latest start/finish/action time seen so far).

### CLI Flags

`serve` and `replay <FILE>` accept every flag below except `--listen-addr`
and `--port`, which are `serve`-only.

| Flag | Description | Default |
|------|-------------|---------|
| `--listen-addr <IP>` | Bind address for the BES gRPC server. Defaults to loopback; pass `0.0.0.0` only when external access is intentionally needed (conduit has no auth/TLS). | `127.0.0.1` |
| `--port <PORT>` | gRPC server port | 8080 |
| `--export <MODE>` | Export mode: `none`, `stdout` (OTLP-JSON, one request per line), `otlp` (gRPC) | `none` |
| `--otlp-endpoint <URL>` | OTLP endpoint | `http://localhost:4317` |
| `--log-level <LEVEL>` | Log level (trace/debug/info/warn/error) | `info` |
| `--no-redact` | Disable in-process scrubbing of `--client_env=NAME=VALUE` style flags | off (scrubbing on) |
| `--redact-name-pattern <SUBSTR>` | Replace the default sensitive-name list (repeatable) | built-in defaults |
| `--exec-log` | Tail Bazel's compact execution log for per-spawn detail. Without it, conduit ignores the exec log even when the build writes one. | off |
| `--exec-log-max-message-mib <MIB>` | Per-message cap on length-delimited entries in Bazel's `--execution_log_compact_file`. Prevents a malformed varint length prefix from OOM'ing the parser. | `64` |
| `--exec-log-max-decompressed-mib <MIB>` | Cap on total decompressed bytes pulled from a compact execution log's zstd frame. Prevents a small malicious frame from inflating into GiB of in-memory state (zstd zip-bomb). | `2048` |
| `--trace-profile <PROFILE>` | Trace verbosity: `auto`, `lightweight`, or `full`. See [Trace Profiles](#trace-profiles). | `auto` |
| `--trace-min-duration-ms <MS>` | Minimum operation duration kept by the lightweight profile on runtime alone (errors and cache misses are kept regardless). | `500` |

### Secret Redaction

Bazel surfaces environment variables and user-defined values on the command
line via flags like `--client_env=NAME=VALUE`, `--action_env=`, `--test_env=`,
`--repo_env=`, `--host_action_env=`, and `--define=NAME=VALUE`. Workspace
status entries produced by `--workspace_status_command` (`STABLE_*` keys)
routinely carry CI-injected tokens. Progress stdout/stderr can echo any of
the above. Without intervention all of these end up verbatim in the
`bazel.command_line`, `bazel.explicit_cmd_line`, `bazel.startup_options`,
`bazel.action.command_line`, `bazel.spawn.command`, `bazel.workspace.*`,
and `bazel.progress.*` span attributes — and from there into whatever
backend the OTLP exporter is wired to.

Conduit ships with an in-process scrubber (`rust/conduit/src/otel/redact.rs`)
that runs **before** any attribute is set on a span. Three scrubbing
surfaces are used:

- **Argv tokens** (`scrub_arg` / `scrub_args`) — applied to
  `command_line`, `explicit_cmd_line`, `startup_options`, ActionExecuted
  command lines, and SpawnExec command_args. The `VALUE` half of a
  recognized `--flag=NAME=VALUE` is replaced with `***` when `NAME` matches.
- **`(name, value)` pairs** (`scrub_value_by_name`) — applied to
  `workspaceStatus` entries. Replaces VALUE with `***` when the key name
  matches.
- **Free-form text** (`scrub_text`) — applied to progress stdout/stderr.
  Tokenises on whitespace, runs each token through `scrub_arg`, preserves
  whitespace runs.

The pass-through form `--client_env=NAME` (no `=VALUE`, inherits from
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
./conduit serve --port 8080 --export otlp --otlp-endpoint http://localhost:4317

# Custom list (replaces default)
./conduit serve \
  --redact-name-pattern TOKEN --redact-name-pattern JIRA --redact-name-pattern AWS_

# Disabled
./conduit serve --no-redact
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

### Trace Profiles

A Bazel build emits one action/spawn per node in the build graph — tens of
thousands of spans for a large build, almost all fast cache hits with little
diagnostic value. The trace profile controls how much of that survives, at
every emission point (BEP actions, exec-log spawns, orphan-synthesised
actions, and empty targets):

| Profile | Selected by | Behavior |
|---------|-------------|----------|
| **full** | `--trace-profile=full`, or `auto` + `--build_event_publish_all_actions` | Every action, spawn, and target gets a span. Verbose but complete. |
| **lightweight** | `--trace-profile=lightweight`, or `auto` without the Bazel flag | Keeps only spans that carry signal: errors, cacheable cache **misses**, and operations at or above `--trace-min-duration-ms` (500 ms default). Successful fast cache hits are dropped. |
| **auto** (default) | `--trace-profile=auto` | Follows Bazel's `--build_event_publish_all_actions`: present → full, absent → lightweight. Keeps existing setups working. |

Regardless of profile, all aggregate `BuildMetrics` (cache hit/miss counts,
per-mnemonic action counts, timings, artifact/memory metrics) stay on the
root span, and the root → target → action → spawn hierarchy is preserved for
whatever is retained. The root span also carries
`bazel.trace.suppressed_{actions,spawns,targets}` so you can see how much the
lightweight profile trimmed on any given invocation.

Crucially, the lightweight profile also gates the compact execution log:
before this, "lightweight" only dropped BEP action spans while the exec-log
pipeline resurrected a synthetic action + spawn tree for every successful
spawn, so traces stayed enormous. Lightweight now filters spawns at every
exec-log choke point.

**Reducing volume further (backend side).** Source-side filtering is the
primary lever; layer sampling/retention on top if needed:

- **Head-based sampling** at the SDK/agent — e.g.
  `DD_TRACE_SAMPLING_RULES='[{"service":"bazel","sample_rate":0.1}]'` keeps a
  representative fraction of full traces.
- **Agent caps** — `DD_APM_TARGET_TPS`, `DD_APM_MAX_TPS`, and
  `DD_APM_FILTER_TAGS_REJECT` bound or drop trace volume the agent forwards.
- **Span-generated metrics** for dashboard widgets (cache-hit rates, duration
  percentiles) give long retention on the numbers that matter while allowing
  aggressive span sampling.
- **Retention filters** reduce *indexed* storage (only errors, slow
  invocations, cache misses), not ingestion — use them to control storage
  cost after ingestion is already trimmed.

Suggested rollout: run `auto` (or `lightweight`) and compare
`bazel.trace.suppressed_*` against total spans, make lightweight the
operational default, switch to `full` temporarily for deep debugging, then
apply 10–25% head sampling if volume is still high.

### Execution Log Enrichment (live tailed)

Opt-in: conduit must be started with `--exec-log`. When Bazel is then run with
`--execution_log_compact_file=exec.compact`, conduit
detects the flag in `OptionsParsed`, opens the file as it's being written,
and **tail-follows** it in a dedicated blocking thread for the rest of the
build. Each `SpawnExec` it decodes:

1. Looks up its parent action span in conduit's cache by
   `(target_label, mnemonic, primary_output)`.
2. On the **first** matching spawn for an action, backfills a curated set of
   attributes onto the action span (`bazel.action.spawn.runner`,
   `bazel.action.spawn.cache_hit`, the `SpawnMetrics.*_time_ms` breakdown).
   Subsequent spawns for the same action (retries, dynamic-exec races) only
   bump `bazel.action.spawn.count` and emit their own child span.
3. Always emits a `spawn {mnemonic}` child span under the action span with
   the full `bazel.spawn.*` attribute set (command, digest, cache flags,
   per-stage timings, I/O metrics).

If a spawn arrives **before** its `ActionExecuted` BEP event (the compact
log flushes every ~128 KiB of uncompressed input, asynchronously to BEP),
it's buffered briefly and flushed onto the action span the moment it
arrives. Spawns that never match anything by the end of the build are
grouped by `(label, mnemonic, primary_output)` and emitted under
synthesised parent action spans (`bazel.target.synthetic = true`); action
spans with no matching spawn pick up `bazel.action.spawn.missing = true`.

**Format support.** Only `--execution_log_compact_file` (and its
`--experimental_execution_log_compact_file` alias) is consumed.
`--execution_log_binary_file` and `--execution_log_json_file` lack the
dedup table that makes streaming feasible — conduit logs a warning and
records a `bazel.exec_log.unsupported_format` event on the root span when
it sees them.

**Streaming semantics (best-effort live tail).** Bazel writes the compact
log through `AsynchronousMessageOutputStream → ZstdOutputStream →
BufferedOutputStream → FileOutputStream`. `ZstdOutputStream` only emits a
compressed block once it has accumulated 128 KiB of uncompressed proto,
and Bazel doesn't call `flush()` until `close()` at the end of the build.
In practice this means the tailer sees a chunk every ~200–600 spawns on
big builds, and the final <128 KiB of input is only visible after
`close()`. Conduit waits up to 2 s for that final chunk to drain through
`finish()` before sealing the trace.

**Why not stream from `bytestream://`?** Bazel uploads the spawn log to the
remote cache only **after** `close()` (post-build), so the bytestream
endpoint cannot be used as a live source.

#### Exec log source comparison

| Source | Live streaming? | Dedup table? | Conduit support |
|---|---|---|---|
| `--execution_log_compact_file=<path>` | Yes (best-effort, 128 KiB chunks) | Yes | **Live tailed (with `--exec-log`)** |
| `--experimental_execution_log_compact_file=<path>` (alias) | Yes | Yes | Live tailed |
| `--execution_log_binary_file=<path>` | No (would need full buffer) | No | Warned, ignored |
| `--execution_log_json_file=<path>` | No | No | Warned, ignored |
| `bytestream://…/spawn_log` (remote cache) | No (uploaded post-`close()`) | Yes | Not consumed |

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

BEP only emits `TargetConfigured`/`TargetCompleted`/`ActionCompleted` events for **directly requested targets**. If you build `//my:app` and it depends on `@zlib//:zlib`, the zlib actions won't appear in BEP at all. The compact execution log (`--execution_log_compact_file`) is the only way to get full coverage of what Bazel actually spawned.

This is why conduit live-tails the compact log: spawns that don't match any BEP-visible action are buffered and emitted at `finish()` under synthesised parent action spans tagged `bazel.target.synthetic = true`.

### Lesson 2: Datadog and Jaeger Interpret Traces Differently

Datadog uses the span `name` field for operation grouping in its flame graph, while Jaeger uses `operationName` (which maps to the OTel span's display name). When all spans had `SpanKind::Internal`, Datadog showed them all as "Internal" — flattening the hierarchy visually even though parent-child relationships were correct.

Datadog also uses **temporal containment** for flame graph nesting: a child span must start after and end before its parent. This required adding `clamp_time_range()` to ensure spawn child spans never escape their parent action's time bounds.

### Lesson 3: Bazel's `start_time` in `BuildStarted` Can Be Stale

The `start_time` / `start_time_millis` in `BuildStarted` can hold the **Bazel server start time** (potentially weeks old on long-lived daemons), not the current invocation start. Conduit prefers the BES-level `event_time` (when the event was actually emitted) as a more reliable fallback for root span start time in gRPC mode.

### Lesson 4: Cached Timestamps Must Be Clamped to the Invocation Window

Bazel replays the *original* execution timestamps on cached `ActionExecuted` events (and propagates them to target metadata), so a build that replays a weeks-old cache can produce spans whose `start_time` predates the invocation by days. Conduit clamps every BEP-supplied `(start, end)` to the root span's window via `clamp_time_range` (`rust/conduit/src/otel/mapper/utils.rs`); a range that lies entirely before the invocation collapses to zero duration at the build start. The root span itself falls back to wall clock when `BuildStarted` drifts more than 5 minutes from now (stale long-lived daemon).

### Lesson 5: Export Must Never Block the Build Stream

Large Bazel builds produce thousands of spans in bursts. Conduit's exporter (`rust/conduit/src/otel/export.rs`) queues up to **65,536** records, exports batches of **512** every **200 ms**, and retries a failed export at most 3 times (2 s timeout each, retryable gRPC codes only). Anything it cannot queue or export is dropped and counted; the counts are logged at shutdown.

### Lesson 6: Proto-Generated Duration Types Vary

Prost-generated code for `google.protobuf.Duration` may produce different Rust types depending on the proto path (`prost_types::Duration` vs a generated `duration_proto::Duration`). The solution is to access raw fields (`d.seconds`, `d.nanos`) rather than relying on typed parameters.

### Lesson 7: BEP Label Formats Are Inconsistent

BEP action IDs (`actionCompleted.label`) and the exec log's `targetLabel` both use the canonical form (`@@repo+//:target`): the exec log writes `Label.getCanonicalForm()` and BEP writes `Label.toString()`, which returns the same string (true on Bazel 8.4 and 9). Conduit therefore matches labels exactly. BEP still emits the apparent form (`@repo//:target`) in other string fields (options, progress), which conduit does not match on.

### Lesson 9: Not All BEP Actions Appear in the Execution Log

The execution log only records **spawned processes** (actual `execve` calls). Internal Bazel actions like `FileWrite`, `TemplateExpand`, and `SymlinkTree` are not spawns and won't appear. This means some BEP actions will never have matching exec log entries — by design. Action spans for those carry `bazel.action.spawn.missing = true` at the end of the build.

### Lesson 11: ZstdOutputStream Buffers 128 KiB Before Flushing

Bazel's compact exec log goes through `ZstdOutputStream` (zstd-jni) which buffers **128 KiB of uncompressed input** before emitting a compressed block, and Bazel never calls `flush()`. The only way to live-tail is to keep the reader blocked on EOF rather than propagating `Ok(0)` to the zstd decoder, and accept that the trailing &lt;128 KiB of input is only visible after `close()`. Conduit's tailer (`exec_log/tailer.rs`) sleeps 100 ms on EOF and resumes when more bytes appear; `finish()` budgets up to 2 s for the post-`close()` final chunk.

### Lesson 10: BES Progress Event `children` Are Misleading

The `children` field on Progress events is for BEP DAG ordering (announcing which events will follow), **not** content correlation. The stderr/stdout text in a Progress event is not necessarily related to the child events it declares.

## Pitfalls & Caveats

### Build System
- **Must use `bazel build`, not `cargo build`**: The project uses Bazel as its build system with `rules_rust` and `rules_rs` (crate_universe). Cargo is only present for IDE support (`Cargo.toml`).
- **Rust edition 2024 / rustc 1.93+**: The Bazel toolchain is pinned to 1.93.0.
- **OTLP protos are vendored**: `proto/opentelemetry/` holds unmodified opentelemetry-proto v1.11.1 files; conduit writes OTLP directly instead of depending on the OpenTelemetry SDK crates.
- **Protobuf compilation**: BEP proto has deep import chains (`failure_details.proto` → `descriptor.proto`). Proto targets use `rules_rust_prost` with `rules_rs` for crate resolution.

### Runtime
- **Co-location requirement**: Exec log enrichment requires conduit to run on the same machine as Bazel (it tail-follows the local file path from `OptionsParsed`; the `bytestream://` upload that goes to the remote cache only happens after `close()` and isn't a live source).
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
- **Action spans**: `bazel.action.mnemonic`, `bazel.action.exit_code`, `bazel.action.cached`, `bazel.action.runner`, ... (`cached`/`runner` come from `ActionExecuted.strategy_details`, populated by Bazel 10+; older Bazel needs `--exec-log`)
- **Spawn spans**: `bazel.spawn.runner`, `bazel.spawn.cache_hit`, `bazel.spawn.command`, `bazel.spawn.execution_wall_time_ms`, ...
- **Test spans**: `bazel.test.status`, `bazel.test.attempt`, `bazel.test.cached_locally`, `bazel.test.strategy`, ...
- **Build metrics**: `bazel.metrics.wall_time_ms`, `bazel.metrics.cpu_time_ms`, `bazel.metrics.cache_hits`, ...

## Limitations

1. **No distributed / remote build support**: Conduit is a local sidecar. It does not support remote BES endpoints or multi-machine builds.
2. **No phase spans**: Loading, analysis, and execution phases are not modeled as separate spans (metrics for these are captured as attributes on the root span via `BuildMetrics`).
3. **No span links / DAG representation**: BEP's DAG structure (secondary parents) is not represented as OTel span links. All relationships are parent-child.
4. **No conduit-side probabilistic sampling**: The `lightweight` profile drops uninteresting spans deterministically (errors, cache misses, and slow work are always kept), but there is no top-N-slowest or percentage sampling inside conduit — layer head-based sampling / retention filters at the SDK, agent, or backend. See [Trace Profiles](#trace-profiles).
5. **Single invocation at a time**: The gRPC server processes one build stream at a time (sequential invocations via `Arc<Mutex<EventRouter>>`).
6. **Exec log streaming is best-effort, not "live per spawn"**: Bazel flushes the compact log every ~128 KiB of uncompressed input (200–600 spawns on big builds), and the final &lt;128 KiB is only visible after `close()` at `BuildCompleteEvent`. Spawn enrichment lands on action spans in those chunk-sized bursts, with a final post-`close()` drain at `finish()`.
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
│   │   ├── exec_log/             # Compact exec log live-tail + enrichment
│   │   └── state/                # Build state tracking
│   └── tests/                    # Unit tests (decoder, trace profile, router)
├── integration/                  # Integration tests + full scenario script
├── toolchain/                    # Prost toolchain config
└── docs/                         # Additional documentation
```
