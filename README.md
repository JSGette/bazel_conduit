# Bazel Conduit

Conduit turns Bazel's [Build Event Protocol](https://bazel.build/remote/bep) (BEP) into
[OpenTelemetry](https://opentelemetry.io/) traces and logs. It is a
[Build Event Service](https://bazel.build/remote/bep#build-event-service) (BES) backend:
point Bazel at it with `--bes_backend` and every invocation becomes one trace, exported over
OTLP to the Datadog Agent or any other OTLP receiver.

The main way to run it is as a **sidecar of the Datadog Agent**. It also runs standalone next
to a developer's Bazel, or offline against a recorded BEP file.

Requires Bazel 9 or later. Bazel 10 adds per-action cache-hit and runner data to BEP
(`ActionExecuted.strategy_details`), which conduit records when present.

## What you get

One trace per Bazel invocation, with the trace ID derived from the invocation UUID:

```
bazel build //pkg:app                      root: command, patterns, exit code, build metrics
├── target //pkg:lib                       kind, success, cached, tags
│   ├── action CppCompile //pkg:lib        exit code, cache hit, runner
│   │   └── spawn CppCompile foo.cc        with --exec-log: command, runner, stage timings
│   └── test //pkg:lib_test attempt=1      status, cache, strategy; summary on the target
└── skipped targets
    └── target //pkg:broken                abort reason
```

- **Root span:** command and patterns, exit code, `BuildMetrics` (wall/CPU time, action-cache
  hits and misses, per-runner action counts, memory), fetch counters, and how much the trace
  profile trimmed (`bazel.trace.suppressed_*`).
- **Spans** are trimmed by the [trace profile](#trace-profiles): by default, failed actions
  plus the 200 slowest successful ones.
- **Log records** carry the build's `ERROR:`, `FAIL:` and `WARNING:` output, including a
  failed action's stderr and failed test output, correlated with the trace. See [Logs](#logs).

Traces use `service.name=bazel`.

## Running as a Datadog Agent sidecar

Run conduit in the same pod (or on the same host) as the Datadog Agent, with the Agent's
OTLP receiver enabled. Builds send BES to conduit; conduit sends OTLP to the Agent over
localhost.

```
Bazel ──BES gRPC :8080──▶ conduit ──OTLP gRPC :4317──▶ Datadog Agent ──▶ Datadog
```

**1. Enable OTLP ingest on the Agent** (traces are on by default once the receiver is enabled;
logs need their own switch):

```yaml
env:
  - name: DD_OTLP_CONFIG_RECEIVER_PROTOCOLS_GRPC_ENDPOINT
    value: "localhost:4317"
  - name: DD_OTLP_CONFIG_LOGS_ENABLED
    value: "true"
```

**2. Add conduit as a container in the Agent pod.** Build the image from a
[release](../../releases) binary (`conduit-linux-x86_64` or `conduit-linux-arm64`) or from
source. With the Datadog Helm chart or Operator, use their mechanism for extra containers in
the Agent pod.

```yaml
containers:
  - name: conduit
    image: <your-conduit-image>
    args:
      - serve
      - --listen-addr=0.0.0.0
      - --port=8080
      - --export=otlp
      - --otlp-endpoint=http://localhost:4317
    ports:
      - containerPort: 8080
        hostPort: 8080        # node-local Bazel clients reach conduit via the host IP
terminationGracePeriodSeconds: 15
```

**3. Point Bazel at it**, e.g. in `.bazelrc` (CI jobs typically use the node's IP, as they do
for the Agent itself):

```
build --bes_backend=grpc://<agent-host>:8080
build --build_event_publish_all_actions
build --bes_upload_mode=nowait_for_upload_complete
```

`--build_event_publish_all_actions` is needed for successful actions to appear; without it
BEP reports only failed ones. `nowait_for_upload_complete` keeps builds from waiting on BES.

**Operational notes**

- **No auth or TLS.** Keep port 8080 reachable only from build machines (node-local
  `hostPort`, network policy, or firewall). Conduit warns when bound to a non-loopback address.
- **Concurrent builds are isolated.** Each BES stream gets its own trace; streams from many
  builds can share one conduit.
- **Shutdown.** On SIGTERM conduit stops accepting streams, finalizes in-flight traces (up to
  3 s), then flushes the exporter (up to 5 s). Traces of builds still running are ended as
  errors ("conduit shut down before the build finished").
- **Drops are counted, never blocking.** The exporter queues up to 65,536 records and exports
  batches of 512 every 200 ms with a bounded retry; anything it cannot queue or export is
  dropped and the counts are logged on shutdown. BES streams are never slowed down by export.
- **No exec log in sidecar mode.** `--exec-log` tails a file on the build machine, so it only
  works when conduit runs where Bazel runs.

## Running standalone

On a developer machine or a single CI host, run conduit next to Bazel:

```bash
bazel build -c opt //rust/conduit:conduit     # or download a release binary
./bazel-bin/rust/conduit/conduit serve --export otlp --otlp-endpoint http://localhost:4317

bazel build //your:target \
  --bes_backend=grpc://127.0.0.1:8080 \
  --build_event_publish_all_actions
```

- To send to a local Datadog Agent, [`scripts/setup_datadog_agent.sh`](scripts/README.md)
  configures its OTLP receiver.
- To look at traces locally, `docker compose -f dev/docker-compose.yml up -d` starts Jaeger
  on `http://localhost:16686` with OTLP on port 4317.
- `--export stdout` writes OTLP-JSON (one export request per line) instead of sending it,
  which is handy for inspecting what would be sent.

### Replaying a recorded build

```bash
bazel build //your:target --build_event_binary_file=bep.bin
./bazel-bin/rust/conduit/conduit replay bep.bin --export stdout > trace.jsonl
python3 integration/analyze_trace.py trace.jsonl   # structure and timing checks
```

A BEP file carries no BES event times, so untimed events are placed on the build's own clock
(the latest start, finish, or action time seen so far).

## CLI flags

`serve` and `replay <FILE>` accept every flag below except `--listen-addr` and `--port`,
which are `serve`-only.

| Flag | Description | Default |
|------|-------------|---------|
| `--listen-addr <IP>` | Bind address for the BES gRPC server. Use `0.0.0.0` only when access is restricted otherwise; conduit has no auth or TLS. | `127.0.0.1` |
| `--port <PORT>` | BES gRPC port. | `8080` |
| `--export <MODE>` | `none`, `stdout` (OTLP-JSON, one request per line), or `otlp` (gRPC). | `none` |
| `--otlp-endpoint <URL>` | OTLP gRPC endpoint. | `http://localhost:4317` |
| `--trace-profile <PROFILE>` | `lightweight` or `full`. See [Trace profiles](#trace-profiles). | `lightweight` |
| `--max-action-spans <N>` | Lightweight: how many successful actions (the slowest) keep a span per invocation. | `200` |
| `--trace-min-duration-ms <MS>` | Lightweight: minimum duration for a successful cache-hit spawn to keep a span. | `500` |
| `--logs <MODE>` | `errors`, `all`, or `none`. See [Logs](#logs). | `errors` |
| `--exec-log` | Tail Bazel's compact execution log for per-spawn detail. See [Execution log](#execution-log-enrichment). | off |
| `--exec-log-max-message-mib <MIB>` | Cap on one length-delimited exec-log entry, so a corrupt length prefix cannot exhaust memory. | `64` |
| `--exec-log-max-decompressed-mib <MIB>` | Cap on total decompressed exec-log bytes (zstd bomb guard). | `2048` |
| `--no-redact` | Disable secret scrubbing. See [Secret redaction](#secret-redaction). | scrubbing on |
| `--redact-name-pattern <SUBSTR>` | Replace the default sensitive-name list (repeatable). | built-in list |
| `--log-level <LEVEL>` | Conduit's own stderr logging: `trace`, `debug`, `info`, `warn`, `error`. | `info` |

## Trace profiles

A large build runs tens of thousands of actions, almost all fast cache hits. The profile
decides how many of them become spans:

| Profile | Behavior |
|---------|----------|
| **lightweight** (default) | Failed actions, plus the `--max-action-spans` slowest successful ones, and only the targets those belong to. Exec-log spawns are kept when they fail, miss the cache, or take at least `--trace-min-duration-ms`. Successful actions are held until the invocation ends, so their spans export then. |
| **full** | Every target, action, and spawn. Complete but verbose. |

Either way, the aggregate `BuildMetrics` stay on the root span, and
`bazel.trace.suppressed_{actions,spawns,targets,logs}` record how much was trimmed.

To reduce volume further, layer backend controls on top: Agent caps (`DD_APM_MAX_TPS`,
`DD_APM_FILTER_TAGS_REJECT`), span-generated metrics for long-term numbers, and retention
filters for indexed storage.

## Logs

Bazel's console output arrives in BEP `Progress` events. `--logs` picks what becomes OTLP log
records, correlated with the invocation's trace:

- **`errors`** (default): `ERROR:` and `FAIL:` blocks at error severity, including the failed
  action's own stderr that Bazel prints right after the `ERROR:` line, `--test_output` blocks
  at error severity, and `WARNING:` blocks at warn severity. Progress chatter (`INFO:`,
  `Loading:`, `Analyzing:`, `[N / M] ...`) is dropped. Blocks split across progress events
  are reassembled.
- **`all`**: one info record per progress event and output stream.
- **`none`**: no log records.

Records are redacted, ANSI-stripped, and capped at 64 KiB. An invocation emits at most 256
records; the rest are counted in `bazel.trace.suppressed_logs` on the root span.

## Execution log enrichment

BEP describes actions but not the processes they spawn. With `--exec-log`, when a build runs
with `--execution_log_compact_file=<path>`, conduit opens that file (relative paths resolve
against the workspace) and tails it while the build runs. Each decoded `SpawnExec`:

1. Matches its action span by `(target label, mnemonic, primary output)`.
2. On the action's first spawn, copies the runner, cache hit, and `SpawnMetrics` stage
   timings onto the action span. Later spawns (retries, dynamic-execution races) only
   increment `bazel.action.spawn.count`.
3. Becomes a `spawn` child span with its own runner, cache hit, command line, stage timings,
   and input metrics.

Spawns that arrive before their action are held briefly. Spawns that never match an action
(BEP does not report every action, e.g. those of external dependencies) are grouped under
synthesized action spans marked `bazel.target.synthetic = true`. Actions that never spawn a
process (`FileWrite`, `SymlinkTree`, ...) end with `bazel.action.spawn.count = 0`.

Notes:

- Conduit must run on the build machine, since it reads a local file. The copy Bazel uploads
  to the remote cache only appears after the build, so it is not a live source.
- Only the compact format is supported. `--execution_log_binary_file` and
  `--execution_log_json_file` lack the dedup table that makes streaming possible; conduit
  logs a warning and ignores them.
- Streaming is chunky: Bazel's zstd stream emits a block per 128 KiB of uncompressed input
  and is only flushed at `close()`, so spawns arrive in bursts of a few hundred, and
  `finish()` waits up to 2 s for the final chunk.

## Secret redaction

Bazel puts environment and user-defined values on the command line (`--client_env=NAME=VALUE`,
`--action_env=`, `--test_env=`, `--repo_env=`, `--host_action_env=`, `--define=NAME=VALUE`),
workspace status (`STABLE_*` keys from `--workspace_status_command`) often carries CI tokens,
and console output can echo any of it. Conduit scrubs all of these before they reach a span or
log record (`rust/conduit/src/otel/redact.rs`):

- **Argv tokens**: the explicit command line, action command lines, and spawn command lines.
  The `VALUE` in `--flag=NAME=VALUE` becomes `***` when `NAME` matches.
- **Name/value pairs**: workspace status entries, by key name.
- **Free text**: console output in log records, token by token.

The pass-through form `--client_env=NAME` (no value on the wire) is left alone.

Default sensitive-name substrings: `TOKEN`, `SECRET`, `PASSWORD`, `PASSWD`, `CREDENTIAL`,
`COOKIE`, `APIKEY`, `API_KEY`, `ACCESS_KEY`, `PRIVATE_KEY`, `AUTH`. Plain `KEY` is excluded
because it would match `MONKEY`. `--redact-name-pattern` (repeatable) replaces the list;
`--no-redact` turns scrubbing off, which is only safe when the backend is fully trusted.

For defense in depth, also scrub on the Agent with `apm_config.replace_tags`:

```yaml
apm_config:
  replace_tags:
    - name: "bazel.explicit_cmd_line"
      pattern: "--client_env=([A-Z0-9_]*?(TOKEN|SECRET|PASSWORD|KEY|CREDENTIAL)[A-Z0-9_]*)=[^ ]+"
      repl:    "--client_env=$1=***"
```

The OpenTelemetry Collector's
[`redactionprocessor`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/processor/redactionprocessor)
does the same when traces pass through a collector.

## Attribute reference

Attributes use `bazel.<component>.<field>`; `rust/conduit/src/otel/attributes.rs` has the full
list. They are a de-facto schema and may change between releases.

- **Root**: `bazel.invocation_id`, `bazel.command`, `bazel.patterns`, `bazel.exit_code`,
  `bazel.metrics.wall_time_ms`, `bazel.metrics.action_cache_hits`,
  `bazel.metrics.runner.remote_cache_hit`, `bazel.fetch.count`, `bazel.trace.suppressed_*`, ...
- **Target**: `bazel.target.label`, `bazel.target.kind`, `bazel.target.success`,
  `bazel.target.cached`, and for tests the summary (`bazel.test.overall_status`, run, attempt,
  shard, and cached counts).
- **Action**: `bazel.target.label`, `bazel.action.mnemonic`, `bazel.action.exit_code`,
  `bazel.action.cache_hit` and `bazel.action.runner` (Bazel 10+), `bazel.action.spawn.runner`,
  `bazel.action.spawn.cache_hit`, ... (from `--exec-log`).
- **Test**: `bazel.test.status`, `bazel.test.attempt`, `bazel.test.cache`
  (`none`/`local`/`remote`), `bazel.test.strategy`.
- **Spawn**: `bazel.spawn.runner`, `bazel.spawn.cache_hit`, `bazel.spawn.command`,
  `bazel.spawn.execution_wall_time_ms`, `bazel.spawn.remote_cacheable` (only when `false`), ...

## Design notes

- **Timestamps are clamped to the invocation.** Cache hits replay the original execution
  timestamps, which can be days old. Every BEP-supplied range is clamped to the root span's
  window, and children are kept inside their parents, because Datadog nests flame graphs by
  time containment.
- **Targets complete at the end of the invocation.** Tests, test summaries, and late actions
  can arrive after `TargetCompleted`, so target spans close at `finish()` and stretch to cover
  their children. Repeated completions of a label (several configurations) merge into one span.
- **Labels match exactly.** BEP action labels and exec-log target labels both use the canonical
  form (`@@repo+//:target`) on Bazel 9.
- **`Progress.children` is about ordering, not content.** The text in a progress event is not
  related to the children it announces, so conduit only uses the text.
- **No OpenTelemetry SDK.** Conduit writes OTLP directly from vendored
  [opentelemetry-proto](https://github.com/open-telemetry/opentelemetry-proto) definitions
  (`proto/opentelemetry/`), with its own small span model and exporter.

## Limitations

- **No phase spans.** Loading, analysis, and execution are not modeled as spans; their
  metrics are root attributes.
- **No span links.** BEP's DAG is reduced to parent/child relationships.
- **No sampling in conduit.** The trace profile trims deterministically; use backend controls
  for sampling.
- **Exec-log enrichment needs co-location** and arrives in bursts (see above).
- **`TestProgress` and `ExecRequest` events are ignored.**

## Development

The build uses Bazel with `rules_rust` and `rules_rs`; `Cargo.toml` exists for IDE support
and dependency resolution only.

```bash
bazel build //rust/conduit:conduit
bazel test //...                      # unit tests, router tests, hermetic trace test
./integration/run_full_trace_scenario.sh   # real build against `conduit serve --exec-log`
```

| Path | Contents |
|------|----------|
| `rust/conduit/src/bep/` | BEP replay and the event router |
| `rust/conduit/src/grpc/` | BES gRPC server, one routing worker per stream |
| `rust/conduit/src/otel/` | Mapper (BEP to spans and logs), span model, OTLP exporter, attributes, redaction |
| `rust/conduit/src/exec_log/` | Compact exec-log decoder and live tailer |
| `rust/conduit/src/state/` | Trace profile and policy |
| `proto/` | Vendored Bazel BEP, BES, spawn, and OTLP protos |
| `integration/` | Hermetic trace test, full scenario script, trace analyzer |
| `scripts/` | Local Datadog Agent setup |
| `dev/` | Jaeger for local viewing |

See [CONTRIBUTING.md](CONTRIBUTING.md) to contribute. Licensed under the
[Apache License 2.0](LICENSE); third-party licenses are listed in
[LICENSE-3rdparty.csv](LICENSE-3rdparty.csv).
