# BEP to OpenTelemetry Trace Design

## Overview

This document describes the design for streaming Bazel Build Event Protocol (BEP) messages as protobufs and publishing OpenTelemetry traces via OTLP.

## High-Level Architecture

```
BEP Source -> Protobuf Decoder -> Correlator / Model -> OTel Mapper -> OTLP Exporter
```

## Mapping Principles

1. **1 trace = 1 Bazel invocation**
2. **Root span** created on `BuildStarted`, closed on `BuildFinished`
3. **Primary value** comes from **Target spans**
4. `NamedSetOfFiles` is lazily resolved for outputs
5. Correct handling of out-of-order events using `EventID` index
6. Secondary parents are represented using **OTel Span Links**
7. `ActionExecuted` spans are sampled (failed/slow only by default)
8. `Progress` events are ignored if `stdout`/`stderr` are empty

## Span Naming Conventions

| Event Type | Span Name |
|------------|-----------|
| `BuildStarted` | `bazel.invocation` |
| `TargetConfigured` | `bazel.target` |
| `ActionExecuted` | `bazel.action.{mnemonic}` |
| `TestResult` | `bazel.test.attempt` |
| `PatternExpanded` | `bazel.phase.loading` |
| `BuildMetrics` | `bazel.phase.metrics` |

## Common Attributes

### Trace-Level (from BuildStarted)
- `bazel.invocation_id`: UUID of the invocation
- `bazel.command`: Build command (build, test, run, etc.)
- `bazel.workspace`: Workspace root path

### Target Span Attributes
- `bazel.target.label`: Target label (//pkg:target)
- `bazel.target.kind`: Target rule kind (cc_library, go_binary, etc.)
- `bazel.target.config_hash`: Configuration hash

### Action Span Attributes
- `bazel.action.mnemonic`: Action type (CppCompile, GoCompile, etc.)
- `bazel.action.exit_code`: Exit code
- `bazel.action.primary_output`: Primary output file

## BEP Event → OTel Mapping

| BEP Event | Creates Span | Parent | Key Attributes | Action |
|-----------|--------------|--------|----------------|--------|
| `BuildStarted` | `bazel.invocation` | (root) | invocation_id, command | Start trace |
| `OptionsParsed` | - | - | startup_options | Add to invocation |
| `WorkspaceStatus` | - | - | user, host | Add to invocation |
| `Configuration` | - | - | platform, mnemonic | Cache for targets |
| `PatternExpanded` | `bazel.phase.loading` | invocation | patterns | Optional phase span |
| `TargetConfigured` | `bazel.target` | phase/invocation | label, kind | Start target span |
| `NamedSetOfFiles` | - | - | files | Cache for outputs |
| `TargetCompleted` | - | - | success, outputs | End target span |
| `ActionExecuted` | `bazel.action.{type}` | target | mnemonic, exit_code | Sampled |
| `TestResult` | `bazel.test.attempt` | target | status, attempt | Test spans |
| `TestSummary` | - | - | overall_status | Add to target |
| `BuildFinished` | - | - | exit_code, success | End trace |
| `BuildMetrics` | `bazel.phase.metrics` | invocation | action_counts | Metrics span |
| `Progress` | - | - | - | Ignore if empty |

## Recommended Phases

1. **Loading Phase**: `PatternExpanded` to first `TargetConfigured`
2. **Analysis Phase**: `TargetConfigured` events
3. **Execution Phase**: `ActionExecuted` events
4. **Testing Phase**: `TestResult` events
5. **Metrics Phase**: `BuildMetrics` event

## Correlation / DAG Handling

- Use `EventID` to track parent-child relationships
- `children` field in events declares expected child events
- Build an index: `EventID -> Event` for out-of-order handling
- Secondary parents become **Span Links** (not parent-child)

## Outputs Handling

1. `NamedSetOfFiles` events arrive with `id` (e.g., "3")
2. Cache: `namedset_id -> [FileInfo]`
3. `TargetCompleted` references `fileSets` with namedset IDs
4. Resolve outputs lazily when completing target span

## Span Status Rules

| Condition | OTel Status |
|-----------|-------------|
| `success = true` | `OK` |
| `success = false` | `ERROR` |
| `aborted` | `ERROR` with description |
| Test failed | `ERROR` |
| Action exit_code != 0 | `ERROR` |

## Sampling Policy

| Span Type | Policy |
|-----------|--------|
| Target spans | Always |
| Action spans | Failed + top N slowest |
| Test attempts | Only on failure |
| Progress | Never |

## CLI Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--input` | BEP input (file or `-` for stdin) | - |
| `--grpc-port` | BES gRPC server port | 8080 |
| `--otel-endpoint` | OTLP endpoint | localhost:4317 |
| `--actions` | Action sampling: `none`, `failed`, `all` | `failed` |
| `--log-level` | Log verbosity | `info` |

## Minimal MVP

1. Parse BEP from JSON file or gRPC stream
2. Create invocation span (BuildStarted → BuildFinished)
3. Create target spans (TargetConfigured → TargetCompleted)
4. Resolve outputs from NamedSetOfFiles
5. Export to Datadog via OTLP
6. Handle failed actions only (lightweight mode)
