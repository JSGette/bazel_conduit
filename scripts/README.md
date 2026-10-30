# Datadog Agent + Bazel Conduit setup

Two paths to wire your Bazel builds into Datadog via a local agent:

1. **Automated** — run [`./setup_datadog_agent.sh`](./setup_datadog_agent.sh) and follow the printed instructions.
2. **Manual** — the steps below. Use this if you want to understand exactly what the script does, or if your environment differs from what the script handles.

The script is just an idempotent wrapper around the manual steps; both end at the same configuration.

---

## Prerequisites

- Datadog Agent installed locally
  - macOS: `brew install --cask datadog-agent` (or the official `.dmg`)
  - Linux: `DD_API_KEY=… DD_SITE=… bash -c "$(curl -L https://install.datadoghq.com/scripts/install_script_agent7.sh)"`
- The Agent already configured with a valid API key and `site` for an org that can ingest OTLP traces and logs. Neither the script nor these steps manage the key; check it with `datadog-agent status` (`API Key valid`).
- `bazel` / `bazelisk` on `PATH`

---

## 1. Edit `datadog.yaml`

| Platform | Path                                  | Owner                                   | Edit with     |
|----------|---------------------------------------|-----------------------------------------|---------------|
| macOS    | `/opt/datadog-agent/etc/datadog.yaml` | `_dd-agent` (system install) or your user | `sudo $EDITOR` or direct edit |
| Linux    | `/etc/datadog-agent/datadog.yaml`     | `dd-agent` user                         | `sudo $EDITOR`|

Add an OTLP receiver block (skip if `otlp_config:` is already present and configured for both `traces` and `logs`):

```yaml
logs_enabled: true
otlp_config:
  receiver:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318
  traces:
    enabled: true
  logs:
    enabled: true
```

If something else on the host already binds `4317`/`4318`, pick a different pair (the script's fallback is `14317`/`14318`, then `24317`/`24318`, …). Whatever ports you pick, you'll pass them to conduit's `--otlp-endpoint` in step 4.

---

## 2. Restart the agent

| Platform | Command                                                       |
|----------|---------------------------------------------------------------|
| macOS (system daemon) | `sudo launchctl kickstart -k system/com.datadoghq.agent` |
| macOS (per-user agent) | `launchctl kickstart -k gui/$(id -u)/com.datadoghq.agent` |
| Linux    | `sudo systemctl restart datadog-agent`                        |

Verify the OTLP receiver is up:

```bash
datadog-agent status 2>&1 | rg -i 'OTLP|API key status'
```

You should see `OTLP receiver: enabled` and `API Keys status: API key ending with …: API Key valid`.

---

## 3. Configure Bazel

Add to `user.bazelrc` (or `~/.bazelrc` for global, or `.bazelrc` if your repo allows committed config):

```
common --bes_backend=grpc://127.0.0.1:8080

# Optional: emit BEP for every action, not just failed/test ones.
build --build_event_publish_all_actions

# Optional, needs conduit started with --exec-log: per-spawn detail.
# common --execution_log_compact_file=/tmp/conduit-exec.log
```

`bes_backend` points at the conduit gRPC server (default port `8080`). For per-spawn detail (commands, cache hits, timing breakdown), also set `execution_log_compact_file` and start conduit with `--exec-log` (`CONDUIT_EXEC_LOG=/tmp/conduit-exec.log` does both in `setup_datadog_agent.sh`).

Bazel builds emit tens of thousands of spans, mostly fast cache hits. By default conduit emits a `lightweight` trace: failed actions plus the 200 slowest successful ones, and only failing, cache-missed, or slow (>= 500 ms) spawns. Use `--trace-profile full` for everything, and tune `--max-action-spans` and `--trace-min-duration-ms`. See the [Trace Profiles](../README.md#trace-profiles) section for the ingestion-reduction rollout (including agent-side sampling and retention filters).

---

## 4. Run conduit

In a dedicated terminal:

```bash
bazel run //rust/conduit:conduit -c opt -- \
  serve --port 8080 \
  --export otlp --otlp-endpoint http://localhost:4317
```

(Replace `4317` with whatever you put in `datadog.yaml`. Default Datadog Agent OTLP port is `4317`; corp/IT installs sometimes use `14317` to avoid collisions.)

Leave it running and trigger a build in another terminal:

```bash
bazel build //your:target
```

Traces should appear in your Datadog org under **APM → Services → bazel** within ~10 seconds of the build finishing.

---

## Troubleshooting

| Symptom                                                          | Likely cause                                                                                       | Fix                                                                                                  |
|------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| `datadog-agent restart-service` "unknown command"                | That subcommand is Linux-only.                                                                     | macOS: `sudo launchctl kickstart -k system/com.datadoghq.agent` (or `gui/$(id -u)/…` for a per-user install). |
| `OTLP receiver: disabled` in `datadog-agent status`              | `otlp_config:` block missing or malformed in `datadog.yaml`.                                       | Re-check step 1; YAML indentation matters.                                                           |
| Bazel build hangs at `--bes_backend=grpc://127.0.0.1:8080`       | conduit isn't running on that port.                                                                | Start conduit (step 4) or remove the `bes_backend` line from `bazelrc`.                              |
| Traces show up but spawn child spans don't                       | conduit not started with `--exec-log`, `--execution_log_compact_file=` not set, or conduit and Bazel are on different hosts. | Start conduit with `--exec-log` and add the flag to `bazelrc`. Conduit reads the file locally — it must run on the same host as Bazel. |
| Agent reports the API key invalid, or traces land in the wrong org | `api_key` / `site` in `datadog.yaml` are wrong or the key lacks ingest permission.                 | Fix the key and `site` in `datadog.yaml` yourself (the script doesn't touch them) and restart the agent. |
