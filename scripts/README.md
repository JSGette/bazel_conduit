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
- `bazel` / `bazelisk` on `PATH`
- `dd-auth` (Datadog corp tooling) — only required if you don't already have a personal API key

---

## 1. Get an API key

If you have one already, skip to step 2.

```bash
# Prints DD_API_KEY=…, DD_APP_KEY=…, DD_SITE=… one per line.
# Add --no-cache to bypass the keychain cache and force a fresh fetch.
dd-auth --domain app.datadoghq.com -o
```

The first time, macOS Keychain Access will prompt for your login password three times (once per stored item). Click **Always Allow** on each prompt to suppress future prompts. If you accidentally clicked plain "Allow", delete the entries and let `dd-auth` recreate them:

```bash
security delete-generic-password -s 'dd-auth' -a 'app.datadoghq.com:api-key' 2>/dev/null
security delete-generic-password -s 'dd-auth' -a 'app.datadoghq.com:app-key' 2>/dev/null
security delete-generic-password -s 'dd-auth' -a 'app.datadoghq.com:pat'     2>/dev/null
```

---

## 2. Edit `datadog.yaml`

| Platform | Path                                  | Owner             | Edit with     |
|----------|---------------------------------------|-------------------|---------------|
| macOS    | `/opt/datadog-agent/etc/datadog.yaml` | your user account | direct edit   |
| Linux    | `/etc/datadog-agent/datadog.yaml`     | `dd-agent` user   | `sudo $EDITOR`|

Set / replace these top-level keys:

```yaml
api_key: <DD_API_KEY from step 1>
site: <DD_SITE from step 1, e.g. datadoghq.com>
```

If your file already has a top-level `api_key:` (Datadog corp installs do), replace its value rather than adding a duplicate. **Do not** touch any `additional_endpoints:` blocks — those carry separate keys and are typically managed by IT.

Then add an OTLP receiver block (skip if `otlp_config:` is already present and configured for both `traces` and `logs`):

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

## 3. Restart the agent

| Platform | Command                                                       |
|----------|---------------------------------------------------------------|
| macOS    | `launchctl kickstart -k gui/$(id -u)/com.datadoghq.agent`     |
| Linux    | `sudo systemctl restart datadog-agent`                        |

Verify the OTLP receiver is up:

```bash
datadog-agent status 2>&1 | rg -i 'OTLP|API key status'
```

You should see `OTLP receiver: enabled` and `API Keys status: API key ending with …: API Key valid`.

---

## 4. Configure Bazel

Add to `user.bazelrc` (or `~/.bazelrc` for global, or `.bazelrc` if your repo allows committed config):

```
common --bes_backend=grpc://localhost:8080
common --execution_log_compact_file=/tmp/conduit-exec.log

# Optional: emit BEP for every action, not just failed/test ones.
build --build_event_publish_all_actions
```

`bes_backend` points at the conduit gRPC server (default port `8080`); `execution_log_compact_file` makes Bazel write the post-build spawn log that conduit ingests for full per-spawn detail (commands, cache hits, timings).

---

## 5. Run conduit

In a dedicated terminal:

```bash
bazel run //rust/conduit:conduit -c opt -- \
  --serve --port 8080 \
  --export otlp --otlp-endpoint http://localhost:4317
```

(Replace `4317` with whatever you put in `datadog.yaml`. Default Datadog Agent OTLP port is `4317`; corp/IT installs sometimes use `14317` to avoid collisions.)

Leave it running and trigger a build in another terminal:

```bash
bazel build //your:target
```

Traces should appear in your Datadog org under **APM → Services → bazel-conduit** within ~10 seconds of the build finishing.

---

## Troubleshooting

| Symptom                                                          | Likely cause                                                                                       | Fix                                                                                                  |
|------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| `datadog-agent restart-service` "unknown command"                | That subcommand is Linux-only.                                                                     | macOS: `launchctl kickstart -k gui/$(id -u)/com.datadoghq.agent`.                                    |
| Repeated keychain password prompts on every `dd-auth` invocation | Keychain ACL is set to "Confirm before allowing".                                                  | Click "Always Allow" on the next prompt, or delete + recreate the entries (see step 1).              |
| `OTLP receiver: disabled` in `datadog-agent status`              | `otlp_config:` block missing or malformed in `datadog.yaml`.                                       | Re-check step 2; YAML indentation matters.                                                           |
| Bazel build hangs at `--bes_backend=grpc://localhost:8080`       | conduit isn't running on that port.                                                                | Start conduit (step 5) or remove the `bes_backend` line from `bazelrc`.                              |
| Traces show up but spawn child spans don't                       | `--execution_log_compact_file=` not set, or conduit and Bazel are on different hosts.              | Add the flag to `bazelrc`. Conduit reads the file locally — it must run on the same host as Bazel. |
| API key in datadog.yaml doesn't match the org I'm targeting      | `dd-auth` cached an older key, or you authed against the wrong `--domain`.                         | `./setup_datadog_agent.sh --refresh-token --domain <correct-domain>` (or `dd-auth --no-cache …`).    |
