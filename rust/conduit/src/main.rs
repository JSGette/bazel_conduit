// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

use clap::{Args, Parser, Subcommand};
use conduit_lib::bep::{replay, EventRouter};
use conduit_lib::grpc::run_server;
use conduit_lib::otel::{
    ExportConfig, Exporter, LogMode, Redactor, DEFAULT_OTLP_MAX_EXPORT_BATCH_SIZE,
    DEFAULT_REDACT_PATTERNS,
};
use conduit_lib::state::{
    TracePolicy, TraceProfile, DEFAULT_MAX_ACTION_SPANS, DEFAULT_TRACE_MIN_DURATION_MS,
};
use std::fs::File;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use tokio::signal::unix::{signal, SignalKind};
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

/// BEP to OpenTelemetry trace converter
#[derive(Parser, Debug)]
#[command(name = "conduit")]
#[command(about = "Convert Bazel Build Event Protocol events to OpenTelemetry traces")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the BES gRPC server; point Bazel at it with
    /// `--bes_backend=grpc://127.0.0.1:<port>`.
    Serve {
        /// gRPC server port
        #[arg(short = 'p', long, default_value = "8080")]
        port: u16,

        /// Bind address. Defaults to loopback because conduit has no auth
        /// or TLS; any other address must be paired with external access
        /// control (firewall, sidecar mTLS, network policy).
        #[arg(long, value_name = "IP", default_value_t = IpAddr::V4(Ipv4Addr::LOCALHOST))]
        listen_addr: IpAddr,

        #[command(flatten)]
        common: Common,
    },
    /// Replay a binary BEP file written with `--build_event_binary_file`.
    Replay {
        file: PathBuf,

        #[command(flatten)]
        common: Common,
    },
}

#[derive(Args, Debug)]
struct Common {
    /// Export mode: none, stdout (OTLP-JSON lines), otlp (gRPC)
    #[arg(long, default_value = "none")]
    export: String,

    /// OTLP endpoint (used when --export=otlp)
    #[arg(long, default_value = "http://localhost:4317")]
    otlp_endpoint: String,

    /// Max spans per OTLP export RPC. Keep below the receiver's gRPC
    /// `max_recv_msg_size` (4 MiB default) to avoid RESOURCE_EXHAUSTED.
    #[arg(long, default_value_t = DEFAULT_OTLP_MAX_EXPORT_BATCH_SIZE)]
    otlp_max_export_batch_size: usize,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
    log_level: String,

    /// Disable in-process scrubbing of `--client_env=NAME=VALUE` style flags
    /// in command-line span attributes. Use only when the receiving backend
    /// is fully trusted (or has its own scrubbing layer).
    #[arg(long, default_value_t = false)]
    no_redact: bool,

    /// Case-insensitive substring(s) marking an env-var name as sensitive.
    /// Repeatable. When omitted, conduit uses a built-in default list
    /// (TOKEN, SECRET, PASSWORD, …). When provided at least once, the
    /// supplied list fully replaces the default.
    #[arg(long = "redact-name-pattern", value_name = "SUBSTR")]
    redact_name_patterns: Vec<String>,

    /// Enrich actions with per-spawn detail from Bazel's compact execution
    /// log (`--execution_log_compact_file`). Off by default: conduit ignores
    /// the exec log even when the build writes one.
    #[arg(long, default_value_t = false)]
    exec_log: bool,

    /// Per-message cap (in MiB) on length-delimited entries in Bazel's
    /// `--execution_log_{binary,compact}_file`. A malformed varint length
    /// prefix would otherwise be fed straight to `Vec::resize`. The default
    /// (64 MiB) is ~10x the largest realistic entry; bump it only when
    /// ingesting pathological logs.
    #[arg(long, value_name = "MIB", default_value_t = 64)]
    exec_log_max_message_mib: u32,

    /// Cap (in MiB) on total *decompressed* bytes pulled out of a compact
    /// execution log's zstd frame. The per-message cap above only bounds
    /// one entry; this one bounds the overall stream so a tiny malicious
    /// frame can't expand to GiB of valid-but-bogus messages. Default
    /// (2 GiB) is ~4x the largest realistic decompressed compact log.
    #[arg(long, value_name = "MIB", default_value_t = 2048)]
    exec_log_max_decompressed_mib: u32,

    /// Trace verbosity: `lightweight` (default) or `full`.
    ///
    /// `full` emits a span per action/spawn/target. `lightweight` keeps
    /// failed actions plus the `--max-action-spans` slowest successful ones,
    /// and only the targets they belong to; aggregate metrics stay on the
    /// root span either way.
    #[arg(long, default_value = "lightweight")]
    trace_profile: String,

    /// Lightweight only: minimum duration (ms) for a successful, cache-hit
    /// spawn to keep its span. Failed and cache-missed spawns are kept.
    #[arg(long, default_value_t = DEFAULT_TRACE_MIN_DURATION_MS)]
    trace_min_duration_ms: i64,

    /// Lightweight only: how many successful actions (the slowest) keep
    /// their span per invocation.
    #[arg(long, default_value_t = DEFAULT_MAX_ACTION_SPANS)]
    max_action_spans: usize,

    /// Which Bazel console output becomes OTLP log records: `errors`
    /// (ERROR/FAIL/WARNING blocks and failed test output), `all`, or `none`.
    /// Records are capped at 64 KiB and 256 per invocation.
    #[arg(long, default_value = "errors")]
    logs: String,
}

impl Common {
    fn build_redactor(&self) -> Redactor {
        if self.no_redact {
            return Redactor::disabled();
        }
        if self.redact_name_patterns.is_empty() {
            return Redactor::new(true, DEFAULT_REDACT_PATTERNS.iter().copied());
        }
        Redactor::new(true, self.redact_name_patterns.iter().map(String::as_str))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let args = match &cli.command {
        Command::Serve { common, .. } | Command::Replay { common, .. } => common,
    };

    // Set up logging
    let level = match args.log_level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    let subscriber = FmtSubscriber::builder()
        .with_max_level(level)
        .with_target(true)
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Conduit - BEP to OpenTelemetry converter");
    info!("Log level: {}", args.log_level);

    // Parse export config
    let export_config = match args.export.as_str() {
        "stdout" => ExportConfig::Stdout,
        "otlp" => ExportConfig::Otlp {
            endpoint: args.otlp_endpoint.clone(),
            max_export_batch_size: args.otlp_max_export_batch_size,
        },
        _ => ExportConfig::None,
    };
    info!("Export mode: {:?}", export_config);

    let exporter = Exporter::start(&export_config)?;

    let redactor = args.build_redactor();
    if redactor.is_enabled() {
        info!("Command-line redaction: enabled");
    } else {
        info!("Command-line redaction: DISABLED — secrets in --client_env may leak to traces");
    }

    let exec_log_max_message_bytes =
        (args.exec_log_max_message_mib as usize).saturating_mul(1024 * 1024);
    let exec_log_max_decompressed_bytes =
        (args.exec_log_max_decompressed_mib as usize).saturating_mul(1024 * 1024);
    if args.exec_log {
        info!(
            "Execution log enrichment: enabled ({} MiB per message, {} MiB decompressed total)",
            args.exec_log_max_message_mib, args.exec_log_max_decompressed_mib
        );
    }

    let trace_profile = TraceProfile::parse(&args.trace_profile).ok_or_else(|| {
        anyhow::anyhow!(
            "invalid --trace-profile '{}' (expected lightweight or full)",
            args.trace_profile
        )
    })?;
    let trace_policy = TracePolicy::new(
        trace_profile,
        args.trace_min_duration_ms,
        args.max_action_spans,
    );
    info!("Trace profile: {trace_policy}");
    let log_mode = LogMode::parse(&args.logs).ok_or_else(|| {
        anyhow::anyhow!("invalid --logs '{}' (expected errors, all or none)", args.logs)
    })?;
    info!("Logs: {log_mode}");

    let export = exporter.as_ref().map(|e| (e.tracer(), e.logger()));
    let exec_log = args.exec_log;
    let make_router = move || {
        let router = EventRouter::new()
            .with_exec_log(exec_log)
            .with_redactor(redactor.clone())
            .with_exec_log_max_message_bytes(exec_log_max_message_bytes)
            .with_exec_log_max_decompressed_bytes(exec_log_max_decompressed_bytes)
            .with_trace_policy(trace_policy)
            .with_log_mode(log_mode);
        match &export {
            Some((tracer, logger)) => router.with_export(tracer.clone(), logger.clone()),
            None => router,
        }
    };

    match cli.command {
        Command::Replay { file, .. } => {
            let mut router = make_router();
            let count = replay(File::open(&file)?, &mut router)?;
            router.finish();
            info!("Replayed {count} events from {}: {}", file.display(), router.summary());
        }
        Command::Serve { port, listen_addr, .. } => {
            serve(SocketAddr::new(listen_addr, port), make_router).await?;
        }
    }

    if let Some(exporter) = exporter {
        exporter.shutdown().await;
    }
    Ok(())
}

async fn serve(
    addr: SocketAddr,
    make_router: impl Fn() -> EventRouter + Send + Sync + 'static,
) -> anyhow::Result<()> {
    if !addr.ip().is_loopback() {
        warn!(
            %addr,
            "Binding gRPC server to a non-loopback address. Conduit has no auth or TLS; \
             ensure access is restricted via firewall / sidecar / network policy."
        );
    }
    let mut sigterm = signal(SignalKind::terminate())?;
    let shutdown = async move {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => info!("SIGINT received, shutting down"),
            _ = sigterm.recv() => info!("SIGTERM received, shutting down"),
        }
    };
    run_server(addr, make_router, shutdown).await
}
