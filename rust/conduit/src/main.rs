use clap::Parser;
use conduit_lib::bep::{BepDecoder, EventRouter};
use conduit_lib::grpc::run_server;
use conduit_lib::otel::{
    init_logger_provider, ExportConfig, Redactor, DEFAULT_OTLP_MAX_EXPORT_BATCH_SIZE,
    DEFAULT_REDACT_PATTERNS,
};
use std::fs::File;
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

/// BEP to OpenTelemetry trace converter
#[derive(Parser, Debug)]
#[command(name = "conduit")]
#[command(about = "Convert Bazel Build Event Protocol events to OpenTelemetry traces")]
struct Args {
    /// Input BEP JSON file (mutually exclusive with gRPC server mode)
    #[arg(short, long)]
    input: Option<PathBuf>,

    /// Start gRPC server for BES (Build Event Service)
    #[arg(short, long)]
    serve: bool,

    /// gRPC server port
    #[arg(short = 'p', long, default_value = "8080")]
    port: u16,

    /// Export mode: none, stdout, otlp
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
}

impl Args {
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
    let args = Args::parse();

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

    // LoggerProvider for OTel logs (and for mapper when using with_export)
    let log_provider = init_logger_provider(&export_config)?;

    let redactor = args.build_redactor();
    if redactor.is_enabled() {
        info!("Command-line redaction: enabled");
    } else {
        info!("Command-line redaction: DISABLED — secrets in --client_env may leak to traces");
    }

    let mut router = EventRouter::new().with_redactor(redactor);
    if let Some(lp) = log_provider {
        router = router.with_export(export_config.clone(), lp);
    }

    if let Some(input_path) = args.input {
        process_json_file(&input_path, &mut router)?;
        router.shutdown_providers()?;
    } else if args.serve {
        let addr: SocketAddr = format!("0.0.0.0:{}", args.port).parse()?;
        run_server(addr, router).await?;
    } else {
        info!("Usage:");
        info!("  Process JSON file: conduit --input <file.json> [--export stdout|otlp]");
        info!("  Start gRPC server: conduit --serve [--port 8080] [--export stdout|otlp]");
        info!("");
        info!("For Bazel, use: bazel build //... --bes_backend=grpc://localhost:{}", args.port);
    }

    Ok(())
}

fn process_json_file(path: &PathBuf, router: &mut EventRouter) -> anyhow::Result<()> {
    info!("Processing BEP JSON file: {}", path.display());

    let file = File::open(path)?;
    let decoder = BepDecoder::new();
    let events = decoder.decode_json_file(file)?;

    info!("Loaded {} events from file", events.len());

    for event in &events {
        if let Err(e) = router.route(event) {
            error!("Error routing event: {}", e);
        }

        if event.is_last_message() {
            info!("Received last message");
            break;
        }
    }

    // Print summary
    let summary = router.state().summary();
    println!("\n{}", summary);

    Ok(())
}
