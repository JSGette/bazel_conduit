use clap::Parser;
use conduit_lib::bep::{BepDecoder, EventRouter};
use conduit_lib::grpc::run_server;
use conduit_lib::otel::{ExportConfig, OtelMapper, init_tracer_provider};
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

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
    log_level: String,
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
        },
        _ => ExportConfig::None,
    };
    info!("Export mode: {:?}", export_config);

    // Initialise TracerProvider (None when export=none)
    let provider = init_tracer_provider(&export_config)?;

    // Build the mapper if we have a provider
    let make_mapper = || -> Option<OtelMapper> {
        use opentelemetry::trace::TracerProvider;
        let p = provider.as_ref()?;
        Some(OtelMapper::new(p.tracer("conduit")))
    };

    if let Some(input_path) = args.input {
        let mut router = EventRouter::new();
        if let Some(m) = make_mapper() {
            router = router.with_mapper(m);
        }
        process_json_file(&input_path, &mut router)?;
        router.finish();
    } else if args.serve {
        let addr: SocketAddr = format!("0.0.0.0:{}", args.port).parse()?;
        let mut router = EventRouter::new();
        if let Some(m) = make_mapper() {
            router = router.with_mapper(m);
        }
        run_server(addr, router).await?;
    } else {
        info!("Usage:");
        info!("  Process JSON file: conduit --input <file.json> [--export stdout|otlp]");
        info!("  Start gRPC server: conduit --serve [--port 8080] [--export stdout|otlp]");
        info!("");
        info!("For Bazel, use: bazel build //... --bes_backend=grpc://localhost:{}", args.port);
    }

    // Flush & shutdown the TracerProvider
    if let Some(p) = provider {
        p.shutdown()?;
        info!("TracerProvider shut down");
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
