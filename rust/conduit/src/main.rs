use clap::Parser;
use conduit_lib::bep::{BepDecoder, EventRouter};
use std::fs::File;
use std::path::PathBuf;
use tracing::{info, error, Level};
use tracing_subscriber::FmtSubscriber;

/// BEP to OpenTelemetry trace converter
#[derive(Parser, Debug)]
#[command(name = "conduit")]
#[command(about = "Convert Bazel Build Event Protocol events to OpenTelemetry traces")]
struct Args {
    /// Input BEP JSON file
    #[arg(short, long)]
    input: Option<PathBuf>,

    /// gRPC server port for BES (Build Event Service)
    #[arg(short = 'p', long, default_value = "8080")]
    grpc_port: u16,

    /// Datadog Agent endpoint for OTLP export
    #[arg(long, default_value = "localhost:4317")]
    dd_agent: String,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
    log_level: String,
}

fn main() -> anyhow::Result<()> {
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

    if let Some(input_path) = args.input {
        // Process BEP JSON file
        process_json_file(&input_path)?;
    } else {
        // Start gRPC server (not yet implemented)
        info!("gRPC server mode not yet implemented");
        info!("Use --input <file> to process a BEP JSON file");
    }

    Ok(())
}

fn process_json_file(path: &PathBuf) -> anyhow::Result<()> {
    info!("Processing BEP JSON file: {}", path.display());

    let file = File::open(path)?;
    let decoder = BepDecoder::new();
    let events = decoder.decode_json_file(file)?;

    info!("Loaded {} events from file", events.len());

    let mut router = EventRouter::new();

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
