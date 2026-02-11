//! BES gRPC server implementation
//!
//! NOTE: gRPC support is not yet fully implemented due to crate version conflicts
//! between rules_rs (Cargo dependencies) and rules_rust_prost (protobuf generation).
//!
//! For now, use JSON file input with:
//!   bazel build //... --build_event_json_file=/tmp/bep.json
//!   conduit --input /tmp/bep.json

use std::net::SocketAddr;
use tracing::{error, info};

/// Run the BES gRPC server (stub)
pub async fn run_server(addr: SocketAddr) -> anyhow::Result<()> {
    info!("BES gRPC server requested on {}", addr);
    error!(
        "gRPC support is not yet implemented due to crate version conflicts."
    );
    info!("");
    info!("Workaround: Use JSON file input instead:");
    info!("  1. Run: bazel build //... --build_event_json_file=/tmp/bep.json");
    info!("  2. Run: conduit --input /tmp/bep.json");
    info!("");
    info!("For gRPC support, the following is needed:");
    info!("  - Resolve prost/tonic version conflicts between rules_rs and rules_rust_prost");
    info!("  - Or use a unified dependency management approach");
    
    // Return error to indicate not implemented
    anyhow::bail!("gRPC server not yet implemented")
}
