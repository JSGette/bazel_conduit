//! gRPC server for Build Event Service (BES)
//!
//! NOTE: gRPC support is not yet fully implemented.
//! See server.rs for details.

mod server;

pub use server::run_server;
