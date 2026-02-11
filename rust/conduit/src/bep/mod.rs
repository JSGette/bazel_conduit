//! BEP (Build Event Protocol) parsing and decoding

// Public so gRPC server can access BepJsonEvent for proto→JSON conversion
pub mod decoder;
mod router;

pub use decoder::{BepDecoder, BepJsonEvent};
pub use router::EventRouter;
