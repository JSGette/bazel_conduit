// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//! BEP (Build Event Protocol) parsing and decoding

// Public so gRPC server can access BepJsonEvent for proto→JSON conversion
pub mod decoder;
mod router;

pub use decoder::{BepDecoder, BepJsonEvent};
pub use router::EventRouter;
