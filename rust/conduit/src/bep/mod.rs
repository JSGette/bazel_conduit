// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//! BEP (Build Event Protocol) routing and binary-file replay

mod replay;
mod router;

pub use replay::replay;
pub use router::EventRouter;
