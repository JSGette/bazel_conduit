// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//! Trace profile and emission policy.

mod trace_profile;

pub use trace_profile::{TracePolicy, TraceProfile, DEFAULT_TRACE_MIN_DURATION_MS};
