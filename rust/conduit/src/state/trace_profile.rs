// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//! Trace emission policy.
//!
//! A single knob that decides which action / spawn / target spans are worth
//! emitting, consulted at every emission choke point so the compact exec log
//! can't resurrect what the profile dropped.

/// Default minimum duration (ms) for a spawn to survive the lightweight
/// profile on the strength of its runtime alone. Errors and cache misses are
/// kept regardless.
pub const DEFAULT_TRACE_MIN_DURATION_MS: i64 = 500;

/// Default number of successful action spans kept by the lightweight profile
/// (the slowest ones). Failed actions don't count against it.
pub const DEFAULT_MAX_ACTION_SPANS: usize = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TraceProfile {
    /// Failed actions plus the slowest successful ones, interesting spawns,
    /// and the targets they hang under. Aggregate metrics stay on the root.
    #[default]
    Lightweight,
    /// Every span. Verbose but complete.
    Full,
}

impl TraceProfile {
    /// Parse a CLI value (case-insensitive). Returns `None` for unknown input
    /// so the caller can surface a usage error.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "lightweight" | "light" => Some(Self::Lightweight),
            "full" => Some(Self::Full),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Lightweight => "lightweight",
            Self::Full => "full",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TracePolicy {
    profile: TraceProfile,
    min_duration_ms: i64,
    max_action_spans: usize,
}

impl TracePolicy {
    pub fn new(profile: TraceProfile, min_duration_ms: i64, max_action_spans: usize) -> Self {
        Self {
            profile,
            min_duration_ms: min_duration_ms.max(0),
            max_action_spans,
        }
    }

    pub fn profile(&self) -> TraceProfile {
        self.profile
    }

    pub fn is_full(&self) -> bool {
        self.profile == TraceProfile::Full
    }

    /// Successful action spans kept by the lightweight profile.
    pub fn max_action_spans(&self) -> usize {
        self.max_action_spans
    }

    /// Should a spawn span be emitted? Non-zero exit, a cacheable miss, or a
    /// slow spawn survive the lightweight profile.
    pub fn should_emit_spawn(
        &self,
        exit_code: i32,
        cache_hit: bool,
        cacheable: bool,
        duration_ms: Option<i64>,
    ) -> bool {
        if self.is_full() {
            return true;
        }
        exit_code != 0
            || (cacheable && !cache_hit)
            || duration_ms.is_some_and(|ms| ms >= self.min_duration_ms)
    }
}

impl std::fmt::Display for TracePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.profile {
            TraceProfile::Lightweight => write!(
                f,
                "lightweight (failed actions plus the {} slowest; spawns that fail, miss, or take >= {} ms)",
                self.max_action_spans, self.min_duration_ms
            ),
            TraceProfile::Full => write!(f, "full (all spans)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_known_profiles() {
        assert_eq!(TraceProfile::parse("Full"), Some(TraceProfile::Full));
        assert_eq!(TraceProfile::parse("LIGHT"), Some(TraceProfile::Lightweight));
        assert_eq!(TraceProfile::parse("auto"), None);
        assert_eq!(TraceProfile::default(), TraceProfile::Lightweight);
    }

    #[test]
    fn full_emits_every_spawn() {
        let p = TracePolicy::new(TraceProfile::Full, 500, 0);
        assert!(p.should_emit_spawn(0, true, true, Some(1)));
    }

    #[test]
    fn lightweight_keeps_failed_missed_and_slow_spawns() {
        let p = TracePolicy::new(TraceProfile::Lightweight, 500, 200);
        assert!(!p.should_emit_spawn(0, true, true, Some(10)));
        assert!(p.should_emit_spawn(1, true, true, Some(1)));
        assert!(p.should_emit_spawn(0, false, true, Some(1)));
        assert!(p.should_emit_spawn(0, true, true, Some(999)));
        // A non-cacheable miss is not a "miss" worth keeping on its own.
        assert!(!p.should_emit_spawn(0, false, false, Some(10)));
    }
}
