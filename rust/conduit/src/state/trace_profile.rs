// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

//! Trace emission policy.
//!
//! A single knob that decides which action / spawn / target spans are worth
//! emitting. It replaces the old action-only `ActionProcessingMode`: that one
//! gated BEP actions but left the compact exec-log pipeline free to re-emit
//! every spawn (and synthesise the parent actions back), so "lightweight"
//! traces were still enormous. `TracePolicy` is consulted at every emission
//! choke point instead.

/// Default minimum duration (ms) for an operation to survive the lightweight
/// profile on the strength of its runtime alone. Fast actions/spawns rarely
/// need individual investigation; errors and cache misses are kept regardless.
pub const DEFAULT_TRACE_MIN_DURATION_MS: i64 = 500;

/// What the user asked for on the CLI.
///
/// `Auto` defers to Bazel's `--build_event_publish_all_actions`: present ->
/// `Full`, absent -> `Lightweight`. This keeps existing setups working while
/// letting operators pin a profile explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TraceProfile {
    #[default]
    Auto,
    /// Only diagnostically interesting spans: errors, cache misses, and
    /// operations at or above the duration threshold. Aggregate metrics stay
    /// on the root span regardless.
    Lightweight,
    /// Every span (previous default behaviour). Verbose but complete.
    Full,
}

impl TraceProfile {
    /// Parse a CLI value (case-insensitive). Returns `None` for unknown input
    /// so the caller can surface a usage error.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "auto" => Some(Self::Auto),
            "lightweight" | "light" => Some(Self::Lightweight),
            "full" => Some(Self::Full),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Lightweight => "lightweight",
            Self::Full => "full",
        }
    }
}

/// Resolved emission policy consulted at every span choke point.
///
/// `profile` is always resolved to a concrete `Lightweight`/`Full` once
/// [`TracePolicy::resolve`] has run; a bare default (before Bazel options are
/// seen) is permissive (`Full`) so nothing is dropped by accident.
#[derive(Debug, Clone, Copy)]
pub struct TracePolicy {
    profile: TraceProfile,
    min_duration_ms: i64,
}

impl Default for TracePolicy {
    fn default() -> Self {
        Self {
            profile: TraceProfile::Full,
            min_duration_ms: DEFAULT_TRACE_MIN_DURATION_MS,
        }
    }
}

impl TracePolicy {
    /// Resolve a requested profile against the observed Bazel flag.
    pub fn resolve(requested: TraceProfile, min_duration_ms: i64, publish_all_actions: bool) -> Self {
        let profile = match requested {
            TraceProfile::Auto => {
                if publish_all_actions {
                    TraceProfile::Full
                } else {
                    TraceProfile::Lightweight
                }
            }
            other => other,
        };
        Self {
            profile,
            min_duration_ms: min_duration_ms.max(0),
        }
    }

    pub fn profile(&self) -> TraceProfile {
        self.profile
    }

    pub fn min_duration_ms(&self) -> i64 {
        self.min_duration_ms
    }

    /// True when everything should be emitted. An unresolved `Auto` is treated
    /// as full (permissive) so a missing `resolve()` never silently drops data.
    pub fn is_full(&self) -> bool {
        !matches!(self.profile, TraceProfile::Lightweight)
    }

    /// Should a BEP action span be emitted? Errors, cache misses, and slow
    /// actions survive the lightweight profile.
    pub fn should_emit_action(
        &self,
        success: bool,
        cached: Option<bool>,
        duration_ms: Option<i64>,
    ) -> bool {
        if self.is_full() {
            return true;
        }
        !success || cached == Some(false) || self.is_slow(duration_ms)
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
        exit_code != 0 || (cacheable && !cache_hit) || self.is_slow(duration_ms)
    }

    fn is_slow(&self, duration_ms: Option<i64>) -> bool {
        duration_ms.is_some_and(|ms| ms >= self.min_duration_ms)
    }
}

impl std::fmt::Display for TracePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.profile {
            TraceProfile::Lightweight => write!(
                f,
                "lightweight (errors, cache misses, and operations >= {} ms)",
                self.min_duration_ms
            ),
            _ => write!(f, "full (all spans)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_known_profiles() {
        assert_eq!(TraceProfile::parse("auto"), Some(TraceProfile::Auto));
        assert_eq!(TraceProfile::parse("Full"), Some(TraceProfile::Full));
        assert_eq!(TraceProfile::parse("LIGHT"), Some(TraceProfile::Lightweight));
        assert_eq!(TraceProfile::parse("nope"), None);
    }

    #[test]
    fn auto_follows_bazel_flag() {
        assert_eq!(
            TracePolicy::resolve(TraceProfile::Auto, 500, true).profile(),
            TraceProfile::Full
        );
        assert_eq!(
            TracePolicy::resolve(TraceProfile::Auto, 500, false).profile(),
            TraceProfile::Lightweight
        );
    }

    #[test]
    fn explicit_profile_ignores_bazel_flag() {
        assert_eq!(
            TracePolicy::resolve(TraceProfile::Full, 500, false).profile(),
            TraceProfile::Full
        );
        assert_eq!(
            TracePolicy::resolve(TraceProfile::Lightweight, 500, true).profile(),
            TraceProfile::Lightweight
        );
    }

    #[test]
    fn full_emits_everything() {
        let p = TracePolicy::resolve(TraceProfile::Full, 500, false);
        assert!(p.should_emit_action(true, Some(true), Some(1)));
        assert!(p.should_emit_spawn(0, true, true, Some(1)));
    }

    #[test]
    fn lightweight_keeps_errors_misses_and_slow() {
        let p = TracePolicy::resolve(TraceProfile::Lightweight, 500, false);
        // Fast successful cache hit -> dropped.
        assert!(!p.should_emit_action(true, Some(true), Some(10)));
        assert!(!p.should_emit_spawn(0, true, true, Some(10)));
        // Errors kept.
        assert!(p.should_emit_action(false, None, Some(1)));
        assert!(p.should_emit_spawn(1, true, true, Some(1)));
        // Cache miss kept.
        assert!(p.should_emit_action(true, Some(false), Some(1)));
        assert!(p.should_emit_spawn(0, false, true, Some(1)));
        // Slow kept.
        assert!(p.should_emit_action(true, Some(true), Some(500)));
        assert!(p.should_emit_spawn(0, true, true, Some(999)));
        // Non-cacheable cache miss is not a "miss" worth keeping on its own.
        assert!(!p.should_emit_spawn(0, false, false, Some(10)));
    }

    #[test]
    fn default_is_permissive() {
        assert!(TracePolicy::default().is_full());
    }
}
