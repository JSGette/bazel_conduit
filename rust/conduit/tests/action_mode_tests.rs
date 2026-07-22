// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

use conduit_lib::state::{TracePolicy, TraceProfile};

#[test]
fn auto_resolves_from_bazel_flag() {
    // --build_event_publish_all_actions present -> full, absent -> lightweight.
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
fn explicit_profile_wins_over_bazel_flag() {
    assert_eq!(
        TracePolicy::resolve(TraceProfile::Lightweight, 500, true).profile(),
        TraceProfile::Lightweight
    );
    assert_eq!(
        TracePolicy::resolve(TraceProfile::Full, 500, false).profile(),
        TraceProfile::Full
    );
}

#[test]
fn full_emits_all_actions_and_spawns() {
    let p = TracePolicy::resolve(TraceProfile::Full, 500, false);
    assert!(p.should_emit_action(true, Some(true), Some(1)));
    assert!(p.should_emit_spawn(0, true, true, Some(1)));
}

#[test]
fn lightweight_drops_fast_successful_cache_hits() {
    let p = TracePolicy::resolve(TraceProfile::Lightweight, 500, false);
    assert!(!p.should_emit_action(true, Some(true), Some(10)));
    assert!(!p.should_emit_spawn(0, true, true, Some(10)));
}

#[test]
fn lightweight_keeps_errors_misses_and_slow() {
    let p = TracePolicy::resolve(TraceProfile::Lightweight, 500, false);
    // Error.
    assert!(p.should_emit_action(false, None, None));
    assert!(p.should_emit_spawn(1, true, true, None));
    // Cacheable miss.
    assert!(p.should_emit_action(true, Some(false), Some(1)));
    assert!(p.should_emit_spawn(0, false, true, Some(1)));
    // Slow (>= threshold).
    assert!(p.should_emit_action(true, Some(true), Some(500)));
    assert!(p.should_emit_spawn(0, true, true, Some(500)));
}

#[test]
fn parse_rejects_unknown_profile() {
    assert!(TraceProfile::parse("nonsense").is_none());
    assert_eq!(TraceProfile::parse("full"), Some(TraceProfile::Full));
}
