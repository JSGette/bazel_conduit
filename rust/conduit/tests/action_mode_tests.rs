// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

use conduit_lib::state::ActionProcessingMode;

#[test]
fn test_lightweight_mode() {
    let mode = ActionProcessingMode::Lightweight;
    assert!(mode.should_create_span(false)); // Failed action -> create span
    assert!(!mode.should_create_span(true)); // Successful action -> skip
}

#[test]
fn test_full_mode() {
    let mode = ActionProcessingMode::Full;
    assert!(mode.should_create_span(false)); // Failed action -> create span
    assert!(mode.should_create_span(true)); // Successful action -> create span
}

#[test]
fn test_default_is_lightweight() {
    let mode = ActionProcessingMode::default();
    assert_eq!(mode, ActionProcessingMode::Lightweight);
}

#[test]
fn test_display() {
    assert_eq!(
        ActionProcessingMode::Lightweight.to_string(),
        "lightweight (failed actions only)"
    );
    assert_eq!(
        ActionProcessingMode::Full.to_string(),
        "full (all actions)"
    );
}
