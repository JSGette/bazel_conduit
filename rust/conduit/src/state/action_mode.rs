//! Action processing mode detection
//!
//! Determines whether to process all actions or only failed ones
//! based on the presence of --build_event_publish_all_actions flag.

/// Action processing mode, auto-detected from BEP stream
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ActionProcessingMode {
    /// Default: only failed actions get spans (flag absent)
    #[default]
    Lightweight,
    /// Full: all actions get spans (flag present)
    /// NOTE: Not yet implemented in MVP
    Full,
}

impl ActionProcessingMode {
    /// Should this action create a span based on the current mode?
    pub fn should_create_span(&self, success: bool) -> bool {
        match self {
            ActionProcessingMode::Lightweight => !success, // Only failed actions
            ActionProcessingMode::Full => true, // All actions
        }
    }

    /// Human-readable description of the mode
    pub fn description(&self) -> &'static str {
        match self {
            ActionProcessingMode::Lightweight => "lightweight (failed actions only)",
            ActionProcessingMode::Full => "full (all actions)",
        }
    }
}

impl std::fmt::Display for ActionProcessingMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description())
    }
}
