//! BEP event decoder
//!
//! Handles decoding BEP events from JSON files.

use std::io::{BufRead, BufReader, Read};
use thiserror::Error;

/// Errors that can occur during BEP decoding
#[derive(Error, Debug)]
pub enum DecodeError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Invalid BEP format: {0}")]
    InvalidFormat(String),
}

/// BEP event decoder supporting multiple input formats
pub struct BepDecoder;

impl BepDecoder {
    /// Create a new decoder
    pub fn new() -> Self {
        Self
    }

    /// Decode BEP events from a JSON file
    ///
    /// The JSON format is an array of objects with `bazelEvent` containing
    /// the JSON representation of the protobuf.
    pub fn decode_json_file<R: Read>(
        &self,
        reader: R,
    ) -> Result<Vec<BepJsonEvent>, DecodeError> {
        let reader = BufReader::new(reader);
        let events: Vec<BepJsonEvent> = serde_json::from_reader(reader)?;
        Ok(events)
    }

    /// Decode BEP events from a newline-delimited JSON stream
    pub fn decode_ndjson<R: Read>(
        &self,
        reader: R,
    ) -> impl Iterator<Item = Result<BepJsonEvent, DecodeError>> {
        let reader = BufReader::new(reader);
        reader.lines().map(|line| {
            let line = line?;
            let event: BepJsonEvent = serde_json::from_str(&line)?;
            Ok(event)
        })
    }
}

impl Default for BepDecoder {
    fn default() -> Self {
        Self::new()
    }
}

/// A BEP event as it appears in JSON format (from Bazel's --build_event_json_file)
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BepJsonEvent {
    /// Event timestamp
    #[serde(default)]
    pub event_time: Option<String>,

    /// The actual build event (JSON representation of protobuf)
    pub bazel_event: serde_json::Value,
}

impl BepJsonEvent {
    /// Extract the event ID type from the JSON event
    pub fn event_type(&self) -> Option<&str> {
        self.bazel_event
            .get("id")
            .and_then(|id| id.as_object())
            .and_then(|obj| obj.keys().next())
            .map(|s| s.as_str())
    }

    /// Check if this is the last message in the stream
    pub fn is_last_message(&self) -> bool {
        self.bazel_event
            .get("lastMessage")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    }

    /// Get the event payload for a specific event type
    pub fn get_payload(&self, event_type: &str) -> Option<&serde_json::Value> {
        self.bazel_event.get(event_type)
    }

    /// Get the children event IDs
    pub fn children(&self) -> Vec<&serde_json::Value> {
        self.bazel_event
            .get("children")
            .and_then(|c| c.as_array())
            .map(|arr| arr.iter().collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bep_json_event_type() {
        let json = r#"{
            "eventTime": "2025-12-03T09:43:51.130Z",
            "bazelEvent": {
                "@type": "type.googleapis.com/build_event_stream.BuildEvent",
                "id": {
                    "started": {}
                },
                "started": {
                    "uuid": "test-uuid",
                    "command": "build"
                }
            }
        }"#;

        let event: BepJsonEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.event_type(), Some("started"));
    }

    #[test]
    fn test_last_message_detection() {
        let json = r#"{
            "bazelEvent": {
                "id": {"buildFinished": {}},
                "lastMessage": true
            }
        }"#;

        let event: BepJsonEvent = serde_json::from_str(json).unwrap();
        assert!(event.is_last_message());
    }
}
