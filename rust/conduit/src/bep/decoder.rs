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

    /// Decode BEP events from a JSON file (NDJSON format - one event per line)
    ///
    /// This is the format produced by Bazel's --build_event_json_file flag.
    pub fn decode_json_file<R: Read>(
        &self,
        reader: R,
    ) -> Result<Vec<BepJsonEvent>, DecodeError> {
        let reader = BufReader::new(reader);
        let mut events = Vec::new();
        
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let event: BepJsonEvent = serde_json::from_str(&line)?;
            events.push(event);
        }
        
        Ok(events)
    }

    /// Decode BEP events from a newline-delimited JSON stream (iterator version)
    pub fn decode_ndjson<R: Read>(
        &self,
        reader: R,
    ) -> impl Iterator<Item = Result<BepJsonEvent, DecodeError>> {
        let reader = BufReader::new(reader);
        reader.lines().filter_map(|line| {
            match line {
                Ok(l) if l.trim().is_empty() => None,
                Ok(l) => Some(serde_json::from_str(&l).map_err(DecodeError::from)),
                Err(e) => Some(Err(DecodeError::from(e))),
            }
        })
    }
}

impl Default for BepDecoder {
    fn default() -> Self {
        Self::new()
    }
}

/// A BEP event as it appears in JSON format (from Bazel's --build_event_json_file)
///
/// The JSON format directly contains the BuildEvent fields (id, children, payload).
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BepJsonEvent {
    /// Event ID - determines the event type
    #[serde(default)]
    pub id: serde_json::Value,

    /// Child event IDs
    #[serde(default)]
    pub children: Vec<serde_json::Value>,

    /// Whether this is the last message
    #[serde(default)]
    pub last_message: bool,

    /// All other fields (payload) captured dynamically
    #[serde(flatten)]
    pub payload: serde_json::Map<String, serde_json::Value>,
}

impl BepJsonEvent {
    /// Extract the event ID type from the JSON event
    pub fn event_type(&self) -> Option<&str> {
        self.id
            .as_object()
            .and_then(|obj| obj.keys().next())
            .map(|s| s.as_str())
    }

    /// Check if this is the last message in the stream
    pub fn is_last_message(&self) -> bool {
        self.last_message
    }

    /// Get the event payload for a specific event type
    pub fn get_payload(&self, event_type: &str) -> Option<&serde_json::Value> {
        self.payload.get(event_type)
    }

    /// Get the raw event as a JSON value (for compatibility)
    pub fn as_json(&self) -> serde_json::Value {
        let mut map = self.payload.clone();
        map.insert("id".to_string(), self.id.clone());
        map.insert("children".to_string(), serde_json::json!(self.children));
        if self.last_message {
            map.insert("lastMessage".to_string(), serde_json::Value::Bool(true));
        }
        serde_json::Value::Object(map)
    }
}
