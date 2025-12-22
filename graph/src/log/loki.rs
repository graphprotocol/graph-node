use std::collections::HashMap;
use std::fmt;
use std::fmt::Write as FmtWrite;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::prelude::{SecondsFormat, Utc};
use reqwest::Client;
use serde::Serialize;
use serde_json::json;
use slog::*;

/// Configuration for `LokiDrain`.
#[derive(Clone, Debug)]
pub struct LokiDrainConfig {
    /// The Loki endpoint (e.g., http://loki:3100)
    pub endpoint: String,
    /// Optional tenant ID for multi-tenancy (X-Scope-OrgID header)
    pub tenant_id: Option<String>,
    /// The batching/flush interval
    pub flush_interval: Duration,
    /// The subgraph ID for labeling
    pub subgraph_id: String,
}

/// A log entry to be sent to Loki
#[derive(Clone, Debug)]
struct LokiLogEntry {
    timestamp_ns: String,            // Nanoseconds since epoch as string
    line: String,                    // JSON-serialized log entry
    labels: HashMap<String, String>, // Stream labels (subgraphId, level, etc.)
}

/// Log document structure for JSON serialization
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LokiLogDocument {
    id: String,
    subgraph_id: String,
    timestamp: String,
    level: String,
    text: String,
    arguments: HashMap<String, String>,
    meta: LokiLogMeta,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LokiLogMeta {
    module: String,
    line: i64,
    column: i64,
}

/// Serializer for extracting key-value pairs into a HashMap
struct HashMapKVSerializer {
    kvs: Vec<(String, String)>,
}

impl HashMapKVSerializer {
    fn new() -> Self {
        HashMapKVSerializer {
            kvs: Default::default(),
        }
    }

    fn finish(self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        self.kvs.into_iter().for_each(|(k, v)| {
            map.insert(k, v);
        });
        map
    }
}

impl Serializer for HashMapKVSerializer {
    fn emit_arguments(&mut self, key: Key, val: &fmt::Arguments) -> slog::Result {
        self.kvs.push((key.into(), format!("{}", val)));
        Ok(())
    }
}

/// Serializer for concatenating key-value arguments into a string
struct SimpleKVSerializer {
    kvs: Vec<(String, String)>,
}

impl SimpleKVSerializer {
    fn new() -> Self {
        SimpleKVSerializer {
            kvs: Default::default(),
        }
    }

    fn finish(self) -> (usize, String) {
        (
            self.kvs.len(),
            self.kvs
                .iter()
                .map(|(k, v)| format!("{}: {}", k, v))
                .collect::<Vec<_>>()
                .join(", "),
        )
    }
}

impl Serializer for SimpleKVSerializer {
    fn emit_arguments(&mut self, key: Key, val: &fmt::Arguments) -> slog::Result {
        self.kvs.push((key.into(), format!("{}", val)));
        Ok(())
    }
}

/// An slog `Drain` for logging to Loki.
///
/// Loki expects logs in the following format:
/// ```json
/// {
///   "streams": [
///     {
///       "stream": {"subgraphId": "QmXxx", "level": "error"},
///       "values": [
///         ["<unix_epoch_nanoseconds>", "<log_line_json>"],
///         ["<unix_epoch_nanoseconds>", "<log_line_json>"]
///       ]
///     }
///   ]
/// }
/// ```
pub struct LokiDrain {
    config: LokiDrainConfig,
    client: Client,
    error_logger: Logger,
    logs: Arc<Mutex<Vec<LokiLogEntry>>>,
}

impl LokiDrain {
    /// Creates a new `LokiDrain`.
    pub fn new(config: LokiDrainConfig, error_logger: Logger) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("failed to create HTTP client for LokiDrain");

        let drain = LokiDrain {
            config,
            client,
            error_logger,
            logs: Arc::new(Mutex::new(vec![])),
        };
        drain.periodically_flush_logs();
        drain
    }

    fn periodically_flush_logs(&self) {
        let flush_logger = self.error_logger.clone();
        let logs = self.logs.clone();
        let config = self.config.clone();
        let client = self.client.clone();
        let mut interval = tokio::time::interval(self.config.flush_interval);

        crate::tokio::spawn(async move {
            loop {
                interval.tick().await;

                let logs_to_send = {
                    let mut logs = logs.lock().unwrap();
                    let logs_to_send = (*logs).clone();
                    logs.clear();
                    logs_to_send
                };

                // Do nothing if there are no logs to flush
                if logs_to_send.is_empty() {
                    continue;
                }

                // Group logs by labels (Loki streams)
                let streams = group_by_labels(logs_to_send);

                // Build Loki push request body
                let streams_json: Vec<_> = streams
                    .into_iter()
                    .map(|(labels, entries)| {
                        json!({
                            "stream": labels,
                            "values": entries.into_iter()
                                .map(|e| vec![e.timestamp_ns, e.line])
                                .collect::<Vec<_>>()
                        })
                    })
                    .collect();

                let body = json!({
                    "streams": streams_json
                });

                // Build request URL
                let url = format!("{}/loki/api/v1/push", config.endpoint);

                // Build request with optional tenant header
                let mut request = client
                    .post(&url)
                    .json(&body)
                    .timeout(Duration::from_secs(30));

                if let Some(ref tenant_id) = config.tenant_id {
                    request = request.header("X-Scope-OrgID", tenant_id);
                }

                // Send to Loki
                match request.send().await {
                    Ok(resp) if resp.status().is_success() => {
                        // Success
                    }
                    Ok(resp) => {
                        error!(
                            flush_logger,
                            "Loki push failed with status: {}",
                            resp.status()
                        );
                    }
                    Err(e) => {
                        error!(flush_logger, "Failed to send logs to Loki: {}", e);
                    }
                }
            }
        });
    }
}

impl Drain for LokiDrain {
    type Ok = ();
    type Err = ();

    fn log(&self, record: &Record, values: &OwnedKVList) -> std::result::Result<(), ()> {
        // Don't send `trace` logs to Loki
        if record.level() == Level::Trace {
            return Ok(());
        }

        let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true);
        let id = format!("{}-{}", self.config.subgraph_id, timestamp);

        // Get timestamp in nanoseconds
        let timestamp_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .to_string();

        // Get log level as string
        let level = match record.level() {
            Level::Critical => "critical",
            Level::Error => "error",
            Level::Warning => "warning",
            Level::Info => "info",
            Level::Debug => "debug",
            Level::Trace => "trace",
        };

        // Serialize logger arguments
        let mut serializer = SimpleKVSerializer::new();
        record
            .kv()
            .serialize(record, &mut serializer)
            .expect("failed to serialize logger arguments");
        let (n_logger_kvs, logger_kvs) = serializer.finish();

        // Serialize log message arguments
        let mut serializer = SimpleKVSerializer::new();
        values
            .serialize(record, &mut serializer)
            .expect("failed to serialize log message arguments");
        let (n_value_kvs, value_kvs) = serializer.finish();

        // Serialize arguments into hash map
        let mut serializer = HashMapKVSerializer::new();
        record
            .kv()
            .serialize(record, &mut serializer)
            .expect("failed to serialize log message arguments into hash map");
        let arguments = serializer.finish();

        // Build text with all key-value pairs
        let mut text = format!("{}", record.msg());
        if n_logger_kvs > 0 {
            write!(text, ", {}", logger_kvs).unwrap();
        }
        if n_value_kvs > 0 {
            write!(text, ", {}", value_kvs).unwrap();
        }

        // Build log document
        let log_doc = LokiLogDocument {
            id,
            subgraph_id: self.config.subgraph_id.clone(),
            timestamp: timestamp.clone(),
            level: level.to_string(),
            text,
            arguments,
            meta: LokiLogMeta {
                module: record.module().into(),
                line: record.line() as i64,
                column: record.column() as i64,
            },
        };

        // Serialize to JSON line
        let line = match serde_json::to_string(&log_doc) {
            Ok(l) => l,
            Err(e) => {
                error!(self.error_logger, "Failed to serialize log to JSON: {}", e);
                return Ok(());
            }
        };

        // Build labels for Loki stream
        let mut labels = HashMap::new();
        labels.insert("subgraphId".to_string(), self.config.subgraph_id.clone());
        labels.insert("level".to_string(), level.to_string());

        // Create log entry
        let entry = LokiLogEntry {
            timestamp_ns,
            line,
            labels,
        };

        // Push to buffer
        let mut logs = self.logs.lock().unwrap();
        logs.push(entry);

        Ok(())
    }
}

/// Groups log entries by their labels to create Loki streams
/// Returns a HashMap where the key is the labels and the value is a vec of entries
fn group_by_labels(
    entries: Vec<LokiLogEntry>,
) -> Vec<(HashMap<String, String>, Vec<LokiLogEntry>)> {
    let mut streams: HashMap<String, (HashMap<String, String>, Vec<LokiLogEntry>)> = HashMap::new();
    for entry in entries {
        // Create a deterministic string key from the labels
        let label_key = serde_json::to_string(&entry.labels).unwrap_or_default();

        streams
            .entry(label_key)
            .or_insert_with(|| (entry.labels.clone(), Vec::new()))
            .1
            .push(entry);
    }

    // Convert to a vec of (labels, entries) tuples
    streams.into_iter().map(|(_, v)| v).collect()
}

/// Creates a new asynchronous Loki logger.
///
/// Uses `error_logger` to print any Loki logging errors,
/// so they don't go unnoticed.
pub fn loki_logger(config: LokiDrainConfig, error_logger: Logger) -> Logger {
    let loki_drain = LokiDrain::new(config, error_logger).fuse();
    let async_drain = slog_async::Async::new(loki_drain)
        .chan_size(20000)
        .overflow_strategy(slog_async::OverflowStrategy::Block)
        .build()
        .fuse();
    Logger::root(async_drain, o!())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_by_labels() {
        let mut labels1 = HashMap::new();
        labels1.insert("subgraphId".to_string(), "QmTest".to_string());
        labels1.insert("level".to_string(), "error".to_string());

        let mut labels2 = HashMap::new();
        labels2.insert("subgraphId".to_string(), "QmTest".to_string());
        labels2.insert("level".to_string(), "info".to_string());

        let entries = vec![
            LokiLogEntry {
                timestamp_ns: "1000000000".to_string(),
                line: "log1".to_string(),
                labels: labels1.clone(),
            },
            LokiLogEntry {
                timestamp_ns: "2000000000".to_string(),
                line: "log2".to_string(),
                labels: labels1.clone(),
            },
            LokiLogEntry {
                timestamp_ns: "3000000000".to_string(),
                line: "log3".to_string(),
                labels: labels2.clone(),
            },
        ];

        let streams = group_by_labels(entries);

        // Should have 2 streams (one for each unique label set)
        assert_eq!(streams.len(), 2);

        // Find streams by label and verify counts
        for (labels, entries) in streams {
            if labels.get("level") == Some(&"error".to_string()) {
                assert_eq!(entries.len(), 2, "Error stream should have 2 entries");
            } else if labels.get("level") == Some(&"info".to_string()) {
                assert_eq!(entries.len(), 1, "Info stream should have 1 entry");
            } else {
                panic!("Unexpected label combination");
            }
        }
    }

    #[test]
    fn test_loki_log_document_serialization() {
        let mut arguments = HashMap::new();
        arguments.insert("key1".to_string(), "value1".to_string());

        let doc = LokiLogDocument {
            id: "test-id".to_string(),
            subgraph_id: "QmTest".to_string(),
            timestamp: "2024-01-15T10:30:00Z".to_string(),
            level: "error".to_string(),
            text: "Test error".to_string(),
            arguments,
            meta: LokiLogMeta {
                module: "test.rs".to_string(),
                line: 42,
                column: 10,
            },
        };

        let json = serde_json::to_string(&doc).unwrap();
        assert!(json.contains("\"id\":\"test-id\""));
        assert!(json.contains("\"subgraphId\":\"QmTest\""));
        assert!(json.contains("\"level\":\"error\""));
        assert!(json.contains("\"text\":\"Test error\""));
    }
}
