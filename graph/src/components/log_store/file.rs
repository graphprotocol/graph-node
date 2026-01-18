use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use crate::prelude::DeploymentHash;

use super::{LogEntry, LogMeta, LogQuery, LogStore, LogStoreError};

pub struct FileLogStore {
    directory: PathBuf,
    retention_hours: u32,
}

impl FileLogStore {
    pub fn new(directory: PathBuf, retention_hours: u32) -> Result<Self, LogStoreError> {
        // Create directory if it doesn't exist
        std::fs::create_dir_all(&directory)
            .map_err(|e| LogStoreError::InitializationFailed(e.into()))?;

        let store = Self {
            directory,
            retention_hours,
        };

        // Run cleanup on startup for all existing log files
        if retention_hours > 0 {
            if let Ok(entries) = std::fs::read_dir(&store.directory) {
                for entry in entries.filter_map(Result::ok) {
                    let path = entry.path();

                    // Only process .jsonl files
                    if path.extension().and_then(|s| s.to_str()) == Some("jsonl") {
                        // Run cleanup, but don't fail initialization if cleanup fails
                        if let Err(e) = store.cleanup_old_logs(&path) {
                            eprintln!("Warning: Failed to cleanup old logs for {:?}: {}", path, e);
                        }
                    }
                }
            }
        }

        Ok(store)
    }

    /// Get log file path for a subgraph
    fn log_file_path(&self, subgraph_id: &DeploymentHash) -> PathBuf {
        self.directory.join(format!("{}.jsonl", subgraph_id))
    }

    /// Parse a JSON line into a LogEntry
    fn parse_line(&self, line: &str) -> Option<LogEntry> {
        let doc: FileLogDocument = serde_json::from_str(line).ok()?;

        let level = doc.level.parse().ok()?;
        let subgraph_id = DeploymentHash::new(&doc.subgraph_id).ok()?;

        Some(LogEntry {
            id: doc.id,
            subgraph_id,
            timestamp: doc.timestamp,
            level,
            text: doc.text,
            arguments: doc.arguments,
            meta: LogMeta {
                module: doc.meta.module,
                line: doc.meta.line,
                column: doc.meta.column,
            },
        })
    }

    /// Check if an entry matches the query filters
    fn matches_filters(&self, entry: &LogEntry, query: &LogQuery) -> bool {
        // Level filter
        if let Some(level) = query.level {
            if entry.level != level {
                return false;
            }
        }

        // Time range filters
        if let Some(ref from) = query.from {
            if entry.timestamp < *from {
                return false;
            }
        }

        if let Some(ref to) = query.to {
            if entry.timestamp > *to {
                return false;
            }
        }

        // Text search (case-insensitive)
        if let Some(ref search) = query.search {
            if !entry.text.to_lowercase().contains(&search.to_lowercase()) {
                return false;
            }
        }

        true
    }

    /// Delete log entries older than retention_hours
    fn cleanup_old_logs(&self, file_path: &std::path::Path) -> Result<(), LogStoreError> {
        if self.retention_hours == 0 {
            return Ok(()); // Cleanup disabled, keep all logs
        }

        use chrono::{DateTime, Duration, Utc};
        use std::io::Write;

        // Calculate cutoff time
        let cutoff = Utc::now() - Duration::hours(self.retention_hours as i64);

        // Read all log entries
        let file = File::open(file_path).map_err(|e| LogStoreError::QueryFailed(e.into()))?;
        let reader = BufReader::new(file);

        let kept_entries: Vec<String> = reader
            .lines()
            .filter_map(|line| line.ok())
            .filter(|line| {
                // Parse timestamp from log entry
                if let Some(entry) = self.parse_line(line) {
                    // Parse RFC3339 timestamp
                    if let Ok(timestamp) = DateTime::parse_from_rfc3339(&entry.timestamp) {
                        return timestamp.with_timezone(&Utc) >= cutoff;
                    }
                }
                // Keep if we can't parse (don't delete on error)
                true
            })
            .collect();

        // Write filtered file atomically
        let temp_path = file_path.with_extension("jsonl.tmp");
        let mut temp_file =
            File::create(&temp_path).map_err(|e| LogStoreError::QueryFailed(e.into()))?;

        for entry in kept_entries {
            writeln!(temp_file, "{}", entry).map_err(|e| LogStoreError::QueryFailed(e.into()))?;
        }

        temp_file
            .sync_all()
            .map_err(|e| LogStoreError::QueryFailed(e.into()))?;

        // Atomic rename
        std::fs::rename(&temp_path, file_path).map_err(|e| LogStoreError::QueryFailed(e.into()))?;

        Ok(())
    }
}

/// Helper struct to enable timestamp-based comparisons for BinaryHeap
/// Implements Ord based on timestamp field for maintaining a min-heap of recent entries
struct TimestampedEntry {
    entry: LogEntry,
}

impl PartialEq for TimestampedEntry {
    fn eq(&self, other: &Self) -> bool {
        self.entry.timestamp == other.entry.timestamp
    }
}

impl Eq for TimestampedEntry {}

impl PartialOrd for TimestampedEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimestampedEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.entry.timestamp.cmp(&other.entry.timestamp)
    }
}

#[async_trait]
impl LogStore for FileLogStore {
    async fn query_logs(&self, query: LogQuery) -> Result<Vec<LogEntry>, LogStoreError> {
        let file_path = self.log_file_path(&query.subgraph_id);

        if !file_path.exists() {
            return Ok(vec![]);
        }

        let file = File::open(&file_path).map_err(|e| LogStoreError::QueryFailed(e.into()))?;
        let reader = BufReader::new(file);

        // Calculate how many entries we need to keep in memory
        // We need skip + first entries to handle pagination
        let needed_entries = (query.skip + query.first) as usize;

        // Use a min-heap (via Reverse) to maintain only the top N most recent entries
        // This bounds memory usage to O(skip + first) instead of O(total_log_entries)
        let mut top_entries: BinaryHeap<Reverse<TimestampedEntry>> =
            BinaryHeap::with_capacity(needed_entries + 1);

        // Stream through the file line-by-line, applying filters and maintaining bounded collection
        for line in reader.lines() {
            // Skip malformed lines
            let line = match line {
                Ok(l) => l,
                Err(_) => continue,
            };

            // Parse the line into a LogEntry
            let entry = match self.parse_line(&line) {
                Some(e) => e,
                None => continue,
            };

            // Apply filters early to avoid keeping filtered-out entries in memory
            if !self.matches_filters(&entry, &query) {
                continue;
            }

            let timestamped = TimestampedEntry { entry };

            // Maintain only the top N most recent entries by timestamp
            // BinaryHeap with Reverse creates a min-heap, so we can efficiently
            // keep the N largest (most recent) timestamps
            if top_entries.len() < needed_entries {
                top_entries.push(Reverse(timestamped));
            } else if let Some(Reverse(oldest)) = top_entries.peek() {
                // If this entry is more recent than the oldest in our heap, replace it
                if timestamped.entry.timestamp > oldest.entry.timestamp {
                    top_entries.pop();
                    top_entries.push(Reverse(timestamped));
                }
            }
        }

        // Convert heap to sorted vector (most recent first)
        let mut result: Vec<LogEntry> = top_entries
            .into_iter()
            .map(|Reverse(te)| te.entry)
            .collect();

        // Sort by timestamp (direction based on query)
        match query.order_direction {
            super::OrderDirection::Desc => result.sort_by(|a, b| b.timestamp.cmp(&a.timestamp)),
            super::OrderDirection::Asc => result.sort_by(|a, b| a.timestamp.cmp(&b.timestamp)),
        }

        // Apply skip and take to get the final page
        Ok(result
            .into_iter()
            .skip(query.skip as usize)
            .take(query.first as usize)
            .collect())
    }

    fn is_available(&self) -> bool {
        self.directory.exists() && self.directory.is_dir()
    }
}

// File log document format (JSON Lines)
#[derive(Debug, Serialize, Deserialize)]
struct FileLogDocument {
    id: String,
    #[serde(rename = "subgraphId")]
    subgraph_id: String,
    timestamp: String,
    level: String,
    text: String,
    arguments: Vec<(String, String)>,
    meta: FileLogMeta,
}

#[derive(Debug, Serialize, Deserialize)]
struct FileLogMeta {
    module: String,
    line: i64,
    column: i64,
}

#[cfg(test)]
mod tests {
    use super::super::LogLevel;
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn test_file_log_store_initialization() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileLogStore::new(temp_dir.path().to_path_buf(), 0);
        assert!(store.is_ok());

        let store = store.unwrap();
        assert!(store.is_available());
    }

    #[test]
    fn test_log_file_path() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileLogStore::new(temp_dir.path().to_path_buf(), 0).unwrap();

        let subgraph_id = DeploymentHash::new("QmTest").unwrap();
        let path = store.log_file_path(&subgraph_id);

        assert_eq!(path, temp_dir.path().join("QmTest.jsonl"));
    }

    #[tokio::test]
    async fn test_query_nonexistent_file() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileLogStore::new(temp_dir.path().to_path_buf(), 0).unwrap();

        let query = LogQuery {
            subgraph_id: DeploymentHash::new("QmNonexistent").unwrap(),
            level: None,
            from: None,
            to: None,
            search: None,
            first: 100,
            skip: 0,
            order_direction: super::super::OrderDirection::Desc,
        };

        let result = store.query_logs(query).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_query_with_sample_data() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileLogStore::new(temp_dir.path().to_path_buf(), 0).unwrap();

        let subgraph_id = DeploymentHash::new("QmTest").unwrap();
        let file_path = store.log_file_path(&subgraph_id);

        // Write some test data
        let mut file = File::create(&file_path).unwrap();
        let log_entry = FileLogDocument {
            id: "log-1".to_string(),
            subgraph_id: "QmTest".to_string(),
            timestamp: "2024-01-15T10:30:00Z".to_string(),
            level: "error".to_string(),
            text: "Test error message".to_string(),
            arguments: vec![],
            meta: FileLogMeta {
                module: "test.ts".to_string(),
                line: 42,
                column: 10,
            },
        };
        writeln!(file, "{}", serde_json::to_string(&log_entry).unwrap()).unwrap();

        // Query
        let query = LogQuery {
            subgraph_id,
            level: None,
            from: None,
            to: None,
            search: None,
            first: 100,
            skip: 0,
            order_direction: super::super::OrderDirection::Desc,
        };

        let result = store.query_logs(query).await;
        assert!(result.is_ok());

        let entries = result.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].id, "log-1");
        assert_eq!(entries[0].text, "Test error message");
        assert_eq!(entries[0].level, LogLevel::Error);
    }

    #[tokio::test]
    async fn test_query_with_level_filter() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileLogStore::new(temp_dir.path().to_path_buf(), 0).unwrap();

        let subgraph_id = DeploymentHash::new("QmTest").unwrap();
        let file_path = store.log_file_path(&subgraph_id);

        // Write test data with different levels
        let mut file = File::create(&file_path).unwrap();
        for (id, level) in [("log-1", "error"), ("log-2", "info"), ("log-3", "error")] {
            let log_entry = FileLogDocument {
                id: id.to_string(),
                subgraph_id: "QmTest".to_string(),
                timestamp: format!("2024-01-15T10:30:{}Z", id),
                level: level.to_string(),
                text: format!("Test {} message", level),
                arguments: vec![],
                meta: FileLogMeta {
                    module: "test.ts".to_string(),
                    line: 42,
                    column: 10,
                },
            };
            writeln!(file, "{}", serde_json::to_string(&log_entry).unwrap()).unwrap();
        }

        // Query for errors only
        let query = LogQuery {
            subgraph_id,
            level: Some(LogLevel::Error),
            from: None,
            to: None,
            search: None,
            first: 100,
            skip: 0,
            order_direction: super::super::OrderDirection::Desc,
        };

        let result = store.query_logs(query).await;
        assert!(result.is_ok());

        let entries = result.unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|e| e.level == LogLevel::Error));
    }

    #[tokio::test]
    async fn test_cleanup_old_logs() {
        use chrono::{Duration, Utc};

        let temp_dir = TempDir::new().unwrap();
        let store = FileLogStore::new(temp_dir.path().to_path_buf(), 24).unwrap();

        let subgraph_id = DeploymentHash::new("QmTest").unwrap();
        let file_path = store.log_file_path(&subgraph_id);

        // Create test data with old and new entries
        let mut file = File::create(&file_path).unwrap();

        // Old entry (48 hours ago)
        let old_timestamp =
            (Utc::now() - Duration::hours(48)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let old_entry = FileLogDocument {
            id: "log-old".to_string(),
            subgraph_id: "QmTest".to_string(),
            timestamp: old_timestamp,
            level: "info".to_string(),
            text: "Old log entry".to_string(),
            arguments: vec![],
            meta: FileLogMeta {
                module: "test.ts".to_string(),
                line: 1,
                column: 1,
            },
        };
        writeln!(file, "{}", serde_json::to_string(&old_entry).unwrap()).unwrap();

        // New entry (12 hours ago)
        let new_timestamp =
            (Utc::now() - Duration::hours(12)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let new_entry = FileLogDocument {
            id: "log-new".to_string(),
            subgraph_id: "QmTest".to_string(),
            timestamp: new_timestamp,
            level: "info".to_string(),
            text: "New log entry".to_string(),
            arguments: vec![],
            meta: FileLogMeta {
                module: "test.ts".to_string(),
                line: 2,
                column: 1,
            },
        };
        writeln!(file, "{}", serde_json::to_string(&new_entry).unwrap()).unwrap();
        drop(file);

        // Run cleanup
        store.cleanup_old_logs(&file_path).unwrap();

        // Query to verify only new entry remains
        let query = LogQuery {
            subgraph_id,
            level: None,
            from: None,
            to: None,
            search: None,
            first: 100,
            skip: 0,
            order_direction: super::super::OrderDirection::Desc,
        };

        let result = store.query_logs(query).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, "log-new");
    }

    #[tokio::test]
    async fn test_cleanup_keeps_unparseable_entries() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileLogStore::new(temp_dir.path().to_path_buf(), 24).unwrap();

        let subgraph_id = DeploymentHash::new("QmTest").unwrap();
        let file_path = store.log_file_path(&subgraph_id);

        // Create test data with valid and unparseable entries
        let mut file = File::create(&file_path).unwrap();

        // Valid entry
        let valid_entry = FileLogDocument {
            id: "log-valid".to_string(),
            subgraph_id: "QmTest".to_string(),
            timestamp: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            level: "info".to_string(),
            text: "Valid entry".to_string(),
            arguments: vec![],
            meta: FileLogMeta {
                module: "test.ts".to_string(),
                line: 1,
                column: 1,
            },
        };
        writeln!(file, "{}", serde_json::to_string(&valid_entry).unwrap()).unwrap();

        // Unparseable entry (invalid JSON)
        writeln!(file, "{{invalid json}}").unwrap();

        // Entry with invalid timestamp
        writeln!(
            file,
            r#"{{"id":"log-bad-time","subgraphId":"QmTest","timestamp":"not-a-timestamp","level":"info","text":"Bad timestamp","arguments":[],"meta":{{"module":"test.ts","line":2,"column":1}}}}"#
        )
        .unwrap();
        drop(file);

        // Run cleanup
        store.cleanup_old_logs(&file_path).unwrap();

        // Read file contents directly
        let file_contents = std::fs::read_to_string(&file_path).unwrap();
        let lines: Vec<&str> = file_contents.lines().collect();

        // All 3 entries should be kept (don't delete on error)
        assert_eq!(lines.len(), 3);
    }

    #[tokio::test]
    async fn test_startup_cleanup() {
        use chrono::{Duration, Utc};

        let temp_dir = TempDir::new().unwrap();

        // Create a log file with old entries before initializing the store
        let file_path = temp_dir.path().join("QmTestStartup.jsonl");
        let mut file = File::create(&file_path).unwrap();

        // Old entry (48 hours ago)
        let old_timestamp =
            (Utc::now() - Duration::hours(48)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let old_entry = FileLogDocument {
            id: "log-old".to_string(),
            subgraph_id: "QmTestStartup".to_string(),
            timestamp: old_timestamp,
            level: "info".to_string(),
            text: "Old log entry".to_string(),
            arguments: vec![],
            meta: FileLogMeta {
                module: "test.ts".to_string(),
                line: 1,
                column: 1,
            },
        };
        writeln!(file, "{}", serde_json::to_string(&old_entry).unwrap()).unwrap();

        // New entry (12 hours ago)
        let new_timestamp =
            (Utc::now() - Duration::hours(12)).to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let new_entry = FileLogDocument {
            id: "log-new".to_string(),
            subgraph_id: "QmTestStartup".to_string(),
            timestamp: new_timestamp,
            level: "info".to_string(),
            text: "New log entry".to_string(),
            arguments: vec![],
            meta: FileLogMeta {
                module: "test.ts".to_string(),
                line: 2,
                column: 1,
            },
        };
        writeln!(file, "{}", serde_json::to_string(&new_entry).unwrap()).unwrap();
        drop(file);

        // Initialize store with 24-hour retention - should cleanup on startup
        let store = FileLogStore::new(temp_dir.path().to_path_buf(), 24).unwrap();

        // Verify old entry was cleaned up
        let query = LogQuery {
            subgraph_id: DeploymentHash::new("QmTestStartup").unwrap(),
            level: None,
            from: None,
            to: None,
            search: None,
            first: 100,
            skip: 0,
            order_direction: super::super::OrderDirection::Desc,
        };

        let result = store.query_logs(query).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].id, "log-new");
    }
}
