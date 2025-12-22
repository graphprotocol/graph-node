use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use crate::prelude::DeploymentHash;

use super::{LogEntry, LogLevel, LogMeta, LogQuery, LogStore, LogStoreError};

pub struct FileLogStore {
    directory: PathBuf,
    max_file_size: u64,
    retention_days: u32,
}

impl FileLogStore {
    pub fn new(
        directory: PathBuf,
        max_file_size: u64,
        retention_days: u32,
    ) -> Result<Self, LogStoreError> {
        // Create directory if it doesn't exist
        std::fs::create_dir_all(&directory)
            .map_err(|e| LogStoreError::InitializationFailed(e.into()))?;

        Ok(Self {
            directory,
            max_file_size,
            retention_days,
        })
    }

    /// Get log file path for a subgraph
    fn log_file_path(&self, subgraph_id: &DeploymentHash) -> PathBuf {
        self.directory.join(format!("{}.jsonl", subgraph_id))
    }

    /// Parse a JSON line into a LogEntry
    fn parse_line(&self, line: &str) -> Option<LogEntry> {
        let doc: FileLogDocument = serde_json::from_str(line).ok()?;

        let level = LogLevel::from_str(&doc.level)?;
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
        if let Some(ref text) = query.text {
            if !entry.text.to_lowercase().contains(&text.to_lowercase()) {
                return false;
            }
        }

        true
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
        let mut entries = Vec::new();
        let mut skipped = 0;

        // Read all lines and collect matching entries
        // Note: For large files, this loads everything into memory
        // A production implementation would use reverse iteration or indexing
        let all_entries: Vec<LogEntry> = reader
            .lines()
            .filter_map(|line| line.ok())
            .filter_map(|line| self.parse_line(&line))
            .collect();

        // Sort by timestamp descending (most recent first)
        let mut sorted_entries = all_entries;
        sorted_entries.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

        // Apply filters and pagination
        for entry in sorted_entries {
            if !self.matches_filters(&entry, &query) {
                continue;
            }

            // Skip the first N entries
            if skipped < query.skip {
                skipped += 1;
                continue;
            }

            entries.push(entry);

            // Stop once we have enough entries
            if entries.len() >= query.first as usize {
                break;
            }
        }

        Ok(entries)
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
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn test_file_log_store_initialization() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileLogStore::new(temp_dir.path().to_path_buf(), 1024 * 1024, 30);
        assert!(store.is_ok());

        let store = store.unwrap();
        assert!(store.is_available());
    }

    #[test]
    fn test_log_file_path() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileLogStore::new(temp_dir.path().to_path_buf(), 1024 * 1024, 30).unwrap();

        let subgraph_id = DeploymentHash::new("QmTest").unwrap();
        let path = store.log_file_path(&subgraph_id);

        assert_eq!(path, temp_dir.path().join("QmTest.jsonl"));
    }

    #[tokio::test]
    async fn test_query_nonexistent_file() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileLogStore::new(temp_dir.path().to_path_buf(), 1024 * 1024, 30).unwrap();

        let query = LogQuery {
            subgraph_id: DeploymentHash::new("QmNonexistent").unwrap(),
            level: None,
            from: None,
            to: None,
            text: None,
            first: 100,
            skip: 0,
        };

        let result = store.query_logs(query).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_query_with_sample_data() {
        let temp_dir = TempDir::new().unwrap();
        let store = FileLogStore::new(temp_dir.path().to_path_buf(), 1024 * 1024, 30).unwrap();

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
            text: None,
            first: 100,
            skip: 0,
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
        let store = FileLogStore::new(temp_dir.path().to_path_buf(), 1024 * 1024, 30).unwrap();

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
            text: None,
            first: 100,
            skip: 0,
        };

        let result = store.query_logs(query).await;
        assert!(result.is_ok());

        let entries = result.unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|e| e.level == LogLevel::Error));
    }
}
