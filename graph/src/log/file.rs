use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use chrono::prelude::{SecondsFormat, Utc};
use serde::Serialize;
use slog::*;

use super::common::{create_async_logger, LogEntryBuilder, LogMeta};

/// Configuration for `FileDrain`.
#[derive(Clone, Debug)]
pub struct FileDrainConfig {
    /// Directory where log files will be stored
    pub directory: PathBuf,
    /// The subgraph ID used for the log filename
    pub subgraph_id: String,
    /// Maximum file size in bytes
    pub max_file_size: u64,
    /// Retention period in days
    pub retention_days: u32,
}

/// Log document structure for JSON Lines format
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FileLogDocument {
    id: String,
    subgraph_id: String,
    timestamp: String,
    level: String,
    text: String,
    arguments: Vec<(String, String)>,
    meta: FileLogMeta,
}

type FileLogMeta = LogMeta;

/// An slog `Drain` for logging to local files in JSON Lines format.
///
/// Each subgraph gets its own .jsonl file with log entries.
/// Format: One JSON object per line
/// ```jsonl
/// {"id":"QmXxx-2024-01-15T10:30:00Z","subgraphId":"QmXxx","timestamp":"2024-01-15T10:30:00Z","level":"error","text":"Error message","arguments":[],"meta":{"module":"test.rs","line":42,"column":10}}
/// ```
pub struct FileDrain {
    config: FileDrainConfig,
    error_logger: Logger,
    writer: Arc<Mutex<BufWriter<File>>>,
}

impl FileDrain {
    /// Creates a new `FileDrain`.
    pub fn new(config: FileDrainConfig, error_logger: Logger) -> std::io::Result<Self> {
        std::fs::create_dir_all(&config.directory)?;

        let path = config
            .directory
            .join(format!("{}.jsonl", config.subgraph_id));
        let file = OpenOptions::new().create(true).append(true).open(path)?;

        Ok(FileDrain {
            config,
            error_logger,
            writer: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }
}

impl Drain for FileDrain {
    type Ok = ();
    type Err = Never;

    fn log(&self, record: &Record, values: &OwnedKVList) -> std::result::Result<(), Never> {
        // Don't write `trace` logs to file
        if record.level() == Level::Trace {
            return Ok(());
        }

        let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true);
        let builder =
            LogEntryBuilder::new(record, values, self.config.subgraph_id.clone(), timestamp);

        // Build log document
        let log_doc = FileLogDocument {
            id: builder.build_id(),
            subgraph_id: builder.subgraph_id().to_string(),
            timestamp: builder.timestamp().to_string(),
            level: builder.level_str().to_string(),
            text: builder.build_text(),
            arguments: builder.build_arguments_vec(),
            meta: builder.build_meta(),
        };

        // Write JSON line (synchronous, buffered)
        let mut writer = self.writer.lock().unwrap();
        if let Err(e) = serde_json::to_writer(&mut *writer, &log_doc) {
            error!(self.error_logger, "Failed to serialize log to JSON: {}", e);
            return Ok(());
        }

        if let Err(e) = writeln!(&mut *writer) {
            error!(self.error_logger, "Failed to write newline: {}", e);
            return Ok(());
        }

        // Flush to ensure durability
        if let Err(e) = writer.flush() {
            error!(self.error_logger, "Failed to flush log file: {}", e);
        }

        Ok(())
    }
}

/// Creates a new asynchronous file logger.
///
/// Uses `error_logger` to print any file logging errors,
/// so they don't go unnoticed.
pub fn file_logger(config: FileDrainConfig, error_logger: Logger) -> Logger {
    let file_drain = match FileDrain::new(config, error_logger.clone()) {
        Ok(drain) => drain,
        Err(e) => {
            error!(error_logger, "Failed to create FileDrain: {}", e);
            // Return a logger that discards all logs
            return Logger::root(slog::Discard, o!());
        }
    };

    create_async_logger(file_drain, 20000, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_file_drain_creation() {
        let temp_dir = TempDir::new().unwrap();
        let error_logger = Logger::root(slog::Discard, o!());

        let config = FileDrainConfig {
            directory: temp_dir.path().to_path_buf(),
            subgraph_id: "QmTest".to_string(),
            max_file_size: 1024 * 1024,
            retention_days: 30,
        };

        let drain = FileDrain::new(config, error_logger);
        assert!(drain.is_ok());

        // Verify file was created
        let file_path = temp_dir.path().join("QmTest.jsonl");
        assert!(file_path.exists());
    }

    #[test]
    fn test_log_entry_format() {
        let arguments = vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ];

        let doc = FileLogDocument {
            id: "test-id".to_string(),
            subgraph_id: "QmTest".to_string(),
            timestamp: "2024-01-15T10:30:00Z".to_string(),
            level: "error".to_string(),
            text: "Test error message".to_string(),
            arguments,
            meta: FileLogMeta {
                module: "test.rs".to_string(),
                line: 42,
                column: 10,
            },
        };

        let json = serde_json::to_string(&doc).unwrap();
        assert!(json.contains("\"id\":\"test-id\""));
        assert!(json.contains("\"subgraphId\":\"QmTest\""));
        assert!(json.contains("\"level\":\"error\""));
        assert!(json.contains("\"text\":\"Test error message\""));
        assert!(json.contains("\"arguments\""));
    }

    #[test]
    fn test_file_drain_writes_jsonl() {
        use std::io::{BufRead, BufReader};

        let temp_dir = TempDir::new().unwrap();
        let error_logger = Logger::root(slog::Discard, o!());

        let config = FileDrainConfig {
            directory: temp_dir.path().to_path_buf(),
            subgraph_id: "QmTest".to_string(),
            max_file_size: 1024 * 1024,
            retention_days: 30,
        };

        let drain = FileDrain::new(config.clone(), error_logger).unwrap();

        // Create a test record
        let logger = Logger::root(drain, o!());
        info!(logger, "Test message"; "key" => "value");

        // Give async drain time to write (in real test we'd use proper sync)
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Read the file
        let file_path = temp_dir.path().join("QmTest.jsonl");
        let file = File::open(file_path).unwrap();
        let reader = BufReader::new(file);

        let lines: Vec<String> = reader.lines().map_while(|r| r.ok()).collect();

        // Should have written at least one line
        assert!(!lines.is_empty());

        // Each line should be valid JSON
        for line in lines {
            let parsed: serde_json::Value = serde_json::from_str(&line).unwrap();
            assert!(parsed.get("id").is_some());
            assert!(parsed.get("subgraphId").is_some());
            assert!(parsed.get("timestamp").is_some());
            assert!(parsed.get("level").is_some());
            assert!(parsed.get("text").is_some());
        }
    }
}
