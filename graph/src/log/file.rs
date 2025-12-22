use std::fmt;
use std::fmt::Write as FmtWrite;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use chrono::prelude::{SecondsFormat, Utc};
use serde::Serialize;
use slog::*;

/// Configuration for `FileDrain`.
#[derive(Clone, Debug)]
pub struct FileDrainConfig {
    /// Directory where log files will be stored
    pub directory: PathBuf,
    /// The subgraph ID (used for filename)
    pub subgraph_id: String,
    /// Maximum file size in bytes (for future rotation feature)
    pub max_file_size: u64,
    /// Retention period in days (for future cleanup feature)
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

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FileLogMeta {
    module: String,
    line: i64,
    column: i64,
}

/// Serializer for extracting key-value pairs into a Vec
struct VecKVSerializer {
    kvs: Vec<(String, String)>,
}

impl VecKVSerializer {
    fn new() -> Self {
        VecKVSerializer {
            kvs: Default::default(),
        }
    }

    fn finish(self) -> Vec<(String, String)> {
        self.kvs
    }
}

impl Serializer for VecKVSerializer {
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
        // Create directory if it doesn't exist
        std::fs::create_dir_all(&config.directory)?;

        // Open file for subgraph (append mode)
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
    type Err = ();

    fn log(&self, record: &Record, values: &OwnedKVList) -> std::result::Result<(), ()> {
        // Don't write `trace` logs to file
        if record.level() == Level::Trace {
            return Ok(());
        }

        let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true);
        let id = format!("{}-{}", self.config.subgraph_id, timestamp);

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

        // Serialize arguments into vec for storage
        let mut serializer = VecKVSerializer::new();
        record
            .kv()
            .serialize(record, &mut serializer)
            .expect("failed to serialize log message arguments into vec");
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
        let log_doc = FileLogDocument {
            id,
            subgraph_id: self.config.subgraph_id.clone(),
            timestamp,
            level: level.to_string(),
            text,
            arguments,
            meta: FileLogMeta {
                module: record.module().into(),
                line: record.line() as i64,
                column: record.column() as i64,
            },
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

    let async_drain = slog_async::Async::new(file_drain.fuse())
        .chan_size(20000)
        .overflow_strategy(slog_async::OverflowStrategy::Block)
        .build()
        .fuse();
    Logger::root(async_drain, o!())
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

        let lines: Vec<String> = reader.lines().filter_map(Result::ok).collect();

        // Should have written at least one line
        assert!(lines.len() > 0);

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
