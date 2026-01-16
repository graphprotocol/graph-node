use std::collections::HashMap;
use std::fmt;

use serde::Serialize;
use slog::*;

/// Serializer for concatenating key-value arguments into a string
pub struct SimpleKVSerializer {
    kvs: Vec<(String, String)>,
}

impl Default for SimpleKVSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl SimpleKVSerializer {
    pub fn new() -> Self {
        Self { kvs: Vec::new() }
    }

    /// Returns the number of key-value pairs and the concatenated string
    pub fn finish(self) -> (usize, String) {
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
        self.kvs.push((key.into(), val.to_string()));
        Ok(())
    }
}

/// Serializer for extracting key-value pairs into a Vec
pub struct VecKVSerializer {
    kvs: Vec<(String, String)>,
}

impl Default for VecKVSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl VecKVSerializer {
    pub fn new() -> Self {
        Self { kvs: Vec::new() }
    }

    pub fn finish(self) -> Vec<(String, String)> {
        self.kvs
    }
}

impl Serializer for VecKVSerializer {
    fn emit_arguments(&mut self, key: Key, val: &fmt::Arguments) -> slog::Result {
        self.kvs.push((key.into(), val.to_string()));
        Ok(())
    }
}

/// Serializer for extracting key-value pairs into a HashMap
pub struct HashMapKVSerializer {
    kvs: Vec<(String, String)>,
}

impl Default for HashMapKVSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl HashMapKVSerializer {
    pub fn new() -> Self {
        HashMapKVSerializer { kvs: Vec::new() }
    }

    pub fn finish(self) -> HashMap<String, String> {
        self.kvs.into_iter().collect()
    }
}

impl Serializer for HashMapKVSerializer {
    fn emit_arguments(&mut self, key: Key, val: &fmt::Arguments) -> slog::Result {
        self.kvs.push((key.into(), val.to_string()));
        Ok(())
    }
}

/// Log metadata structure
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogMeta {
    pub module: String,
    pub line: i64,
    pub column: i64,
}

/// Converts an slog Level to a string representation
pub fn level_to_str(level: Level) -> &'static str {
    match level {
        Level::Critical => "critical",
        Level::Error => "error",
        Level::Warning => "warning",
        Level::Info => "info",
        Level::Debug => "debug",
        Level::Trace => "trace",
    }
}

/// Builder for common log entry fields across different drain implementations
pub struct LogEntryBuilder<'a> {
    record: &'a Record<'a>,
    values: &'a OwnedKVList,
    subgraph_id: String,
    timestamp: String,
}

impl<'a> LogEntryBuilder<'a> {
    pub fn new(
        record: &'a Record<'a>,
        values: &'a OwnedKVList,
        subgraph_id: String,
        timestamp: String,
    ) -> Self {
        Self {
            record,
            values,
            subgraph_id,
            timestamp,
        }
    }

    /// Builds the log ID in the format: subgraph_id-timestamp
    pub fn build_id(&self) -> String {
        format!("{}-{}", self.subgraph_id, self.timestamp)
    }

    /// Builds the text field by concatenating the message with all key-value pairs
    pub fn build_text(&self) -> String {
        // Serialize logger arguments
        let mut serializer = SimpleKVSerializer::new();
        self.record
            .kv()
            .serialize(self.record, &mut serializer)
            .expect("failed to serialize logger arguments");
        let (n_logger_kvs, logger_kvs) = serializer.finish();

        // Serialize log message arguments
        let mut serializer = SimpleKVSerializer::new();
        self.values
            .serialize(self.record, &mut serializer)
            .expect("failed to serialize log message arguments");
        let (n_value_kvs, value_kvs) = serializer.finish();

        // Build text with all key-value pairs
        let mut text = format!("{}", self.record.msg());
        if n_logger_kvs > 0 {
            use std::fmt::Write;
            write!(text, ", {}", logger_kvs).unwrap();
        }
        if n_value_kvs > 0 {
            use std::fmt::Write;
            write!(text, ", {}", value_kvs).unwrap();
        }

        text
    }

    /// Builds arguments as a Vec of tuples (for file drain)
    pub fn build_arguments_vec(&self) -> Vec<(String, String)> {
        let mut serializer = VecKVSerializer::new();
        self.record
            .kv()
            .serialize(self.record, &mut serializer)
            .expect("failed to serialize log message arguments into vec");
        serializer.finish()
    }

    /// Builds arguments as a HashMap (for elastic and loki drains)
    pub fn build_arguments_map(&self) -> HashMap<String, String> {
        let mut serializer = HashMapKVSerializer::new();
        self.record
            .kv()
            .serialize(self.record, &mut serializer)
            .expect("failed to serialize log message arguments into hash map");
        serializer.finish()
    }

    /// Builds metadata from the log record
    pub fn build_meta(&self) -> LogMeta {
        LogMeta {
            module: self.record.module().into(),
            line: self.record.line() as i64,
            column: self.record.column() as i64,
        }
    }

    /// Gets the level as a string
    pub fn level_str(&self) -> &'static str {
        level_to_str(self.record.level())
    }

    /// Gets the timestamp
    pub fn timestamp(&self) -> &str {
        &self.timestamp
    }

    /// Gets the subgraph ID
    pub fn subgraph_id(&self) -> &str {
        &self.subgraph_id
    }
}

/// Creates a new asynchronous logger with consistent configuration
pub fn create_async_logger<D>(drain: D, chan_size: usize, use_block_overflow: bool) -> Logger
where
    D: Drain + Send + 'static,
    D::Err: std::fmt::Debug,
{
    let mut builder = slog_async::Async::new(drain.fuse()).chan_size(chan_size);

    if use_block_overflow {
        builder = builder.overflow_strategy(slog_async::OverflowStrategy::Block);
    }

    Logger::root(builder.build().fuse(), o!())
}
