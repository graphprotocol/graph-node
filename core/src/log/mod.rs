use chrono::prelude::{SecondsFormat, Utc};
use itertools;
use reqwest;
use serde::ser::Serializer as SerdeSerializer;
use std::fmt;
use std::fmt::Write;
use std::result::Result as StdResult;

use graph::slog::*;
use graph::slog_async;

/// Serializes an slog log level using a serde Serializer.
fn serialize_log_level<S>(level: &Level, serializer: S) -> StdResult<S::Ok, S::Error>
where
    S: SerdeSerializer,
{
    serializer.serialize_str(match level {
        Level::Critical => "critical",
        Level::Error => "error",
        Level::Warning => "warn",
        Level::Info => "info",
        Level::Debug => "debug",
        Level::Trace => "trace",
    })
}

// Log message meta data.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ElasticLogMeta {
    module: String,
    line: i64,
    column: i64,
}

// Log message to be written to Elasticsearch.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ElasticLog {
    id: String,
    subgraph_id: String,
    timestamp: String,
    text: String,
    #[serde(serialize_with = "serialize_log_level")]
    level: Level,
    meta: ElasticLogMeta,
}

/// A super-simple slog Serializer for concatenating key/value arguments.
struct SimpleKVSerializer {
    kvs: Vec<(String, String)>,
}

impl SimpleKVSerializer {
    /// Creates a new `SimpleKVSerializer`.
    fn new() -> Self {
        SimpleKVSerializer {
            kvs: Default::default(),
        }
    }

    /// Collects all key/value arguments into a single, comma-separated string.
    /// Returns the number of key/value pairs and the string itself.
    fn finish(self) -> (usize, String) {
        (
            self.kvs.len(),
            itertools::join(self.kvs.iter().map(|(k, v)| format!("{}: {}", k, v)), ", "),
        )
    }
}

impl Serializer for SimpleKVSerializer {
    fn emit_arguments(&mut self, key: Key, val: &fmt::Arguments) -> Result {
        Ok(self.kvs.push((key.into(), format!("{}", val))))
    }
}

/// Configuration for `ElasticDrain`.
pub struct ElasticDrainConfig {
    /// Elasticsearch service to log to.
    pub endpoint: String,
    /// Username for the service.
    pub username: String,
    /// Password for the service (optional).
    pub password: Option<String>,
    /// The Elasticsearch index to log to.
    pub index: String,
    /// The Elasticsearch type to use for logs.
    pub document_type: String,
    /// The subgraph ID that the drain is for.
    pub subgraph_id: String,
}

/// An slog `Drain` for logging to Elasticsearch.
struct ElasticDrain {
    config: ElasticDrainConfig,
}

impl ElasticDrain {
    pub fn new(config: ElasticDrainConfig) -> Self {
        ElasticDrain { config }
    }
}

impl Drain for ElasticDrain {
    type Ok = ();
    type Err = reqwest::Error;

    fn log(&self, record: &Record, values: &OwnedKVList) -> StdResult<Self::Ok, Self::Err> {
        let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true);
        let id = format!("{}-{}", self.config.subgraph_id, timestamp);

        // Serialize logger arguments
        let mut serializer = SimpleKVSerializer::new();
        record
            .kv()
            .serialize(record, &mut serializer)
            .expect("failed to serializer logger arguments");
        let (n_logger_kvs, logger_kvs) = serializer.finish();

        // Serialize log message arguments
        let mut serializer = SimpleKVSerializer::new();
        values
            .serialize(record, &mut serializer)
            .expect("failed to serialize log message arguments");
        let (n_value_kvs, value_kvs) = serializer.finish();

        let mut text = format!("{}", record.msg());
        if n_logger_kvs > 0 {
            write!(text, ", {}", logger_kvs);
        }
        if n_value_kvs > 0 {
            write!(text, ", {}", value_kvs);
        }

        let meta = ElasticLogMeta {
            module: record.module().into(),
            line: record.line() as i64,
            column: record.column() as i64,
        };

        let log = ElasticLog {
            id: id.clone(),
            subgraph_id: self.config.subgraph_id.clone(),
            timestamp,
            text,
            level: record.level(),
            meta,
        };

        // Build the document URL
        let mut document_url =
            reqwest::Url::parse(self.config.endpoint.as_str()).expect("invalid Elasticsearch URL");
        document_url
            .path_segments_mut()
            .expect("failed to set the Elasticsearch documenet path")
            .push(self.config.index.as_str())
            .push(self.config.document_type.as_str())
            .push(id.as_str());

        // Send log to Elasticsearch
        let client = reqwest::Client::new();
        let response = client
            .put(document_url)
            .basic_auth(self.config.username.clone(), self.config.password.clone())
            .json(&log)
            .send()?;

        // Return an error if the server returned an error response
        response.error_for_status().map(|_| ())
    }
}

pub fn elastic_logger(config: ElasticDrainConfig) -> Logger {
    let elastic_drain = ElasticDrain::new(config).fuse();
    let async_drain = slog_async::Async::new(elastic_drain).build().fuse();
    Logger::root(async_drain, o!())
}
