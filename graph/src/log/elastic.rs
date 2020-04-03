use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::result::Result;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::prelude::{SecondsFormat, Utc};
use futures03::TryFutureExt;
use reqwest;
use reqwest::Client;
use serde::ser::Serializer as SerdeSerializer;
use serde::Serialize;
use serde_json::json;
use slog::*;
use slog_async;

use crate::prelude::CheapClone;

/// General configuration parameters for Elasticsearch logging.
#[derive(Clone, Debug)]
pub struct ElasticLoggingConfig {
    /// The Elasticsearch service to log to.
    pub endpoint: String,
    /// The Elasticsearch username.
    pub username: Option<String>,
    /// The Elasticsearch password (optional).
    pub password: Option<String>,
}

/// Serializes an slog log level using a serde Serializer.
fn serialize_log_level<S>(level: &Level, serializer: S) -> Result<S::Ok, S::Error>
where
    S: SerdeSerializer,
{
    serializer.serialize_str(match level {
        Level::Critical => "critical",
        Level::Error => "error",
        Level::Warning => "warning",
        Level::Info => "info",
        Level::Debug => "debug",
        Level::Trace => "trace",
    })
}

// Log message source.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Source {
    module: String,
    line: i64,
    column: i64,
}

// Log message to be written to Elasticsearch.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ElasticLog {
    id: String,
    timestamp: String,
    #[serde(serialize_with = "serialize_log_level")]
    level: Level,
    text: String,
    #[serde(flatten)]
    custom_id: HashMap<String, String>,
    arguments: BTreeMap<String, String>,
    source: Source,
}

struct KVSerializer<'a> {
    kvs: &'a mut BTreeMap<String, String>,
}

impl<'a> KVSerializer<'a> {
    fn new(kvs: &'a mut BTreeMap<String, String>) -> Self {
        Self { kvs }
    }
}

impl<'a> Serializer for KVSerializer<'a> {
    fn emit_arguments(&mut self, key: Key, val: &fmt::Arguments) -> slog::Result {
        self.kvs.insert(key.into(), format!("{}", val));
        Ok(())
    }
}

/// Configuration for `ElasticDrain`.
#[derive(Clone, Debug)]
pub struct ElasticDrainConfig {
    /// General Elasticsearch logging configuration.
    pub general: ElasticLoggingConfig,
    /// The Elasticsearch index to log to.
    pub index: String,
    /// The Elasticsearch type to use for logs.
    pub document_type: String,
    /// The name of the custom object id that the drain is for.
    pub custom_id_key: String,
    /// The custom id for the object that the drain is for.
    pub custom_id_value: String,
    /// The batching interval.
    pub flush_interval: Duration,
}

/// An slog `Drain` for logging to Elasticsearch.
///
/// Writes logs to Elasticsearch using the following format:
/// ```ignore
/// {
///   "_index": "subgraph-logs"
///   "_type": "log",
///   "_id": "Qmb31zcpzqga7ERaUTp83gVdYcuBasz4rXUHFufikFTJGU-2018-11-08T00:54:52.589258000Z",
///   "_source": {
///     "level": "debug",
///     "timestamp": "2018-11-08T00:54:52.589258000Z",
///     "subgraphId": "Qmb31zcpzqga7ERaUTp83gVdYcuBasz4rXUHFufikFTJGU",
///     "source": {
///       "module": "graph_chain_ethereum::block_stream",
///       "line": 220,
///       "column": 9
///     },
///     "text": "Chain head pointer, number: 6661038, hash: 0xf089c457700a57798ced06bd3f18eef53bb8b46510bcefaf13615a8a26e4424a, component: BlockStream",
///     "id": "Qmb31zcpzqga7ERaUTp83gVdYcuBasz4rXUHFufikFTJGU-2018-11-08T00:54:52.589258000Z"
///   }
/// }
/// ```
pub struct ElasticDrain {
    custom_id_key: String,
    custom_id_value: String,
    logs: Arc<Mutex<Vec<ElasticLog>>>,
    system_logger: Logger,
}

impl ElasticDrain {
    /// Creates a new `ElasticDrain`.
    pub fn new(config: ElasticDrainConfig, system_logger: Logger) -> Self {
        // Parse the endpoint URL
        let mut endpoint = reqwest::Url::parse(config.general.endpoint.as_str())
            .expect("invalid Elasticsearch URL");
        endpoint.set_path("_bulk");

        let drain = ElasticDrain {
            custom_id_key: config.custom_id_key,
            custom_id_value: config.custom_id_value,
            logs: Arc::new(Mutex::new(vec![])),

            system_logger,
        };
        drain.periodically_flush_logs(
            endpoint,
            config.general,
            config.flush_interval,
            config.index,
            config.document_type,
        );
        drain
    }

    fn periodically_flush_logs(
        &self,
        endpoint: reqwest::Url,
        general: ElasticLoggingConfig,
        flush_interval: Duration,
        index: String,
        document_type: String,
    ) {
        use futures03::stream::StreamExt;

        let system_logger = self.system_logger.cheap_clone();
        let logs = self.logs.cheap_clone();

        crate::task_spawn::spawn(async move {
            let mut interval = tokio::time::interval(flush_interval);

            while interval.next().await.is_some() {
                let logs_to_send = {
                    let mut logs = logs.lock().unwrap();
                    std::mem::take(&mut *logs)
                };

                // Do nothing if there are no logs to flush
                if logs_to_send.is_empty() {
                    continue;
                }

                debug!(
                    system_logger,
                    "Flushing {} logs to Elasticsearch",
                    logs_to_send.len(),
                );

                // Send logs in chunks of a 1000 messages per request
                let mut stream = futures03::stream::iter(logs_to_send.chunks(1000));

                while let Some(chunk) = stream.next().await {
                    // The Elasticsearch batch API takes requests with the following format:
                    // ```ignore
                    // action_and_meta_data\n
                    // optional_source\n
                    // action_and_meta_data\n
                    // optional_source\n
                    // ```

                    let mut body = String::new();
                    for log in chunk.into_iter() {
                        // Try to serialize the log message to JSON
                        match serde_json::to_string(log) {
                            Ok(msg) => {
                                // Generate an action line for the log message
                                let action = json!({
                                    "index": {
                                      "_index": &index,
                                      "_type": &document_type,
                                      "_id": log.id,
                                    }
                                })
                                .to_string();

                                // Add action line and \n to the body
                                body.push_str(action.as_str());
                                body.push_str("\n");

                                // Add log message and \n to the body
                                body.push_str(msg.as_str());
                                body.push_str("\n");
                            }
                            Err(e) => {
                                warn!(
                                    system_logger,
                                    "Failed to serialize log message to JSON";
                                    "error" => format!("{}", e)
                                );
                            }
                        }
                    }

                    // Send batch of logs to Elasticsearch
                    let client = Client::new();

                    let header = match &general.username {
                        Some(username) => client
                            .post(endpoint.as_str())
                            .header("Content-Type", "application/json")
                            .basic_auth(username, general.password.clone()),
                        None => client
                            .post(endpoint.as_str())
                            .header("Content-Type", "application/json"),
                    };

                    if let Err(e) = header
                        .body(body)
                        .send()
                        .and_then(|response| async { response.error_for_status() })
                        .await
                    {
                        // Log if there was a problem sending the logs
                        error!(
                            system_logger,
                            "Failed to send logs to Elasticsearch";
                            "error" => format!("{}", e),
                        );
                    }
                }
            }
        });
    }
}

impl Drain for ElasticDrain {
    type Ok = ();
    type Err = ();

    fn log(&self, record: &Record, values: &OwnedKVList) -> Result<Self::Ok, Self::Err> {
        // Don't sent `trace` logs to ElasticSearch.
        if record.level() == Level::Trace {
            return Ok(());
        }

        let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true);
        let id = format!("{}-{}", self.custom_id_value, timestamp);

        let mut arguments = BTreeMap::new();

        // Serialize log message arguments
        let mut serializer = KVSerializer::new(&mut arguments);
        values
            .serialize(record, &mut serializer)
            .expect("failed to serialize log message arguments");

        // Serialize logger arguments
        record
            .kv()
            .serialize(record, &mut serializer)
            .expect("failed to serialize logger arguments");

        // Serialize the log message itself
        let text = format!("{}", record.msg());

        // Prepare custom id for log document
        let mut custom_id = HashMap::new();
        custom_id.insert(self.custom_id_key.clone(), self.custom_id_value.clone());

        // Prepare log document
        let log = ElasticLog {
            id: id.clone(),
            custom_id,
            arguments,
            timestamp,
            text,
            level: record.level(),
            source: Source {
                module: record.module().into(),
                line: record.line() as i64,
                column: record.column() as i64,
            },
        };

        // Push the log into the queue
        let mut logs = self.logs.lock().unwrap();
        logs.push(log);

        Ok(())
    }
}

/// Creates a new asynchronous Elasticsearch logger.
///
/// Uses `error_logger` to print any Elasticsearch logging errors,
/// so they don't go unnoticed.
pub fn elastic_logger(config: ElasticDrainConfig, error_logger: Logger) -> Logger {
    let elastic_drain = ElasticDrain::new(config, error_logger).fuse();
    let async_drain = slog_async::Async::new(elastic_drain)
        .chan_size(10000)
        .build()
        .fuse();
    Logger::root(async_drain, o!())
}
