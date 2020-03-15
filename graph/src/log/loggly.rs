use std::collections::BTreeMap;
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
use slog::*;
use slog_async;

/// General Loggly configuration.
#[derive(Clone, Debug)]
pub struct LogglyConfig {
    /// API token for Loggly.
    pub token: String,
}

/// Serializes an slog log level using a serde Serializer.
fn serialize_log_level<S>(level: &Level, serializer: S) -> Result<S::Ok, S::Error>
where
    S: SerdeSerializer,
{
    serializer.serialize_str(level.as_str())
}

// Log message source.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Source {
    module: String,
    line: i64,
    column: i64,
}

// Log message to be written to Loggly.
#[derive(Clone, Debug, Serialize)]
struct Log {
    timestamp: String,
    #[serde(serialize_with = "serialize_log_level")]
    level: Level,
    text: String,
    params: BTreeMap<String, String>,
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

/// Configuration for `LogglyDrain`.
#[derive(Clone, Debug)]
pub struct LogglyDrainConfig {
    /// General Loggly configuration.
    pub general: LogglyConfig,
    /// Prefix string for log messages.
    pub id_prefix: String,
    /// The batching interval.
    pub flush_interval: Duration,
}

/// An slog `Drain` for logging to Loggly.
///
/// Writes logs to Loggly using the following JSON format:
/// ```ignore
/// {
///   "id": "Qmb31zcpzqga7ERaUTp83gVdYcuBasz4rXUHFufikFTJGU-2018-11-08T00:54:52.589258000Z"
///   "level": "debug",
///   "timestamp": "2018-11-08T00:54:52.589258000Z",
///   "text": "Chain head pointer",
///   "params": {
///     "subgraph_id": "Qmb31zcpzqga7ERaUTp83gVdYcuBasz4rXUHFufikFTJGU",
///     "number": 6661038,
///     "hash": "0xf089c457700a57798ced06bd3f18eef53bb8b46510bcefaf13615a8a26e4424a",
///     "component": "BlockStream"
///   },
///   "source": {
///     "module": "graph_chain_ethereum::block_stream",
///     "line": 220,
///     "column": 9
///   }
/// }
/// ```
pub struct LogglyDrain {
    endpoint: reqwest::Url,
    config: LogglyDrainConfig,
    error_logger: Logger,
    logs: Arc<Mutex<Vec<Log>>>,
}

impl LogglyDrain {
    /// Creates a new `LogglyDrain`.
    pub fn new(config: LogglyDrainConfig, error_logger: Logger) -> Self {
        // Build the batch API URL
        let endpoint = reqwest::Url::parse(
            format!(
                "http://logs-01.loggly.com/bulk/{}/tag/bulk/",
                config.general.token
            )
            .as_str(),
        )
        .expect("failed to parse Loggly URL with API token");

        let drain = LogglyDrain {
            endpoint,
            config,
            error_logger,
            logs: Arc::new(Mutex::new(vec![])),
        };
        drain.periodically_flush_logs();
        drain
    }

    fn periodically_flush_logs(&self) {
        use futures03::stream::StreamExt;

        let flush_logger = self.error_logger.clone();
        let logs = self.logs.clone();
        let endpoint = self.endpoint.clone();

        crate::task_spawn::spawn(tokio::time::interval(self.config.flush_interval).for_each(
            move |_| {
                let logs = logs.clone();
                let flush_logger = flush_logger.clone();
                let endpoint = endpoint.clone();
                async move {
                    let logs_to_send = {
                        let mut logs = logs.lock().unwrap();
                        std::mem::take(&mut *logs)
                    };

                    // Do nothing if there are no logs to flush
                    if logs_to_send.is_empty() {
                        return;
                    }

                    debug!(
                        flush_logger,
                        "Flushing {} logs to Loggly",
                        logs_to_send.len()
                    );

                    let chunks = logs_to_send
                        .chunks(1000)
                        .map(|logs| {
                            logs.into_iter()
                                .filter_map(|log| match serde_json::to_string(log) {
                                    Ok(s) => Some(s),
                                    Err(e) => {
                                        warn!(
                                            flush_logger,
                                            "Failed to serialize log message to JSON";
                                            "error" => format!("{}", e)
                                        );
                                        None
                                    }
                                })
                                .collect::<Vec<_>>()
                        })
                        .collect::<Vec<_>>();

                    let mut stream = futures03::stream::iter(chunks);
                    while let Some(chunk) = stream.next().await {
                        // The Loggly batch API takes requests with the following format:
                        // ```ignore
                        // log1\n
                        // log2\n
                        // log3\n
                        // ```
                        // For more details, see:
                        // https://www.loggly.com/docs/http-bulk-endpoint/
                        //
                        // We're assembly the request body in the same way below:

                        debug!(flush_logger, "Sending {} logs to Loggly now", chunk.len());
                        let body = chunk.join("\n");

                        // Send batch of logs to Loggly
                        let client = Client::new();
                        let logger_for_err = flush_logger.clone();

                        client
                            .post(endpoint.as_str())
                            .header("Content-Type", "application/json")
                            .body(body)
                            .send()
                            .and_then(|response| async { response.error_for_status() })
                            .map_ok(|_| ())
                            .unwrap_or_else(move |e| {
                                // Log if there was a problem sending the logs
                                error!(logger_for_err, "Failed to send logs to Loggly: {}", e);
                            })
                            .await
                    }
                }
            },
        ));
    }
}

impl Drain for LogglyDrain {
    type Ok = ();
    type Err = ();

    fn log(&self, record: &Record, values: &OwnedKVList) -> Result<Self::Ok, Self::Err> {
        // Don't sent `trace` logs to Loggly
        if record.level() == Level::Trace {
            return Ok(());
        }

        let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true);

        let mut params = BTreeMap::new();

        // Serialize log message params
        let mut serializer = KVSerializer::new(&mut params);
        values
            .serialize(record, &mut serializer)
            .expect("failed to serialize log message params");

        // Serialize logger params into hash map
        let mut serializer = KVSerializer::new(&mut params);
        record
            .kv()
            .serialize(record, &mut serializer)
            .expect("failed to serialize logger params");

        let text = format!("{}", record.msg());

        // Prepare log document
        let log = Log {
            timestamp,
            params,
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

/// Creates a new asynchronous Loggly logger.
///
/// Uses `error_logger` to print any Loggly logging errors,
/// so they don't go unnoticed.
pub fn loggly_logger<T>(
    config: LogglyDrainConfig,
    values: slog::OwnedKV<T>,
    error_logger: Logger,
) -> Logger
where
    T: SendSyncRefUnwindSafeKV + 'static,
{
    let loggly_drain = LogglyDrain::new(config, error_logger).fuse();
    let async_drain = slog_async::Async::new(loggly_drain)
        .chan_size(10000)
        .build()
        .fuse();
    Logger::root(async_drain, values)
}
