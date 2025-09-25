use std::{
    hash::{Hash, Hasher},
    time::Duration,
};

use ahash::AHasher;
use arrow::{array::RecordBatch, datatypes::Schema, error::ArrowError};
use arrow_flight::{
    error::FlightError, flight_service_client::FlightServiceClient,
    sql::client::FlightSqlServiceClient,
};
use async_stream::try_stream;
use bytes::Bytes;
use futures03::{future::BoxFuture, stream::BoxStream, StreamExt};
use lazy_regex::regex_is_match;
use slog::{debug, Logger};
use thiserror::Error;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use crate::{
    nozzle::{
        client::Client,
        error,
        log::{one_line, Logger as _},
    },
    prelude::CheapClone,
};

/// A client for the Nozzle Flight gRPC service.
///
/// This client connects to a Nozzle server and executes SQL queries
/// using the Apache Arrow Flight protocol.
pub struct FlightClient {
    channel: Channel,
}

#[derive(Debug, Error)]
pub enum Error {
    // Address excluded to avoid leaking sensitive details in logs
    #[error("invalid address")]
    InvalidAddress,

    #[error("service failed: {0:#}")]
    Service(#[source] ArrowError),

    #[error("stream failed: {0:#}")]
    Stream(#[source] FlightError),
}

impl FlightClient {
    /// Constructs a new Nozzle client connected to the specified Nozzle Flight service address.
    pub fn new(addr: impl Into<Bytes>) -> Result<Self, Error> {
        let addr: Bytes = addr.into();
        let is_https = std::str::from_utf8(&addr).map_or(false, |a| a.starts_with("https://"));

        let mut endpoint = Endpoint::from_shared(addr)
            .map_err(|_e| Error::InvalidAddress)?
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .keep_alive_while_idle(true)
            .http2_adaptive_window(true)
            .initial_connection_window_size(Some(32 * 1024 * 1024))
            .initial_stream_window_size(Some(16 * 1024 * 1024))
            .connect_timeout(Duration::from_secs(10));

        if is_https {
            let mut tls_config = ClientTlsConfig::new();
            tls_config = tls_config.with_native_roots();

            endpoint = endpoint.tls_config(tls_config).unwrap();
        }

        Ok(Self {
            channel: endpoint.connect_lazy(),
        })
    }

    fn raw_client(&self) -> FlightSqlServiceClient<Channel> {
        let channel = self.channel.cheap_clone();
        let client = FlightServiceClient::new(channel)
            .max_encoding_message_size(256 * 1024 * 1024)
            .max_decoding_message_size(256 * 1024 * 1024);

        FlightSqlServiceClient::new_from_inner(client)
    }
}

impl Client for FlightClient {
    type Error = Error;

    fn schema(
        &self,
        logger: &Logger,
        query: impl ToString,
    ) -> BoxFuture<'static, Result<Schema, Self::Error>> {
        let logger = logger.component("nozzle::FlightClient");
        let mut raw_client = self.raw_client();
        let query = query.to_string();

        Box::pin(async move {
            const TXN_ID: Option<Bytes> = None;

            debug!(logger, "Executing SQL query";
                "query" => &*one_line(&query)
            );

            let flight_info = raw_client
                .execute(query, TXN_ID)
                .await
                .map_err(Error::Service)?;

            flight_info.try_decode_schema().map_err(Error::Service)
        })
    }

    fn query(
        &self,
        logger: &Logger,
        query: impl ToString,
    ) -> BoxStream<'static, Result<RecordBatch, Self::Error>> {
        let logger = logger.component("nozzle::FlightClient");
        let mut raw_client = self.raw_client();
        let query = query.to_string();
        let query_id = query_id(&query);

        try_stream! {
            const TXN_ID: Option<Bytes> = None;

            debug!(logger, "Executing SQL query";
                "query" => &*one_line(&query),
                "query_id" => query_id
            );

            let flight_info = raw_client
                .execute(query, TXN_ID)
                .await
                .map_err(Error::Service)?;

            for endpoint in flight_info.endpoint {
                let Some(ticket) = endpoint.ticket else {
                    continue;
                };

                let mut stream = raw_client.do_get(ticket).await.map_err(Error::Service)?;
                let mut batch_index = 0u32;

                while let Some(batch_result) = stream.next().await {
                    debug!(logger, "Received a new record batch";
                        "query_id" => query_id,
                        "batch_index" => batch_index,
                        "num_rows" => batch_result.as_ref().map_or(0, |b| b.num_rows()),
                        "memory_size_bytes" => batch_result.as_ref().map_or(0, |b| b.get_array_memory_size())
                    );

                    let record_batch = batch_result.map_err(Error::Stream)?;
                    yield record_batch;

                    batch_index += 1;
                }

                debug!(logger, "Query execution completed successfully";
                    "query_id" => query_id,
                    "batch_count" => batch_index
                );
            }
        }
        .boxed()
    }
}

impl error::IsDeterministic for Error {
    fn is_deterministic(&self) -> bool {
        static PATTERNS: &[&str] = &[
            r#", message: "SQL parse error:"#,
            r#", message: "error looking up datasets:"#,
            r#", message: "planning error:"#,
        ];

        let msg = self.to_string();

        for &pattern in PATTERNS {
            if msg.contains(pattern) {
                return true;
            }
        }

        if regex_is_match!(r#", message: "dataset '.*?' not found, full error:"#, &msg) {
            return true;
        }

        false
    }
}

/// Generates an ID from a SQL query for log correlation.
///
/// The ID allows connecting related logs without including the full SQL
/// query in every log message.
fn query_id(query: &str) -> u32 {
    let mut hasher = AHasher::default();
    query.hash(&mut hasher);
    hasher.finish() as u32
}
