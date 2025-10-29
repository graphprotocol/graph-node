use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    ops::RangeInclusive,
    time::Duration,
};

use ahash::AHasher;
use alloy::primitives::{BlockHash, BlockNumber};
use arrow::{datatypes::Schema, error::ArrowError};
use arrow_flight::{
    decode::DecodedPayload, error::FlightError, flight_service_client::FlightServiceClient,
    sql::client::FlightSqlServiceClient,
};
use async_stream::try_stream;
use bytes::Bytes;
use futures03::{future::BoxFuture, stream::BoxStream, StreamExt};
use http::Uri;
use serde::{Deserialize, Serialize};
use slog::{debug, trace, Logger};
use thiserror::Error;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use crate::{
    amp::{
        client::{
            Client, LatestBlockBeforeReorg, RequestMetadata, ResponseBatch, ResumeStreamingQuery,
        },
        error,
        log::{one_line, Logger as _},
    },
    prelude::CheapClone,
};

/// A client for the Amp Flight gRPC service.
///
/// This client connects to a Amp server and executes SQL queries
/// using the Apache Arrow Flight protocol.
pub struct FlightClient {
    channel: Channel,
}

impl FlightClient {
    /// Creates a new Amp client connected to the specified Amp Flight service address.
    pub async fn new(addr: Uri) -> Result<Self, Error> {
        let is_https = addr.scheme() == Some(&http::uri::Scheme::HTTPS);
        let mut endpoint = Endpoint::from(addr)
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
            channel: endpoint.connect().await.map_err(Error::Connection)?,
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
        let logger = logger.component("AmpFlightClient");
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
        request_metadata: Option<RequestMetadata>,
    ) -> BoxStream<'static, Result<ResponseBatch, Self::Error>> {
        let query = query.to_string();
        let logger = logger
            .component("AmpFlightClient")
            .new(slog::o!("query_id" => query_id(&query)));

        let mut raw_client = self.raw_client();
        let mut prev_block_ranges: Vec<BlockRange> = Vec::new();

        if let Some(request_metadata) = request_metadata {
            let RequestMetadata {
                resume_streaming_query,
            } = request_metadata;

            if let Some(resume_streaming_query) = resume_streaming_query {
                prev_block_ranges = resume_streaming_query
                    .iter()
                    .cloned()
                    .map(Into::into)
                    .collect();

                raw_client.set_header(
                    "amp-resume",
                    serialize_resume_streaming_query(resume_streaming_query),
                );
            }
        }

        try_stream! {
            const TXN_ID: Option<Bytes> = None;

            debug!(logger, "Executing SQL query";
                "query" => &*one_line(&query)
            );

            let flight_info = raw_client
                .execute(query, TXN_ID)
                .await
                .map_err(Error::Service)?;

            for (endpoint_index, endpoint) in flight_info.endpoint.into_iter().enumerate() {
                let Some(ticket) = endpoint.ticket else {
                    continue;
                };

                let mut stream = raw_client.do_get(ticket).await.map_err(Error::Service)?.into_inner();
                let mut batch_index = 0u32;
                let mut prev_block_ranges = prev_block_ranges.clone();

                while let Some(batch_result) = stream.next().await {
                    let flight_data = batch_result.map_err(Error::Stream)?;
                    let app_metadata = flight_data.inner.app_metadata;
                    let payload = flight_data.payload;

                    let record_batch = match payload {
                        DecodedPayload::None => {
                            trace!(logger, "Received empty data";
                                "endpoint_index" => endpoint_index
                            );
                            continue
                        },
                        DecodedPayload::Schema(_) => {
                            trace!(logger, "Received schema only";
                                "endpoint_index" => endpoint_index
                            );
                            continue
                        }
                        DecodedPayload::RecordBatch(record_batch) => record_batch,
                    };
                    let block_ranges = Metadata::parse(&app_metadata)?.ranges;

                    trace!(logger, "Received a new record batch";
                        "endpoint_index" => endpoint_index,
                        "batch_index" => batch_index,
                        "num_rows" => record_batch.num_rows(),
                        "memory_size_bytes" => record_batch.get_array_memory_size(),
                        "block_ranges" => ?block_ranges
                    );

                    if let Some(reorg) = detect_reorg(&block_ranges, &prev_block_ranges) {
                        yield ResponseBatch::Reorg(reorg);
                    }

                    yield ResponseBatch::Batch { data: record_batch };

                    batch_index += 1;
                    prev_block_ranges = block_ranges;
                }

                debug!(logger, "Query execution completed successfully";
                    "batch_count" => batch_index
                );
            }
        }
        .boxed()
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid metadata: {0:#}")]
    InvalidMetadata(#[source] anyhow::Error),

    #[error("connection failed: {0:#}")]
    Connection(#[source] tonic::transport::Error),

    #[error("service failed: {0:#}")]
    Service(#[source] ArrowError),

    #[error("stream failed: {0:#}")]
    Stream(#[source] FlightError),
}

impl error::IsDeterministic for Error {
    fn is_deterministic(&self) -> bool {
        let msg = match self {
            Self::InvalidMetadata(_) => return true,
            Self::Connection(_) => return false,
            Self::Service(e) => e.to_string(),
            Self::Stream(_) => return false,
        };

        static DETERMINISTIC_ERROR_PATTERNS: &[&str] = &[
            // Example SQL query: SELECT;
            r#"code: InvalidArgument, message: ""#,
            // Example SQL query: SELECT * FROM invalid_dataset;
            //                    SELECT * FROM valid_dataset.invalid_table;
            r#"code: Internal, message: "error creating planning context: "#,
            // Example SQL query: SELECT invalid_column FROM valid_dataset.valid_table;
            r#"code: Internal, message: "planning error: "#,
        ];

        for &pattern in DETERMINISTIC_ERROR_PATTERNS {
            if msg.contains(pattern) {
                return true;
            }
        }

        false
    }
}

/// Metadata received with every record batch.
#[derive(Debug, Clone, Deserialize)]
struct Metadata {
    /// Block ranges processed by the Amp server to produce the record batch.
    ranges: Vec<BlockRange>,
}

impl Metadata {
    /// Parses and returns the metadata.
    fn parse(app_metadata: &[u8]) -> Result<Self, Error> {
        if app_metadata.is_empty() {
            return Ok(Self { ranges: Vec::new() });
        }

        serde_json::from_slice::<Self>(app_metadata).map_err(|e| Error::InvalidMetadata(e.into()))
    }
}

/// Block range processed by the Amp server to produce a record batch.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct BlockRange {
    /// Network that contains the source data for the dataset.
    network: String,

    /// Block numbers processed.
    numbers: RangeInclusive<BlockNumber>,

    /// Hash of the last block in the block range.
    hash: BlockHash,

    /// Hash of the parent block of the first block in the block range.
    prev_hash: Option<BlockHash>,
}

impl BlockRange {
    /// Returns the first block number in the range.
    fn start(&self) -> BlockNumber {
        *self.numbers.start()
    }

    /// Returns the last block number in the range.
    fn end(&self) -> BlockNumber {
        *self.numbers.end()
    }
}

impl From<ResumeStreamingQuery> for BlockRange {
    fn from(resume: ResumeStreamingQuery) -> Self {
        Self {
            network: resume.network,
            numbers: resume.block_number..=resume.block_number,
            hash: resume.block_hash,
            prev_hash: None,
        }
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

/// Serializes the information required to resume a streaming SQL query to JSON.
fn serialize_resume_streaming_query(resume_streaming_query: Vec<ResumeStreamingQuery>) -> String {
    #[derive(Serialize)]
    struct Block {
        number: BlockNumber,
        hash: BlockHash,
    }

    let mapping: HashMap<String, Block> = resume_streaming_query
        .into_iter()
        .map(
            |ResumeStreamingQuery {
                 network,
                 block_number: number,
                 block_hash: hash,
             }| { (network, Block { number, hash }) },
        )
        .collect();

    serde_json::to_string(&mapping).unwrap()
}

/// Detects whether a reorg occurred during query execution.
///
/// Compares current block ranges with block ranges from the previous record batch
/// to detect non-incremental batches. When a non-incremental batch is detected,
/// returns the block number and hash of the parent block of the first block
/// after reorg for every processed network.
///
/// Returns `None` when no reorgs are detected.
fn detect_reorg(
    block_ranges: &[BlockRange],
    prev_block_ranges: &[BlockRange],
) -> Option<Vec<LatestBlockBeforeReorg>> {
    Some(
        block_ranges
            .iter()
            .filter_map(|block_range| {
                let prev_block_range = prev_block_ranges
                    .iter()
                    .find(|prev_block_range| prev_block_range.network == block_range.network)?;

                if block_range != prev_block_range && block_range.start() <= prev_block_range.end()
                {
                    return Some(LatestBlockBeforeReorg {
                        network: block_range.network.clone(),
                        block_number: block_range.start().checked_sub(1),
                        block_hash: block_range.prev_hash,
                    });
                }

                None
            })
            .collect::<Vec<_>>(),
    )
    .filter(|v| !v.is_empty())
}
