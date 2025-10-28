pub mod flight_client;

use std::error::Error;

use alloy::primitives::{BlockHash, BlockNumber};
use arrow::{array::RecordBatch, datatypes::Schema};
use futures03::{future::BoxFuture, stream::BoxStream};
use slog::Logger;

use crate::nozzle::error;

/// Client for connecting to Nozzle core and executing SQL queries.
pub trait Client {
    type Error: Error + error::IsDeterministic + Send + Sync + 'static;

    /// Executes a SQL query and returns the corresponding schema.
    fn schema(
        &self,
        logger: &Logger,
        query: impl ToString,
    ) -> BoxFuture<'static, Result<Schema, Self::Error>>;

    /// Executes a SQL query and streams the requested data in batches.
    fn query(
        &self,
        logger: &Logger,
        query: impl ToString,
        request_metadata: Option<RequestMetadata>,
    ) -> BoxStream<'static, Result<ResponseBatch, Self::Error>>;
}

/// Metadata sent to the Nozzle server with the SQL query.
#[derive(Debug, Clone)]
pub struct RequestMetadata {
    /// Allows resuming streaming SQL queries from any block.
    pub resume_streaming_query: Option<Vec<ResumeStreamingQuery>>,
}

/// Resumes a streaming SQL query from the specified block.
#[derive(Debug, Clone)]
pub struct ResumeStreamingQuery {
    /// Network that contains the source data for the dataset.
    pub network: String,

    /// Block number after which the SQL query should resume.
    ///
    /// An invalid block number triggers a reorg message.
    pub block_number: BlockNumber,

    /// Block hash of the block after which the SQL query should resume.
    ///
    /// An invalid block hash triggers a reorg message.
    pub block_hash: BlockHash,
}

/// Represents a batch response resulting from query execution on the Nozzle server.
#[derive(Debug, Clone)]
pub enum ResponseBatch {
    /// Contains the batch data received from the Nozzle server.
    Batch { data: RecordBatch },

    /// Contains the reorg message received from the Nozzle server.
    ///
    /// It is received before the record batch that contains the data after the reorg.
    Reorg(Vec<LatestBlockBeforeReorg>),
}

/// Represents the parent block of the first block after the reorg.
#[derive(Debug, Clone)]
pub struct LatestBlockBeforeReorg {
    /// Network that contains the source data for the dataset.
    pub network: String,

    /// Block number of the parent block of the first block after the reorg.
    ///
    /// It is `None` when the reorg affects every block in the blockchain.
    pub block_number: Option<BlockNumber>,

    /// Block hash of the parent block of the first block after the reorg.
    ///
    /// It is `None` when the reorg affects every block in the blockchain.
    pub block_hash: Option<BlockHash>,
}
