//! The `blockchain` module exports the necessary traits and data structures to integrate a
//! blockchain into Graph Node. A blockchain is represented by an implementation of the `Blockchain`
//! trait which is the centerpiece of this module.

pub mod block_ingestor;
pub mod block_stream;
mod types;

// Try to reexport most of the necessary types
use crate::{
    components::store::DeploymentLocator,
    data::subgraph::{Mapping, Source},
    prelude::DataSourceContext,
    runtime::AscType,
};
use crate::{
    components::store::{BlockNumber, ChainStore},
    prelude::{thiserror::Error, DeploymentHash, LinkResolver},
};
use anyhow::Error;
use async_trait::async_trait;
use slog;
use slog::Logger;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use web3::types::H256;

pub use block_stream::{
    BlockStream, ChainHeadUpdateListener, ChainHeadUpdateStream, TriggersAdapter,
};
pub use types::{BlockHash, BlockPtr};

use self::block_stream::BlockStreamMetrics;

pub trait Block: Send + Sync {
    fn ptr(&self) -> BlockPtr;
    fn parent_ptr(&self) -> Option<BlockPtr>;

    fn number(&self) -> i32 {
        self.ptr().number
    }

    fn hash(&self) -> BlockHash {
        self.ptr().hash
    }

    fn parent_hash(&self) -> Option<BlockHash> {
        self.parent_ptr().map(|ptr| ptr.hash)
    }
}

pub trait Blockchain: Sized + Send + Sync + 'static {
    type Block: Block;
    type DataSource: DataSource<C = Self>;
    type DataSourceTemplate;
    type Manifest: Manifest<Self>;

    type TriggersAdapter: TriggersAdapter<Self>;

    /// Trigger data as parsed from the triggers adapter.
    type TriggerData;

    /// Decoded trigger ready to be processed by the mapping.
    type MappingTrigger: AscType;

    /// Trigger filter used as input to the triggers adapter.
    type TriggerFilter: TriggerFilter<Self>;

    type NodeCapabilities: std::fmt::Display;

    type IngestorAdapter: IngestorAdapter<Self>;

    // type RuntimeAdapter: RuntimeAdapter;
    // ...WIP

    fn reorg_threshold() -> u32;

    fn triggers_adapter(
        &self,
        loc: &DeploymentLocator,
        capabilities: &Self::NodeCapabilities,
    ) -> Result<Arc<Self::TriggersAdapter>, Error>;

    fn new_block_stream(
        &self,
        deployment: DeploymentLocator,
        start_blocks: Vec<BlockNumber>,
        filter: Self::TriggerFilter,
        metrics: Arc<BlockStreamMetrics>,
    ) -> Result<BlockStream<Self>, Error>;

    fn ingestor_adapter(&self) -> Arc<Self::IngestorAdapter>;

    fn chain_store(&self) -> Arc<dyn ChainStore>;
}

pub type BlockchainMap<C> = HashMap<String, Arc<C>>;

#[derive(Error, Debug)]
pub enum IngestorError {
    /// The Ethereum node does not know about this block for some reason, probably because it
    /// disappeared in a chain reorg.
    #[error("Block data unavailable, block was likely uncled (block hash = {0:?})")]
    BlockUnavailable(H256),

    /// An unexpected error occurred.
    #[error("Ingestor error: {0}")]
    Unknown(Error),
}

impl From<Error> for IngestorError {
    fn from(e: Error) -> Self {
        IngestorError::Unknown(e)
    }
}

#[async_trait]
pub trait IngestorAdapter<C: Blockchain> {
    fn logger(&self) -> &Logger;

    /// How many ancestors of the current chain head to ingest. For chains
    /// that can experience reorgs, this should be large enough to cover all
    /// blocks that could be subject to reorgs to ensure that `graph-node`
    /// has enough blocks in its local cache to traverse a sidechain back to
    /// the main chain even if those blocks get removed from the network
    /// client.
    fn ancestor_count(&self) -> BlockNumber;

    /// Get the latest block from the chain
    async fn latest_block(&self) -> Result<BlockPtr, IngestorError>;

    /// Retrieve all necessary data for the block  `hash` from the chain and
    /// store it in the database
    async fn ingest_block(&self, hash: &BlockHash) -> Result<Option<BlockHash>, IngestorError>;

    /// Return the chain head that is stored locally, and therefore visible
    /// to the block streams of subgraphs
    fn chain_head_ptr(&self) -> Result<Option<BlockPtr>, Error>;

    /// Remove old blocks from the database cache and return a pair
    /// containing the number of the oldest block retained and the number of
    /// blocks deleted if anything was removed. This is generally only used
    /// in small test installations, and can remain a noop without
    /// influencing correctness.
    fn cleanup_cached_blocks(&self) -> Result<Option<(i32, usize)>, Error> {
        Ok(None)
    }
}

pub trait TriggerFilter<C: Blockchain>: Default + Clone + Send + Sync {
    fn from_data_sources<'a>(
        data_sources: impl Iterator<Item = &'a C::DataSource> + Clone,
    ) -> Self {
        let mut this = Self::default();
        this.extend(data_sources);
        this
    }

    fn extend<'a>(&mut self, data_sources: impl Iterator<Item = &'a C::DataSource> + Clone);

    fn node_capabilities(&self) -> C::NodeCapabilities;
}

// ETHDEP: `Source` and `Mapping`, at least, are Ethereum-specific.
pub trait DataSource: 'static + Sized + Send + Sync {
    type C: Blockchain;

    fn mapping(&self) -> &Mapping;
    fn source(&self) -> &Source;

    fn from_manifest(
        kind: String,
        network: Option<String>,
        name: String,
        source: Source,
        mapping: Mapping,
        context: Option<DataSourceContext>,
    ) -> Result<Self, Error>;

    fn name(&self) -> &str;
    fn kind(&self) -> &str;
    fn network(&self) -> Option<&str>;
    fn context(&self) -> Option<&DataSourceContext>;
    fn creation_block(&self) -> Option<BlockNumber>;

    /// Checks if `trigger` matches this data source, and if so decodes it into a `MappingTrigger`.
    /// A return of `Ok(None)` mean the trigger does not match.
    fn match_and_decode(
        &self,
        trigger: &<Self::C as Blockchain>::TriggerData,
        block: Arc<<Self::C as Blockchain>::Block>,
        logger: &Logger,
    ) -> Result<Option<<Self::C as Blockchain>::MappingTrigger>, Error>;

    fn is_duplicate_of(&self, other: &Self) -> bool;
}

#[async_trait]
pub trait Manifest<C: Blockchain>: Sized {
    async fn resolve_from_raw(
        id: DeploymentHash,
        raw: serde_yaml::Mapping,
        resolver: &impl LinkResolver,
        logger: &Logger,
    ) -> Result<Self, Error>;

    fn data_sources(&self) -> &[C::DataSource];
    fn templates(&self) -> &[C::DataSourceTemplate];
}
