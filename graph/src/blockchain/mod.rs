//! The `blockchain` module exports the necessary traits and data structures to integrate a
//! blockchain into Graph Node. A blockchain is represented by an implementation of the `Blockchain`
//! trait which is the centerpiece of this module.

pub mod block_ingestor;
pub mod block_stream;
mod types;

// Try to reexport most of the necessary types
use crate::{
    cheap_clone::CheapClone,
    components::store::{DeploymentLocator, StoredDynamicDataSource},
    data::subgraph::{Mapping, Source, TemplateSource},
    prelude::DataSourceContext,
    runtime::{AscHeap, AscPtr, DeterministicHostError, HostExportError},
};
use crate::{
    components::{
        store::{BlockNumber, ChainStore},
        subgraph::DataSourceTemplateInfo,
    },
    prelude::{thiserror::Error, DeploymentHash, LinkResolver},
};
use anyhow::Error;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use slog::Logger;
use slog::{self, SendSyncRefUnwindSafeKV};
use std::fmt::Debug;
use std::sync::Arc;
use std::{collections::HashMap, convert::TryFrom};
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

#[async_trait]
// This is only `Debug` because some tests require that
pub trait Blockchain: Debug + Sized + Send + Sync + 'static {
    // The `Clone` bound is used when reprocessing a block, because `triggers_in_block` requires an
    // owned `Block`. It would be good to come up with a way to remove this bound.
    type Block: Block + Clone;
    type DataSource: DataSource<Self>;
    type UnresolvedDataSource: UnresolvedDataSource<Self>;

    type DataSourceTemplate: DataSourceTemplate<Self>;
    type UnresolvedDataSourceTemplate: UnresolvedDataSourceTemplate<Self>;

    type Manifest: Manifest<Self>;

    type TriggersAdapter: TriggersAdapter<Self>;

    /// Trigger data as parsed from the triggers adapter.
    type TriggerData: TriggerData + Ord;

    /// Decoded trigger ready to be processed by the mapping.
    type MappingTrigger: MappingTrigger + Debug;

    /// Trigger filter used as input to the triggers adapter.
    type TriggerFilter: TriggerFilter<Self>;

    type NodeCapabilities: std::fmt::Display;

    type IngestorAdapter: IngestorAdapter<Self>;

    type RuntimeAdapter: RuntimeAdapter<Self>;

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

    async fn block_pointer_from_number(
        &self,
        logger: &Logger,
        number: BlockNumber,
    ) -> Result<BlockPtr, IngestorError>;

    fn runtime_adapter(&self) -> Arc<Self::RuntimeAdapter>;
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
pub trait DataSource<C: Blockchain>:
    'static + Sized + Send + Sync + TryFrom<DataSourceTemplateInfo<C>, Error = anyhow::Error>
{
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
    fn context(&self) -> Arc<Option<DataSourceContext>>;
    fn creation_block(&self) -> Option<BlockNumber>;

    /// Checks if `trigger` matches this data source, and if so decodes it into a `MappingTrigger`.
    /// A return of `Ok(None)` mean the trigger does not match.
    fn match_and_decode(
        &self,
        trigger: &C::TriggerData,
        block: Arc<C::Block>,
        logger: &Logger,
    ) -> Result<Option<C::MappingTrigger>, Error>;

    fn is_duplicate_of(&self, other: &Self) -> bool;

    fn as_stored_dynamic_data_source(&self) -> StoredDynamicDataSource;
}

#[async_trait]
pub trait UnresolvedDataSourceTemplate<C: Blockchain>:
    'static + Sized + Send + Sync + DeserializeOwned + Default
{
    async fn resolve(
        self,
        resolver: &impl LinkResolver,
        logger: &Logger,
    ) -> Result<C::DataSourceTemplate, anyhow::Error>;
}

pub trait DataSourceTemplate<C: Blockchain>: Send + Sync + Clone + Debug {
    fn mapping(&self) -> &Mapping;
    fn name(&self) -> &str;
    fn source(&self) -> &TemplateSource;
    fn kind(&self) -> &str;
    fn network(&self) -> Option<&str>;
}

#[async_trait]
pub trait UnresolvedDataSource<C: Blockchain>:
    'static + Sized + Send + Sync + DeserializeOwned
{
    async fn resolve(
        self,
        resolver: &impl LinkResolver,
        logger: &Logger,
    ) -> Result<C::DataSource, anyhow::Error>;
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

pub trait TriggerData {
    /// If there is an error when processing this trigger, this will called to add relevant context.
    /// For example an useful return is: `"block #<N> (<hash>), transaction <tx_hash>".
    fn error_context(&self) -> String;
}

pub trait MappingTrigger: Send + Sync {
    fn handler_name(&self) -> &str;

    /// A flexible interface for writing a type to AS memory, any pointer can be returned.
    /// Use `AscPtr::erased` to convert `AscPtr<T>` into `AscPtr<()>`.
    fn to_asc<H: AscHeap>(self, heap: &mut H) -> Result<AscPtr<()>, DeterministicHostError>;

    /// Additional key-value pairs to be logged with the "Done processing trigger" message.
    fn logging_extras(&self) -> Box<dyn SendSyncRefUnwindSafeKV> {
        Box::new(slog::o! {})
    }
}

pub struct HostFnCtx<'a> {
    pub logger: Logger,
    pub block_ptr: BlockPtr,
    pub heap: &'a mut dyn AscHeap,
}

/// Host fn that receives one u32 argument and returns an u32.
/// The name for an AS fuction is in the format `<namespace>.<function>`.
#[derive(Clone)]
pub struct HostFn {
    pub name: &'static str,
    pub func: Arc<dyn Send + Sync + Fn(HostFnCtx, u32) -> Result<u32, HostExportError>>,
}

impl CheapClone for HostFn {
    fn cheap_clone(&self) -> Self {
        HostFn {
            name: self.name,
            func: self.func.cheap_clone(),
        }
    }
}

pub trait RuntimeAdapter<C: Blockchain>: Send + Sync {
    fn host_fns(&self, ds: &C::DataSource) -> Result<Vec<HostFn>, Error>;
}
