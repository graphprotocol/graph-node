//! The `blockchain` module exports the necessary traits and data structures to integrate a
//! blockchain into Graph Node. A blockchain is represented by an implementation of the `Blockchain`
//! trait which is the centerpiece of this module.

pub mod block_stream;
pub mod firehose_block_ingestor;
pub mod firehose_block_stream;
pub mod mock;
pub mod polling_block_stream;
mod types;

// Try to reexport most of the necessary types
use crate::{
    cheap_clone::CheapClone,
    components::store::{DeploymentLocator, StoredDynamicDataSource},
    data::subgraph::UnifiedMappingApiVersion,
    prelude::DataSourceContext,
    runtime::{gas::GasCounter, AscHeap, AscPtr, DeterministicHostError, HostExportError},
};
use crate::{
    components::{
        store::{BlockNumber, ChainStore},
        subgraph::DataSourceTemplateInfo,
    },
    prelude::{thiserror::Error, LinkResolver},
};
use anyhow::{anyhow, Context, Error};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use slog::Logger;
use slog::{self, SendSyncRefUnwindSafeKV};
use std::{
    any::Any,
    collections::{BTreeMap, HashMap},
    convert::TryFrom,
    fmt::{self, Debug},
    str::FromStr,
    sync::Arc,
};
use web3::types::H256;

pub use block_stream::{ChainHeadUpdateListener, ChainHeadUpdateStream, TriggersAdapter};
pub use types::{BlockHash, BlockPtr, ChainIdentifier};

use self::block_stream::BlockStream;

pub trait TriggersAdapterSelector<C: Blockchain>: Sync + Send {
    fn triggers_adapter(
        &self,
        loc: &DeploymentLocator,
        capabilities: &C::NodeCapabilities,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Arc<dyn TriggersAdapter<C>>, Error>;
}

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

    /// The data that should be stored for this block in the `ChainStore`
    fn data(&self) -> Result<serde_json::Value, serde_json::Error> {
        Ok(serde_json::Value::Null)
    }
}

#[async_trait]
// This is only `Debug` because some tests require that
pub trait Blockchain: Debug + Sized + Send + Sync + Unpin + 'static {
    const KIND: BlockchainKind;

    // The `Clone` bound is used when reprocessing a block, because `triggers_in_block` requires an
    // owned `Block`. It would be good to come up with a way to remove this bound.
    type Block: Block + Clone + Debug;
    type DataSource: DataSource<Self>;
    type UnresolvedDataSource: UnresolvedDataSource<Self>;

    type DataSourceTemplate: DataSourceTemplate<Self> + Clone;
    type UnresolvedDataSourceTemplate: UnresolvedDataSourceTemplate<Self>;

    /// Trigger data as parsed from the triggers adapter.
    type TriggerData: TriggerData + Ord + Send + Sync + Debug;

    /// Decoded trigger ready to be processed by the mapping.
    /// New implementations should have this be the same as `TriggerData`.
    type MappingTrigger: MappingTrigger + Debug;

    /// Trigger filter used as input to the triggers adapter.
    type TriggerFilter: TriggerFilter<Self>;

    type NodeCapabilities: NodeCapabilities<Self> + std::fmt::Display;

    fn triggers_adapter(
        &self,
        log: &DeploymentLocator,
        capabilities: &Self::NodeCapabilities,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Arc<dyn TriggersAdapter<Self>>, Error>;

    async fn new_firehose_block_stream(
        &self,
        deployment: DeploymentLocator,
        block_cursor: Option<String>,
        start_blocks: Vec<BlockNumber>,
        subgraph_current_block: Option<BlockPtr>,
        filter: Arc<Self::TriggerFilter>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Self>>, Error>;

    async fn new_polling_block_stream(
        &self,
        deployment: DeploymentLocator,
        start_blocks: Vec<BlockNumber>,
        subgraph_current_block: Option<BlockPtr>,
        filter: Arc<Self::TriggerFilter>,
        unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Self>>, Error>;

    fn chain_store(&self) -> Arc<dyn ChainStore>;

    async fn block_pointer_from_number(
        &self,
        logger: &Logger,
        number: BlockNumber,
    ) -> Result<BlockPtr, IngestorError>;

    fn runtime_adapter(&self) -> Arc<dyn RuntimeAdapter<Self>>;

    fn is_firehose_supported(&self) -> bool;
}

#[derive(Error, Debug)]
pub enum IngestorError {
    /// The Ethereum node does not know about this block for some reason, probably because it
    /// disappeared in a chain reorg.
    #[error("Block data unavailable, block was likely uncled (block hash = {0:?})")]
    BlockUnavailable(H256),

    /// The Ethereum node does not know about this block for some reason, probably because it
    /// disappeared in a chain reorg.
    #[error("Receipt for tx {1:?} unavailable, block was likely uncled (block hash = {0:?})")]
    ReceiptUnavailable(H256, H256),

    /// An unexpected error occurred.
    #[error("Ingestor error: {0:#}")]
    Unknown(Error),
}

impl From<Error> for IngestorError {
    fn from(e: Error) -> Self {
        IngestorError::Unknown(e)
    }
}

impl From<web3::Error> for IngestorError {
    fn from(e: web3::Error) -> Self {
        IngestorError::Unknown(anyhow::anyhow!(e))
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

    fn extend_with_template(&mut self, data_source: impl Iterator<Item = C::DataSourceTemplate>);

    fn extend<'a>(&mut self, data_sources: impl Iterator<Item = &'a C::DataSource> + Clone);

    fn node_capabilities(&self) -> C::NodeCapabilities;

    fn to_firehose_filter(self) -> Vec<prost_types::Any>;
}

pub trait DataSource<C: Blockchain>:
    'static + Sized + Send + Sync + Clone + TryFrom<DataSourceTemplateInfo<C>, Error = anyhow::Error>
{
    fn address(&self) -> Option<&[u8]>;
    fn start_block(&self) -> BlockNumber;
    fn name(&self) -> &str;
    fn kind(&self) -> &str;
    fn network(&self) -> Option<&str>;
    fn context(&self) -> Arc<Option<DataSourceContext>>;
    fn creation_block(&self) -> Option<BlockNumber>;
    fn api_version(&self) -> semver::Version;
    fn runtime(&self) -> &[u8];

    /// Checks if `trigger` matches this data source, and if so decodes it into a `MappingTrigger`.
    /// A return of `Ok(None)` mean the trigger does not match.
    ///
    /// Performance note: This is very hot code, because in the worst case it could be called a
    /// quadratic T*D times where T is the total number of triggers in the chain and D is the number
    /// of data sources in the subgraph. So it could be called billions, or even trillions, of times
    /// in the sync time of a subgraph.
    ///
    /// This is typicaly reduced by the triggers being pre-filtered in the block stream. But with
    /// dynamic data sources the block stream does not filter on the dynamic parameters, so the
    /// matching should efficently discard false positives.
    fn match_and_decode(
        &self,
        trigger: &C::TriggerData,
        block: &Arc<C::Block>,
        logger: &Logger,
    ) -> Result<Option<TriggerWithHandler<C>>, Error>;

    fn is_duplicate_of(&self, other: &Self) -> bool;

    fn as_stored_dynamic_data_source(&self) -> StoredDynamicDataSource;

    fn from_stored_dynamic_data_source(
        templates: &BTreeMap<&str, &C::DataSourceTemplate>,
        stored: StoredDynamicDataSource,
    ) -> Result<Self, Error>;

    /// Used as part of manifest validation. If there are no errors, return an empty vector.
    fn validate(&self) -> Vec<Error>;
}

#[async_trait]
pub trait UnresolvedDataSourceTemplate<C: Blockchain>:
    'static + Sized + Send + Sync + DeserializeOwned + Default
{
    async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
    ) -> Result<C::DataSourceTemplate, anyhow::Error>;
}

pub trait DataSourceTemplate<C: Blockchain>: Send + Sync + Debug {
    fn api_version(&self) -> semver::Version;
    fn runtime(&self) -> &[u8];
    fn name(&self) -> &str;
}

#[async_trait]
pub trait UnresolvedDataSource<C: Blockchain>:
    'static + Sized + Send + Sync + DeserializeOwned
{
    async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
    ) -> Result<C::DataSource, anyhow::Error>;
}

pub trait TriggerData {
    /// If there is an error when processing this trigger, this will called to add relevant context.
    /// For example an useful return is: `"block #<N> (<hash>), transaction <tx_hash>".
    fn error_context(&self) -> String;
}

pub trait MappingTrigger: Send + Sync {
    /// A flexible interface for writing a type to AS memory, any pointer can be returned.
    /// Use `AscPtr::erased` to convert `AscPtr<T>` into `AscPtr<()>`.
    fn to_asc_ptr<H: AscHeap>(
        self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscPtr<()>, DeterministicHostError>;
}

pub struct HostFnCtx<'a> {
    pub logger: Logger,
    pub block_ptr: BlockPtr,
    pub heap: &'a mut dyn AscHeap,
    pub gas: GasCounter,
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

pub trait NodeCapabilities<C: Blockchain> {
    fn from_data_sources(data_sources: &[C::DataSource]) -> Self;
}

/// Blockchain technologies supported by Graph Node.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum BlockchainKind {
    /// Ethereum itself or chains that are compatible.
    Ethereum,

    /// NEAR chains (Mainnet, Testnet) or chains that are compatible
    Near,

    /// Tendermint chains including cosmoshub
    Tendermint,
}

impl fmt::Display for BlockchainKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let value = match self {
            BlockchainKind::Ethereum => "ethereum",
            BlockchainKind::Near => "near",
            BlockchainKind::Tendermint => "tendermint",
        };
        write!(f, "{}", value)
    }
}

impl FromStr for BlockchainKind {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ethereum" => Ok(BlockchainKind::Ethereum),
            "near" => Ok(BlockchainKind::Near),
            "tendermint" => Ok(BlockchainKind::Tendermint),
            _ => Err(anyhow!("unknown blockchain kind {}", s)),
        }
    }
}

impl BlockchainKind {
    pub fn from_manifest(manifest: &serde_yaml::Mapping) -> Result<Self, Error> {
        use serde_yaml::Value;

        // The `kind` field of the first data source in the manifest.
        //
        // Split by `/` to, for example, read 'ethereum' in 'ethereum/contracts'.
        manifest
            .get(&Value::String("dataSources".to_owned()))
            .and_then(|ds| ds.as_sequence())
            .and_then(|ds| ds.first())
            .and_then(|ds| ds.as_mapping())
            .and_then(|ds| ds.get(&Value::String("kind".to_owned())))
            .and_then(|kind| kind.as_str())
            .and_then(|kind| kind.split('/').next())
            .context("invalid manifest")
            .and_then(BlockchainKind::from_str)
    }
}

/// A collection of blockchains, keyed by `BlockchainKind` and network.
#[derive(Default, Debug, Clone)]
pub struct BlockchainMap(HashMap<(BlockchainKind, String), Arc<dyn Any + Send + Sync>>);

impl BlockchainMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert<C: Blockchain>(&mut self, network: String, chain: Arc<C>) {
        self.0.insert((C::KIND, network), chain);
    }

    pub fn get<C: Blockchain>(&self, network: String) -> Result<Arc<C>, Error> {
        self.0
            .get(&(C::KIND, network.clone()))
            .with_context(|| format!("no network {} found on chain {}", network, C::KIND))?
            .cheap_clone()
            .downcast()
            .map_err(|_| anyhow!("unable to downcast, wrong type for blockchain {}", C::KIND))
    }
}

pub struct TriggerWithHandler<C: Blockchain> {
    trigger: C::MappingTrigger,
    handler: String,
    logging_extras: Arc<dyn SendSyncRefUnwindSafeKV>,
}

impl<C: Blockchain> fmt::Debug for TriggerWithHandler<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("TriggerWithHandler");
        builder.field("trigger", &self.trigger);
        builder.field("handler", &self.handler);
        builder.finish()
    }
}

impl<C: Blockchain> TriggerWithHandler<C> {
    pub fn new(trigger: C::MappingTrigger, handler: String) -> Self {
        TriggerWithHandler {
            trigger,
            handler,
            logging_extras: Arc::new(slog::o! {}),
        }
    }

    pub fn new_with_logging_extras(
        trigger: C::MappingTrigger,
        handler: String,
        logging_extras: Arc<dyn SendSyncRefUnwindSafeKV>,
    ) -> Self {
        TriggerWithHandler {
            trigger,
            handler,
            logging_extras,
        }
    }

    /// Additional key-value pairs to be logged with the "Done processing trigger" message.
    pub fn logging_extras(&self) -> Arc<dyn SendSyncRefUnwindSafeKV> {
        self.logging_extras.cheap_clone()
    }

    pub fn handler_name(&self) -> &str {
        &self.handler
    }

    pub fn to_asc_ptr<H: AscHeap>(
        self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscPtr<()>, DeterministicHostError> {
        self.trigger.to_asc_ptr(heap, gas)
    }
}
