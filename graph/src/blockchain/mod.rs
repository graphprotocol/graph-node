//! The `blockchain` module exports the necessary traits and data structures to integrate a
//! blockchain into Graph Node. A blockchain is represented by an implementation of the `Blockchain`
//! trait which is the centerpiece of this module.

pub mod block_stream;

// Try to reexport most of the necessary types
use crate::prelude::{BlockPtr, CheapClone, DeploymentHash, LinkResolver};
use crate::runtime::AscType;
use anyhow::Error;
use async_trait::async_trait;
use slog;
use slog::Logger;
use std::fmt;
use std::sync::Arc;
use web3::types::H256;

use block_stream::{BlockStream, TriggersAdapter};

/// A simple marker for byte arrays that are really block hashes
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct BlockHash(pub Box<[u8]>);

impl fmt::Display for BlockHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "0x{}", hex::encode(&self.0))
    }
}

impl CheapClone for BlockHash {}

impl From<H256> for BlockHash {
    fn from(hash: H256) -> Self {
        BlockHash(hash.as_bytes().into())
    }
}

impl From<Vec<u8>> for BlockHash {
    fn from(bytes: Vec<u8>) -> Self {
        BlockHash(bytes.as_slice().into())
    }
}

pub trait Block {
    fn ptr(&self) -> BlockPtr;
    fn parent_ptr(&self) -> Option<BlockPtr>;

    fn number(&self) -> i32 {
        self.ptr().number
    }

    fn hash(&self) -> BlockHash {
        self.ptr().hash.clone()
    }
}

pub trait Blockchain: Sized + Send + Sync + 'static {
    type Block: Block;
    type DataSource: DataSource<Self>;
    type DataSourceTemplate;
    type Manifest: Manifest<Self>;

    type TriggersAdapter: TriggersAdapter<Self>;
    type BlockStream: BlockStream<Self>;

    /// Trigger data as parsed from the triggers adapter.
    type TriggerData;

    /// Decoded trigger ready to be processed by the mapping.
    type MappingTrigger: AscType;

    /// Trigger filter used as input to the triggers adapter.
    type TriggerFilter: TriggerFilter<Self>;

    type NodeCapabilities;

    // type IngestorAdapter: IngestorAdapter<Self>;
    // type RuntimeAdapter: RuntimeAdapter;
    // ...WIP

    fn reorg_threshold() -> u32;
    fn triggers_adapter(
        &self,
        network: &str,
        capabilities: Self::NodeCapabilities,
    ) -> Arc<Self::TriggersAdapter>;

    fn new_block_stream(
        &self,
        current_head: BlockPtr,
        filter: Self::TriggerFilter,
    ) -> Result<Self::BlockStream, Error>;
}

pub trait TriggerFilter<C: Blockchain>: Default {
    fn from_data_sources<'a>(data_sources: impl Iterator<Item = &'a C::DataSource>) -> Self {
        let mut this = Self::default();
        this.extend(data_sources);
        this
    }

    fn extend<'a>(&mut self, data_sources: impl Iterator<Item = &'a C::DataSource>);
}

pub trait DataSource<C: Blockchain>: 'static {
    /// Checks if `trigger` matches this data source, and if so decodes it into a `MappingTrigger`.
    /// A return of `Ok(None)` mean the trigger does not match.
    fn match_and_decode(
        &self,
        trigger: &C::TriggerData,
        block: &C::Block,
        logger: &Logger,
    ) -> Result<Option<C::MappingTrigger>, Error>;
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
