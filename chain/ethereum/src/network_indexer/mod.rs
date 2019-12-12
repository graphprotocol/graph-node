use graph::prelude::*;
use std::fmt;
use web3::types::{Block, H256};

mod block_writer;
mod convert;
mod metrics;
mod network_indexer;
mod subgraph;

pub use self::block_writer::*;
pub use self::convert::*;
pub use self::network_indexer::*;
pub use self::subgraph::*;

pub use self::network_indexer::NetworkIndexerEvent;

const NETWORK_INDEXER_VERSION: u32 = 0;

/// Helper type to represent ommer blocks.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Ommer(Block<H256>);

/// Helper type to bundle blocks and their ommers together.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BlockWithOmmers {
    pub block: EthereumBlock,
    pub ommers: Vec<Ommer>,
}

impl BlockWithOmmers {
    pub fn inner(&self) -> &LightEthereumBlock {
        &self.block.block
    }
}

impl fmt::Display for BlockWithOmmers {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.inner().format())
    }
}

pub trait NetworkStore: Store + ChainStore {}

impl<S: Store + ChainStore> NetworkStore for S {}
