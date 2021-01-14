use graph::prelude::*;
use std::fmt;
use std::ops::Deref;
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

impl From<Block<H256>> for Ommer {
    fn from(block: Block<H256>) -> Self {
        Self(block)
    }
}

impl From<LightEthereumBlock> for Ommer {
    fn from(block: LightEthereumBlock) -> Self {
        Self(Block {
            hash: block.hash,
            parent_hash: block.parent_hash,
            uncles_hash: block.uncles_hash,
            author: block.author,
            state_root: block.state_root,
            transactions_root: block.transactions_root,
            receipts_root: block.receipts_root,
            number: block.number,
            gas_used: block.gas_used,
            gas_limit: block.gas_limit,
            extra_data: block.extra_data,
            logs_bloom: block.logs_bloom,
            timestamp: block.timestamp,
            difficulty: block.difficulty,
            total_difficulty: block.total_difficulty,
            seal_fields: block.seal_fields,
            uncles: block.uncles,
            transactions: block.transactions.into_iter().map(|tx| tx.hash).collect(),
            size: block.size,
            mix_hash: block.mix_hash,
            nonce: block.nonce,
        })
    }
}

impl Deref for Ommer {
    type Target = Block<H256>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

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

pub trait NetworkStore: SubgraphStore + ChainStore {}

impl<S: SubgraphStore + ChainStore> NetworkStore for S {}
