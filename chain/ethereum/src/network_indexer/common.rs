use graph::prelude::*;
use web3::types::{Block, TransactionReceipt, H256};

/// Helper type to bundle blocks and their uncles together.
pub struct BlockWithUncles {
    pub block: EthereumBlock,
    pub uncles: Vec<Option<Block<H256>>>,
}

impl BlockWithUncles {
    pub fn inner(&self) -> &LightEthereumBlock {
        &self.block.block
    }

    pub fn _transaction_receipts(&self) -> &Vec<TransactionReceipt> {
        &self.block.transaction_receipts
    }
}
