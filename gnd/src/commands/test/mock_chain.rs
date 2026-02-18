//! Block pointer utilities for mock chains.

use graph::blockchain::block_stream::BlockWithTriggers;
use graph::prelude::alloy::primitives::B256;
use graph::prelude::BlockPtr;
use graph_chain_ethereum::Chain;

/// Last block pointer — used as the indexer's stop target.
pub fn final_block_ptr(blocks: &[BlockWithTriggers<Chain>]) -> Option<BlockPtr> {
    blocks.last().map(|b| b.ptr())
}

/// Genesis block (block 0, zero hash) — used as stop target when there are no blocks.
pub fn genesis_ptr() -> BlockPtr {
    BlockPtr {
        hash: B256::ZERO.into(),
        number: 0,
    }
}
