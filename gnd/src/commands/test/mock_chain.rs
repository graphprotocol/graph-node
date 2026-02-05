//! Mock blockchain helpers for test block streams.
//!
//! Provides utility functions for working with mock block pointers.
//! The actual block stream infrastructure (StaticStream, StaticStreamBuilder)
//! lives in `runner.rs` since it's tightly coupled to the test execution flow.

use graph::blockchain::block_stream::BlockWithTriggers;
use graph::prelude::alloy::primitives::B256;
use graph::prelude::BlockPtr;
use graph_chain_ethereum::Chain;

/// Get the final block pointer from a list of mock blocks.
///
/// Used as the "stop block" target — the indexer will process blocks
/// until it reaches this pointer, at which point we know all test
/// data has been indexed and we can run assertions.
pub fn final_block_ptr(blocks: &[BlockWithTriggers<Chain>]) -> Option<BlockPtr> {
    blocks.last().map(|b| b.ptr())
}

/// Get a genesis block pointer (block 0 with zero hash).
///
/// Used as a fallback stop block for test files with no blocks,
/// so the indexer has a valid target to sync to.
pub fn genesis_ptr() -> BlockPtr {
    BlockPtr {
        hash: B256::ZERO.into(),
        number: 0,
    }
}
