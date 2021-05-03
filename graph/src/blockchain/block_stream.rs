use crate::components::store::BlockNumber;

use super::{Block, BlockPtr, Blockchain};
use anyhow::Error;
use async_trait::async_trait;
use futures03::Stream;

pub struct BlockWithTriggers<C: Blockchain> {
    pub block: Box<C::Block>,
    pub trigger_data: Vec<C::TriggerData>,
}

impl<C: Blockchain> BlockWithTriggers<C> {
    pub fn new(block: Box<C::Block>, trigger_data: Vec<C::TriggerData>) -> Self {
        Self {
            block,
            trigger_data,
        }
    }

    pub fn trigger_count(&self) -> usize {
        self.trigger_data.len()
    }

    pub fn ptr(&self) -> BlockPtr {
        self.block.ptr()
    }
}

pub enum ScanTriggersError {
    // The chain base could not be found. It should be reverted.
    ChainBaseNotFound,
    Unknown(Error),
}

#[async_trait]
pub trait TriggersAdapter<C: Blockchain>: Send + Sync {
    // Return the block that is `offset` blocks before the block pointed to
    // by `ptr` from the local cache. An offset of 0 means the block itself,
    // an offset of 1 means the block's parent etc. If the block is not in
    // the local cache, return `None`
    fn ancestor_block(&self, ptr: BlockPtr, offset: BlockNumber)
        -> Result<Option<C::Block>, Error>;

    // Returns a sequence of blocks in increasing order of block number.
    // Each block will include all of its triggers that match the given `filter`.
    // The sequence may omit blocks that contain no triggers,
    // but all returned blocks must part of a same chain starting at `chain_base`.
    // At least one block will be returned, even if it contains no triggers.
    // `step_size` is the suggested number blocks to be scanned.
    async fn scan_triggers(
        &self,
        from: BlockNumber,
        to: BlockNumber,
        filter: C::TriggerFilter,
    ) -> Result<Vec<BlockWithTriggers<C>>, Error>;

    // Used for reprocessing blocks when creating a data source.
    async fn triggers_in_block(
        &self,
        block: C::Block,
        filter: C::TriggerFilter,
    ) -> Result<BlockWithTriggers<C>, Error>;

    /// Return `true` if the block with the given hash and number is on the
    /// main chain, i.e., the chain going back from the current chain head.
    async fn is_on_main_chain(&self, ptr: BlockPtr) -> Result<bool, Error>;
}

pub enum BlockStreamEvent<C: Blockchain> {
    // ETHDEP: The meaning of the ptr needs to be clarified. Right now, it
    // has the same meaning as the pointer in `NextBlocks::Revert`, and it's
    // not clear whether that pointer should become the new subgraph head
    // pointer, or if we should revert that block, and make the block's
    // parent the new subgraph head. To not risk introducing bugs, for now,
    // we take it to mean whatever `NextBlocks::Revert` means
    Revert(BlockPtr),
    ProcessBlock(BlockWithTriggers<C>),
}

pub trait BlockStream<C: Blockchain>: Stream<Item = Result<BlockStreamEvent<C>, Error>> {}
