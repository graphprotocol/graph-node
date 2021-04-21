use super::{BlockPtr, Blockchain};
use anyhow::Error;
use async_trait::async_trait;
use futures03::Stream;

pub struct BlockWithTriggers<C: Blockchain> {
    pub block: Box<C::Block>,
    pub trigger_data: Vec<C::TriggerData>,
}

pub enum ScanTriggersError {
    // The chain base could not be found. It should be reverted.
    ChainBaseNotFound,
    Unknown(Error),
}

#[async_trait]
pub trait TriggersAdapter<C: Blockchain> {
    // Returns a sequence of blocks in increasing order of block number.
    // Each block will include all of its triggers that match the given `filter`.
    // The sequence may omit blocks that contain no triggers,
    // but all returned blocks must part of a same chain starting at `chain_base`.
    // At least one block will be returned, even if it contains no triggers.
    // `step_size` is the suggested number blocks to be scanned.
    async fn scan_triggers(
        &self,
        chain_base: BlockPtr,
        step_size: u32,
        filter: C::TriggerFilter,
    ) -> Result<Vec<BlockWithTriggers<C>>, ScanTriggersError>;

    // Used for reprocessing blocks when creating a data source.
    async fn triggers_in_block(
        &self,
        block: C::Block,
        filter: C::TriggerFilter,
    ) -> Result<BlockWithTriggers<C>, Error>;
}

pub enum BlockStreamEvent<C: Blockchain> {
    RevertTo(BlockPtr),
    ProcessBlock(BlockWithTriggers<C>),
}

pub trait BlockStream<C: Blockchain>: Stream<Item = Result<BlockStreamEvent<C>, Error>> {}
