#[path = "protobuf/fig.tendermint.codec.v1.rs"]
mod pbcodec;

pub use pbcodec::*;

use graph::blockchain::Block as BlockchainBlock;
use graph::{blockchain::BlockPtr, prelude::BlockNumber};

use std::convert::TryFrom;

impl EventList {
    pub fn block(&self) -> &EventBlock {
        self.new_block.as_ref().unwrap()
    }

    pub fn block_id(&self) -> &BlockId {
        self.block().block_id()
    }

    pub fn header(&self) -> &Header {
        self.block().header()
    }

    pub fn events(&self) -> impl Iterator<Item = &Event> {
        self.begin_block_events()
            .chain(self.tx_events())
            .chain(self.end_block_events())
    }

    pub fn begin_block_events(&self) -> impl Iterator<Item = &Event> {
        self.block()
            .result_begin_block
            .as_ref()
            .unwrap()
            .events
            .iter()
    }

    pub fn tx_events(&self) -> impl Iterator<Item = &Event> {
        self.transaction.iter().flat_map(|tx| {
            tx.tx_result
                .as_ref()
                .unwrap()
                .result
                .as_ref()
                .unwrap()
                .events
                .iter()
        })
    }

    pub fn end_block_events(&self) -> impl Iterator<Item = &Event> {
        self.block()
            .result_end_block
            .as_ref()
            .unwrap()
            .events
            .iter()
    }

    pub fn parent_ptr(&self) -> Option<BlockPtr> {
        self.header().last_block_id.as_ref().map(|last_block_id| {
            BlockPtr::from((last_block_id.hash.clone(), self.header().height - 1))
        })
    }
}

impl From<EventList> for BlockPtr {
    fn from(b: EventList) -> BlockPtr {
        (&b).into()
    }
}

impl<'a> From<&'a EventList> for BlockPtr {
    fn from(b: &'a EventList) -> BlockPtr {
        BlockPtr::from((b.block_id().hash.clone(), b.header().height))
    }
}

impl BlockchainBlock for EventList {
    fn number(&self) -> i32 {
        BlockNumber::try_from(self.header().height).unwrap()
    }

    fn ptr(&self) -> BlockPtr {
        self.into()
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        self.parent_ptr()
    }
}

impl EventData {
    pub fn event(&self) -> &Event {
        self.event.as_ref().unwrap()
    }

    pub fn block(&self) -> &EventBlock {
        self.block.as_ref().unwrap()
    }
}

impl EventBlock {
    pub fn block_id(&self) -> &BlockId {
        self.block_id.as_ref().unwrap()
    }

    pub fn header(&self) -> &Header {
        self.block.as_ref().unwrap().header.as_ref().unwrap()
    }

    pub fn parent_ptr(&self) -> Option<BlockPtr> {
        self.header().last_block_id.as_ref().map(|last_block_id| {
            BlockPtr::from((last_block_id.hash.clone(), self.header().height - 1))
        })
    }
}

impl From<EventBlock> for BlockPtr {
    fn from(b: EventBlock) -> BlockPtr {
        (&b).into()
    }
}

impl<'a> From<&'a EventBlock> for BlockPtr {
    fn from(b: &'a EventBlock) -> BlockPtr {
        BlockPtr::from((b.block_id().hash.clone(), b.header().height))
    }
}

impl BlockchainBlock for EventBlock {
    fn number(&self) -> i32 {
        BlockNumber::try_from(self.header().height).unwrap()
    }

    fn ptr(&self) -> BlockPtr {
        self.into()
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        self.parent_ptr()
    }
}
