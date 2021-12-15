#[path = "protobuf/fig.tendermint.codec.v1.rs"]
mod pbcodec;

pub use pbcodec::*;

use graph::blockchain::Block as BBlock;
use graph::{blockchain::BlockPtr, prelude::BlockNumber};

use std::convert::TryFrom;

impl EventList {
    pub fn block(&self) -> &EventBlock {
        self.newblock.as_ref().unwrap()
    }

    pub fn header(&self) -> &Header {
        self.block()
            .block
            .as_ref()
            .unwrap()
            .header
            .as_ref()
            .unwrap()
    }

    pub fn events(&self) -> Vec<Event> {
        self.begin_block_events()
            .into_iter()
            .chain(self.tx_events().into_iter())
            .chain(self.end_block_events().into_iter())
            .collect()
    }

    pub fn begin_block_events(&self) -> Vec<Event> {
        self.block()
            .result_begin_block
            .as_ref()
            .unwrap()
            .events
            .clone()
    }

    pub fn tx_events(&self) -> Vec<Event> {
        self.transaction
            .iter()
            .flat_map(|tx| {
                tx.tx_result
                    .as_ref()
                    .unwrap()
                    .result
                    .as_ref()
                    .unwrap()
                    .events
                    .clone()
            })
            .collect()
    }

    pub fn end_block_events(&self) -> Vec<Event> {
        self.block()
            .result_end_block
            .as_ref()
            .unwrap()
            .events
            .clone()
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
        BlockPtr::from((b.header().data_hash.clone(), b.header().height))
    }
}

impl BBlock for EventList {
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
