#[rustfmt::skip]
#[path = "protobuf/sf.cosmos.r#type.v1.rs"]
mod pbcosmos;

pub use pbcosmos::*;

use graph::blockchain::Block as BlockchainBlock;
use graph::{blockchain::BlockPtr, prelude::BlockNumber};

use std::convert::TryFrom;

impl Block {
    pub fn header(&self) -> &Header {
        self.header.as_ref().unwrap()
    }

    pub fn events(&self) -> impl Iterator<Item = &Event> {
        self.begin_block_events()
            .chain(self.tx_events())
            .chain(self.end_block_events())
    }

    pub fn begin_block_events(&self) -> impl Iterator<Item = &Event> {
        self.result_begin_block
            .as_ref()
            .map(|b| b.events.iter())
            .into_iter()
            .flatten()
    }

    pub fn tx_events(&self) -> impl Iterator<Item = &Event> {
        self.transactions.iter().flat_map(|tx| {
            tx.result
                .as_ref()
                .map(|b| b.events.iter())
                .into_iter()
                .flatten()
        })
    }

    pub fn end_block_events(&self) -> impl Iterator<Item = &Event> {
        self.result_end_block
            .as_ref()
            .map(|b| b.events.iter())
            .into_iter()
            .flatten()
    }

    pub fn transactions(&self) -> impl Iterator<Item = &TxResult> {
        self.transactions.iter()
    }

    pub fn parent_ptr(&self) -> Option<BlockPtr> {
        self.header().last_block_id.as_ref().map(|last_block_id| {
            BlockPtr::from((last_block_id.hash.clone(), self.header().height - 1))
        })
    }
}

impl From<Block> for BlockPtr {
    fn from(b: Block) -> BlockPtr {
        (&b).into()
    }
}

impl<'a> From<&'a Block> for BlockPtr {
    fn from(b: &'a Block) -> BlockPtr {
        BlockPtr::from((b.header().hash.clone(), b.header().height))
    }
}

impl BlockchainBlock for Block {
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

impl HeaderOnlyBlock {
    pub fn header(&self) -> &Header {
        self.header.as_ref().unwrap()
    }

    pub fn parent_ptr(&self) -> Option<BlockPtr> {
        self.header().last_block_id.as_ref().map(|last_block_id| {
            BlockPtr::from((last_block_id.hash.clone(), self.header().height - 1))
        })
    }
}

impl From<&Block> for HeaderOnlyBlock {
    fn from(b: &Block) -> HeaderOnlyBlock {
        HeaderOnlyBlock {
            header: b.header.clone(),
        }
    }
}

impl From<HeaderOnlyBlock> for BlockPtr {
    fn from(b: HeaderOnlyBlock) -> BlockPtr {
        (&b).into()
    }
}

impl<'a> From<&'a HeaderOnlyBlock> for BlockPtr {
    fn from(b: &'a HeaderOnlyBlock) -> BlockPtr {
        BlockPtr::from((b.header().hash.clone(), b.header().height))
    }
}

impl BlockchainBlock for HeaderOnlyBlock {
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
    pub fn block(&self) -> &HeaderOnlyBlock {
        self.block.as_ref().unwrap()
    }
}

impl TransactionData {
    pub fn tx_result(&self) -> &TxResult {
        self.tx.as_ref().unwrap()
    }

    pub fn response_deliver_tx(&self) -> &ResponseDeliverTx {
        self.tx_result().result.as_ref().unwrap()
    }

    pub fn block(&self) -> &HeaderOnlyBlock {
        self.block.as_ref().unwrap()
    }
}
