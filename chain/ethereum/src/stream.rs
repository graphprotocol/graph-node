use anyhow::Error;
use futures::Stream;

use graph::prelude::*;

pub enum BlockStreamEvent {
    Block(EthereumBlockWithTriggers),
    Revert(BlockPtr),
}

pub trait BlockStream: Stream<Item = BlockStreamEvent, Error = Error> {}
