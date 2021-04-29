use anyhow::Error;
use futures::Stream;

use graph::prelude::*;

pub enum BlockStreamEvent {
    Block(EthereumBlockWithTriggers),
    Revert(BlockPtr),
}
