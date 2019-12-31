use graph::prelude::{EthereumBlock as _, *};
use graph_chain_common::prelude::*;

pub struct EthereumBlock {}

impl Block for EthereumBlock {}

pub struct EthereumNetwork {}

pub struct EthereumNetworkOptions {}

impl EthereumNetwork {
    pub fn new(options: EthereumNetworkOptions) -> Self {
        unimplemented!();
    }
}

impl Network for EthereumNetwork {
    type Block = EthereumBlock;
    type NetworkIndexer = DefaultNetworkIndexer;

    fn indexer(&self) -> Result<Self::NetworkIndexer, Error> {
        unimplemented!();
    }
}
