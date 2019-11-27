use graph::prelude::chains::*;

use super::network::EthereumNetwork;

pub struct Ethereum {}

impl Blockchain for Ethereum {
    type Network = EthereumNetwork;

    fn new(options: BlockchainOptions) -> Self {
        Self {}
    }

    fn network(&self, name: String) -> Option<Self::Network> {
        unimplemented!();
    }
}
