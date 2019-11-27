use graph::components::chains::*;

use super::block::EthereumBlock;

pub struct EthereumNetwork {}

impl Network for EthereumNetwork {
    type Block = EthereumBlock;
    type Indexer = ();
}
