use std::collections::HashMap;
use std::sync::Arc;

use graph::data::store::scalar::{BigInt, Bytes};
use graph::prelude::chain::{BlockStream, *};
use graph::prelude::{BlockStream as _, EthereumBlock as _, *};

/// The Ethereum blockchain.
#[derive(Debug)]
pub struct Ethereum {
    networks: HashMap<String, Arc<EthereumNetwork>>,
}

impl Chain for Ethereum {
    type ChainOptions = HashMap<String, Arc<dyn EthereumAdapter>>;
    type Network = EthereumNetwork;

    fn new(options: Self::ChainOptions) -> Self {
        Ethereum {
            networks: options
                .into_iter()
                .map(|(name, options)| {
                    (name.clone(), Arc::new(EthereumNetwork::new(name, options)))
                })
                .collect(),
        }
    }

    fn network(&self, name: String) -> Result<Arc<Self::Network>, Error> {
        self.networks
            .get(&name)
            .cloned()
            .ok_or_else(|| format_err!("Ethereum network `{}` not supported", name))
    }
}

/// An Ethereum network like mainnet or ropsten.
#[derive(Debug)]
pub struct EthereumNetwork {
    name: String,
    adapter: Arc<dyn EthereumAdapter>,
}

impl EthereumNetwork {
    pub fn new(name: String, adapter: Arc<dyn EthereumAdapter>) -> Self {
        EthereumNetwork { name, adapter }
    }
}

impl Network for EthereumNetwork {
    type Block = EthereumBlock;
    type NetworkTrigger = EthereumTrigger;
    type NetworkIndexer = EthereumNetworkIndexer;
    type BlockStream = EthereumBlockStream;

    fn latest_block(&self) -> BlockFuture<Self::Block> {
        unimplemented!();
    }

    fn block_by_number(&self, number: BigInt) -> BlockFuture<Self::Block> {
        unimplemented!();
    }

    fn block_by_hash(&self, hash: Bytes) -> BlockFuture<Self::Block> {
        unimplemented!();
    }

    fn parent_block(&self, child: BlockPointer) -> BlockFuture<Self::Block> {
        unimplemented!();
    }

    fn indexer(&self) -> Result<Self::NetworkIndexer, Error> {
        unimplemented!();
    }

    fn block_stream(&self) -> Result<Self::BlockStream, Error> {
        unimplemented!();
    }
}

/// An Ethereum block.
pub struct EthereumBlock {
    block: web3::types::Block<web3::types::Transaction>,
}

impl Block for EthereumBlock {
    fn number(&self) -> BigInt {
        self.block.number.unwrap().into()
    }

    fn hash(&self) -> Bytes {
        self.block.hash.unwrap().into()
    }

    fn pointer(&self) -> BlockPointer {
        BlockPointer::from((self.number(), self.hash()))
    }
}

/// An Ethereum trigger for subgraphs.
pub enum EthereumTrigger {
    Block(Arc<EthereumBlock>),
    Call,
    Event,
}

impl NetworkTrigger for EthereumTrigger {}

/// An Ethereum network indexer.
pub struct EthereumNetworkIndexer {}

impl EventProducer<NetworkIndexerEvent<EthereumBlock>> for EthereumNetworkIndexer {
    fn take_event_stream(
        &mut self,
    ) -> Option<Box<dyn Stream<Item = NetworkIndexerEvent<EthereumBlock>, Error = ()> + Send>> {
        unimplemented!();
    }
}

impl NetworkIndexer<EthereumBlock> for EthereumNetworkIndexer {}

/// An Ethereum block stream.
pub struct EthereumBlockStream {}

impl EventProducer<BlockStreamEvent<EthereumBlock, EthereumTrigger>> for EthereumBlockStream {
    fn take_event_stream(
        &mut self,
    ) -> Option<
        Box<dyn Stream<Item = BlockStreamEvent<EthereumBlock, EthereumTrigger>, Error = ()> + Send>,
    > {
        unimplemented!();
    }
}

impl BlockStream<EthereumBlock, EthereumTrigger> for EthereumBlockStream {}
