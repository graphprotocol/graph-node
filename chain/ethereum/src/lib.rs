mod adapter;
mod block_stream;
mod config;
mod ethereum_adapter;
pub mod network_indexer;
mod transport;

use graph::prelude::{BlockPtr, EthereumBlockWithTriggers};

pub use self::block_stream::{BlockStream, BlockStreamBuilder};
pub use self::ethereum_adapter::{triggers_in_block, EthereumAdapter};
pub use self::transport::{EventLoopHandle, Transport};

mod chain;
mod network;

pub use crate::adapter::{
    BlockStreamMetrics, EthereumAdapter as EthereumAdapterTrait, EthereumContractCall,
    EthereumContractCallError, MockEthereumAdapter, ProviderEthRpcMetrics, SubgraphEthRpcMetrics,
    TriggerFilter,
};
pub use crate::chain::Chain;
pub use crate::network::EthereumNetworks;

pub enum BlockStreamEvent {
    Block(EthereumBlockWithTriggers),
    Revert(BlockPtr),
}
