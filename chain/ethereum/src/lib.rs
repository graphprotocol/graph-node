mod adapter;
mod config;
mod ethereum_adapter;
pub mod network_indexer;
mod transport;

use graph::prelude::{BlockPtr, EthereumBlockWithTriggers};

pub use self::ethereum_adapter::EthereumAdapter;
pub use self::transport::{EventLoopHandle, Transport};

pub mod chain;
mod network;

pub use crate::adapter::{
    EthereumAdapter as EthereumAdapterTrait, EthereumContractCall, EthereumContractCallError,
    MockEthereumAdapter, ProviderEthRpcMetrics, SubgraphEthRpcMetrics, TriggerFilter,
};
pub use crate::chain::{Chain, WrappedBlockFinality};
pub use crate::network::EthereumNetworks;

pub enum BlockStreamEvent {
    Block(EthereumBlockWithTriggers),
    Revert(BlockPtr),
}
