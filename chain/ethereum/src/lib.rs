mod adapter;
mod config;
mod ethereum_adapter;
pub mod network_indexer;
mod transport;

use graph::prelude::{BlockPtr, EthereumBlockWithTriggers};

pub use self::ethereum_adapter::{triggers_in_block, EthereumAdapter};
pub use self::transport::{EventLoopHandle, Transport};

mod chain;
mod network;

pub use crate::adapter::{
    EthereumAdapter as EthereumAdapterTrait, EthereumContractCall, EthereumContractCallError,
    MockEthereumAdapter, ProviderEthRpcMetrics, SubgraphEthRpcMetrics, TriggerFilter,
};
pub use crate::chain::Chain;
pub use crate::network::EthereumNetworks;

pub enum BlockStreamEvent {
    Block(EthereumBlockWithTriggers),
    Revert(BlockPtr),
}
