mod adapter;
mod config;
mod data_source;
mod ethereum_adapter;
pub mod network_indexer;
mod transport;

pub use self::ethereum_adapter::EthereumAdapter;
pub use self::transport::{EventLoopHandle, Transport};

// ETHDEP: This concrete type should probably not be exposed.
pub use data_source::DataSource;

pub mod chain;
mod network;

pub use crate::adapter::{
    EthereumAdapter as EthereumAdapterTrait, EthereumContractCall, EthereumContractCallError,
    MockEthereumAdapter, ProviderEthRpcMetrics, SubgraphEthRpcMetrics, TriggerFilter,
};
pub use crate::chain::{Chain, WrappedBlockFinality};
pub use crate::network::EthereumNetworks;
