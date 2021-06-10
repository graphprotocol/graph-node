mod adapter;
mod config;
mod data_source;
mod ethereum_adapter;
pub mod network_indexer;
mod runtime;
mod transport;

pub use self::ethereum_adapter::EthereumAdapter;
pub use self::runtime::RuntimeAdapter;
pub use self::transport::{EventLoopHandle, Transport};

// ETHDEP: These concrete types should probably not be exposed.
pub use data_source::{DataSource, DataSourceTemplate};
pub use trigger::MappingTrigger;

pub mod chain;

mod network;
mod trigger;

pub use crate::adapter::{
    EthereumAdapter as EthereumAdapterTrait, EthereumContractCall, EthereumContractCallError,
    MockEthereumAdapter, ProviderEthRpcMetrics, SubgraphEthRpcMetrics, TriggerFilter,
};
pub use crate::chain::{Chain, WrappedBlockFinality};
pub use crate::network::EthereumNetworks;

#[cfg(test)]
mod tests;
