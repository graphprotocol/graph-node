mod adapter;
mod capabilities;
pub mod codec;
mod data_source;
mod ethereum_adapter;
pub mod runtime;
mod transport;

pub use self::capabilities::NodeCapabilities;
pub use self::ethereum_adapter::EthereumAdapter;
pub use self::runtime::RuntimeAdapter;
pub use self::transport::Transport;

// ETHDEP: These concrete types should probably not be exposed.
pub use data_source::{DataSource, DataSourceTemplate, Mapping, MappingABI, TemplateSource};
pub use trigger::MappingTrigger;

pub mod chain;

mod network;
mod trigger;

pub use crate::adapter::{
    EthereumAdapter as EthereumAdapterTrait, EthereumContractCall, EthereumContractCallError,
    MockEthereumAdapter, ProviderEthRpcMetrics, SubgraphEthRpcMetrics, TriggerFilter,
};
pub use crate::chain::Chain;
pub use crate::network::EthereumNetworks;

#[cfg(test)]
mod tests;
