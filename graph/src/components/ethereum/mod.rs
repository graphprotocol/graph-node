mod network;
mod types;

use web3::types::H256;

pub use self::network::NodeCapabilities;
pub use self::types::{
    BlockFinality, EthereumBlock, EthereumBlockData, EthereumBlockTriggerType,
    EthereumBlockWithCalls, EthereumCall, EthereumCallData, EthereumEventData,
    EthereumTransactionData, EthereumTrigger, LightEthereumBlock, LightEthereumBlockExt,
    MappingTrigger,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
/// A collection of attributes that (kind of) uniquely identify an Ethereum blockchain.
pub struct EthereumNetworkIdentifier {
    pub net_version: String,
    pub genesis_block_hash: H256,
}
