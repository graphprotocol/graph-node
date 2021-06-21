mod types;

use web3::types::H256;

pub use self::types::{
    evaluate_transaction_status, EthereumBlock, EthereumBlockWithCalls, EthereumCall,
    LightEthereumBlock, LightEthereumBlockExt,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
/// A collection of attributes that (kind of) uniquely identify an Ethereum blockchain.
pub struct EthereumNetworkIdentifier {
    pub net_version: String,
    pub genesis_block_hash: H256,
}
