mod header;
pub mod json_patch;
mod network;
mod types;

pub use self::header::LenientHeader;
pub use self::network::AnyNetworkBare;
pub use self::types::{
    AnyBlock, AnyTransaction, AnyTransactionReceiptBare, CachedBlock, EthereumBlock,
    EthereumBlockWithCalls, EthereumCall, LightEthereumBlock, LightEthereumBlockExt,
};

// Re-export Alloy network types for convenience
pub use alloy::network::{AnyHeader, AnyTxEnvelope};
