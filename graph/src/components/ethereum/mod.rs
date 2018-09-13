mod adapter;

pub use self::adapter::{
    BlockNumberRange, EthereumAdapter, EthereumContractCall, EthereumContractCallError,
    EthereumContractState, EthereumContractStateError, EthereumContractStateRequest, EthereumEvent,
    EthereumEventFilter, EthereumEventSubscription, EthereumSubscriptionError,
};

pub use web3::types::BlockNumber;

pub use ethabi::{Contract, Event};
