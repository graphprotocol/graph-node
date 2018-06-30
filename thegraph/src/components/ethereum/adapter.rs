use ethabi::{Bytes, Error as ABIError, Event, Function, LogParam, Token};
use ethereum_types::{Address, H256};
use futures::{Future, Stream};
use std::error::Error;
use std::fmt;
use web3::error::Error as Web3Error;
use web3::types::BlockNumber;

/// A request for the state of a contract at a specific block hash and address.
pub struct EthereumContractStateRequest {
    pub address: Address,
    pub block_hash: H256,
}

/// An error that can occur when trying to obtain the state of a contract.
pub enum EthereumContractStateError {
    Failed,
}

/// Representation of an Ethereum contract state.
pub struct EthereumContractState {
    pub address: Address,
    pub block_hash: H256,
    pub data: Bytes,
}

pub struct EthereumContractCallRequest {
    pub address: Address,
    pub block_number: Option<BlockNumber>,
    pub function: Function,
    pub args: Vec<Token>,
}

#[derive(Debug)]
pub enum EthereumContractCallError {
    CallError(Web3Error),
    ABIError(ABIError),
}

impl From<Web3Error> for EthereumContractCallError {
    fn from(e: Web3Error) -> Self {
        EthereumContractCallError::CallError(e)
    }
}

impl From<ABIError> for EthereumContractCallError {
    fn from(e: ABIError) -> Self {
        EthereumContractCallError::ABIError(e)
    }
}

#[derive(Debug)]
pub enum EthereumSubscriptionError {
    RpcError(Web3Error),
    ABIError(ABIError),
}

impl Error for EthereumSubscriptionError {
    fn description(&self) -> &str {
        "Ethereum subscription error"
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            EthereumSubscriptionError::RpcError(ref e) => Some(e),
            EthereumSubscriptionError::ABIError(ref e) => Some(e),
        }
    }
}

impl fmt::Display for EthereumSubscriptionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EthereumSubscriptionError::RpcError(e) => write!(f, "RPC error: {}", e),
            EthereumSubscriptionError::ABIError(e) => write!(f, "ABI error: {}", e),
        }
    }
}

/// A range to allow event subscriptions to limit the block numbers to consider.
#[derive(Debug)]
pub struct BlockNumberRange {
    pub from: BlockNumber,
    pub to: BlockNumber,
}

/// A subscription to a specific contract address, event signature and block range.
#[derive(Debug)]
pub struct EthereumEventSubscription {
    /// An ID that uniquely identifies the subscription (e.g. a GUID).
    pub subscription_id: String,
    pub address: Address,
    pub range: BlockNumberRange,
    pub event: Event,
}

/// An event logged for a specific contract address and event signature.
#[derive(Debug)]
pub struct EthereumEvent {
    pub address: Address,
    pub event_signature: H256,
    pub block_hash: H256,
    pub params: Vec<LogParam>,
}

/// Common trait for components that watch and manage access to Ethereum.
///
/// Implementations may be implemented against an in-process Ethereum node
/// or a remote node over RPC.
pub trait EthereumAdapter {
    /// Obtain the state of a smart contract.
    fn contract_state(
        &mut self,
        request: EthereumContractStateRequest,
    ) -> Result<EthereumContractState, EthereumContractStateError>;

    fn contract_call(
        &mut self,
        request: EthereumContractCallRequest,
    ) -> Box<Future<Item = Vec<Token>, Error = EthereumContractCallError>>;

    /// Subscribe to an event of a smart contract.
    fn subscribe_to_event(
        &mut self,
        subscription: EthereumEventSubscription,
    ) -> Box<Stream<Item = EthereumEvent, Error = EthereumSubscriptionError>>;

    /// Cancel a specific event subscription. Returns true when the subscription existed before.
    fn unsubscribe_from_event(&mut self, subscription_id: String) -> bool;
}
