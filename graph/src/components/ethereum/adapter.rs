use ethabi::{Bytes, Error as ABIError, Event, Function, LogParam, ParamType, Token};
use failure::SyncFailure;
use futures::Future;
use std::collections::HashMap;
use web3::error::Error as Web3Error;
use web3::types::{Address, BlockId, BlockNumber, H2048, H256, Log};

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

#[derive(Clone, Debug)]
pub struct EthereumContractCall {
    pub address: Address,
    pub block_id: BlockId,
    pub function: Function,
    pub args: Vec<Token>,
}

#[derive(Fail, Debug)]
pub enum EthereumContractCallError {
    #[fail(display = "call error: {}", _0)]
    CallError(SyncFailure<Web3Error>),
    #[fail(display = "ABI error: {}", _0)]
    ABIError(SyncFailure<ABIError>),
    /// `Token` is not of expected `ParamType`
    #[fail(display = "type mismatch, token {:?} is not of kind {:?}", _0, _1)]
    TypeError(Token, ParamType),
}

impl From<Web3Error> for EthereumContractCallError {
    fn from(e: Web3Error) -> Self {
        EthereumContractCallError::CallError(SyncFailure::new(e))
    }
}

impl From<ABIError> for EthereumContractCallError {
    fn from(e: ABIError) -> Self {
        EthereumContractCallError::ABIError(SyncFailure::new(e))
    }
}

#[derive(Fail, Debug)]
pub enum EthereumSubscriptionError {
    #[fail(display = "RPC error: {}", _0)]
    RpcError(SyncFailure<Web3Error>),
    #[fail(display = "ABI error: {}", _0)]
    ABIError(SyncFailure<ABIError>),
}

impl From<Web3Error> for EthereumSubscriptionError {
    fn from(err: Web3Error) -> EthereumSubscriptionError {
        EthereumSubscriptionError::RpcError(SyncFailure::new(err))
    }
}

impl From<ABIError> for EthereumSubscriptionError {
    fn from(err: ABIError) -> EthereumSubscriptionError {
        EthereumSubscriptionError::ABIError(SyncFailure::new(err))
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
    pub removed: bool,
}

#[derive(Clone, Debug)]
pub struct EthereumEventFilter {
    // Event types stored in nested hash tables for faster lookups
    pub event_types_by_contract_address_and_sig: HashMap<Address, HashMap<H256, Event>>,
}

impl EthereumEventFilter {
    /// Check if log bloom filter indicates a possible match for this event filter.
    /// Returns `true` to indicate that a matching `Log` _might_ be contained.
    /// Returns `false` to indicate that a matching `Log` _is not_ contained.
    pub fn check_bloom(&self, _bloom: H2048) -> bool {
        // TODO
        true // not even wrong
    }

    /// Try to match a `Log` to one of the event types in this filter.
    pub fn match_event(&self, log: &Log) -> Option<Event> {
        // First topic should be event sig
        log.topics.first()
            .and_then(|sig| {
                // Look up contract address
                self.event_types_by_contract_address_and_sig
                    .get(&log.address)
                    .and_then(|event_types_by_sig| {
                        // Look up event sig
                        event_types_by_sig.get(sig).map(Clone::clone)
                    })
            })
    }
}

/// Common trait for components that watch and manage access to Ethereum.
///
/// Implementations may be implemented against an in-process Ethereum node
/// or a remote node over RPC.
pub trait EthereumAdapter: Send + 'static {
    /// Call the function of a smart contract.
    fn contract_call(
        &mut self,
        call: EthereumContractCall,
    ) -> Box<Future<Item = Vec<Token>, Error = EthereumContractCallError>>;

    /// Subscribe to an event of a smart contract.
    fn subscribe_to_event(
        &mut self,
        subscription: EthereumEventSubscription,
    ) -> Box<Stream<Item = EthereumEvent, Error = EthereumSubscriptionError>>;

    /// Cancel a specific event subscription. Returns true when the subscription existed before.
    fn unsubscribe_from_event(&mut self, subscription_id: String) -> bool;
}
