use ethabi::{Bytes, Error as ABIError, Event, Function, LogParam, ParamType, Token};
use failure::Error;
use failure::SyncFailure;
use futures::Future;
use std::collections::HashMap;
use std::iter::Sum;
use std::ops::Add;
use std::ops::AddAssign;
use web3::error::Error as Web3Error;
use web3::types::Address;
use web3::types::Block;
use web3::types::BlockId;
use web3::types::BlockNumber;
use web3::types::H2048;
use web3::types::H256;
use web3::types::Log;
use web3::types::Transaction;

use components::store::EthereumBlockPointer;

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
    pub block: Block<Transaction>,
    pub params: Vec<LogParam>,
    pub removed: bool,
}

// Implemented manually because LogParam is not Clone.
impl Clone for EthereumEvent {
    fn clone(&self) -> Self {
        EthereumEvent {
            address: self.address.clone(),
            event_signature: self.event_signature,
            block: self.block.clone(),
            params: self
                .params
                .iter()
                .map(|param| LogParam {
                    name: param.name.clone(),
                    value: param.value.clone(),
                })
                .collect(),
            removed: self.removed,
        }
    }
}

#[derive(Clone, Debug)]
pub struct EthereumEventFilter {
    // Event types stored in nested hash tables for faster lookups
    pub event_types_by_contract_address_and_sig: HashMap<Address, HashMap<H256, Event>>,
}

impl EthereumEventFilter {
    /// Create an event filter that matches no events.
    pub fn empty() -> Self {
        EthereumEventFilter {
            event_types_by_contract_address_and_sig: HashMap::new(),
        }
    }

    /// Create an event filter that matches one event type from one contract only.
    pub fn from_single(contract_address: Address, event_type: Event) -> Self {
        EthereumEventFilter {
            event_types_by_contract_address_and_sig: [(
                contract_address,
                [(event_type.signature(), event_type)]
                    .iter()
                    .cloned()
                    .collect(),
            )].iter()
                .cloned()
                .collect(),
        }
    }

    /// Check if log bloom filter indicates a possible match for this event filter.
    /// Returns `true` to indicate that a matching `Log` _might_ be contained.
    /// Returns `false` to indicate that a matching `Log` _is not_ contained.
    pub fn check_bloom(&self, _bloom: H2048) -> bool {
        // TODO issue #352: implement bloom filter check
        true // not even wrong
    }

    /// Try to match a `Log` to one of the event types in this filter.
    pub fn match_event(&self, log: &Log) -> Option<Event> {
        // First topic should be event sig
        log.topics.first().and_then(|sig| {
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

impl Add for EthereumEventFilter {
    type Output = EthereumEventFilter;

    /// Take the union of two event filters.
    /// Produce a new event filter that matches all events
    /// that would have matched either of the two original event filters.
    fn add(mut self, rhs: Self) -> Self {
        self += rhs;
        self
    }
}

impl AddAssign for EthereumEventFilter {
    /// Take the union of two event filters.
    /// Update the left-hand-side event filter to match all events
    /// that would have matched either of the two original event filters.
    fn add_assign(&mut self, rhs: Self) {
        for (addr, rhs_event_types_by_sig) in
            rhs.event_types_by_contract_address_and_sig.into_iter()
        {
            let event_types_by_sig = self
                .event_types_by_contract_address_and_sig
                .entry(addr)
                .or_insert(HashMap::new());

            for (sig, rhs_event_type) in rhs_event_types_by_sig.into_iter() {
                event_types_by_sig
                    .entry(sig)
                    .and_modify(|event_type| {
                        // Don't actuall modify, just check for conflict
                        if event_type != &rhs_event_type {
                            panic!("event filters had different versions of the same event: {:?} and {:?}",
                                   event_type, rhs_event_type);
                        }
                    })
                    .or_insert(rhs_event_type);
            }
        }
    }
}

impl Sum for EthereumEventFilter {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(Self::empty(), |acc, f| acc + f)
    }
}

/// Common trait for components that watch and manage access to Ethereum.
///
/// Implementations may be implemented against an in-process Ethereum node
/// or a remote node over RPC.
pub trait EthereumAdapter: Send + 'static {
    /// Find a block by its hash.
    ///
    /// Use this method instead of `block_by_number` whenever possible.
    fn block_by_hash(
        &self,
        block_hash: H256,
    ) -> Box<Future<Item = Block<Transaction>, Error = Error> + Send>;

    /// Find a block by its number.
    ///
    /// Careful: don't use this function without considering race conditions.
    /// Chain reorgs could happen at any time, and could affect the answer received.
    /// Generally, it is only safe to use this function with blocks that have received enough
    /// confirmations to guarantee no further reorgs, **and** where the Ethereum node is aware of
    /// those confirmations.
    /// If the Ethereum node is far behind in processing blocks, even old blocks can be subject to
    /// reorgs.
    fn block_by_number(
        &self,
        block_number: u64,
    ) -> Box<Future<Item = Block<Transaction>, Error = Error> + Send>;

    /// Check if `block_ptr` refers to a block that is on the main chain, according to the Ethereum
    /// node.
    ///
    /// Careful: don't use this function without considering race conditions.
    /// Chain reorgs could happen at any time, and could affect the answer received.
    /// Generally, it is only safe to use this function with blocks that have received enough
    /// confirmations to guarantee no further reorgs, **and** where the Ethereum node is aware of
    /// those confirmations.
    /// If the Ethereum node is far behind in processing blocks, even old blocks can be subject to
    /// reorgs.
    fn is_on_main_chain(
        &self,
        block_ptr: EthereumBlockPointer,
    ) -> Box<Future<Item = bool, Error = Error> + Send>;

    /// Find the first few blocks in the specified range containing at least one transaction with
    /// at least one log entry matching the specified `event_filter`.
    ///
    /// Careful: don't use this function without considering race conditions.
    /// Chain reorgs could happen at any time, and could affect the answer received.
    /// Generally, it is only safe to use this function with blocks that have received enough
    /// confirmations to guarantee no further reorgs, **and** where the Ethereum node is aware of
    /// those confirmations.
    /// If the Ethereum node is far behind in processing blocks, even old blocks can be subject to
    /// reorgs.
    /// It is recommended that `to` be far behind the block number of latest block the Ethereum
    /// node is aware of.
    fn find_first_blocks_with_events(
        &self,
        from: u64,
        to: u64,
        event_filter: EthereumEventFilter,
    ) -> Box<Future<Item = Vec<EthereumBlockPointer>, Error = Error> + Send>;

    /// Find all events from transactions in the specified `block` that match the specified
    /// `event_filter`.
    fn get_events_in_block(
        &self,
        block: Block<Transaction>,
        event_filter: EthereumEventFilter,
    ) -> Box<Future<Item = Vec<EthereumEvent>, Error = EthereumSubscriptionError>>;

    /// Call the function of a smart contract.
    fn contract_call(
        &mut self,
        call: EthereumContractCall,
    ) -> Box<Future<Item = Vec<Token>, Error = EthereumContractCallError>>;
}
