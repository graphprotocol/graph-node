use ethabi::{Bytes, Error as ABIError, Function, ParamType, Token};
use failure::{Error, SyncFailure};
use futures::Future;
use slog::Logger;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use tiny_keccak::keccak256;
use web3::error::Error as Web3Error;
use web3::types::*;

use super::types::*;
use crate::prelude::BlockHandlerFilter;
use crate::prelude::DataSource;
use crate::util::extend::Extend;

/// A collection of attributes that (kind of) uniquely identify an Ethereum blockchain.
pub struct EthereumNetworkIdentifier {
    pub net_version: String,
    pub genesis_block_hash: H256,
}

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
    pub block_ptr: EthereumBlockPointer,
    pub function: Function,
    pub args: Vec<Token>,
}

#[derive(Fail, Debug)]
pub enum EthereumContractCallError {
    #[fail(display = "call error: {}", _0)]
    CallError(Error),
    #[fail(display = "ABI error: {}", _0)]
    ABIError(SyncFailure<ABIError>),
    /// `Token` is not of expected `ParamType`
    #[fail(display = "type mismatch, token {:?} is not of kind {:?}", _0, _1)]
    TypeError(Token, ParamType),
    #[fail(display = "call error: {}", _0)]
    Error(Error),
}

impl From<Web3Error> for EthereumContractCallError {
    fn from(e: Web3Error) -> Self {
        EthereumContractCallError::CallError(failure::err_msg(e.to_string()))
    }
}

impl From<ABIError> for EthereumContractCallError {
    fn from(e: ABIError) -> Self {
        EthereumContractCallError::ABIError(SyncFailure::new(e))
    }
}

impl From<Error> for EthereumContractCallError {
    fn from(e: Error) -> Self {
        EthereumContractCallError::Error(e)
    }
}

#[derive(Fail, Debug)]
pub enum EthereumAdapterError {
    /// The Ethereum node does not know about this block for some reason, probably because it
    /// disappeared in a chain reorg.
    #[fail(
        display = "Block data unavailable, block was likely uncled (block hash = {:?})",
        _0
    )]
    BlockUnavailable(H256),

    /// An unexpected error occurred.
    #[fail(display = "Ethereum adapter error: {}", _0)]
    Unknown(Error),
}

impl From<Error> for EthereumAdapterError {
    fn from(e: Error) -> Self {
        EthereumAdapterError::Unknown(e)
    }
}

#[derive(Clone, Debug)]
pub struct EthereumLogFilter {
    pub contract_address_and_event_sig_pairs: HashSet<(Option<Address>, H256)>,
}

impl EthereumLogFilter {
    /// Check if log bloom filter indicates a possible match for this log filter.
    /// Returns `true` to indicate that a matching `Log` _might_ be contained.
    /// Returns `false` to indicate that a matching `Log` _is not_ contained.
    pub fn check_bloom(&self, _bloom: H2048) -> bool {
        // TODO issue #352: implement bloom filter check
        true // not even wrong
    }

    /// Check if this filter matches the specified `Log`.
    pub fn matches(&self, log: &Log) -> bool {
        // First topic should be event sig
        match log.topics.first() {
            None => false,
            Some(sig) => self
                .contract_address_and_event_sig_pairs
                .iter()
                .any(|pair| match pair {
                    // The `Log` matches the filter either if the filter contains
                    // a (contract address, event signature) pair that matches the
                    // `Log`...
                    (Some(addr), s) => addr == &log.address && s == sig,

                    // ...or if the filter contains a pair with no contract address
                    // but an event signature that matches the event
                    (None, s) => s == sig,
                }),
        }
    }

    pub fn from_data_sources_opt<'a, I>(iter: I) -> Option<Self>
    where
        I: IntoIterator<Item = &'a DataSource>,
    {
        iter.into_iter().fold(None, |filter_opt, data_source| {
            let contract_addr = data_source.source.address;

            if let Some(ref handlers) = data_source.mapping.event_handlers {
                filter_opt.extend(
                    handlers
                        .iter()
                        .map(move |event_handler| {
                            let event_sig = event_handler.topic0();
                            (contract_addr, event_sig)
                        })
                        .collect::<EthereumLogFilter>(),
                )
            } else {
                filter_opt
            }
        })
    }
}

impl Extend<EthereumLogFilter> for EthereumLogFilter {
    /// Extends this log filter with another one.
    fn extend(mut self, other: EthereumLogFilter) -> Self {
        self.contract_address_and_event_sig_pairs
            .extend(other.contract_address_and_event_sig_pairs.iter());
        self
    }
}

impl FromIterator<(Option<Address>, H256)> for EthereumLogFilter {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (Option<Address>, H256)>,
    {
        EthereumLogFilter {
            contract_address_and_event_sig_pairs: iter.into_iter().collect(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct EthereumCallFilter {
    pub contract_addresses_function_signatures: HashMap<Address, HashSet<[u8; 4]>>,
}

impl EthereumCallFilter {
    pub fn matches(&self, call: &EthereumCall) -> bool {
        // Ensure the call is to a contract the filter expressed an interest in
        if !self
            .contract_addresses_function_signatures
            .contains_key(&call.to)
        {
            return false;
        }
        // If the call is to a contract with no specified functions, keep the call
        if self
            .contract_addresses_function_signatures
            .get(&call.to)
            .unwrap()
            .is_empty()
        {
            // Allow the ability to match on calls to a contract generally
            // If you want to match on a generic call to contract this limits you
            // from matching with a specific call to a contract
            return true;
        }
        // Ensure the call is to run a function the filter expressed an interest in
        self.contract_addresses_function_signatures
            .get(&call.to)
            .unwrap()
            .contains(&call.input.0[..4])
    }

    pub fn from_data_sources_opt<'a, I>(iter: I) -> Option<Self>
    where
        I: IntoIterator<Item = &'a DataSource>,
    {
        iter.into_iter()
            .filter(|data_source| data_source.source.address.is_some())
            .fold(None, |filter_opt, data_source| {
                let contract_addr = data_source.source.address.unwrap();

                if let Some(ref handlers) = data_source.mapping.call_handlers {
                    filter_opt.extend(
                        handlers
                            .iter()
                            .map(move |call_handler| {
                                let sig = keccak256(call_handler.function.as_bytes());
                                (contract_addr, [sig[0], sig[1], sig[2], sig[3]])
                            })
                            .collect::<EthereumCallFilter>(),
                    )
                } else {
                    filter_opt
                }
            })
    }
}

impl Extend<EthereumCallFilter> for EthereumCallFilter {
    /// Extends this call filter with another one.
    fn extend(mut self, other: EthereumCallFilter) -> Self {
        self.contract_addresses_function_signatures
            .extend(other.contract_addresses_function_signatures.into_iter());
        self
    }
}

impl FromIterator<(Address, [u8; 4])> for EthereumCallFilter {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (Address, [u8; 4])>,
    {
        let mut lookup: HashMap<Address, HashSet<[u8; 4]>> = HashMap::new();
        iter.into_iter().for_each(|(address, function_signature)| {
            if !lookup.contains_key(&address) {
                lookup.insert(address, HashSet::default());
            }
            lookup.get_mut(&address).map(|set| {
                set.insert(function_signature);
                set
            });
        });
        EthereumCallFilter {
            contract_addresses_function_signatures: lookup,
        }
    }
}

impl From<EthereumBlockFilter> for EthereumCallFilter {
    fn from(ethereum_block_filter: EthereumBlockFilter) -> Self {
        Self {
            contract_addresses_function_signatures: ethereum_block_filter
                .contract_addresses
                .into_iter()
                .map(|address| (address, HashSet::default()))
                .collect::<HashMap<Address, HashSet<[u8; 4]>>>(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct EthereumBlockFilter {
    pub contract_addresses: HashSet<Address>,
    pub trigger_every_block: bool,
}

impl EthereumBlockFilter {
    pub fn from_data_sources_opt<'a, I>(iter: I) -> Option<Self>
    where
        I: IntoIterator<Item = &'a DataSource>,
    {
        iter.into_iter()
            .filter(|data_source| data_source.source.address.is_some())
            .fold(None, |filter_opt, data_source| {
                let has_block_handler_with_call_filter = data_source
                    .mapping
                    .block_handlers
                    .clone()
                    .unwrap_or(vec![])
                    .into_iter()
                    .any(|block_handler| match block_handler.filter {
                        Some(ref filter) if *filter == BlockHandlerFilter::Call => return true,
                        _ => return false,
                    });

                let has_block_handler_without_filter = data_source
                    .mapping
                    .block_handlers
                    .clone()
                    .unwrap_or(vec![])
                    .into_iter()
                    .any(|block_handler| block_handler.filter.is_none());

                if has_block_handler_without_filter || has_block_handler_with_call_filter {
                    filter_opt.extend(Self {
                        trigger_every_block: has_block_handler_without_filter,
                        contract_addresses: if has_block_handler_with_call_filter {
                            vec![data_source.source.address.unwrap().to_owned()]
                                .into_iter()
                                .collect()
                        } else {
                            HashSet::default()
                        },
                    })
                } else {
                    filter_opt
                }
            })
    }
}

impl Extend<EthereumBlockFilter> for EthereumBlockFilter {
    fn extend(mut self, other: EthereumBlockFilter) -> Self {
        self.trigger_every_block = self.trigger_every_block || other.trigger_every_block;
        self.contract_addresses.extend(other.contract_addresses);
        self
    }
}

/// Common trait for components that watch and manage access to Ethereum.
///
/// Implementations may be implemented against an in-process Ethereum node
/// or a remote node over RPC.
pub trait EthereumAdapter: Send + Sync + 'static {
    /// Ask the Ethereum node for some identifying information about the Ethereum network it is
    /// connected to.
    fn net_identifiers(
        &self,
        logger: &Logger,
    ) -> Box<Future<Item = EthereumNetworkIdentifier, Error = Error> + Send>;

    /// Find the most recent block.
    fn latest_block(
        &self,
        logger: &Logger,
    ) -> Box<Future<Item = Block<Transaction>, Error = EthereumAdapterError> + Send>;

    /// Find a block by its hash.
    fn block_by_hash(
        &self,
        logger: &Logger,
        block_hash: H256,
    ) -> Box<Future<Item = Option<Block<Transaction>>, Error = Error> + Send>;

    /// Load full information for the specified `block` (in particular, transaction receipts).
    fn load_full_block(
        &self,
        logger: &Logger,
        block: Block<Transaction>,
    ) -> Box<Future<Item = EthereumBlock, Error = EthereumAdapterError> + Send>;

    // Find the hash for the parent block of the provided block hash
    fn block_parent_hash_by_block_hash(
        &self,
        logger: &Logger,
        block_hash: H256,
    ) -> Box<Future<Item = Option<H256>, Error = Error> + Send>;

    /// Find a block by its number.
    ///
    /// Careful: don't use this function without considering race conditions.
    /// Chain reorgs could happen at any time, and could affect the answer received.
    /// Generally, it is only safe to use this function with blocks that have received enough
    /// confirmations to guarantee no further reorgs, **and** where the Ethereum node is aware of
    /// those confirmations.
    /// If the Ethereum node is far behind in processing blocks, even old blocks can be subject to
    /// reorgs.
    fn block_hash_by_block_number(
        &self,
        logger: &Logger,
        block_number: u64,
    ) -> Box<Future<Item = Option<H256>, Error = Error> + Send>;

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
        logger: &Logger,
        block_ptr: EthereumBlockPointer,
    ) -> Box<Future<Item = bool, Error = Error> + Send>;

    fn calls_in_block(
        &self,
        logger: &Logger,
        block_number: u64,
        block_hash: H256,
    ) -> Box<Future<Item = Vec<EthereumCall>, Error = Error> + Send>;

    fn blocks_with_triggers(
        &self,
        logger: &Logger,
        from: u64,
        to: u64,
        log_filter: Option<EthereumLogFilter>,
        tx_filter: Option<EthereumCallFilter>,
        block_filter: Option<EthereumBlockFilter>,
    ) -> Box<Future<Item = Vec<EthereumBlockPointer>, Error = Error> + Send>;

    /// Find the first few blocks in the specified range containing at least one transaction with
    /// at least one log entry matching the specified `log_filter`.
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
    fn blocks_with_logs(
        &self,
        logger: &Logger,
        from: u64,
        to: u64,
        log_filter: EthereumLogFilter,
    ) -> Box<Future<Item = Vec<EthereumBlockPointer>, Error = Error> + Send>;

    fn blocks_with_calls(
        &self,
        logger: &Logger,
        from: u64,
        to: u64,
        call_filter: EthereumCallFilter,
    ) -> Box<Future<Item = Vec<EthereumBlockPointer>, Error = Error> + Send>;

    fn blocks(
        &self,
        logger: &Logger,
        from: u64,
        to: u64,
    ) -> Box<Future<Item = Vec<EthereumBlockPointer>, Error = Error> + Send>;

    /// Call the function of a smart contract.
    fn contract_call(
        &self,
        logger: &Logger,
        call: EthereumContractCall,
    ) -> Box<Future<Item = Vec<Token>, Error = EthereumContractCallError> + Send>;
}
