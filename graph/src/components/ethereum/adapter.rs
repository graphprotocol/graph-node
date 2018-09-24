use ethabi::{Bytes, Error as ABIError, Event, Function, LogParam, ParamType, Token};
use failure::{Error, SyncFailure};
use futures::{Future, Stream};
use std::collections::HashSet;
use std::iter::FromIterator;
use web3::error::Error as Web3Error;
use web3::types::*;

/// A collection of attributes that (kind of) uniquely identify an Ethereum blockchain.
pub struct EthereumNetworkIdentifiers {
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
    #[fail(
        display = "type mismatch, token {:?} is not of kind {:?}",
        _0,
        _1
    )]
    TypeError(Token, ParamType),
    #[fail(display = "call error: {}", _0)]
    Error(Error),
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

impl From<Error> for EthereumContractCallError {
    fn from(e: Error) -> Self {
        EthereumContractCallError::Error(e)
    }
}

#[derive(Fail, Debug)]
pub enum EthereumError {
    #[fail(display = "RPC error: {}", _0)]
    RpcError(SyncFailure<Web3Error>),
    #[fail(display = "ABI error: {}", _0)]
    ABIError(SyncFailure<ABIError>),
}

impl From<Web3Error> for EthereumError {
    fn from(err: Web3Error) -> EthereumError {
        EthereumError::RpcError(SyncFailure::new(err))
    }
}

impl From<ABIError> for EthereumError {
    fn from(err: ABIError) -> EthereumError {
        EthereumError::ABIError(SyncFailure::new(err))
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

impl Clone for EthereumEvent {
    fn clone(&self) -> Self {
        EthereumEvent {
            address: self.address.clone(),
            event_signature: self.event_signature.clone(),
            block_hash: self.block_hash.clone(),
            params: self
                .params
                .iter()
                .map(|log_param| LogParam {
                    name: log_param.name.clone(),
                    value: log_param.value.clone(),
                }).collect(),
            removed: self.removed.clone(),
        }
    }
}

/// A block hash and block number from a specific Ethereum block.
///
/// Maximum block number supported: 2^63 - 1
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EthereumBlockPointer {
    pub hash: H256,
    pub number: u64,
}

impl EthereumBlockPointer {
    /// Creates a pointer to the parent of the specified block.
    pub fn to_parent<T>(b: &Block<T>) -> EthereumBlockPointer {
        EthereumBlockPointer {
            hash: b.parent_hash,
            number: b.number.unwrap().as_u64() - 1,
        }
    }

    /// Encodes the block hash into a hexadecimal string **without** a "0x" prefix.
    /// Hashes are stored in the database in this format.
    ///
    /// This mainly exists because of backwards incompatible changes in how the Web3 library
    /// implements `H256::to_string`.
    pub fn hash_hex(&self) -> String {
        format!("{:x}", self.hash)
    }
}

impl<T> From<Block<T>> for EthereumBlockPointer {
    fn from(b: Block<T>) -> EthereumBlockPointer {
        EthereumBlockPointer {
            hash: b.hash.unwrap(),
            number: b.number.unwrap().as_u64(),
        }
    }
}

impl From<(H256, u64)> for EthereumBlockPointer {
    fn from((hash, number): (H256, u64)) -> EthereumBlockPointer {
        if number >= (1 << 63) {
            panic!("block number out of range: {}", number);
        }

        EthereumBlockPointer { hash, number }
    }
}

impl From<(H256, i64)> for EthereumBlockPointer {
    fn from((hash, number): (H256, i64)) -> EthereumBlockPointer {
        if number < 0 {
            panic!("block number out of range: {}", number);
        }

        EthereumBlockPointer {
            hash,
            number: number as u64,
        }
    }
}

#[derive(Clone, Debug)]
pub struct EthereumLogFilter {
    pub contract_address_and_event_sig_pairs: HashSet<(Address, H256)>,
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
                .contains(&(log.address, *sig)),
        }
    }
}

impl FromIterator<(Address, H256)> for EthereumLogFilter {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (Address, H256)>,
    {
        EthereumLogFilter {
            contract_address_and_event_sig_pairs: iter.into_iter().collect(),
        }
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
    ) -> Box<Future<Item = EthereumNetworkIdentifiers, Error = Error> + Send>;

    /// Ask the Ethereum node for the block number of the most recent block that it has.
    fn latest_block_number(&self) -> Box<Future<Item = U256, Error = Error> + Send>;

    /// Find a block by its hash.
    ///
    /// Use this method instead of `block_by_number` whenever possible.
    fn block_by_hash(
        &self,
        block_hash: H256,
    ) -> Box<Future<Item = Option<Block<Transaction>>, Error = Error> + Send>;

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
    ) -> Box<Future<Item = Option<Block<Transaction>>, Error = Error> + Send>;

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
    fn find_first_blocks_with_logs(
        &self,
        from: u64,
        to: u64,
        log_filter: EthereumLogFilter,
    ) -> Box<Future<Item = Vec<EthereumBlockPointer>, Error = Error> + Send>;

    /// Find all logs from transactions in the specified `block` that match the specified
    /// `log_filter`.
    fn get_logs_in_block(
        &self,
        block: Block<Transaction>,
        log_filter: EthereumLogFilter,
    ) -> Box<Future<Item = Vec<Log>, Error = EthereumError>>;

    /// Call the function of a smart contract.
    fn contract_call(
        &self,
        call: EthereumContractCall,
    ) -> Box<Future<Item = Vec<Token>, Error = EthereumContractCallError>>;
}
