use ethabi::{Bytes, EventParam};
use ethereum_types::{Address, H256};
use futures::sync::mpsc::Receiver;

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

/// A range to allow event subscriptions to limit the block numbers to consider.
#[derive(Debug)]
pub struct BlockNumberRange {
    pub from: Option<u64>,
    pub to: Option<u64>,
}

/// A subscription to a specific contract address, event signature and block range.
#[derive(Debug)]
pub struct EthereumEventSubscription {
    /// An ID that uniquely identifies the subscription (e.g. a GUID).
    pub subscription_id: String,
    pub address: Address,
    pub event_signature: String,
    pub range: BlockNumberRange,
}

/// An event logged for a specific contract address and event signature.
#[derive(Debug)]
pub struct EthereumEvent {
    pub address: Address,
    pub event_signature: H256,
    pub block_hash: H256,
    pub params: Vec<EventParam>,
}

/// Common trait for components that watch and manage access to Ethereum.
///
/// Implementations may be implemented against an in-process Ethereum node
/// or a remote node over RPC.
pub trait EthereumWatcher {
    /// Obtain the state of a smart contract.
    fn contract_state(
        &mut self,
        request: EthereumContractStateRequest,
    ) -> Result<EthereumContractState, EthereumContractStateError>;

    /// Subscribe to an event of a smart contract.
    fn subscribe_to_event(
        &mut self,
        subscription: EthereumEventSubscription,
    ) -> Receiver<EthereumEvent>;

    /// Cancel a specific event subscription. Returns true when the subscription existed before.
    fn unsubscribe_from_event(&mut self, subscription_id: String) -> bool;
}
