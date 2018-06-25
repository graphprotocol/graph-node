use futures::prelude::*;
use futures::sync::mpsc::Receiver;

/// A request for the state of a contract at a specific block hash and address.
pub struct EthereumContractStateRequest {
    pub address: String,
    pub block_hash: String,
}

/// An error that can occur when trying to obtain the state of a contract.
pub enum EthereumContractStateError {
    Failed,
}

/// Representation of an Ethereum contract state.
pub struct EthereumContractState {
    pub address: String,
    pub block_hash: String,
    pub data: String,
}

/// A range to allow event subscriptions to limit the block numbers to consider.
pub struct BlockNumberRange {
    pub from: Option<i64>,
    pub to: Option<i64>,
}

/// A subscription to a specific contract address, event signature and block range.
pub struct EthereumEventSubscription {
    /// An ID that uniquely identifies the subscription (e.g. a GUID).
    pub subscription_id: String,
    pub address: String,
    pub event_signature: String,
    pub range: BlockNumberRange,
}

/// An event logged for a specific contract address and event signature.
pub struct EthereumEvent {
    pub address: String,
    pub event_signature: String,
    pub block_hash: String,
    pub params: Vec<String>,
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
    ) -> Box<Future<Item = EthereumContractState, Error = EthereumContractStateError>>;

    /// Subscribe to an event of a smart contract.
    fn subscribe_to_event(
        &mut self,
        subscription: EthereumEventSubscription,
    ) -> Receiver<EthereumEvent>;

    /// Cancel a specific event subscription. Returns true when the subscription existed before.
    fn unsubscribe_from_event(&mut self, subscription_id: String) -> bool;
}
