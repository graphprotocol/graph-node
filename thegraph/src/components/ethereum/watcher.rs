use futures::prelude::*;
use futures::sync::mpsc::Receiver;

pub struct EthereumContractStateRequest {
    pub address: String,
    pub block_hash: String,
}

pub enum EthereumContractStateError {
    Failed,
}

pub struct EthereumContractState {
    pub address: String,
    pub block_hash: String,
    pub data: String,
}

pub struct BlockNumberRange {
    pub from: Option<i64>,
    pub to: Option<i64>,
}

pub struct EthereumEventSubscription {
    pub unique_id: String,
    pub address: String,
    pub event: String,
    pub range: BlockNumberRange,
}

pub struct EthereumEvent {
    pub address: String,
    pub event: String,
    pub block_hash: String,
}

pub trait EthereumWatcher {
    fn contract_state(
        &mut self,
        request: EthereumContractStateRequest,
    ) -> Box<Future<Item = EthereumContractState, Error = EthereumContractStateError>>;

    fn subscribe_to_event(
        &mut self,
        subscription: EthereumEventSubscription,
    ) -> Receiver<EthereumEvent>;

    fn unsubscribe_from_event(&mut self, unique_id: String) -> bool;
}
