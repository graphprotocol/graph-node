use ethereum_types::{Address, H256};
use futures::future;
use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver};

use thegraph::components::ethereum::{EthereumWatcher as EthereumWatcherTrait, *};

pub struct EthereumWatcher;

impl EthereumWatcher {
    pub fn new() -> Self {
        EthereumWatcher {}
    }
}

impl EthereumWatcherTrait for EthereumWatcher {
    fn contract_state(
        &mut self,
        request: EthereumContractStateRequest,
    ) -> Result<EthereumContractState, EthereumContractStateError> {
        Ok(EthereumContractState {
            address: Address::new(),
            block_hash: H256::new(),
            data: Vec::new(),
        })
    }

    fn subscribe_to_event(
        &mut self,
        subscription: EthereumEventSubscription,
    ) -> Receiver<EthereumEvent> {
        let (sender, receiver) = channel(100);
        receiver
    }

    fn unsubscribe_from_event(&mut self, unique_id: String) -> bool {
        false
    }
}
