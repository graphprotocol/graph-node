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
    ) -> Box<Future<Item = EthereumContractState, Error = EthereumContractStateError>> {
        Box::new(future::ok(EthereumContractState {
            address: String::from("123123"),
            block_hash: String::from("123123"),
            data: String::from("data"),
        }))
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
