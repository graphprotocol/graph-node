use ethereum_types::{Address, H256};
use futures::future;
use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver};
use tokio_core::reactor::Handle;
use web3;
use web3::api::Web3;
use web3::helpers::CallResult;
use web3::transports;
use web3::types::U256;

use thegraph::components::ethereum::{EthereumAdapter as EthereumAdapterTrait, *};

pub struct EthereumAdapterConfig<T: web3::Transport> {
    pub transport: T,
}

pub struct EthereumAdapter<T: web3::Transport> {
    eth_client: Web3<T>,
    runtime: Handle,
}

impl<T: web3::Transport> EthereumAdapter<T> {
    pub fn new(config: EthereumAdapterConfig<T>, runtime: Handle) -> Self {
        EthereumAdapter {
            eth_client: Web3::new(config.transport),
            runtime: runtime,
        }
    }

    pub fn block_number(&self) -> CallResult<U256, T::Out> {
        self.eth_client.eth().block_number()
    }
}

impl<T: web3::Transport> EthereumAdapterTrait for EthereumAdapter<T> {
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
