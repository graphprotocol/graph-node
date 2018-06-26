use ethabi::RawLog;
use futures::prelude::*;
use futures::stream::iter_ok;
use std::time::Duration;
use thegraph::components::ethereum::{EthereumAdapter as EthereumAdapterTrait, *};
use tokio_core::reactor::Handle;
use web3;
use web3::api::CreateFilter;
use web3::api::Web3;
use web3::error::Error as Web3Error;
use web3::helpers::CallResult;
use web3::types::*;

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

    pub fn sha3(&self, data: &str) -> CallResult<H256, T::Out> {
        self.eth_client.web3().sha3(Bytes::from(data))
    }

    pub fn event_filter(&self, subscription: EthereumEventSubscription) -> CreateFilter<T, Log> {
        let filter_builder = FilterBuilder::default();
        let eth_filter: Filter = filter_builder
            .from_block(subscription.range.from)
            .to_block(subscription.range.to)
            .topics(Some(vec![subscription.event_signature]), None, None, None)
            .build();
        self.eth_client.eth_filter().create_logs_filter(eth_filter)
    }
}

impl<T: 'static + web3::Transport> EthereumAdapterTrait for EthereumAdapter<T> {
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
    ) -> Box<Stream<Item = EthereumEvent, Error = Web3Error>> {
        let event = subscription.event.clone();
        Box::new(
            self.event_filter(subscription)
                .map(|base_filter| {
                    let past_logs_stream = base_filter
                        .logs()
                        .map(|logs_vec| iter_ok::<_, web3::error::Error>(logs_vec))
                        .flatten_stream();
                    let future_logs_stream = base_filter.stream(Duration::from_millis(2000));
                    past_logs_stream.chain(future_logs_stream)
                })
                .flatten_stream()
                .map(move |log| EthereumEvent {
                    address: log.address,
                    event_signature: log.topics[0],
                    block_hash: log.block_hash.unwrap(),
                    params: event
                        .parse_log(RawLog {
                            topics: log.topics.clone(),
                            data: log.data.0,
                        })
                        .unwrap()
                        .params,
                }),
        )
    }

    fn unsubscribe_from_event(&mut self, unique_id: String) -> bool {
        false
    }
}
