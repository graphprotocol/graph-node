use ethabi::{RawLog, Token};
use ethereum_types::H256;
use futures::future;
use futures::prelude::*;
use futures::stream::iter_ok;
use std::sync::Arc;
use std::time::Duration;
use web3;
use web3::api::CreateFilter;
use web3::api::{Eth, Web3};
use web3::helpers::CallResult;
use web3::types::*;

use graph::components::ethereum::{EthereumAdapter as EthereumAdapterTrait, *};

pub struct EthereumAdapterConfig<T: web3::Transport> {
    pub transport: T,
}

pub struct EthereumAdapter<T: web3::Transport> {
    eth_client: Arc<Web3<T>>,
}

impl<T: web3::Transport> EthereumAdapter<T> {
    pub fn new(config: EthereumAdapterConfig<T>) -> Self {
        EthereumAdapter {
            eth_client: Arc::new(Web3::new(config.transport)),
        }
    }

    pub fn event_filter(&self, subscription: EthereumEventSubscription) -> CreateFilter<T, Log> {
        let filter_builder = FilterBuilder::default();
        let eth_filter: Filter = filter_builder
            .from_block(subscription.range.from)
            .to_block(subscription.range.to)
            .topics(Some(vec![subscription.event.signature()]), None, None, None)
            .build();
        self.eth_client.eth_filter().create_logs_filter(eth_filter)
    }

    pub fn block(eth: Eth<T>, block_id: BlockId) -> CallResult<Block<H256>, T::Out> {
        eth.block(block_id)
    }

    fn call(
        eth: Eth<T>,
        contract_address: Address,
        call_data: Bytes,
        block_number: Option<BlockNumber>,
    ) -> CallResult<Bytes, T::Out> {
        let req = CallRequest {
            from: None,
            to: contract_address,
            gas: None,
            gas_price: None,
            value: None,
            data: Some(call_data),
        };
        eth.call(req, block_number)
    }
}

impl<T: web3::Transport + Send + Sync + 'static> EthereumAdapterTrait for EthereumAdapter<T> {
    fn contract_call(
        &mut self,
        call: EthereumContractCall,
    ) -> Box<Future<Item = Vec<Token>, Error = EthereumContractCallError>> {
        // Emit custom error for type mismatches.
        for (token, kind) in call
            .args
            .iter()
            .zip(call.function.inputs.iter().map(|p| &p.kind))
        {
            if !token.type_check(kind) {
                return Box::new(future::err(EthereumContractCallError::TypeError(
                    token.clone(),
                    kind.clone(),
                )));
            }
        }

        // Obtain a handle on the Ethereum client
        let eth_client = self.eth_client.clone();

        // Prepare for the function call, encoding the call parameters according
        // to the ABI
        let call_address = call.address;
        let call_data = call.function.encode_input(&call.args).unwrap();

        Box::new(
            // Resolve the block ID into a block number
            Self::block(eth_client.eth(), call.block_id.clone())
                .map_err(EthereumContractCallError::from)
                .and_then(move |block| {
                    // Make the actual function call
                    Self::call(
                        eth_client.eth(),
                        call_address,
                        Bytes(call_data),
                        block
                            .number
                            .map(|number| number.as_u64())
                            .map(BlockNumber::Number),
                    ).map_err(EthereumContractCallError::from)
                })
                // Decode the return values according to the ABI
                .and_then(move |output| {
                    call.function
                        .decode_output(&output.0)
                        .map_err(EthereumContractCallError::from)
                }),
        )
    }

    fn subscribe_to_event(
        &mut self,
        subscription: EthereumEventSubscription,
    ) -> Box<Stream<Item = EthereumEvent, Error = EthereumSubscriptionError>> {
        let event = subscription.event.clone();
        Box::new(
            self.event_filter(subscription)
                .map_err(EthereumSubscriptionError::from)
                .map(|base_filter| {
                    let past_logs_stream = base_filter
                        .logs()
                        .map_err(EthereumSubscriptionError::from)
                        .map(|logs_vec| iter_ok::<_, EthereumSubscriptionError>(logs_vec))
                        .flatten_stream();
                    let future_logs_stream = base_filter
                        .stream(Duration::from_millis(2000))
                        .map_err(EthereumSubscriptionError::from);
                    past_logs_stream.chain(future_logs_stream)
                })
                .flatten_stream()
                .and_then(move |log| {
                    event
                        .parse_log(RawLog {
                            topics: log.topics.clone(),
                            data: log.clone().data.0,
                        })
                        .map_err(EthereumSubscriptionError::from)
                        .map(|log_data| (log, log_data))
                })
                .map(move |(log, log_data)| EthereumEvent {
                    address: log.address,
                    event_signature: log.topics[0],
                    block_hash: log.block_hash.unwrap(),
                    params: log_data.params,
                    removed: log.is_removed(),
                }),
        )
    }

    fn unsubscribe_from_event(&mut self, _unique_id: String) -> bool {
        false
    }
}
