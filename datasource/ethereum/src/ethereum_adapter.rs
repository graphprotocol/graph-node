use failure::Error;
use futures::future;
use futures::prelude::*;
use futures::stream::iter_ok;
use std::sync::Arc;
use std::time::Duration;

use graph::components::ethereum::{EthereumAdapter as EthereumAdapterTrait, *};
use graph::ethabi::{RawLog, Token};
use graph::web3;
use graph::web3::api::{CreateFilter, Eth, Web3};
use graph::web3::helpers::CallFuture;
use graph::web3::types::*;

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

    pub fn block_number(&self) -> CallFuture<U256, T::Out> {
        self.eth_client.eth().block_number()
    }

    pub fn sha3(&self, data: &str) -> CallFuture<H256, T::Out> {
        self.eth_client.web3().sha3(Bytes::from(data))
    }

    pub fn event_filter(&self, subscription: EthereumEventSubscription) -> CreateFilter<T, Log> {
        let filter_builder = FilterBuilder::default();
        let eth_filter: Filter = filter_builder
            .address(vec![subscription.address])
            .from_block(subscription.range.from)
            .to_block(subscription.range.to)
            .topics(Some(vec![subscription.event.signature()]), None, None, None)
            .build();
        self.eth_client.eth_filter().create_logs_filter(eth_filter)
    }

    pub fn block(eth: Eth<T>, block_id: BlockId) -> impl Future<Item = Block<H256>, Error = Error> {
        eth.block(block_id)
            .map_err(|e| format_err!("could not get block from Ethereum: {}", e))
            .and_then(|block| block.ok_or(format_err!("no block returned from Ethereum")))
    }

    fn call(
        eth: Eth<T>,
        contract_address: Address,
        call_data: Bytes,
        block_number: Option<BlockNumber>,
    ) -> CallFuture<Bytes, T::Out> {
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
}
