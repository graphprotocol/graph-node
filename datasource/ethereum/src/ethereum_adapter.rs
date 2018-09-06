use ethabi::{RawLog, Token};
use ethereum_types::H256;
use futures::future;
use futures::prelude::*;
use std::sync::Arc;
use web3;
use web3::api::{Eth, Web3};
use web3::helpers::CallResult;
use web3::types::{Filter, *};

use graph::components::ethereum::{EthereumAdapter as EthereumAdapterTrait, *};
use graph::components::store::EthereumBlockPointer;
use graph::prelude::*;

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

impl<T> EthereumAdapterTrait for EthereumAdapter<T>
where
    T: web3::Transport + Send + Sync + 'static,
    T::Out: Send,
{
    fn block_by_hash(
        &self,
        block_hash: H256,
    ) -> Box<Future<Item = Block<Transaction>, Error = Error> + Send> {
        Box::new(
            self.eth_client
                .eth()
                .block_with_txs(BlockId::Hash(block_hash))
                .map_err(SyncFailure::new)
                .from_err(),
        )
    }

    fn block_by_number(
        &self,
        block_number: u64,
    ) -> Box<Future<Item = Block<Transaction>, Error = Error> + Send> {
        Box::new(
            self.eth_client
                .eth()
                .block_with_txs(BlockId::Number(block_number.into()))
                .map_err(SyncFailure::new)
                .from_err(),
        )
    }

    fn is_on_main_chain(
        &self,
        block_ptr: EthereumBlockPointer,
    ) -> Box<Future<Item = bool, Error = Error> + Send> {
        Box::new(
            self.eth_client
                .eth()
                .block(BlockId::Number(block_ptr.number.into()))
                .map(move |b| b.hash.unwrap() == block_ptr.hash)
                .map_err(SyncFailure::new)
                .from_err(),
        )
    }

    fn find_first_block_with_event(
        &self,
        from: u64,
        to: u64,
        event_filter: EthereumEventFilter,
    ) -> Box<Future<Item = Option<EthereumBlockPointer>, Error = Error> + Send> {
        // TODO use an incremental search instead of one large query

        // Find all event sigs
        let event_sigs = event_filter
            .event_types_by_contract_address_and_sig
            .values()
            .flat_map(|event_types_by_sig| event_types_by_sig.keys())
            .map(|sig| sig.to_owned())
            .collect::<Vec<H256>>();

        // Create a log filter
        // Note: this filter will have false positives
        let log_filter: Filter = FilterBuilder::default()
            .from_block(from.into())
            .to_block(to.into())
            .topics(Some(event_sigs), None, None, None)
            .build();

        Box::new(
            self.eth_client
                .eth()
                .logs(log_filter)
                .map(move |logs| {
                    logs.into_iter()
                    // Filter out false positives
                    .filter(|log| event_filter.match_event(log).is_some())

                    // Get first result (if any)
                    .next()

                    // Get block ptr from log (that's all we care about)
                    .map(|log| {
                        let hash = log
                            .block_hash
                            .expect("log from Eth node is missing block hash");
                        let number = log
                            .block_number
                            .expect("log from Eth node is missing block number");
                        (hash, number.as_u64()).into()
                    })
                })
                .map_err(SyncFailure::new)
                .from_err(),
        )
    }

    // TODO investigate storing receipts in DB and moving this fn to BlockStore
    fn get_events_in_block<'a>(
        &'a self,
        block: Block<Transaction>,
        event_filter: EthereumEventFilter,
    ) -> Box<Stream<Item = EthereumEvent, Error = EthereumSubscriptionError> + 'a> {
        if !event_filter.check_bloom(block.logs_bloom) {
            return Box::new(stream::empty());
        }

        let tx_receipt_futures = block.transactions.into_iter().map(|tx| {
            self.eth_client
                .eth()
                .transaction_receipt(tx.hash)
                .map(move |opt| opt.expect(&format!("missing receipt for TX {:?}", tx.hash)))
                .map_err(EthereumSubscriptionError::from)
        });

        Box::new(
            stream::futures_ordered(tx_receipt_futures)
                .map(move |receipt| {
                    let event_filter = event_filter.clone();

                    stream::iter_result(receipt.logs.into_iter().filter_map(move |log| {
                        // Check log against event filter
                        event_filter
                                .match_event(&log)

                                // If matched: convert Log into an EthereumEvent
                                .map(|event_type| {
                                    // Try to parse log data into an Ethereum event
                                    event_type
                                        .parse_log(RawLog {
                                            topics: log.topics.clone(),
                                            data: log.data.0.clone(),
                                        })
                                        .map_err(EthereumSubscriptionError::from)
                                        .map(|log_data| EthereumEvent {
                                            address: log.address,
                                            event_signature: log.topics[0],
                                            block_hash: log.block_hash.unwrap(),
                                            params: log_data.params,
                                            removed: log.is_removed(), // TODO is this obsolete?
                                        })
                                })
                    }))
                })
                .flatten(),
        )
    }

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
