use graph::components::ethereum::*;
use graph::prelude::{
    ethabi, future,
    web3::types::{Log, H256},
    Arc, ChainStore, Error, EthereumCallCache, Future, Logger, Stream,
};
use std::collections::HashSet;

#[derive(Default)]
pub struct MockEthereumAdapter {}

impl EthereumAdapter for MockEthereumAdapter {
    fn net_identifiers(
        &self,
        _: &Logger,
    ) -> Box<dyn Future<Item = EthereumNetworkIdentifier, Error = Error> + Send> {
        unimplemented!();
    }

    fn latest_block(
        &self,
        _: &Logger,
    ) -> Box<dyn Future<Item = LightEthereumBlock, Error = EthereumAdapterError> + Send> {
        unimplemented!();
    }

    fn load_block(
        &self,
        _: &Logger,
        _: H256,
    ) -> Box<dyn Future<Item = LightEthereumBlock, Error = Error> + Send> {
        unimplemented!()
    }

    fn block_by_hash(
        &self,
        _: &Logger,
        _: H256,
    ) -> Box<dyn Future<Item = Option<LightEthereumBlock>, Error = Error> + Send> {
        unimplemented!();
    }

    fn load_full_block(
        &self,
        _: &Logger,
        _: LightEthereumBlock,
    ) -> Box<dyn Future<Item = EthereumBlock, Error = EthereumAdapterError> + Send> {
        unimplemented!();
    }

    fn block_pointer_from_number(
        &self,
        _: &Logger,
        _: u64,
    ) -> Box<dyn Future<Item = EthereumBlockPointer, Error = EthereumAdapterError> + Send> {
        Box::new(future::ok(EthereumBlockPointer::from((
            H256::zero(),
            0 as i64,
        ))))
    }

    fn block_hash_by_block_number(
        &self,
        _: &Logger,
        _: u64,
    ) -> Box<dyn Future<Item = Option<H256>, Error = Error> + Send> {
        unimplemented!();
    }

    fn is_on_main_chain(
        &self,
        _: &Logger,
        _: Arc<SubgraphEthRpcMetrics>,
        _: EthereumBlockPointer,
    ) -> Box<dyn Future<Item = bool, Error = Error> + Send> {
        unimplemented!();
    }

    fn calls_in_block(
        &self,
        _: &Logger,
        _: Arc<SubgraphEthRpcMetrics>,
        _: u64,
        _: H256,
    ) -> Box<dyn Future<Item = Vec<EthereumCall>, Error = Error> + Send> {
        unimplemented!();
    }

    fn logs_in_block_range(
        &self,
        _: &Logger,
        _: Arc<SubgraphEthRpcMetrics>,
        _: u64,
        _: u64,
        _: EthereumLogFilter,
    ) -> Box<dyn Future<Item = Vec<Log>, Error = Error> + Send> {
        unimplemented!();
    }

    fn calls_in_block_range(
        &self,
        _: &Logger,
        _: Arc<SubgraphEthRpcMetrics>,
        _: u64,
        _: u64,
        _: EthereumCallFilter,
    ) -> Box<dyn Stream<Item = EthereumCall, Error = Error> + Send> {
        unimplemented!();
    }

    fn contract_call(
        &self,
        _: &Logger,
        _: EthereumContractCall,
        _: Arc<dyn EthereumCallCache>,
    ) -> Box<dyn Future<Item = Vec<ethabi::Token>, Error = EthereumContractCallError> + Send> {
        unimplemented!();
    }

    fn triggers_in_block(
        self: Arc<Self>,
        _: Logger,
        _: Arc<dyn ChainStore>,
        _: Arc<SubgraphEthRpcMetrics>,
        _: EthereumLogFilter,
        _: EthereumCallFilter,
        _: EthereumBlockFilter,
        _: BlockFinality,
    ) -> Box<dyn Future<Item = EthereumBlockWithTriggers, Error = Error> + Send> {
        unimplemented!();
    }

    /// Load Ethereum blocks in bulk, returning results as they come back as a Stream.
    fn load_blocks(
        &self,
        _: Logger,
        _: Arc<dyn ChainStore>,
        _: HashSet<H256>,
    ) -> Box<dyn Stream<Item = LightEthereumBlock, Error = Error> + Send> {
        unimplemented!()
    }

    fn block_range_to_ptrs(
        &self,
        _: Logger,
        _: u64,
        _: u64,
    ) -> Box<dyn Future<Item = Vec<EthereumBlockPointer>, Error = Error> + Send> {
        unimplemented!()
    }
}
