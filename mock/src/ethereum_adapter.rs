use std::collections::HashSet;

use graph::components::ethereum::*;
use graph::prelude::{
    ethabi, future,
    web3::types::{Block, Transaction, H256},
    Arc, Error, EthereumCallCache, Future, Logger,
};

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
    ) -> Box<dyn Future<Item = Block<Transaction>, Error = EthereumAdapterError> + Send> {
        unimplemented!();
    }

    fn block_by_hash(
        &self,
        _: &Logger,
        _: H256,
    ) -> Box<dyn Future<Item = Option<Block<Transaction>>, Error = Error> + Send> {
        unimplemented!();
    }

    fn load_full_block(
        &self,
        _: &Logger,
        _: Block<Transaction>,
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

    fn block_parent_hash(
        &self,
        _: &Logger,
        _: H256,
    ) -> Box<dyn Future<Item = Option<H256>, Error = Error> + Send> {
        unimplemented!();
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

    fn blocks_with_triggers(
        &self,
        _: &Logger,
        _: Arc<SubgraphEthRpcMetrics>,
        _: u64,
        _: u64,
        _: EthereumLogFilter,
        _: EthereumCallFilter,
        _: EthereumBlockFilter,
    ) -> Box<dyn Future<Item = Vec<EthereumBlockPointer>, Error = Error> + Send> {
        unimplemented!();
    }

    fn blocks_with_logs(
        &self,
        _: &Logger,
        _: Arc<SubgraphEthRpcMetrics>,
        _: u64,
        _: u64,
        _: EthereumLogFilter,
    ) -> Box<dyn Future<Item = Vec<EthereumBlockPointer>, Error = Error> + Send> {
        unimplemented!();
    }

    fn blocks_with_calls(
        &self,
        _: &Logger,
        _: Arc<SubgraphEthRpcMetrics>,
        _: u64,
        _: u64,
        _: EthereumCallFilter,
    ) -> Box<dyn Future<Item = HashSet<EthereumBlockPointer>, Error = Error> + Send> {
        unimplemented!();
    }

    fn blocks(
        &self,
        _: &Logger,
        _: Arc<SubgraphEthRpcMetrics>,
        _: u64,
        _: u64,
    ) -> Box<dyn Future<Item = Vec<EthereumBlockPointer>, Error = Error> + Send> {
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
}
