use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::{
    test_ptr, MutexBlockStreamBuilder, NoopAdapterSelector, NoopRuntimeAdapter,
    StaticBlockRefetcher, StaticStreamBuilder, Stores, TestChain, NODE_ID,
};
use graph::blockchain::client::ChainClient;
use graph::blockchain::{BlockPtr, TriggersAdapterSelector};
use graph::cheap_clone::CheapClone;
use graph::firehose::{FirehoseEndpoint, FirehoseEndpoints, SubgraphLimit};
use graph::prelude::ethabi::ethereum_types::H256;
use graph::prelude::web3::types::{Address, Log, Transaction, H160};
use graph::prelude::{ethabi, tiny_keccak, LightEthereumBlock, LoggerFactory, NodeId};
use graph::{blockchain::block_stream::BlockWithTriggers, prelude::ethabi::ethereum_types::U64};
use graph_chain_ethereum::{
    chain::BlockFinality,
    trigger::{EthereumBlockTriggerType, EthereumTrigger},
};
use graph_chain_ethereum::{Chain, ENV_VARS};
use graph_mock::MockMetricsRegistry;

pub async fn chain(
    blocks: Vec<BlockWithTriggers<Chain>>,
    stores: &Stores,
    triggers_adapter: Option<Arc<dyn TriggersAdapterSelector<Chain>>>,
) -> TestChain<Chain> {
    let triggers_adapter = triggers_adapter.unwrap_or(Arc::new(NoopAdapterSelector {
        triggers_in_block_sleep: Duration::ZERO,
        x: PhantomData,
    }));
    let logger = graph::log::logger(true);
    let mock_registry = Arc::new(MockMetricsRegistry::new());
    let logger_factory = LoggerFactory::new(logger.cheap_clone(), None, mock_registry.clone());
    let node_id = NodeId::new(NODE_ID).unwrap();

    let chain_store = stores.chain_store.cheap_clone();

    // This is needed bacause the stream builder only works for firehose and this will only be called if there
    // are > 1 firehose endpoints. The endpoint itself is never used because it's mocked.
    let firehose_endpoints: FirehoseEndpoints = vec![Arc::new(FirehoseEndpoint::new(
        "",
        "https://example.com",
        None,
        true,
        false,
        SubgraphLimit::Unlimited,
    ))]
    .into();

    let client = Arc::new(ChainClient::<Chain>::new_firehose(firehose_endpoints));

    let static_block_stream = Arc::new(StaticStreamBuilder { chain: blocks });
    let block_stream_builder = Arc::new(MutexBlockStreamBuilder(Mutex::new(static_block_stream)));

    let chain = Chain::new(
        logger_factory.clone(),
        stores.network_name.clone(),
        node_id,
        mock_registry.clone(),
        chain_store.cheap_clone(),
        chain_store,
        client,
        stores.chain_head_listener.cheap_clone(),
        block_stream_builder.clone(),
        Arc::new(StaticBlockRefetcher { x: PhantomData }),
        triggers_adapter,
        Arc::new(NoopRuntimeAdapter { x: PhantomData }),
        ENV_VARS.reorg_threshold,
        // We assume the tested chain is always ingestible for now
        true,
    );

    TestChain {
        chain: Arc::new(chain),
        block_stream_builder,
    }
}

pub fn genesis() -> BlockWithTriggers<graph_chain_ethereum::Chain> {
    let ptr = test_ptr(0);
    BlockWithTriggers::<graph_chain_ethereum::Chain> {
        block: BlockFinality::Final(Arc::new(LightEthereumBlock {
            hash: Some(H256::from_slice(ptr.hash.as_slice())),
            number: Some(U64::from(ptr.number)),
            ..Default::default()
        })),
        trigger_data: vec![EthereumTrigger::Block(ptr, EthereumBlockTriggerType::Every)],
    }
}

pub fn empty_block(
    parent_ptr: BlockPtr,
    ptr: BlockPtr,
) -> BlockWithTriggers<graph_chain_ethereum::Chain> {
    assert!(ptr != parent_ptr);
    assert!(ptr.number > parent_ptr.number);

    // A 0x000.. transaction is used so `push_test_log` can use it
    let transactions = vec![Transaction {
        hash: H256::zero(),
        block_hash: Some(H256::from_slice(ptr.hash.as_slice().into())),
        block_number: Some(ptr.number.into()),
        transaction_index: Some(0.into()),
        from: Some(H160::zero()),
        to: Some(H160::zero()),
        ..Default::default()
    }];

    BlockWithTriggers::<graph_chain_ethereum::Chain> {
        block: BlockFinality::Final(Arc::new(LightEthereumBlock {
            hash: Some(H256::from_slice(ptr.hash.as_slice())),
            number: Some(U64::from(ptr.number)),
            parent_hash: H256::from_slice(parent_ptr.hash.as_slice()),
            transactions,
            ..Default::default()
        })),
        trigger_data: vec![EthereumTrigger::Block(ptr, EthereumBlockTriggerType::Every)],
    }
}

pub fn push_test_log(block: &mut BlockWithTriggers<Chain>, payload: impl Into<String>) {
    block.trigger_data.push(EthereumTrigger::Log(
        Arc::new(Log {
            address: Address::zero(),
            topics: vec![tiny_keccak::keccak256(b"TestEvent(string)").into()],
            data: ethabi::encode(&[ethabi::Token::String(payload.into())]).into(),
            block_hash: Some(H256::from_slice(block.ptr().hash.as_slice())),
            block_number: Some(block.ptr().number.into()),
            transaction_hash: Some(H256::from_low_u64_be(0).into()),
            transaction_index: Some(0.into()),
            log_index: Some(0.into()),
            transaction_log_index: Some(0.into()),
            log_type: None,
            removed: None,
        }),
        None,
    ))
}
