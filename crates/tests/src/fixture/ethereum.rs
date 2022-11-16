use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use super::{
    test_ptr, NoopAdapterSelector, NoopRuntimeAdapter, StaticBlockRefetcher, StaticStreamBuilder,
    Stores, NODE_ID,
};
use graph::blockchain::{BlockPtr, TriggersAdapterSelector};
use graph::cheap_clone::CheapClone;
use graph::firehose::{FirehoseEndpoint, FirehoseEndpoints};
use graph::prelude::ethabi::ethereum_types::H256;
use graph::prelude::{LightEthereumBlock, LoggerFactory, NodeId};
use graph::{blockchain::block_stream::BlockWithTriggers, prelude::ethabi::ethereum_types::U64};
use graph_chain_ethereum::network::EthereumNetworkAdapters;
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
) -> Chain {
    let triggers_adapter = triggers_adapter.unwrap_or(Arc::new(NoopAdapterSelector {
        triggers_in_block_sleep: Duration::ZERO,
        x: PhantomData,
    }));
    let logger = graph::log::logger(true);
    let logger_factory = LoggerFactory::new(logger.cheap_clone(), None);
    let node_id = NodeId::new(NODE_ID).unwrap();
    let mock_registry = Arc::new(MockMetricsRegistry::new());

    let chain_store = stores.chain_store.cheap_clone();

    // This is needed bacause the stream builder only works for firehose and this will only be called if there
    // are > 1 firehose endpoints. The endpoint itself is never used because it's mocked.
    let firehose_endpoints: FirehoseEndpoints = vec![Arc::new(FirehoseEndpoint::new(
        "",
        "https://example.com",
        None,
        true,
        false,
    ))]
    .into();

    Chain::new(
        logger_factory.clone(),
        stores.network_name.clone(),
        node_id,
        mock_registry.clone(),
        chain_store.cheap_clone(),
        chain_store,
        firehose_endpoints,
        EthereumNetworkAdapters { adapters: vec![] },
        stores.chain_head_listener.cheap_clone(),
        Arc::new(StaticStreamBuilder { chain: blocks }),
        Arc::new(StaticBlockRefetcher { x: PhantomData }),
        triggers_adapter,
        Arc::new(NoopRuntimeAdapter { x: PhantomData }),
        ENV_VARS.reorg_threshold,
        // We assume the tested chain is always ingestible for now
        true,
    )
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

    BlockWithTriggers::<graph_chain_ethereum::Chain> {
        block: BlockFinality::Final(Arc::new(LightEthereumBlock {
            hash: Some(H256::from_slice(ptr.hash.as_slice())),
            number: Some(U64::from(ptr.number)),
            parent_hash: H256::from_slice(parent_ptr.hash.as_slice()),
            ..Default::default()
        })),
        trigger_data: vec![EthereumTrigger::Block(ptr, EthereumBlockTriggerType::Every)],
    }
}
