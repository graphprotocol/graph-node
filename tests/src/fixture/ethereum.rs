use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::{
    test_ptr, CommonChainConfig, MutexBlockStreamBuilder, NoopAdapterSelector,
    NoopRuntimeAdapterBuilder, StaticBlockRefetcher, StaticStreamBuilder, Stores, TestChain,
};
use graph::abi;
use graph::blockchain::block_stream::BlockWithTriggers;
use graph::blockchain::block_stream::{EntityOperationKind, EntitySourceOperation};
use graph::blockchain::client::ChainClient;
use graph::blockchain::{BlockPtr, Trigger, TriggersAdapterSelector};
use graph::cheap_clone::CheapClone;
use graph::data_source::subgraph;
use graph::prelude::alloy::primitives::{Address, B256, U256};
use graph::prelude::alloy::rpc::types::BlockTransactions;
use graph::prelude::{
    create_dummy_transaction, create_minimal_block_for_test, tiny_keccak, DeploymentHash, Entity,
    LightEthereumBlock, ENV_VARS,
};
use graph::schema::EntityType;
use graph_chain_ethereum::network::EthereumNetworkAdapters;
use graph_chain_ethereum::trigger::LogRef;
use graph_chain_ethereum::Chain;
use graph_chain_ethereum::{
    chain::BlockFinality,
    trigger::{EthereumBlockTriggerType, EthereumTrigger},
};

pub async fn chain(
    test_name: &str,
    blocks: Vec<BlockWithTriggers<Chain>>,
    stores: &Stores,
    triggers_adapter: Option<Arc<dyn TriggersAdapterSelector<Chain>>>,
) -> TestChain<Chain> {
    let triggers_adapter = triggers_adapter.unwrap_or(Arc::new(NoopAdapterSelector {
        triggers_in_block_sleep: Duration::ZERO,
        x: PhantomData,
    }));

    let CommonChainConfig {
        logger_factory,
        mock_registry,
        chain_store,
        firehose_endpoints,
    } = CommonChainConfig::new(test_name, stores).await;

    let client = Arc::new(ChainClient::<Chain>::new_firehose(firehose_endpoints));

    let static_block_stream = Arc::new(StaticStreamBuilder { chain: blocks });
    let block_stream_builder = Arc::new(MutexBlockStreamBuilder(Mutex::new(static_block_stream)));

    let eth_adapters = Arc::new(EthereumNetworkAdapters::empty_for_testing());

    let chain = Chain::new(
        logger_factory,
        stores.network_name.clone(),
        mock_registry,
        chain_store.cheap_clone(),
        chain_store,
        client,
        stores.chain_head_listener.cheap_clone(),
        block_stream_builder.clone(),
        Arc::new(StaticBlockRefetcher { x: PhantomData }),
        triggers_adapter,
        Arc::new(NoopRuntimeAdapterBuilder {}),
        eth_adapters,
        ENV_VARS.reorg_threshold(),
        ENV_VARS.ingestor_polling_interval,
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

    let block = create_minimal_block_for_test(ptr.number as u64, ptr.hash.as_b256());

    BlockWithTriggers::<graph_chain_ethereum::Chain> {
        block: BlockFinality::Final(Arc::new(LightEthereumBlock::new(block.into()))),
        trigger_data: vec![Trigger::Chain(EthereumTrigger::Block(
            ptr,
            EthereumBlockTriggerType::End,
        ))],
    }
}

pub fn generate_empty_blocks_for_range(
    parent_ptr: BlockPtr,
    start: i32,
    end: i32,
    add_to_hash: u64, // Use to differentiate forks
) -> Vec<BlockWithTriggers<Chain>> {
    let mut blocks: Vec<BlockWithTriggers<Chain>> = vec![];

    for i in start..(end + 1) {
        let parent_ptr = blocks.last().map(|b| b.ptr()).unwrap_or(parent_ptr.clone());
        let ptr = BlockPtr {
            number: i,
            hash: B256::from(U256::from(i as u64 + add_to_hash)).into(),
        };
        blocks.push(empty_block(parent_ptr, ptr));
    }

    blocks
}

pub fn empty_block(parent_ptr: BlockPtr, ptr: BlockPtr) -> BlockWithTriggers<Chain> {
    assert!(ptr != parent_ptr);
    assert!(ptr.number > parent_ptr.number);

    let dummy_txn =
        create_dummy_transaction(ptr.number as u64, ptr.hash.as_b256(), Some(0), B256::ZERO);
    let transactions = BlockTransactions::Full(vec![dummy_txn]);
    let alloy_block = create_minimal_block_for_test(ptr.number as u64, ptr.hash.as_b256())
        .map_header(|mut header| {
            // Ensure the parent hash matches the given parent_ptr so that parent_ptr() lookups succeed
            header.inner.parent_hash = parent_ptr.hash.as_b256();
            header
        })
        .with_transactions(transactions);

    BlockWithTriggers::<graph_chain_ethereum::Chain> {
        block: BlockFinality::Final(Arc::new(LightEthereumBlock::new(alloy_block.into()))),
        trigger_data: vec![Trigger::Chain(EthereumTrigger::Block(
            ptr,
            EthereumBlockTriggerType::End,
        ))],
    }
}

pub fn push_test_log(block: &mut BlockWithTriggers<Chain>, payload: impl Into<String>) {
    use graph::prelude::alloy::{self, primitives::LogData, rpc::types::Log};

    let log = Arc::new(Log {
        inner: alloy::primitives::Log {
            address: Address::ZERO,
            data: LogData::new_unchecked(
                vec![tiny_keccak::keccak256(b"TestEvent(string)").into()],
                abi::DynSolValue::String(payload.into()).abi_encode().into(),
            ),
        },
        block_hash: Some(B256::from_slice(block.ptr().hash.as_slice())),
        block_number: Some(block.ptr().number as u64),
        transaction_hash: Some(B256::from(U256::from(0))),
        transaction_index: Some(0),
        log_index: Some(0),
        block_timestamp: None,
        removed: false,
    });

    block
        .trigger_data
        .push(Trigger::Chain(EthereumTrigger::Log(LogRef::FullLog(
            log, None,
        ))))
}

pub fn push_test_subgraph_trigger(
    block: &mut BlockWithTriggers<Chain>,
    source: DeploymentHash,
    entity: Entity,
    entity_type: EntityType,
    entity_op: EntityOperationKind,
    vid: i64,
    source_idx: u32,
) {
    let entity = EntitySourceOperation {
        entity: entity,
        entity_type: entity_type,
        entity_op: entity_op,
        vid,
    };

    block
        .trigger_data
        .push(Trigger::Subgraph(subgraph::TriggerData {
            source,
            entity,
            source_idx,
        }));
}

pub fn push_test_command(
    block: &mut BlockWithTriggers<Chain>,
    test_command: impl Into<String>,
    data: impl Into<String>,
) {
    use graph::prelude::alloy::{self, primitives::LogData, rpc::types::Log};

    let log = Arc::new(Log {
        inner: alloy::primitives::Log {
            address: Address::ZERO,
            data: LogData::new_unchecked(
                vec![tiny_keccak::keccak256(b"TestEvent(string,string)").into()],
                abi::DynSolValue::Tuple(vec![
                    abi::DynSolValue::String(test_command.into()),
                    abi::DynSolValue::String(data.into()),
                ])
                .abi_encode_params()
                .into(),
            ),
        },
        block_hash: Some(block.ptr().hash.as_b256()),
        block_number: Some(block.ptr().number as u64),
        transaction_hash: Some(B256::from(U256::from(0))),
        transaction_index: Some(0),
        log_index: Some(0),
        block_timestamp: None,
        removed: false,
    });

    block
        .trigger_data
        .push(Trigger::Chain(EthereumTrigger::Log(LogRef::FullLog(
            log, None,
        ))))
}

pub fn push_test_polling_trigger(block: &mut BlockWithTriggers<Chain>) {
    block
        .trigger_data
        .push(Trigger::Chain(EthereumTrigger::Block(
            block.ptr(),
            EthereumBlockTriggerType::End,
        )))
}
