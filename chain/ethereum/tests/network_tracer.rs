#[macro_use]
extern crate pretty_assertions;

use diesel::connection::Connection;
use diesel::pg::PgConnection;
use std::collections::VecDeque;
use std::sync::Mutex;

use graph::mock::*;
use graph::prelude::*;
use graph_chain_ethereum::network_indexer::{
    ensure_subgraph_exists, BlockWithUncles, NetworkTracer, NetworkTracerEvent,
};
use graph_store_postgres::Store as DieselStore;
use web3::types::H256;

use test_store::*;

// Helper to wipe the store clean.
fn remove_test_data(store: Arc<DieselStore>) {
    let url = postgres_test_url();
    let conn = PgConnection::establish(url.as_str()).expect("Failed to connect to Postgres");
    graph_store_postgres::store::delete_all_entities_for_test_use_only(&store, &conn)
        .expect("Failed to remove entity test data");
}

// Helper to run tests against a clean store.
/// Test harness for running database integration tests.
fn run_test<R, F>(test: F)
where
    F: FnOnce(Arc<DieselStore>) -> R + Send + 'static,
    R: IntoFuture<Item = ()> + Send + 'static,
    R::Error: Send + Debug,
    R::Future: Send,
{
    let store = STORE.clone();

    // Lock regardless of poisoning. This also forces sequential test execution.
    let mut runtime = match STORE_RUNTIME.lock() {
        Ok(guard) => guard,
        Err(err) => err.into_inner(),
    };

    runtime
        .block_on(future::lazy(move || {
            // Reset store before running
            remove_test_data(store.clone());

            // Run test
            test(store.clone())
        }))
        .expect("failed to run test with clean store");
}

// Helper to create a sequence of linked blocks.
fn create_mock_blocks(n: u64) -> Vec<BlockWithUncles> {
    (0..n).fold(vec![], |mut blocks, number| {
        let mut block = BlockWithUncles::default();

        // Use the index as the block number
        block.block.block.number = Some(number.into());

        // Use a random hash as the block hash (should be unique)
        block.block.block.hash = Some(H256::random());

        // Set the parent hash for all blocks but the genesis block
        if number > 0 {
            block.block.block.parent_hash = blocks
                .last()
                .unwrap()
                .block
                .block
                .hash
                .clone()
                .unwrap_or(H256::zero());
        }

        blocks.push(block);
        blocks
    })
}

fn create_mock_ethereum_adapter(
    blocks: Vec<Vec<BlockWithUncles>>,
) -> (
    Arc<MockEthereumAdapter>,
    Box<dyn Fn(Vec<Vec<BlockWithUncles>>)>,
) {
    // Pull remote heads from the block sequences
    let remote_heads = Arc::new(Mutex::new(
        blocks
            .iter()
            .map(|seq| seq.last().unwrap().clone())
            .collect::<VecDeque<_>>(),
    ));

    // Flatten the block sequences into one
    let blocks = Arc::new(Mutex::new(blocks.into_iter().flatten().collect::<Vec<_>>()));

    // Prepare a function that can replace the chain head and all blocks
    // with a new version of the chain
    let remote_heads_for_reorg_trigger = remote_heads.clone();
    let blocks_for_reorg_trigger = blocks.clone();
    let reorg_trigger = move |new_blocks: Vec<Vec<BlockWithUncles>>| {
        let remote_heads = remote_heads_for_reorg_trigger.clone();
        let blocks = blocks_for_reorg_trigger.clone();

        let mut remote_heads = remote_heads.lock().unwrap();
        let mut blocks = blocks.lock().unwrap();

        // Add the new remote heads
        remote_heads.clear();
        remote_heads.extend(
            new_blocks
                .iter()
                .map(|seq| seq.last().unwrap().clone())
                .collect::<Vec<_>>(),
        );

        // Replace the blocks
        blocks.clear();
        blocks.extend(new_blocks.into_iter().flatten().collect::<Vec<_>>());
    };

    // Create the mock Ethereum adapter.
    let mut adapter = MockEthereumAdapter::new();

    // Make it so that each remote head block can only be polled once.
    // This is to simulate that the network is alive and new blocks are
    // added.
    adapter.expect_latest_block().returning(move |_: &Logger| {
        Box::new(future::result(
            remote_heads
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| format_err!("exhausted remote head blocks"))
                .map_err(|e| e.into())
                .map(|block| block.block.block),
        ))
    });

    let blocks_for_block_by_number = blocks.clone();
    adapter
        .expect_block_by_number()
        .returning(move |_, number: u64| {
            Box::new(future::ok(Some(
                blocks_for_block_by_number
                    .lock()
                    .unwrap()
                    .get(number as usize)
                    .expect(format!("block with number {} not found", number).as_str())
                    .clone()
                    .block
                    .block,
            )))
        });

    let blocks_for_load_full_block = blocks.clone();
    adapter
        .expect_load_full_block()
        .returning(move |_, block: LightEthereumBlock| {
            Box::new(future::ok(
                blocks_for_load_full_block
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|block_with_uncles| block_with_uncles.block.block.hash == block.hash)
                    .cloned()
                    .expect(
                        format!(
                            "full block {} [{:x}] not found",
                            block.number.unwrap(),
                            block.hash.unwrap()
                        )
                        .as_str(),
                    )
                    .block,
            ))
        });

    // For now return no uncles
    adapter
        .expect_uncles()
        .returning(move |_, _| Box::new(future::ok(vec![])));

    (Arc::new(adapter), Box::new(reorg_trigger))
}

// GIVEN  a fresh subgraph (local head = None)
// AND    a chain with 10 blocks
// WHEN   running the `NetworkTracer`
// EXPECT 10 `AddBlock` events are emitted, one for each block
#[test]
fn tracer_indexes_from_genesis_to_remote_head() {
    run_test(|store: Arc<DieselStore>| -> Result<(), ()> {
        // Create a subgraph name and ID
        let subgraph_id = SubgraphDeploymentId::new("ethereum_testnet_v0").unwrap();
        let subgraph_name = SubgraphName::new("ethereum/testnet").unwrap();

        let logger = &*LOGGER;

        // Create blocks for simulating the chain
        let blocks = create_mock_blocks(10);

        // Simulate an Ethereum network using a mock adapter
        let (adapter, _) = create_mock_ethereum_adapter(vec![blocks.clone()]);

        // Create required components
        // let mut runtime = tokio::runtime::Runtime::new().unwrap();
        let metrics_registry = Arc::new(MockMetricsRegistry::new());

        // Create the subgraph fresh
        let logger_for_subgraph = logger.clone();
        let subgraph_id_for_subgraph = subgraph_id.clone();
        let store_for_subgraph = store.clone();

        // Ensure subgraph, the wire up the tracer and indexer
        ensure_subgraph_exists(
            subgraph_name,
            subgraph_id_for_subgraph,
            logger_for_subgraph,
            store_for_subgraph,
            None,
        )
        .wait()
        .expect("failed to create network subgraph");

        // Create the network tracer
        let mut tracer = NetworkTracer::new(
            subgraph_id.clone(),
            &logger,
            adapter.clone(),
            store.clone(),
            metrics_registry.clone(),
        );

        // Spawn network tracer and forward its events to the channel
        let events = tracer
            .take_event_stream()
            .expect("failed to get take stream from tracer")
            .take(10) // there should only be 7 blocks but we'll try to pull more
            .fuse()
            .collect()
            .wait()
            .expect("failed to collect events from tracer");

        // Assert that the events emitted by the tracer match all
        // blocks _after_ block #2 (because the network subgraph already
        // had that one)
        assert_eq!(
            events,
            blocks
                .into_iter()
                .map(|block| NetworkTracerEvent::AddBlocks {
                    blocks: vec![block]
                })
                .collect::<Vec<_>>()
        );

        Ok(())
    });
}

// GIVEN  an existing subgraph (local head = block #2)
// AND    a chain with 10 blocks
// WHEN   running the `NetworkTracer`
// EXPECT 7 `AddBlock` events are emitted, one for each remaining block
#[test]
fn tracer_starts_at_local_head() {
    run_test(|store: Arc<DieselStore>| -> Result<(), ()> {
        // Create a subgraph name and ID
        let subgraph_id = SubgraphDeploymentId::new("ethereum_testnet_v0").unwrap();
        let subgraph_name = SubgraphName::new("ethereum/testnet").unwrap();

        let logger = &*LOGGER;

        // Create blocks for simulating the chain
        let blocks = create_mock_blocks(10);

        // Simulate an Ethereum network using a mock adapter
        let (adapter, _) = create_mock_ethereum_adapter(vec![blocks.clone()]);

        // Create required components
        // let mut runtime = tokio::runtime::Runtime::new().unwrap();
        let metrics_registry = Arc::new(MockMetricsRegistry::new());

        // Create the subgraph fresh
        let logger_for_subgraph = logger.clone();
        let subgraph_id_for_subgraph = subgraph_id.clone();
        let store_for_subgraph = store.clone();
        let blocks_for_subgraph = blocks.clone();

        // Ensure subgraph, the wire up the tracer and indexer
        ensure_subgraph_exists(
            subgraph_name,
            subgraph_id_for_subgraph,
            logger_for_subgraph,
            store_for_subgraph,
            Some(blocks_for_subgraph[2].inner().into()),
        )
        .wait()
        .expect("failed to create network subgraph");

        // Create the network tracer
        let mut tracer = NetworkTracer::new(
            subgraph_id.clone(),
            &logger,
            adapter.clone(),
            store.clone(),
            metrics_registry.clone(),
        );

        // Spawn network tracer and forward its events to the channel
        let events = tracer
            .take_event_stream()
            .expect("failed to get take stream from tracer")
            .take(10)
            .fuse()
            .collect()
            .wait()
            .expect("failed to collect events from tracer");

        // Assert that the events emitted by the tracer match the blocks 1:1
        assert_eq!(
            events,
            blocks[3..]
                .to_owned()
                .into_iter()
                .map(|block| NetworkTracerEvent::AddBlocks {
                    blocks: vec![block]
                })
                .collect::<Vec<_>>()
        );

        Ok(())
    });
}

// GIVEN  a fresh subgraph (local head = None)
// AND    10 blocks for one version of the chain
// AND    11 blocks for a for of the chain that starts after block #3
// WHEN   running the `NetworkTracer`
// EXPECT 10 `AddBlock` events is emitted for the first branch,
//        1 `RevertTo` event is emitted to revert to block #3
//        8 `AddBlock` events are emitted for blocks #4-#11 of the fork
