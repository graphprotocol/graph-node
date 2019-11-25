#[macro_use]
extern crate pretty_assertions;

use diesel::connection::Connection;
use diesel::pg::PgConnection;
use std::convert::TryInto;
use std::sync::Mutex;

use graph::mock::*;
use graph::prelude::*;
use graph_chain_ethereum::network_indexer::{
    self as network_indexer, BlockWithUncles, NetworkIndexer, NetworkIndexerEvent,
};
use graph_core::MetricsRegistry;
use graph_store_postgres::Store as DieselStore;
use web3::types::{H256, H64};

use test_store::*;

// Helper to wipe the store clean.
fn remove_test_data(store: Arc<DieselStore>) {
    let url = postgres_test_url();
    let conn = PgConnection::establish(url.as_str()).expect("Failed to connect to Postgres");
    graph_store_postgres::store::delete_all_entities_for_test_use_only(&store, &conn)
        .expect("Failed to remove entity test data");
}

// Helper type for setting up tests.
struct TestSetup {
    // pub subgraph_id: SubgraphDeploymentId,
    // pub subgraph_name: SubgraphName,
    pub logger: Logger,
    pub adapter: Arc<MockEthereumAdapter>,
    pub store: Arc<DieselStore>,
    pub metrics_registry: Arc<MetricsRegistry>,
    pub indexer: NetworkIndexer,
}

impl TestSetup {
    pub fn new(
        store: Arc<DieselStore>,
        start_block: Option<EthereumBlockPointer>,
        chains: Vec<Vec<BlockWithUncles>>,
    ) -> Self {
        // Simulate an Ethereum network using a mock adapter
        let adapter = create_mock_ethereum_adapter(chains);

        let subgraph_name = SubgraphName::new("ethereum/testnet").unwrap();
        let logger = LOGGER.clone();
        let prometheus_registry = Arc::new(Registry::new());
        let metrics_registry = Arc::new(MetricsRegistry::new(logger.clone(), prometheus_registry));

        // Create the network indexer
        let indexer = network_indexer::create(
            subgraph_name.to_string(),
            &logger,
            adapter.clone(),
            store.clone(),
            metrics_registry.clone(),
            start_block,
        )
        .wait()
        .expect("failed to create network indexer");

        Self {
            // subgraph_id,
            // subgraph_name,
            logger,
            adapter,
            store,
            metrics_registry,
            indexer,
        }
    }
}

// Helper to run tests against a clean store.
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
fn create_blocks(n: u64, parent: Option<&BlockWithUncles>) -> Vec<BlockWithUncles> {
    let start = parent.map_or(0, |block| block.inner().number.unwrap().as_u64() + 1);

    (start..start + n).fold(vec![], |mut blocks, number| {
        let mut block = BlockWithUncles::default();

        // Set required fields
        block.block.block.nonce = Some(H64::random());
        block.block.block.mix_hash = Some(H256::random());

        // Use the index as the block number
        block.block.block.number = Some(number.into());

        // Use a random hash as the block hash (should be unique)
        block.block.block.hash = Some(H256::random());

        if number == start {
            // Set the parent hash for the first block only if a
            // parent was passed in; otherwise we're dealing with
            // the genesis block
            if let Some(parent_block) = parent {
                block.block.block.parent_hash = parent_block.inner().hash.unwrap().clone();
            }
        } else {
            // Set the parent hash for all blocks but the genesis block
            block.block.block.parent_hash =
                blocks.last().unwrap().block.block.hash.clone().unwrap();
        }

        blocks.push(block);
        blocks
    })
}

fn create_fork(
    original_blocks: Vec<BlockWithUncles>,
    base: u64,
    total: u64,
) -> Vec<BlockWithUncles> {
    let mut blocks = original_blocks[0..(base as usize) + 1].to_vec();
    let new_blocks = create_blocks((total - base - 1).try_into().unwrap(), blocks.last());
    blocks.extend(new_blocks);
    blocks
}

struct Chains {
    chain_index: Option<usize>,
    chains: Vec<Vec<BlockWithUncles>>,
}

impl Chains {
    pub fn new(chains: Vec<Vec<BlockWithUncles>>) -> Self {
        Self {
            chain_index: None,
            chains,
        }
    }

    pub fn index(&self) -> Option<usize> {
        self.chain_index.clone()
    }

    pub fn next_chain(&mut self) -> Option<&Vec<BlockWithUncles>> {
        let next_index = self.chain_index.map_or(0, |index| index + 1);
        self.chain_index.replace(next_index);
        self.chains.get(next_index)
    }

    pub fn current_chain(&self) -> Option<&Vec<BlockWithUncles>> {
        self.chain_index.and_then(|index| self.chains.get(index))
    }
}

fn create_mock_ethereum_adapter(chains: Vec<Vec<BlockWithUncles>>) -> Arc<MockEthereumAdapter> {
    let chains = Arc::new(Mutex::new(Chains::new(chains)));

    // Create the mock Ethereum adapter.
    let mut adapter = MockEthereumAdapter::new();

    // Make it so that each time we poll a new remote head, we
    // switch to the next version of the chain
    let chains_for_latest_block = chains.clone();
    adapter.expect_latest_block().returning(move |_: &Logger| {
        let mut chains = chains_for_latest_block.lock().unwrap();
        Box::new(future::result(
            chains
                .next_chain()
                .expect("exhausted remote head blocks")
                .last()
                .ok_or_else(|| format_err!("empty block chain"))
                .map_err(Into::into)
                .map(|block| block.block.block.clone()),
        ))
    });

    let chains_for_block_by_number = chains.clone();
    adapter
        .expect_block_by_number()
        .returning(move |_, number: u64| {
            let chains = chains_for_block_by_number.lock().unwrap();
            Box::new(future::result(
                chains
                    .current_chain()
                    .ok_or_else(|| format_err!("unknown chain {:?}", chains.index()))
                    .map(|chain| {
                        chain
                            .iter()
                            .find(|block| block.inner().number.unwrap().as_u64() == number)
                            .map(|block| block.clone().block.block)
                    }),
            ))
        });

    let chains_for_block_by_hash = chains.clone();
    adapter
        .expect_block_by_hash()
        .returning(move |_, hash: H256| {
            let chains = chains_for_block_by_hash.lock().unwrap();
            Box::new(future::result(
                chains
                    .current_chain()
                    .ok_or_else(|| format_err!("unknown chain {:?}", chains.index()))
                    .map(|chain| {
                        chain
                            .iter()
                            .find(|block| block.inner().hash.unwrap() == hash)
                            .map(|block| block.clone().block.block)
                    }),
            ))
        });

    let chains_for_load_full_block = chains.clone();
    adapter
        .expect_load_full_block()
        .returning(move |_, block: LightEthereumBlock| {
            let chains = chains_for_load_full_block.lock().unwrap();
            Box::new(future::result(
                chains
                    .current_chain()
                    .ok_or_else(|| format_err!("unknown chain {:?}", chains.index()))
                    .map_err(Into::into)
                    .map(|chain| {
                        chain
                            .iter()
                            .find(|b| b.inner().number.unwrap() == block.number.unwrap())
                            .expect(
                                format!(
                                    "full block {} [{:x}] not found",
                                    block.number.unwrap(),
                                    block.hash.unwrap()
                                )
                                .as_str(),
                            )
                            .clone()
                            .block
                    }),
            ))
        });

    // For now return no uncles
    adapter
        .expect_uncles()
        .returning(move |_, _| Box::new(future::ok(vec![])));

    Arc::new(adapter)
}

// GIVEN  a fresh subgraph (local head = None)
// AND    a chain with 10 blocks
// WHEN   running the `NetworkIndexer`
// EXPECT 10 `AddBlock` events are emitted, one for each block
#[test]
fn tracing_starts_at_genesis() {
    run_test(|store: Arc<DieselStore>| -> Result<(), ()> {
        // Create blocks for simulating the chain
        let blocks = create_blocks(10, None);

        // Set up the test
        let mut setup = TestSetup::new(store, None, vec![blocks.clone()]);

        // Run network indexer and forward its events to the channel
        let events = setup
            .indexer
            .take_event_stream()
            .expect("failed to take stream from indexer")
            .take(10) // there should only be 7 blocks but we'll try to pull more
            .fuse()
            .collect()
            .wait()
            .expect("failed to collect events from indexer");

        // Assert that the events emitted by the indexer match all
        // blocks _after_ block #2 (because the network subgraph already
        // had that one)
        assert_eq!(
            events,
            blocks
                .into_iter()
                .map(|block| NetworkIndexerEvent::AddBlock(block.inner().into()))
                .collect::<Vec<_>>()
        );

        Ok(())
    });
}

// GIVEN  an existing subgraph (local head = block #2)
// AND    a chain with 10 blocks
// WHEN   running the `NetworkIndexer`
// EXPECT 7 `AddBlock` events are emitted, one for each remaining block
#[test]
fn tracing_resumes_from_local_head() {
    run_test(|store: Arc<DieselStore>| -> Result<(), ()> {
        // Create blocks for simulating the chain
        let blocks = create_blocks(10, None);

        // Set up the test
        let mut setup = TestSetup::new(store, Some(blocks[2].inner().into()), vec![blocks.clone()]);

        // Run network indexer and forward its events to the channel
        let events = setup
            .indexer
            .take_event_stream()
            .expect("failed to take stream from indexer")
            .take(10)
            .fuse()
            .collect()
            .wait()
            .expect("failed to collect events from indexer");

        // Assert that the events emitted by the indexer match the blocks 1:1
        assert_eq!(
            events,
            blocks[3..]
                .to_owned()
                .into_iter()
                .map(|block| NetworkIndexerEvent::AddBlock(block.inner().into()))
                .collect::<Vec<_>>()
        );

        Ok(())
    });
}

// GIVEN  a fresh subgraph (local head = None)
// AND    a chain with 10 blocks
// WHEN   running the `NetworkIndexer`
// EXPECT 10 `AddBlock` events are emitted, one for each block
#[test]
fn tracing_picks_up_new_remote_head() {
    run_test(|store: Arc<DieselStore>| -> Result<(), ()> {
        // The first time we pull the remote head, there are 10 blocks
        let chain_10 = create_blocks(10, None);

        // The second time we pull the remote head, there are 20 blocks;
        // the first 10 blocks are identical to before, so this simulates
        // 10 new blocks being added to the same chain
        let chain_20 = create_fork(chain_10.clone(), 10, 10);

        // Set up the test
        let mut setup = TestSetup::new(store, None, vec![chain_10.clone(), chain_20.clone()]);

        // Run network indexer and forward its events to the channel
        let events = setup
            .indexer
            .take_event_stream()
            .expect("failed to take stream from indexer")
            .take(20)
            .fuse()
            .collect()
            .wait()
            .expect("failed to collect events from indexer");

        // Assert that the events emitted by the indexer match the blocks 1:1,
        // despite them requiring two remote head updates
        assert_eq!(
            events,
            chain_20
                .into_iter()
                .map(|block| NetworkIndexerEvent::AddBlock(block.inner().into()))
                .collect::<Vec<_>>()
        );

        Ok(())
    });
}

// GIVEN  a fresh subgraph (local head = None)
// AND    a chain with 10 blocks with a gap (#5 missing)
// WHEN   running the `NetworkIndexer`
// EXPECT only `AddBlock` events for blocks #0-#4 are emitted
#[test]
fn tracing_fails_if_there_is_a_gap() {
    run_test(|store: Arc<DieselStore>| -> Result<(), ()> {
        // Create blocks for simulating the chain
        let mut blocks = create_blocks(10, None);

        // Remove block #5
        blocks.remove(5);

        // Set up the test
        let mut setup = TestSetup::new(store, None, vec![blocks.clone()]);

        // Run network indexer and forward its events to the channel
        let events = setup
            .indexer
            .take_event_stream()
            .expect("failed to take stream from indexer")
            .take(20)
            .fuse()
            .collect()
            .wait()
            .expect("failed to collect events from indexer");

        // Assert that only blocks #0 - #4 were indexed and nothing more
        assert_eq!(
            events,
            blocks[0..5]
                .to_owned()
                .into_iter()
                .map(|block| NetworkIndexerEvent::AddBlock(block.inner().into()))
                .collect::<Vec<_>>()
        );

        Ok(())
    });
}

// GIVEN  a fresh subgraph (local head = None)
// AND    10 blocks for one version of the chain
// AND    20 blocks for a fork of the chain that starts after block #3
// WHEN   running the `NetworkIndexer`
// EXPECT 10 `AddBlock` events is emitted for the first branch,
//        7 `Revert` events are emitted to revert back to block #3
//        16 `AddBlock` events are emitted for blocks #4-#20 of the fork
#[test]
fn tracing_handles_single_reorg() {
    run_test(|store: Arc<DieselStore>| -> Result<(), ()> {
        // Create blocks for the initial chain
        let initial_chain = create_blocks(10, None);

        // Create blocks for the forked chain after block #3
        let forked_chain = create_fork(initial_chain.clone(), 2, 20);

        // Set up the test
        let chains = vec![initial_chain.clone(), forked_chain.clone()];
        let mut setup = TestSetup::new(store, None, chains);

        debug!(setup.logger, "Original chain:");
        for block in initial_chain.iter() {
            debug!(
                setup.logger,
                "  {} [{:x}]",
                block.inner().number.unwrap(),
                block.inner().hash.unwrap()
            );
        }

        debug!(setup.logger, "Forked chain:");
        for block in forked_chain.iter() {
            debug!(
                setup.logger,
                "  {} [{:x}]",
                block.inner().number.unwrap(),
                block.inner().hash.unwrap()
            );
        }

        // Run network indexer and forward its events to the channel
        let events = setup
            .indexer
            .take_event_stream()
            .expect("failed to take stream from indexer")
            .take(50)
            .fuse()
            .collect()
            .wait()
            .expect("failed to collect events from indexer");

        // Verify that the following events are emitted in exactly the same
        // sequence:
        // 10 `AddBlock` events for blocks #0-#9 of the initial chain
        // 7 `Revert` events from #10 to #9, ..., #4 to #3 (the fork base)
        // 14 `AddBlock` events for blocks #4-#20 of the fork
        assert_eq!(
            events,
            // The 10 `AddBlock` events
            initial_chain
                .iter()
                .map(|block| NetworkIndexerEvent::AddBlock(block.inner().into()))
                .chain(
                    // The 7 `Revert` events
                    vec![(9, 8), (8, 7), (7, 6), (6, 5), (5, 4), (4, 3), (3, 2)]
                        .into_iter()
                        .map(|(from, to)| {
                            NetworkIndexerEvent::Revert {
                                from: initial_chain[from].inner().into(),
                                to: initial_chain[to].inner().into(),
                            }
                        })
                )
                .chain(
                    // The 16 `AddBlock` events for the fork
                    forked_chain[3..]
                        .iter()
                        .map(|block| NetworkIndexerEvent::AddBlock(block.inner().into()))
                )
                .collect::<Vec<_>>()
        );

        Ok(())
    });
}
