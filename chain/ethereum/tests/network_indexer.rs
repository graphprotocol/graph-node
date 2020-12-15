#[macro_use]
extern crate pretty_assertions;

use std::convert::TryInto;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use graph::mock::*;
use graph::prelude::*;
use graph_chain_ethereum::network_indexer::{
    self as network_indexer, BlockWithOmmers, NetworkIndexerEvent,
};
use graph_core::MetricsRegistry;
use graph_store_postgres::NetworkStore as DieselStore;
use web3::types::{H2048, H256, H64, U256};

use test_store::*;

// Helper macros to define indexer events.
macro_rules! add_block {
    ($chain:expr, $n:expr) => {{
        NetworkIndexerEvent::AddBlock($chain[$n].inner().into())
    }};
}
macro_rules! revert {
    ($from_chain:expr, $from_n:expr => $to_chain:expr, $to_n:expr) => {{
        NetworkIndexerEvent::Revert {
            from: $from_chain[$from_n].inner().into(),
            to: $to_chain[$to_n].inner().into(),
        }
    }};
}

// Helper to wipe the store clean.
fn remove_test_data(store: Arc<DieselStore>) {
    store
        .delete_all_entities_for_test_use_only()
        .expect("removing test data succeeds");
}

// Helper to run network indexer against test chains.
fn run_network_indexer(
    store: Arc<DieselStore>,
    start_block: Option<EthereumBlockPointer>,
    chains: Vec<Vec<BlockWithOmmers>>,
    timeout: Duration,
) -> impl Future<
    Item = (
        Arc<Mutex<Chains>>,
        impl Future<Item = Vec<NetworkIndexerEvent>, Error = ()>,
    ),
    Error = (),
> {
    // Simulate an Ethereum network using a mock adapter
    let (adapter, chains) = create_mock_ethereum_adapter(chains);

    let subgraph_name = SubgraphName::new("ethereum/testnet").unwrap();
    let logger = LOGGER.clone();
    let prometheus_registry = Arc::new(Registry::new());
    let metrics_registry = Arc::new(MetricsRegistry::new(logger.clone(), prometheus_registry));

    // Create the network indexer
    let mut indexer = network_indexer::NetworkIndexer::new(
        &logger,
        adapter,
        store.clone(),
        metrics_registry,
        subgraph_name.to_string(),
        start_block,
        "fake_network".to_string(),
    );

    let (event_sink, event_stream) = futures::sync::mpsc::channel(100);

    // Run network indexer and forward its events to the channel
    graph::spawn(
        indexer
            .take_event_stream()
            .expect("failed to take stream from indexer")
            .forward(event_sink.sink_map_err(|_| ()))
            .map(|_| ())
            .timeout(timeout),
    );

    future::ok((chains, event_stream.collect()))
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
        .block_on(
            future::lazy(move || {
                // Reset store before running
                remove_test_data(store.clone());

                // Run test
                test(store.clone())
            })
            .compat(),
        )
        .expect("failed to run test with clean store");
}

// Helper to create a sequence of linked blocks.
fn create_chain(n: u64, parent: Option<&BlockWithOmmers>) -> Vec<BlockWithOmmers> {
    let start = parent.map_or(0, |block| block.inner().number.unwrap().as_u64() + 1);

    (start..start + n).fold(vec![], |mut blocks, number| {
        let mut block = BlockWithOmmers::default();

        // Set required fields
        block.block.block.nonce = Some(H64::random());
        block.block.block.mix_hash = Some(H256::random());
        block.block.block.logs_bloom = Some(H2048::default());
        block.block.block.total_difficulty = Some(U256::default());

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
    original_blocks: Vec<BlockWithOmmers>,
    base: u64,
    total: u64,
) -> Vec<BlockWithOmmers> {
    let mut blocks = original_blocks[0..(base as usize) + 1].to_vec();
    let new_blocks = create_chain((total - base - 1).try_into().unwrap(), blocks.last());
    blocks.extend(new_blocks);
    blocks
}

struct Chains {
    current_chain_index: usize,
    chains: Vec<Vec<BlockWithOmmers>>,
}

impl Chains {
    pub fn new(chains: Vec<Vec<BlockWithOmmers>>) -> Self {
        Self {
            current_chain_index: 0,
            chains,
        }
    }

    pub fn index(&self) -> usize {
        self.current_chain_index
    }

    pub fn current_chain(&self) -> Option<&Vec<BlockWithOmmers>> {
        self.chains.get(self.current_chain_index)
    }

    pub fn advance_to_next_chain(&mut self) {
        self.current_chain_index += 1;
    }
}

fn create_mock_ethereum_adapter(
    chains: Vec<Vec<BlockWithOmmers>>,
) -> (Arc<MockEthereumAdapter>, Arc<Mutex<Chains>>) {
    let chains = Arc::new(Mutex::new(Chains::new(chains)));

    // Create the mock Ethereum adapter.
    let mut adapter = MockEthereumAdapter::new();

    // Make it so that each time we poll a new remote head, we
    // switch to the next version of the chain
    let chains_for_latest_block = chains.clone();
    adapter.expect_latest_block().returning(move |_: &Logger| {
        let chains = chains_for_latest_block.lock().unwrap();
        Box::new(future::result(
            chains
                .current_chain()
                .ok_or_else(|| {
                    format_err!("exhausted chain versions used in this test; this is ok")
                })
                .and_then(|chain| chain.last().ok_or_else(|| format_err!("empty block chain")))
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

    // For now return no ommers
    let chains_for_ommers = chains.clone();
    adapter
        .expect_uncles()
        .returning(move |_, block: &LightEthereumBlock| {
            let chains = chains_for_ommers.lock().unwrap();
            Box::new(future::result(
                chains
                    .current_chain()
                    .ok_or_else(|| format_err!("unknown chain {:?}", chains.index()))
                    .map_err(Into::into)
                    .map(|chain| {
                        chain
                            .iter()
                            .find(|b| b.inner().hash.unwrap() == block.hash.unwrap())
                            .expect(
                                format!(
                                    "block #{} ({:x}) not found",
                                    block.number.unwrap(),
                                    block.hash.unwrap()
                                )
                                .as_str(),
                            )
                            .clone()
                            .ommers
                            .into_iter()
                            .map(|ommer| Some((*ommer).clone()))
                            .collect::<Vec<_>>()
                    }),
            ))
        });

    (Arc::new(adapter), chains)
}

// GIVEN  a fresh subgraph (local head = none)
// AND    a chain with 10 blocks
// WHEN   indexing the network
// EXPECT 10 `AddBlock` events are emitted, one for each block
#[test]
#[ignore] // Flaky on CI.
fn indexing_starts_at_genesis() {
    run_test(|store: Arc<DieselStore>| {
        // Create test chain
        let chain = create_chain(10, None);
        let chains = vec![chain.clone()];

        // Run network indexer and collect its events
        run_network_indexer(store, None, chains, Duration::from_secs(1)).and_then(
            move |(_, events)| {
                events.and_then(move |events| {
                    // Assert that the events emitted by the indexer match all
                    // blocks _after_ block #2 (because the network subgraph already
                    // had that one)
                    assert_eq!(
                        events,
                        (0..10).map(|n| add_block!(chain, n)).collect::<Vec<_>>()
                    );
                    Ok(())
                })
            },
        )
    });
}

// GIVEN  an existing subgraph (local head = block #2)
// AND    a chain with 10 blocks
// WHEN   indexing the network
// EXPECT 7 `AddBlock` events are emitted, one for each remaining block
#[test]
#[ignore] // Flaky on CI.
fn indexing_resumes_from_local_head() {
    run_test(|store: Arc<DieselStore>| {
        // Create test chain
        let chain = create_chain(10, None);
        let chains = vec![chain.clone()];

        // Run network indexer and collect its events
        run_network_indexer(
            store,
            Some(chain[2].inner().into()),
            chains,
            Duration::from_secs(1),
        )
        .and_then(move |(_, events)| {
            events.and_then(move |events| {
                // Assert that the events emitted by the indexer are only
                // for the blocks #3-#9.
                assert_eq!(
                    events,
                    (3..10).map(|n| add_block!(chain, n)).collect::<Vec<_>>()
                );

                Ok(())
            })
        })
    });
}

// GIVEN  a fresh subgraph (local head = none)
// AND    a chain with 10 blocks
// WHEN   indexing the network
// EXPECT 10 `AddBlock` events are emitted, one for each block
#[test]
#[ignore] // Flaky on CI.
fn indexing_picks_up_new_remote_head() {
    run_test(|store: Arc<DieselStore>| {
        // The first time we pull the remote head, there are 10 blocks
        let chain_10 = create_chain(10, None);

        // The second time we pull the remote head, there are 20 blocks;
        // the first 10 blocks are identical to before, so this simulates
        // 10 new blocks being added to the same chain
        let chain_20 = create_fork(chain_10.clone(), 9, 20);

        // The third time we pull the remote head, there are 50 blocks;
        // the first 20 blocks are identical to before
        let chain_50 = create_fork(chain_20.clone(), 19, 50);

        // Use the two above chains in the test
        let chains = vec![chain_10.clone(), chain_20.clone(), chain_50.clone()];

        // Run network indexer and collect its events
        run_network_indexer(store, None, chains, Duration::from_secs(4)).and_then(
            move |(chains, events)| {
                thread::spawn(move || {
                    // Create the first chain update after 1s
                    {
                        thread::sleep(Duration::from_secs(1));
                        chains.lock().unwrap().advance_to_next_chain();
                    }
                    // Create the second chain update after 3s
                    {
                        thread::sleep(Duration::from_secs(2));
                        chains.lock().unwrap().advance_to_next_chain();
                    }
                });

                events.and_then(move |events| {
                    // Assert that the events emitted by the indexer match the blocks 1:1,
                    // despite them requiring two remote head updates
                    assert_eq!(
                        events,
                        (0..50).map(|n| add_block!(chain_50, n)).collect::<Vec<_>>(),
                    );

                    Ok(())
                })
            },
        )
    });
}

// GIVEN  a fresh subgraph (local head = none)
// AND    a chain with 10 blocks with a gap (#6 missing)
// WHEN   indexing the network
// EXPECT only `AddBlock` events for blocks #0-#5 are emitted
#[test]
#[ignore] // Flaky on CI.
fn indexing_does_not_move_past_a_gap() {
    run_test(|store: Arc<DieselStore>| {
        // Create test chain
        let mut blocks = create_chain(10, None);
        // Remove block #6
        blocks.remove(5);
        let chains = vec![blocks.clone()];

        // Run network indexer and collect its events
        run_network_indexer(store, None, chains, Duration::from_secs(1)).and_then(
            move |(_, events)| {
                events.and_then(move |events| {
                    // Assert that only blocks #0 - #4 were indexed and nothing more
                    assert_eq!(
                        events,
                        (0..5).map(|n| add_block!(blocks, n)).collect::<Vec<_>>()
                    );

                    Ok(())
                })
            },
        )
    });
}

// GIVEN  a fresh subgraph (local head = none)
// AND    10 blocks for one version of the chain
// AND    11 blocks for a fork of the chain that starts after block #8
// WHEN   indexing the network
// EXPECT 10 `AddBlock` events are emitted for the first branch,
//        1 `Revert` event is emitted to revert back to block #8
//        2 `AddBlock` events are emitted for blocks #9-#10 of the fork
#[test]
#[ignore] // Flaky on CI.
fn indexing_handles_single_block_reorg() {
    run_test(|store: Arc<DieselStore>| {
        // Create the initial chain
        let initial_chain = create_chain(10, None);

        // Create a forked chain after block #8
        let forked_chain = create_fork(initial_chain.clone(), 8, 11);

        // Run the network indexer and collect its events
        let chains = vec![initial_chain.clone(), forked_chain.clone()];
        run_network_indexer(store, None, chains, Duration::from_secs(2)).and_then(
            move |(chains, events)| {
                // Trigger the reorg after 1s
                thread::spawn(move || {
                    thread::sleep(Duration::from_secs(1));
                    chains.lock().unwrap().advance_to_next_chain();
                });

                events.and_then(move |events| {
                    assert_eq!(
                        events,
                        // The 10 `AddBlock` events for the initial version of the chain
                        (0..10)
                            .map(|n| add_block!(initial_chain, n))
                            // The 1 `Revert` event to go back to #8
                            .chain(vec![revert!(initial_chain, 9 => initial_chain, 8)])
                            // The 2 `AddBlock` events for the new chain
                            .chain((9..11).map(|n| add_block!(forked_chain, n)))
                            .collect::<Vec<_>>()
                    );

                    Ok(())
                })
            },
        )
    });
}

// GIVEN  a fresh subgraph (local head = none)
// AND    10 blocks for one version of the chain
// AND    20 blocks for a fork of the chain that starts after block #2
// WHEN   indexing the network
// EXPECT 10 `AddBlock` events are emitted for the first branch,
//        7 `Revert` events are emitted to revert back to block #2
//        17 `AddBlock` events are emitted for blocks #3-#20 of the fork
#[test]
#[ignore] // Flaky on CI.
fn indexing_handles_simple_reorg() {
    run_test(|store: Arc<DieselStore>| {
        // Create the initial chain
        let initial_chain = create_chain(10, None);

        // Create a forked chain after block #2
        let forked_chain = create_fork(initial_chain.clone(), 2, 20);

        // Run the network indexer and collect its events
        let chains = vec![initial_chain.clone(), forked_chain.clone()];
        run_network_indexer(store, None, chains, Duration::from_secs(2)).and_then(
            move |(chains, events)| {
                // Trigger a reorg after 1s
                thread::spawn(move || {
                    thread::sleep(Duration::from_secs(1));
                    chains.lock().unwrap().advance_to_next_chain();
                });

                events.and_then(move |events| {
                    assert_eq!(
                        events,
                        // - 10 `AddBlock` events for blocks #0 to #9 of the initial chain
                        (0..10)
                            .map(|n| add_block!(initial_chain, n))
                            // - 7 `Revert` events from #9 to #8, ..., #3 to #2 (the fork base)
                            .chain(
                                vec![9, 8, 7, 6, 5, 4, 3]
                                    .into_iter()
                                    .map(|n| revert!(initial_chain, n => initial_chain, n-1))
                            )
                            // 17 `AddBlock` events for the new chain
                            .chain((3..20).map(|n| add_block!(forked_chain, n)))
                            .collect::<Vec<_>>()
                    );

                    Ok(())
                })
            },
        )
    });
}

// GIVEN  a fresh subgraph (local head = none)
// AND    10 blocks for the initial chain
// AND    20 blocks for a fork of the initial chain that starts after block #2
// AND    30 blocks for a fork of the initial chain that starts after block #2
// WHEN   indexing the network
// EXPECT 10 `AddBlock` events are emitted for the first branch,
//        7 `Revert` events are emitted to revert back to block #2
//        17 `AddBlock` events are emitted for blocks #4-#20 of the fork
//        7 `Revert` events are emitted to revert back to block #2
//        17 `AddBlock` events are emitted for blocks #4-#20 of the fork
#[test]
#[ignore] // Flaky on CI.
fn indexing_handles_consecutive_reorgs() {
    run_test(|store: Arc<DieselStore>| {
        // Create the initial chain
        let initial_chain = create_chain(10, None);

        // Create a forked chain after block #2
        let second_chain = create_fork(initial_chain.clone(), 2, 20);

        // Create a forked chain after block #3
        let third_chain = create_fork(initial_chain.clone(), 2, 30);

        // Run the network indexer for 10s and collect its events
        let chains = vec![
            initial_chain.clone(),
            second_chain.clone(),
            third_chain.clone(),
        ];
        run_network_indexer(store, None, chains, Duration::from_secs(6)).and_then(
            move |(chains, events)| {
                thread::spawn(move || {
                    // Trigger the first reorg after 2s
                    {
                        thread::sleep(Duration::from_secs(2));
                        chains.lock().unwrap().advance_to_next_chain();
                    }
                    // Trigger the second reorg after 4s
                    {
                        thread::sleep(Duration::from_secs(2));
                        chains.lock().unwrap().advance_to_next_chain();
                    }
                });

                events.and_then(move |events| {
                    assert_eq!(
                        events,
                        // The 10 add block events for the initial version of the chain
                        (0..10)
                            .map(|n| add_block!(initial_chain, n))
                            // The 7 revert events to go back to #2
                            .chain(
                                vec![9, 8, 7, 6, 5, 4, 3]
                                    .into_iter()
                                    .map(|n| revert!(initial_chain, n => initial_chain, n-1))
                            )
                            // The 17 add block events for the new chain
                            .chain((3..20).map(|n| add_block!(second_chain, n)))
                            // The 17 revert events to go back to #2
                            .chain(
                                vec![19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3]
                                    .into_iter()
                                    .map(|n| revert!(second_chain, n => second_chain, n-1))
                            )
                            // The 27 add block events for the third chain
                            .chain((3..30).map(|n| add_block!(third_chain, n)))
                            .collect::<Vec<_>>()
                    );

                    Ok(())
                })
            },
        )
    });
}

// GIVEN  a fresh subgraph (local head = none)
// AND    5 blocks for one version of the chain (#0 - #4)
// AND    a fork with blocks #0 - #3, #4', #5'
// AND    a fork with blocks #0 - #3, #4, #5'', #6''
// WHEN   indexing the network
// EXPECT 5 `AddBlock` events are emitted for the first chain version,
//        1 `Revert` event is emitted from block #4 to #3
//        2 `AddBlock` events are emitted for blocks #4', #5'
//        2 `Revert` events are emitted from block #5' to #4' and #4' to #3
//        3 `AddBlock` events are emitted for blocks #4, #5'', #6''
#[test]
#[ignore] // Flaky on CI.
fn indexing_handles_reorg_back_and_forth() {
    run_test(|store: Arc<DieselStore>| {
        // Create the initial chain (blocks #0 - #4)
        let initial_chain = create_chain(5, None);

        // Create fork 1 (blocks #0 - #3, #4', #5')
        let fork1 = create_fork(initial_chain.clone(), 3, 6);

        // Create fork 2 (blocks #0 - #4, #5'', #6'');
        // this fork includes the original #4 again, which at this point should
        // no longer be in the store and therefor not be considered as the
        // common ancestor of the fork (that should be #3).
        let fork2 = create_fork(initial_chain.clone(), 4, 7);

        // Run the network indexer and collect its events
        let chains = vec![initial_chain.clone(), fork1.clone(), fork2.clone()];
        run_network_indexer(store, None, chains, Duration::from_secs(3)).and_then(
            move |(chains, events)| {
                thread::spawn(move || {
                    // Trigger the first reorg after 1s
                    {
                        thread::sleep(Duration::from_secs(1));
                        chains.lock().unwrap().advance_to_next_chain();
                    }
                    // Trigger the second reorg after 2s
                    {
                        thread::sleep(Duration::from_secs(1));
                        chains.lock().unwrap().advance_to_next_chain();
                    }
                });

                events.and_then(move |events| {
                    assert_eq!(
                        events,
                        vec![
                            add_block!(initial_chain, 0),
                            add_block!(initial_chain, 1),
                            add_block!(initial_chain, 2),
                            add_block!(initial_chain, 3),
                            add_block!(initial_chain, 4),
                            revert!(initial_chain, 4 => initial_chain, 3),
                            add_block!(fork1, 4),
                            add_block!(fork1, 5),
                            revert!(fork1, 5 => fork1, 4),
                            revert!(fork1, 4 => initial_chain, 3),
                            add_block!(fork2, 4),
                            add_block!(fork2, 5),
                            add_block!(fork2, 6)
                        ]
                    );

                    Ok(())
                })
            },
        )
    });
}

// Test that ommer blocks are not confused with reguar blocks when finding
// common ancestors for reorgs. There was a bug initially where that would
// happen, because any block that was in the store was considered to be on the
// local version of the chain. This assumption is false because ommers are
// stored as `Block` entities as well. To correctly identify the common
// ancestor in a reorg, traversing the old and new chains block by block
// through parent hashes is necessary.
//
// GIVEN  a fresh subgraph (local head = none)
// AND    5 blocks for one version of the chain (#0 - #4)
// AND    a fork with blocks #0 - #3, #4', #5'
//          where block #5' has #4 as an ommer
// AND    a fork with blocks #0 - #3, #4, #5'', #6''
//          where the original #4 is included again
// WHEN   indexing the network
// EXPECT 5 `AddBlock` events are emitted for the first chain version,
//        1 `Revert` event is emitted from block #4 to #3
//        2 `AddBlock` events are emitted for blocks #4', #5'
//        2 `Revert` events are emitted from block #5' to #4' and #4' to #3
//        3 `AddBlock` events are emitted for blocks #4, #5'', #6''
//        block #3 is identified as the common ancestor in both reorgs
#[test]
#[ignore] // Flaky on CI.
fn indexing_identifies_common_ancestor_correctly_despite_ommers() {
    run_test(|store: Arc<DieselStore>| {
        // Create the initial chain (#0 - #4)
        let initial_chain = create_chain(5, None);

        // Create fork 1 (blocks #0 - #3, #4', #5')
        let mut fork1 = create_fork(initial_chain.clone(), 3, 6);

        // Make it so that #5' has #4 as an uncle
        fork1[5].block.block.uncles = vec![initial_chain[4].inner().hash.clone().unwrap()];
        fork1[5].ommers = vec![initial_chain[4].block.block.clone().into()];

        // Create fork 2 (blocks #0 - #4, #5'', #6''); this fork includes the
        // original #4 again, which at this point should no longer be part of
        // the indexed chain in the store and therefor not be considered as the
        // common ancestor of the fork (that should be #3). It is still in the
        // store as an ommer (of #5', from fork1) but that ommer should not be
        // picked as the common ancestor either.
        let fork2 = create_fork(initial_chain.clone(), 4, 7);

        // Run the network indexer and collect its events
        let chains = vec![initial_chain.clone(), fork1.clone(), fork2.clone()];
        run_network_indexer(store, None, chains, Duration::from_secs(3)).and_then(
            move |(chains, events)| {
                thread::spawn(move || {
                    // Trigger the first reorg after 1s
                    {
                        thread::sleep(Duration::from_secs(1));
                        chains.lock().unwrap().advance_to_next_chain();
                    }
                    // Trigger the second reorg after 2s
                    {
                        thread::sleep(Duration::from_secs(1));
                        chains.lock().unwrap().advance_to_next_chain();
                    }
                });

                events.and_then(move |events| {
                    assert_eq!(
                        events,
                        vec![
                            add_block!(initial_chain, 0),
                            add_block!(initial_chain, 1),
                            add_block!(initial_chain, 2),
                            add_block!(initial_chain, 3),
                            add_block!(initial_chain, 4),
                            revert!(initial_chain, 4 => initial_chain, 3),
                            add_block!(fork1, 4),
                            add_block!(fork1, 5),
                            revert!(fork1, 5 => fork1, 4),
                            revert!(fork1, 4 => initial_chain, 3),
                            add_block!(fork2, 4),
                            add_block!(fork2, 5),
                            add_block!(fork2, 6)
                        ]
                    );

                    Ok(())
                })
            },
        )
    });
}
