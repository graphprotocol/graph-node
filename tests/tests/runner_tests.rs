use std::marker::PhantomData;
use std::process::Command;
use std::str::FromStr;
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;
use std::time::Duration;

use assert_json_diff::assert_json_eq;
use graph::blockchain::block_stream::BlockWithTriggers;
use graph::blockchain::{Block, BlockPtr, Blockchain};
use graph::data::store::scalar::Bytes;
use graph::data::subgraph::schema::{SubgraphError, SubgraphHealth};
use graph::data::value::Word;
use graph::data_source::CausalityRegion;
use graph::env::EnvVars;
use graph::ipfs;
use graph::ipfs::test_utils::add_files_to_local_ipfs_node_for_testing;
use graph::object;
use graph::prelude::web3::types::Address;
use graph::prelude::web3::types::H256;
use graph::prelude::{
    hex, CheapClone, DeploymentHash, SubgraphAssignmentProvider, SubgraphName, SubgraphStore,
};
use graph_tests::fixture::ethereum::{
    chain, empty_block, generate_empty_blocks_for_range, genesis, push_test_command, push_test_log,
    push_test_polling_trigger,
};

use graph_tests::fixture::substreams::chain as substreams_chain;
use graph_tests::fixture::{
    self, stores, test_ptr, test_ptr_reorged, MockAdapterSelector, NoopAdapterSelector, Stores,
    TestChainTrait, TestContext, TestInfo,
};
use graph_tests::helpers::run_cmd;
use slog::{o, Discard, Logger};

struct RunnerTestRecipe {
    pub stores: Stores,
    pub test_info: TestInfo,
}

impl RunnerTestRecipe {
    async fn new(test_name: &str, subgraph_name: &str) -> Self {
        let subgraph_name = SubgraphName::new(subgraph_name).unwrap();
        let test_dir = format!("./runner-tests/{}", subgraph_name);

        let (stores, hash) = tokio::join!(
            stores(test_name, "./runner-tests/config.simple.toml"),
            build_subgraph(&test_dir, None)
        );

        Self {
            stores,
            test_info: TestInfo {
                test_dir,
                test_name: test_name.to_string(),
                subgraph_name,
                hash,
            },
        }
    }

    /// Builds a new test subgraph with a custom deploy command.
    async fn new_with_custom_cmd(name: &str, subgraph_name: &str, deploy_cmd: &str) -> Self {
        let subgraph_name = SubgraphName::new(subgraph_name).unwrap();
        let test_dir = format!("./runner-tests/{}", subgraph_name);

        let (stores, hash) = tokio::join!(
            stores(name, "./runner-tests/config.simple.toml"),
            build_subgraph(&test_dir, Some(deploy_cmd))
        );

        Self {
            stores,
            test_info: TestInfo {
                test_dir,
                test_name: name.to_string(),
                subgraph_name,
                hash,
            },
        }
    }
}

fn assert_eq_ignore_backtrace(err: &SubgraphError, expected: &SubgraphError) {
    let equal = {
        if err.subgraph_id != expected.subgraph_id
            || err.block_ptr != expected.block_ptr
            || err.handler != expected.handler
            || err.deterministic != expected.deterministic
        {
            false;
        }

        // Ignore any WASM backtrace in the error message
        let split_err: Vec<&str> = err.message.split("\\twasm backtrace:").collect();
        let split_expected: Vec<&str> = expected.message.split("\\twasm backtrace:").collect();

        split_err.get(0) == split_expected.get(0)
    };

    if !equal {
        // Will fail
        let mut err_no_trace = err.clone();
        err_no_trace.message = expected.message.split("\\twasm backtrace:").collect();
        assert_eq!(&err_no_trace, expected);
    }
}

#[tokio::test]
async fn data_source_revert() -> anyhow::Result<()> {
    let RunnerTestRecipe { stores, test_info } =
        RunnerTestRecipe::new("data_source_revert", "data-source-revert").await;

    let blocks = {
        let block0 = genesis();
        let block1 = empty_block(block0.ptr(), test_ptr(1));
        let block1_reorged_ptr = BlockPtr {
            number: 1,
            hash: H256::from_low_u64_be(12).into(),
        };
        let block1_reorged = empty_block(block0.ptr(), block1_reorged_ptr.clone());
        let block2 = empty_block(block1_reorged_ptr, test_ptr(2));
        let block3 = empty_block(block2.ptr(), test_ptr(3));
        let block4 = empty_block(block3.ptr(), test_ptr(4));
        vec![block0, block1, block1_reorged, block2, block3, block4]
    };

    let chain = chain(&test_info.test_name, blocks.clone(), &stores, None).await;

    let base_ctx = fixture::setup(&test_info, &stores, &chain, None, None).await;

    let stop_block = test_ptr(2);
    base_ctx.start_and_sync_to(stop_block).await;
    base_ctx
        .provider
        .stop(base_ctx.deployment.clone())
        .await
        .unwrap();

    // Test loading data sources from DB.
    let stop_block = test_ptr(3);
    base_ctx.start_and_sync_to(stop_block).await;

    // Test grafted version
    let subgraph_name = SubgraphName::new("data-source-revert-grafted").unwrap();
    let hash = build_subgraph_with_yarn_cmd_and_arg(
        "./runner-tests/data-source-revert",
        "deploy:test-grafted",
        Some(&test_info.hash),
    )
    .await;
    let test_info = TestInfo {
        test_dir: test_info.test_dir.clone(),
        test_name: test_info.test_name.clone(),
        subgraph_name,
        hash,
    };

    let graft_block = Some(test_ptr(3));
    let grafted_ctx = fixture::setup(&test_info, &stores, &chain, graft_block, None).await;
    let stop_block = test_ptr(4);
    grafted_ctx.start_and_sync_to(stop_block).await;

    let query_res = grafted_ctx
        .query(r#"{ dataSourceCount(id: "4") { id, count } }"#)
        .await
        .unwrap();

    // TODO: The semantically correct value for `count` would be 5. But because the test fixture
    // uses a `NoopTriggersAdapter` the data sources are not reprocessed in the block in which they
    // are created.
    assert_eq!(
        query_res,
        Some(object! { dataSourceCount: object!{ id: "4", count: 4 } })
    );

    // This is an entirely different test, but running it here conveniently avoids race conditions
    // since it uses the same deployment id.
    data_source_long_revert().await.unwrap();

    Ok(())
}

async fn data_source_long_revert() -> anyhow::Result<()> {
    let RunnerTestRecipe { stores, test_info } =
        RunnerTestRecipe::new("data_source_long_revert", "data-source-revert").await;

    let blocks = {
        let block0 = genesis();
        let blocks_1_to_5 = generate_empty_blocks_for_range(block0.ptr(), 1, 5, 0);
        let blocks_1_to_5_reorged = generate_empty_blocks_for_range(block0.ptr(), 1, 5, 1);

        let mut blocks = vec![block0];
        blocks.extend(blocks_1_to_5);
        blocks.extend(blocks_1_to_5_reorged);
        blocks
    };
    let last = blocks.last().unwrap().block.ptr();

    let chain = chain(&test_info.test_name, blocks.clone(), &stores, None).await;
    let ctx = fixture::setup(&test_info, &stores, &chain, None, None).await;

    // We sync up to block 5 twice, after the first time there is a revert back to block 1.
    // This tests reverts across more than than a single block.
    for stop_block in [test_ptr(5), last.clone()] {
        ctx.start_and_sync_to(stop_block.clone()).await;

        let query_res = ctx
            .query(r#"{ dataSourceCount(id: "5") { id, count } }"#)
            .await
            .unwrap();

        // TODO: The semantically correct value for `count` would be 6. But because the test fixture
        // uses a `NoopTriggersAdapter` the data sources are not reprocessed in the block in which they
        // are created.
        assert_eq!(
            query_res,
            Some(object! { dataSourceCount: object!{ id: "5", count: 5 } })
        );
    }

    // Restart the subgraph once more, which runs more consistency checks on dynamic data sources.
    ctx.start_and_sync_to(last).await;

    Ok(())
}

#[tokio::test]
async fn typename() -> anyhow::Result<()> {
    let RunnerTestRecipe { stores, test_info } =
        RunnerTestRecipe::new("typename", "typename").await;

    let blocks = {
        let block_0 = genesis();
        let block_1 = empty_block(block_0.ptr(), test_ptr(1));
        let block_1_reorged_ptr = BlockPtr {
            number: 1,
            hash: H256::from_low_u64_be(12).into(),
        };
        let block_1_reorged = empty_block(block_0.ptr(), block_1_reorged_ptr);
        let block_2 = empty_block(block_1_reorged.ptr(), test_ptr(2));
        let block_3 = empty_block(block_2.ptr(), test_ptr(3));
        vec![block_0, block_1, block_1_reorged, block_2, block_3]
    };

    let stop_block = blocks.last().unwrap().block.ptr();

    let chain = chain(&test_info.test_name, blocks, &stores, None).await;
    let ctx = fixture::setup(&test_info, &stores, &chain, None, None).await;

    ctx.start_and_sync_to(stop_block).await;

    Ok(())
}

#[tokio::test]
async fn api_version_0_0_7() {
    let RunnerTestRecipe { stores, test_info } = RunnerTestRecipe::new_with_custom_cmd(
        "api_version_0_0_7",
        "api-version",
        "deploy:test-0-0-7",
    )
    .await;

    // Before apiVersion 0.0.8 we allowed setting fields not defined in the schema.
    // This test tests that it is still possible for lower apiVersion subgraphs
    // to set fields not defined in the schema.

    let blocks = {
        let block_0 = genesis();
        let mut block_1 = empty_block(block_0.ptr(), test_ptr(1));
        push_test_log(&mut block_1, "0.0.7");
        vec![block_0, block_1]
    };

    let stop_block = blocks.last().unwrap().block.ptr();

    let chain = chain(&test_info.test_name, blocks, &stores, None).await;
    let ctx = fixture::setup(&test_info, &stores, &chain, None, None).await;

    ctx.start_and_sync_to(stop_block).await;

    let query_res = ctx
        .query(&format!(r#"{{ testResults{{ id, message }} }}"#,))
        .await
        .unwrap();

    assert_json_eq!(
        query_res,
        Some(object! {
            testResults: vec![
                object! { id: "0.0.7", message: "0.0.7" },
            ]
        })
    );
}

#[tokio::test]
async fn api_version_0_0_8() {
    let RunnerTestRecipe { stores, test_info } = RunnerTestRecipe::new_with_custom_cmd(
        "api_version_0_0_8",
        "api-version",
        "deploy:test-0-0-8",
    )
    .await;

    // From apiVersion 0.0.8 we disallow setting fields not defined in the schema.
    // This test tests that it is not possible to set fields not defined in the schema.

    let blocks = {
        let block_0 = genesis();
        let mut block_1 = empty_block(block_0.ptr(), test_ptr(1));
        push_test_log(&mut block_1, "0.0.8");
        vec![block_0, block_1]
    };

    let chain = chain(&test_info.test_name, blocks.clone(), &stores, None).await;
    let ctx = fixture::setup(&test_info, &stores, &chain, None, None).await;
    let stop_block = blocks.last().unwrap().block.ptr();
    let err = ctx.start_and_sync_to_error(stop_block.clone()).await;
    let message = "transaction 0000000000000000000000000000000000000000000000000000000000000000: Attempted to set undefined fields [invalid_field] for the entity type `TestResult`. Make sure those fields are defined in the schema.".to_string();
    let expected_err = SubgraphError {
        subgraph_id: ctx.deployment.hash.clone(),
        message,
        block_ptr: Some(stop_block),
        handler: None,
        deterministic: true,
    };
    assert_eq_ignore_backtrace(&err, &expected_err);
}

#[tokio::test]
async fn derived_loaders() {
    let RunnerTestRecipe { stores, test_info } =
        RunnerTestRecipe::new("derived_loaders", "derived-loaders").await;

    let blocks = {
        let block_0 = genesis();
        let mut block_1 = empty_block(block_0.ptr(), test_ptr(1));
        push_test_log(&mut block_1, "1_0");
        push_test_log(&mut block_1, "1_1");
        let mut block_2 = empty_block(block_1.ptr(), test_ptr(2));
        push_test_log(&mut block_2, "2_0");
        vec![block_0, block_1, block_2]
    };

    let stop_block = blocks.last().unwrap().block.ptr();

    let chain = chain(&test_info.test_name, blocks, &stores, None).await;
    let ctx = fixture::setup(&test_info, &stores, &chain, None, None).await;

    ctx.start_and_sync_to(stop_block).await;

    // This test tests that derived loaders work correctly.
    // The test fixture has 2 entities, `Bar` and `BBar`, which are derived from `Foo` and `BFoo`.
    // Where `Foo` and `BFoo` are the same entity, but `BFoo` uses Bytes as the ID type.
    // This test tests multiple edge cases of derived loaders:
    // - The derived loader is used in the same handler as the entity is created.
    // - The derived loader is used in the same block as the entity is created.
    // - The derived loader is used in a later block than the entity is created.
    // This is to test the cases where the entities are loaded from the store, `EntityCache.updates` and `EntityCache.handler_updates`
    // It also tests cases where derived entities are updated and deleted when
    // in same handler, same block and later block as the entity is created/updated.
    // For more details on the test cases, see `tests/runner-tests/derived-loaders/src/mapping.ts`
    // Where the test cases are documented in the code.

    let query_res = ctx
    .query(&format!(
        r#"{{ testResult(id:"1_0", block: {{ number: 1 }} ){{ id barDerived{{id value value2}} bBarDerived{{id value value2}} }} }}"#,
    ))
    .await
    .unwrap();

    assert_json_eq!(
        query_res,
        Some(object! {
            testResult: object! {
                id: "1_0",
                barDerived: vec![
                    object! {
                        id: "0_1_0",
                        value: "0",
                        value2: "0"
                    },
                    object! {
                        id: "1_1_0",
                        value: "0",
                        value2: "0"
                    },
                    object! {
                        id: "2_1_0",
                        value: "0",
                        value2: "0"
                    }
                ],
                bBarDerived: vec![
                    object! {
                        id: "0x305f315f30",
                        value: "0",
                        value2: "0"
                    },
                    object! {
                        id: "0x315f315f30",
                        value: "0",
                        value2: "0"
                    },
                    object! {
                        id: "0x325f315f30",
                        value: "0",
                        value2: "0"
                    }
                ]
            }
        })
    );

    let query_res = ctx
    .query(&format!(
        r#"{{ testResult(id:"1_1", block: {{ number: 1 }} ){{ id barDerived{{id value value2}} bBarDerived{{id value value2}} }} }}"#,
    ))
    .await
    .unwrap();

    assert_json_eq!(
        query_res,
        Some(object! {
            testResult: object! {
                id: "1_1",
                barDerived: vec![
                    object! {
                        id: "0_1_1",
                        value: "1",
                        value2: "0"
                    },
                    object! {
                        id: "2_1_1",
                        value: "0",
                        value2: "0"
                    }
                ],
                bBarDerived: vec![
                    object! {
                        id: "0x305f315f31",
                        value: "1",
                        value2: "0"
                    },
                    object! {
                        id: "0x325f315f31",
                        value: "0",
                        value2: "0"
                    }
                ]
            }
        })
    );

    let query_res = ctx.query(
    &format!(
        r#"{{ testResult(id:"2_0" ){{ id barDerived{{id value value2}} bBarDerived{{id value value2}} }} }}"#
    )
)
.await
.unwrap();
    assert_json_eq!(
        query_res,
        Some(object! {
            testResult: object! {
                id: "2_0",
                barDerived: vec![
                    object! {
                        id: "0_2_0",
                        value: "2",
                        value2: "0"
                    }
                ],
                bBarDerived: vec![
                    object! {
                        id: "0x305f325f30",
                        value: "2",
                        value2: "0"
                    }
                ]
            }
        })
    );
}

// This PR https://github.com/graphprotocol/graph-node/pull/4787
// changed the way TriggerFilters were built
// A bug was introduced in the PR which resulted in filters for substreams not being included
// This test tests that the TriggerFilter is built correctly for substreams
#[tokio::test]
async fn substreams_trigger_filter_construction() -> anyhow::Result<()> {
    let RunnerTestRecipe { stores, test_info } =
        RunnerTestRecipe::new("substreams", "substreams").await;

    let chain = substreams_chain(&test_info.test_name, &stores).await;
    let ctx = fixture::setup(&test_info, &stores, &chain, None, None).await;

    let runner = ctx.runner_substreams(test_ptr(0)).await;
    let filter = runner.build_filter_for_test();

    assert_eq!(filter.module_name(), "graph_out");
    assert_eq!(filter.modules().as_ref().unwrap().modules.len(), 2);
    assert_eq!(filter.start_block().unwrap(), 0);
    assert_eq!(filter.data_sources_len(), 1);
    Ok(())
}

#[tokio::test]
async fn end_block() -> anyhow::Result<()> {
    let RunnerTestRecipe { stores, test_info } =
        RunnerTestRecipe::new("end_block", "end-block").await;
    // This test is to test the end_block feature which enables datasources to stop indexing
    // At a user specified block, this test tests whether the subgraph stops indexing at that
    // block, rebuild the filters accurately when a revert occurs etc

    // test if the TriggerFilter includes the given contract address
    async fn test_filter(
        ctx: &TestContext,
        block_ptr: BlockPtr,
        addr: &Address,
        should_contain_addr: bool,
    ) {
        let runner = ctx.runner(block_ptr.clone()).await;
        let runner = runner.run_for_test(false).await.unwrap();
        let filter = runner.context().filter.as_ref().unwrap();
        let addresses = filter.log().contract_addresses().collect::<Vec<_>>();

        if should_contain_addr {
            assert!(addresses.contains(&addr));
        } else {
            assert!(!addresses.contains(&addr));
        };
    }

    let blocks = {
        let block_0 = genesis();
        let block_1 = empty_block(block_0.ptr(), test_ptr(1));
        let block_2 = empty_block(block_1.ptr(), test_ptr(2));
        let block_3 = empty_block(block_2.ptr(), test_ptr(3));
        let block_4 = empty_block(block_3.ptr(), test_ptr(4));
        let block_5 = empty_block(block_4.ptr(), test_ptr(5));
        let block_6 = empty_block(block_5.ptr(), test_ptr(6));
        let block_7 = empty_block(block_6.ptr(), test_ptr(7));
        let block_8 = empty_block(block_7.ptr(), test_ptr(8));
        let block_9 = empty_block(block_8.ptr(), test_ptr(9));
        let block_10 = empty_block(block_9.ptr(), test_ptr(10));
        vec![
            block_0, block_1, block_2, block_3, block_4, block_5, block_6, block_7, block_8,
            block_9, block_10,
        ]
    };

    let stop_block = blocks.last().unwrap().block.ptr();

    let chain = chain(&test_info.test_name, blocks.clone(), &stores, None).await;
    let ctx = fixture::setup(&test_info, &stores, &chain, None, None).await;

    let addr = Address::from_str("0x0000000000000000000000000000000000000000").unwrap();

    // Test if the filter includes the contract address before the stop block.
    test_filter(&ctx, test_ptr(5), &addr, true).await;

    // Test if the filter excludes the contract address after the stop block.
    test_filter(&ctx, stop_block, &addr, false).await;

    // Query the subgraph to ensure the last indexed block is number 8, indicating the end block feature works.
    let query_res = ctx
        .query(r#"{ blocks(first: 1, orderBy: number, orderDirection: desc) { number hash } }"#)
        .await
        .unwrap();

    assert_eq!(
        query_res,
        Some(
            object! { blocks: vec![object!{ number: "8", hash:"0x0000000000000000000000000000000000000000000000000000000000000008"  }] }
        )
    );

    // Simulate a chain reorg and ensure the filter rebuilds accurately post-reorg.
    {
        ctx.rewind(test_ptr(6));

        let mut blocks = blocks[0..8].to_vec().clone();

        // Create new blocks to represent a fork from block 7 onwards, including a reorged block 8.
        let block_8_1_ptr = test_ptr_reorged(8, 1);
        let block_8_1 = empty_block(test_ptr(7), block_8_1_ptr.clone());
        blocks.push(block_8_1);
        blocks.push(empty_block(block_8_1_ptr, test_ptr(9)));

        let stop_block = blocks.last().unwrap().block.ptr();

        chain.set_block_stream(blocks.clone());

        // Test the filter behavior in the presence of the reorganized chain.
        test_filter(&ctx, test_ptr(7), &addr, true).await;
        test_filter(&ctx, stop_block, &addr, false).await;

        // Verify that after the reorg, the last Block entity still reflects block number 8, but with a different hash.
        let query_res = ctx
            .query(
                r#"{
                blocks(first: 1, orderBy: number, orderDirection: desc) {
                    number
                    hash
                }
            }"#,
            )
            .await
            .unwrap();

        assert_eq!(
            query_res,
            Some(object! {
                blocks: vec![
                    object!{
                        number: "8",
                        hash: "0x0000000100000000000000000000000000000000000000000000000000000008"
                    }
                ],
            })
        );
    }

    Ok(())
}

#[tokio::test]
async fn file_data_sources() {
    let RunnerTestRecipe { stores, test_info } =
        RunnerTestRecipe::new("file-data-sourcess", "file-data-sources").await;

    async fn add_content_to_ipfs(content: &str) -> String {
        add_files_to_local_ipfs_node_for_testing([content.as_bytes().to_vec()])
            .await
            .unwrap()[0]
            .hash
            .to_owned()
    }

    let hash_1 = add_content_to_ipfs("EXAMPLE_1").await;
    let hash_2 = add_content_to_ipfs("EXAMPLE_2").await;
    let hash_3 = add_content_to_ipfs("EXAMPLE_3").await;
    let hash_4 = add_content_to_ipfs("EXAMPLE_4").await;

    //concatenate hash2 and hash3
    let hash_2_comma_3 = format!("{},{}", hash_2, hash_3);

    let blocks = {
        let block_0 = genesis();
        let mut block_1 = empty_block(block_0.ptr(), test_ptr(1));
        push_test_command(&mut block_1, "CREATE_FILE", &hash_1);
        let mut block_2 = empty_block(block_1.ptr(), test_ptr(2));
        push_test_command(&mut block_2, "CREATE_FILE", &hash_1);

        let mut block_3 = empty_block(block_2.ptr(), test_ptr(3));
        push_test_command(
            &mut block_3,
            "SPAWN_FDS_FROM_OFFCHAIN_HANDLER",
            &hash_2_comma_3,
        );

        let block_4 = empty_block(block_3.ptr(), test_ptr(4));

        let mut block_5 = empty_block(block_4.ptr(), test_ptr(5));
        push_test_command(
            &mut block_5,
            "CREATE_ONCHAIN_DATASOURCE_FROM_OFFCHAIN_HANDLER",
            &hash_3,
        );

        let mut block_6 = empty_block(block_5.ptr(), test_ptr(6));

        push_test_command(&mut block_6, "CREATE_UNDEFINED_ENTITY", &hash_4);

        vec![
            block_0, block_1, block_2, block_3, block_4, block_5, block_6,
        ]
    };

    // This test assumes the file data sources will be processed in the same block in which they are
    // created. But the test might fail due to a race condition if for some reason it takes longer
    // than expected to fetch the file from IPFS. The sleep here will conveniently happen after the
    // data source is added to the offchain monitor but before the monitor is checked, in an an
    // attempt to ensure the monitor has enough time to fetch the file.
    let adapter_selector = NoopAdapterSelector {
        x: PhantomData,
        triggers_in_block_sleep: Duration::from_millis(150),
    };
    let chain = chain(
        &test_info.test_name,
        blocks.clone(),
        &stores,
        Some(Arc::new(adapter_selector)),
    )
    .await;
    let ctx = fixture::setup(&test_info, &stores, &chain, None, None).await;

    {
        ctx.start_and_sync_to(test_ptr(1)).await;

        let content = "EXAMPLE_1";
        let query_res = ctx
            .query(&format!(
                r#"{{ fileEntity(id: "{}") {{ id, content }} }}"#,
                hash_1.clone()
            ))
            .await
            .unwrap();

        let store = ctx.store.cheap_clone();
        let writable = store
            .writable(ctx.logger.clone(), ctx.deployment.id, Arc::new(Vec::new()))
            .await
            .unwrap();
        let datasources = writable.load_dynamic_data_sources(vec![]).await.unwrap();
        assert!(datasources.len() == 1);

        assert_json_eq!(
            query_res,
            Some(object! { fileEntity: object!{ id: hash_1.clone(), content: content } })
        );
    }

    // Should not create duplicate datasource
    {
        ctx.start_and_sync_to(test_ptr(2)).await;

        let store = ctx.store.cheap_clone();
        let writable = store
            .writable(ctx.logger.clone(), ctx.deployment.id, Arc::new(Vec::new()))
            .await
            .unwrap();
        let datasources = writable.load_dynamic_data_sources(vec![]).await.unwrap();
        assert!(datasources.len() == 1);
    }

    // Create a File data source from a same type of file data source handler
    {
        ctx.start_and_sync_to(test_ptr(4)).await;

        let content = "EXAMPLE_3";
        let query_res = ctx
            .query(&format!(
                r#"{{ fileEntity(id: "{}") {{ id, content }} }}"#,
                hash_3.clone()
            ))
            .await
            .unwrap();
        assert_json_eq!(
            query_res,
            Some(object! { fileEntity: object!{ id: hash_3.clone(), content: content } })
        );
    }

    // Should not allow creating on-chain data source from off-chain data source handler
    {
        let err = ctx.start_and_sync_to_error(test_ptr(5)).await;
        let message =
            "Attempted to create on-chain data source in offchain data source handler.".to_string();
        assert!(err.to_string().contains(&message));
    }

    // Should not allow creating conflicting entity. ie: Entity created in offchain handler cannot be created in onchain handler
    {
        ctx.rewind(test_ptr(4));

        let mut blocks = blocks.clone();
        blocks.retain(|block| block.block.number() <= 4);

        let mut block_5 = empty_block(test_ptr(4), test_ptr(5));
        push_test_command(&mut block_5, "CREATE_CONFLICTING_ENTITY", &hash_1);
        blocks.push(block_5.clone());

        chain.set_block_stream(blocks);

        let message = "writing FileEntity entities at block 5 failed: conflicting key value violates exclusion constraint \"file_entity_id_block_range_excl\" Query: insert 1 rows with ids [QmYiiCtcXmSHXN3m2nyqLaTM7zi81KjVdZ9WXkcrCKrkjr@[5, ∞)]";

        let runner = ctx.runner(block_5.ptr()).await;
        let err = runner
            .run()
            .await
            .err()
            .unwrap_or_else(|| panic!("subgraph ran successfully but an error was expected"));

        assert_eq!(err.to_string(), message);
    }

    // Should not allow accessing entities created in offchain handlers in onchain handlers
    {
        ctx.rewind(test_ptr(4));

        let mut blocks = blocks.clone();
        blocks.retain(|block| block.block.number() <= 4);

        let mut block_5 = empty_block(test_ptr(4), test_ptr(5));
        push_test_command(
            &mut block_5,
            "ACCESS_AND_UPDATE_OFFCHAIN_ENTITY_IN_ONCHAIN_HANDLER",
            &hash_1,
        );
        blocks.push(block_5.clone());

        chain.set_block_stream(blocks);

        ctx.start_and_sync_to(block_5.ptr()).await;

        let content = "EXAMPLE_1";
        let query_res = ctx
            .query(&format!(
                r#"{{ fileEntity(id: "{}") {{ id, content }} }}"#,
                hash_1.clone()
            ))
            .await
            .unwrap();
        assert_json_eq!(
            query_res,
            Some(object! { fileEntity: object!{ id: hash_1.clone(), content: content } })
        );
    }

    // Prevent access to entities created by offchain handlers when using derived loaders in onchain handlers.
    {
        ctx.rewind(test_ptr(4));

        let mut blocks = blocks.clone();
        blocks.retain(|block| block.block.number() <= 4);

        let hash_5 = add_content_to_ipfs("EXAMPLE_5").await;

        let mut block_5 = empty_block(test_ptr(4), test_ptr(5));
        push_test_command(&mut block_5, "CREATE_FOO", &hash_5);
        blocks.push(block_5.clone());

        let mut block_6 = empty_block(block_5.ptr(), test_ptr(6));
        push_test_command(
            &mut block_6,
            "ACCESS_FILE_ENTITY_THROUGH_DERIVED_FIELD",
            &hash_5,
        );
        blocks.push(block_6.clone());

        chain.set_block_stream(blocks);

        ctx.start_and_sync_to(block_5.ptr()).await;

        let query_res = ctx
            .query(&format!(
                r#"{{ foo(id: "{}") {{ id, ipfs {{ id, content }} }} }}"#,
                hash_5.clone(),
            ))
            .await
            .unwrap();
        let content = "EXAMPLE_5";
        assert_json_eq!(
            query_res,
            Some(
                object! { foo: object!{ id: hash_5.clone(), ipfs: object!{id: hash_5.clone(), content: content}} }
            )
        );

        ctx.start_and_sync_to(block_6.ptr()).await;
    }

    // Should not allow creating entity that is not declared in the manifest for the offchain datasource
    {
        ctx.rewind(test_ptr(4));

        let mut blocks = blocks.clone();
        blocks.retain(|block| block.block.number() <= 4);

        let mut block_5 = empty_block(test_ptr(4), test_ptr(5));
        push_test_command(&mut block_5, "CREATE_UNDEFINED_ENTITY", &hash_1);
        blocks.push(block_5.clone());

        chain.set_block_stream(blocks);

        let message = "error while executing at wasm backtrace:\t    0: 0x3490 - <unknown>!generated/schema/Foo#save\t    1: 0x3eb2 - <unknown>!src/mapping/handleFile: entity type `Foo` is not on the 'entities' list for data source `File`. Hint: Add `Foo` to the 'entities' list, which currently is: `FileEntity`. in handler `handleFile` at block #5 () at block #5 (0000000000000000000000000000000000000000000000000000000000000005)";

        let err = ctx.start_and_sync_to_error(block_5.ptr()).await;

        assert_eq!(err.to_string(), message);
    }
}

#[tokio::test]
async fn block_handlers() {
    let RunnerTestRecipe { stores, test_info } =
        RunnerTestRecipe::new("block_handlers", "block-handlers").await;

    let blocks = {
        let block_0 = genesis();
        let block_1_to_3 = generate_empty_blocks_for_range(block_0.ptr(), 1, 3, 0);
        let block_4 = {
            let mut block = empty_block(block_1_to_3.last().unwrap().ptr(), test_ptr(4));
            push_test_polling_trigger(&mut block);
            push_test_log(&mut block, "create_template");
            block
        };
        let block_5 = {
            let mut block = empty_block(block_4.ptr(), test_ptr(5));
            push_test_polling_trigger(&mut block);
            block
        };
        let block_6 = {
            let mut block = empty_block(block_5.ptr(), test_ptr(6));
            push_test_polling_trigger(&mut block);
            block
        };
        let block_7 = {
            let mut block = empty_block(block_6.ptr(), test_ptr(7));
            push_test_polling_trigger(&mut block);
            block
        };
        let block_8 = {
            let mut block = empty_block(block_7.ptr(), test_ptr(8));
            push_test_polling_trigger(&mut block);
            block
        };
        let block_9 = {
            let mut block = empty_block(block_8.ptr(), test_ptr(9));
            push_test_polling_trigger(&mut block);
            block
        };
        let block_10 = {
            let mut block = empty_block(block_9.ptr(), test_ptr(10));
            push_test_polling_trigger(&mut block);
            block
        };

        // return the blocks
        vec![block_0]
            .into_iter()
            .chain(block_1_to_3)
            .chain(vec![
                block_4, block_5, block_6, block_7, block_8, block_9, block_10,
            ])
            .collect()
    };

    let chain = chain(&test_info.test_name, blocks, &stores, None).await;

    let mut env_vars = EnvVars::default();
    env_vars.experimental_static_filters = true;

    let ctx = fixture::setup(&test_info, &stores, &chain, None, Some(env_vars)).await;

    ctx.start_and_sync_to(test_ptr(10)).await;

    let query = format!(
        r#"{{ blockFromPollingHandlers(first: {first}) {{ id, hash }} }}"#,
        first = 3
    );
    let query_res = ctx.query(&query).await.unwrap();

    assert_eq!(
        query_res,
        Some(object! {
            blockFromPollingHandlers: vec![
                object! {
                    id: test_ptr(0).number.to_string(),
                    hash:format!("0x{}",test_ptr(0).hash_hex()) ,
                },
                object! {
                id: test_ptr(4).number.to_string(),
                hash:format!("0x{}",test_ptr(4).hash_hex()) ,
                },
                object! {
                    id: test_ptr(8).number.to_string(),
                    hash:format!("0x{}",test_ptr(8).hash_hex()) ,
                },
            ]
        })
    );

    let query = format!(
        r#"{{ blockFromOtherPollingHandlers(first: {first}, orderBy: number) {{ id, hash }} }}"#,
        first = 4
    );
    let query_res = ctx.query(&query).await.unwrap();

    assert_eq!(
        query_res,
        Some(object! {
            blockFromOtherPollingHandlers: vec![
                // TODO: The block in which the handler was created is not included
                // in the result. This is because for runner tests we mock the triggers_adapter
                // A mock triggers adapter which can be used here is to be implemented
                // object! {
                //     id: test_ptr(4).number.to_string(),
                //     hash:format!("0x{}",test_ptr(10).hash_hex()) ,
                // },
                object!{
                    id: test_ptr(6).number.to_string(),
                    hash:format!("0x{}",test_ptr(6).hash_hex()) ,
                },
                object!{
                    id: test_ptr(8).number.to_string(),
                    hash:format!("0x{}",test_ptr(8).hash_hex()) ,
                },
                object!{
                    id: test_ptr(10).number.to_string(),
                    hash:format!("0x{}",test_ptr(10).hash_hex()) ,
                },
            ]
        })
    );
}

#[tokio::test]
async fn template_static_filters_false_positives() {
    let RunnerTestRecipe { stores, test_info } = RunnerTestRecipe::new(
        "template_static_filters_false_positives",
        "dynamic-data-source",
    )
    .await;

    let blocks = {
        let block_0 = genesis();
        let block_1 = empty_block(block_0.ptr(), test_ptr(1));
        let block_2 = empty_block(block_1.ptr(), test_ptr(2));
        vec![block_0, block_1, block_2]
    };
    let stop_block = test_ptr(1);
    let chain = chain(&test_info.test_name, blocks, &stores, None).await;

    let mut env_vars = EnvVars::default();
    env_vars.experimental_static_filters = true;

    let ctx = fixture::setup(&test_info, &stores, &chain, None, Some(env_vars)).await;
    ctx.start_and_sync_to(stop_block).await;

    let poi = ctx
        .store
        .get_proof_of_indexing(&ctx.deployment.hash, &None, test_ptr(1))
        .await
        .unwrap();

    // This check exists to prevent regression of https://github.com/graphprotocol/graph-node/issues/3963
    // when false positives go through the block stream, they should be discarded by
    // `DataSource::match_and_decode`. The POI below is generated consistently from the empty
    // POI table. If this fails it's likely that either the bug was re-introduced or there is
    // a change in the POI infrastructure. Or the subgraph id changed.
    assert_eq!(
        hex::encode(poi.unwrap()),
        "c72af01a19a4e35a35778821a354b7a781062a9320ac8796ea65b115cb9844bf"
    );
}

#[tokio::test]
async fn parse_data_source_context() {
    let RunnerTestRecipe { stores, test_info } =
        RunnerTestRecipe::new("parse_data_source_context", "data-sources").await;

    let blocks = {
        let block_0 = genesis();
        let block_1 = empty_block(block_0.ptr(), test_ptr(1));
        let block_2 = empty_block(block_1.ptr(), test_ptr(2));
        vec![block_0, block_1, block_2]
    };
    let stop_block = blocks.last().unwrap().block.ptr();
    let chain = chain(&test_info.test_name, blocks, &stores, None).await;

    let ctx = fixture::setup(&test_info, &stores, &chain, None, None).await;
    ctx.start_and_sync_to(stop_block).await;

    let query_res = ctx
        .query(r#"{ data(id: "0") { id, foo, bar } }"#)
        .await
        .unwrap();

    assert_eq!(
        query_res,
        Some(object! { data: object!{ id: "0", foo: "test", bar: 1 } })
    );
}

#[tokio::test]
async fn retry_create_ds() {
    let RunnerTestRecipe { stores, test_info } =
        RunnerTestRecipe::new("retry_create_ds", "data-source-revert2").await;

    let blocks = {
        let block0 = genesis();
        let block1 = empty_block(block0.ptr(), test_ptr(1));
        let block1_reorged_ptr = BlockPtr {
            number: 1,
            hash: H256::from_low_u64_be(12).into(),
        };
        let block1_reorged = empty_block(block0.ptr(), block1_reorged_ptr);
        let block2 = empty_block(block1_reorged.ptr(), test_ptr(2));
        vec![block0, block1, block1_reorged, block2]
    };
    let stop_block = blocks.last().unwrap().block.ptr();

    let called = AtomicBool::new(false);
    let triggers_in_block = Arc::new(
        move |block: <graph_chain_ethereum::Chain as Blockchain>::Block| {
            let logger = Logger::root(Discard, o!());
            // Comment this out and the test will pass.
            if block.number() > 0 && !called.load(atomic::Ordering::SeqCst) {
                called.store(true, atomic::Ordering::SeqCst);
                return Err(anyhow::anyhow!("This error happens once"));
            }
            Ok(BlockWithTriggers::new(block, Vec::new(), &logger))
        },
    );
    let triggers_adapter = Arc::new(MockAdapterSelector {
        x: PhantomData,
        triggers_in_block_sleep: Duration::ZERO,
        triggers_in_block,
    });
    let chain = chain(
        &test_info.test_name,
        blocks,
        &stores,
        Some(triggers_adapter),
    )
    .await;

    let mut env_vars = EnvVars::default();
    env_vars.subgraph_error_retry_ceil = Duration::from_secs(1);

    let ctx = fixture::setup(&test_info, &stores, &chain, None, Some(env_vars)).await;

    let runner = ctx
        .runner(stop_block)
        .await
        .run_for_test(true)
        .await
        .unwrap();
    assert_eq!(runner.context().hosts_len(), 2);
}

#[tokio::test]
async fn fatal_error() -> anyhow::Result<()> {
    let RunnerTestRecipe { stores, test_info } =
        RunnerTestRecipe::new("fatal_error", "fatal-error").await;

    let blocks = {
        let block_0 = genesis();
        let block_1 = empty_block(block_0.ptr(), test_ptr(1));
        let block_2 = empty_block(block_1.ptr(), test_ptr(2));
        let block_3 = empty_block(block_2.ptr(), test_ptr(3));
        vec![block_0, block_1, block_2, block_3]
    };

    let stop_block = blocks.last().unwrap().block.ptr();

    let chain = chain(&test_info.test_name, blocks, &stores, None).await;
    let ctx = fixture::setup(&test_info, &stores, &chain, None, None).await;

    ctx.start_and_sync_to_error(stop_block).await;

    // Go through the indexing status API to also test it.
    let status = ctx.indexing_status().await;
    assert!(status.health == SubgraphHealth::Failed);
    assert!(status.entity_count == 1.into()); // Only PoI
    let err = status.fatal_error.unwrap();
    assert!(err.block.number == 3.into());
    assert!(err.deterministic);

    let sg_store = stores.network_store.subgraph_store();

    let poi2 = sg_store
        .get_proof_of_indexing(&test_info.hash, &None, test_ptr(2))
        .await
        .unwrap();

    // All POIs past this point should be the same
    let poi3 = sg_store
        .get_proof_of_indexing(&test_info.hash, &None, test_ptr(3))
        .await
        .unwrap();
    assert!(poi2 != poi3);

    let poi4 = sg_store
        .get_proof_of_indexing(&test_info.hash, &None, test_ptr(4))
        .await
        .unwrap();
    assert_eq!(poi3, poi4);
    assert!(poi2 != poi4);

    let poi100 = sg_store
        .get_proof_of_indexing(&test_info.hash, &None, test_ptr(100))
        .await
        .unwrap();
    assert_eq!(poi4, poi100);
    assert!(poi2 != poi100);

    // Test that rewind unfails the subgraph.
    ctx.rewind(test_ptr(1));
    let status = ctx.indexing_status().await;
    assert!(status.health == SubgraphHealth::Healthy);
    assert!(status.fatal_error.is_none());

    Ok(())
}

#[tokio::test]
async fn arweave_file_data_sources() {
    let RunnerTestRecipe { stores, test_info } =
        RunnerTestRecipe::new("arweave_file_data_sources", "arweave-file-data-sources").await;

    let blocks = {
        let block_0 = genesis();
        let block_1 = empty_block(block_0.ptr(), test_ptr(1));
        let block_2 = empty_block(block_1.ptr(), test_ptr(2));
        vec![block_0, block_1, block_2]
    };

    // HASH used in the mappings.
    let id = "8APeQ5lW0-csTcBaGdPBDLAL2ci2AT9pTn2tppGPU_8";

    // This test assumes the file data sources will be processed in the same block in which they are
    // created. But the test might fail due to a race condition if for some reason it takes longer
    // than expected to fetch the file from arweave. The sleep here will conveniently happen after the
    // data source is added to the offchain monitor but before the monitor is checked, in an an
    // attempt to ensure the monitor has enough time to fetch the file.
    let adapter_selector = NoopAdapterSelector {
        x: PhantomData,
        triggers_in_block_sleep: Duration::from_millis(1500),
    };
    let chain = chain(
        &test_info.test_name,
        blocks.clone(),
        &stores,
        Some(Arc::new(adapter_selector)),
    )
    .await;
    let ctx = fixture::setup(&test_info, &stores, &chain, None, None).await;
    ctx.start_and_sync_to(test_ptr(2)).await;

    let store = ctx.store.cheap_clone();
    let writable = store
        .writable(ctx.logger.clone(), ctx.deployment.id, Arc::new(Vec::new()))
        .await
        .unwrap();
    let datasources = writable.load_dynamic_data_sources(vec![]).await.unwrap();
    assert_eq!(datasources.len(), 1);
    let ds = datasources.first().unwrap();
    assert_ne!(ds.causality_region, CausalityRegion::ONCHAIN);
    assert_eq!(ds.done_at.is_some(), true);
    assert_eq!(
        ds.param.as_ref().unwrap(),
        &Bytes::from(Word::from(id).as_bytes())
    );

    let content_bytes = ctx.arweave_resolver.get(&Word::from(id)).await.unwrap();
    let content = String::from_utf8(content_bytes.into()).unwrap();
    let query_res = ctx
        .query(&format!(r#"{{ file(id: "{id}") {{ id, content }} }}"#,))
        .await
        .unwrap();

    assert_json_eq!(
        query_res,
        Some(object! { file: object!{ id: id, content: content.clone() } })
    );
}

/// deploy_cmd is the command to run to deploy the subgraph. If it is None, the
/// default `yarn deploy:test` is used.
async fn build_subgraph(dir: &str, deploy_cmd: Option<&str>) -> DeploymentHash {
    build_subgraph_with_yarn_cmd(dir, deploy_cmd.unwrap_or("deploy:test")).await
}

async fn build_subgraph_with_yarn_cmd(dir: &str, yarn_cmd: &str) -> DeploymentHash {
    build_subgraph_with_yarn_cmd_and_arg(dir, yarn_cmd, None).await
}

async fn build_subgraph_with_yarn_cmd_and_arg(
    dir: &str,
    yarn_cmd: &str,
    arg: Option<&str>,
) -> DeploymentHash {
    // Test that IPFS is up.
    ipfs::IpfsRpcClient::new(ipfs::ServerAddress::local_rpc_api(), &graph::log::discard())
        .await
        .expect("Could not connect to IPFS, make sure it's running at port 5001");

    // Make sure dependencies are present.

    run_cmd(
        Command::new("yarn")
            .arg("install")
            .arg("--mutex")
            .arg("file:.yarn-mutex")
            .current_dir("./runner-tests/"),
    );

    // Run codegen.
    run_cmd(Command::new("yarn").arg("codegen").current_dir(dir));

    let mut args = vec![yarn_cmd];
    args.extend(arg);

    // Run `deploy` for the side effect of uploading to IPFS, the graph node url
    // is fake and the actual deploy call is meant to fail.
    let deploy_output = run_cmd(
        Command::new("yarn")
            .args(&args)
            .env("IPFS_URI", "http://127.0.0.1:5001")
            .env("GRAPH_NODE_ADMIN_URI", "http://localhost:0")
            .current_dir(dir),
    );

    // Hack to extract deployment id from `graph deploy` output.
    const ID_PREFIX: &str = "Build completed: ";
    let Some(mut line) = deploy_output.lines().find(|line| line.contains(ID_PREFIX)) else {
        panic!("No deployment id found, graph deploy probably had an error")
    };
    if !line.starts_with(ID_PREFIX) {
        line = &line[5..line.len() - 5]; // workaround for colored output
    }
    DeploymentHash::new(line.trim_start_matches(ID_PREFIX)).unwrap()
}
