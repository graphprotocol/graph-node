use std::marker::PhantomData;
use std::process::Command;
use std::sync::atomic::{self, AtomicBool};
use std::sync::Arc;
use std::time::Duration;

use assert_json_diff::assert_json_eq;
use graph::blockchain::block_stream::BlockWithTriggers;
use graph::blockchain::{Block, BlockPtr, Blockchain};
use graph::data::subgraph::schema::{SubgraphError, SubgraphHealth};
use graph::data_source::CausalityRegion;
use graph::env::EnvVars;
use graph::ipfs_client::IpfsClient;
use graph::object;
use graph::prelude::ethabi::ethereum_types::H256;
use graph::prelude::{
    CheapClone, DeploymentHash, SubgraphAssignmentProvider, SubgraphName, SubgraphStore,
};
use graph_tests::fixture::ethereum::{chain, empty_block, genesis, push_test_log};
use graph_tests::fixture::{
    self, stores, test_ptr, test_ptr_reorged, MockAdapterSelector, NoopAdapterSelector, Stores,
};
use graph_tests::helpers::run_cmd;
use slog::{o, Discard, Logger};

struct RunnerTestRecipe {
    stores: Stores,
    subgraph_name: SubgraphName,
    hash: DeploymentHash,
}

impl RunnerTestRecipe {
    async fn new(subgraph_name: &str) -> Self {
        let subgraph_name = SubgraphName::new(subgraph_name).unwrap();
        let test_dir = format!("./runner-tests/{}", subgraph_name);

        let (stores, hash) = tokio::join!(
            stores("./runner-tests/config.simple.toml"),
            build_subgraph(&test_dir)
        );

        Self {
            stores,
            subgraph_name,
            hash,
        }
    }
}

#[tokio::test]
async fn data_source_revert() -> anyhow::Result<()> {
    let RunnerTestRecipe {
        stores,
        subgraph_name,
        hash,
    } = RunnerTestRecipe::new("data-source-revert").await;

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

    let chain = chain(blocks.clone(), &stores, None).await;
    let ctx = fixture::setup(subgraph_name.clone(), &hash, &stores, &chain, None, None).await;

    let stop_block = test_ptr(2);
    ctx.start_and_sync_to(stop_block).await;
    ctx.provider.stop(ctx.deployment.clone()).await.unwrap();

    // Test loading data sources from DB.
    let stop_block = test_ptr(3);
    ctx.start_and_sync_to(stop_block).await;

    // Test grafted version
    let subgraph_name = SubgraphName::new("data-source-revert-grafted").unwrap();
    let hash =
        build_subgraph_with_yarn_cmd("./runner-tests/data-source-revert", "deploy:test-grafted")
            .await;
    let graft_block = Some(test_ptr(3));
    let ctx = fixture::setup(
        subgraph_name.clone(),
        &hash,
        &stores,
        &chain,
        graft_block,
        None,
    )
    .await;
    let stop_block = test_ptr(4);
    ctx.start_and_sync_to(stop_block).await;

    let query_res = ctx
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

    Ok(())
}

#[tokio::test]
async fn typename() -> anyhow::Result<()> {
    let RunnerTestRecipe {
        stores,
        subgraph_name,
        hash,
    } = RunnerTestRecipe::new("typename").await;

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

    let chain = chain(blocks, &stores, None).await;
    let ctx = fixture::setup(subgraph_name.clone(), &hash, &stores, &chain, None, None).await;

    ctx.start_and_sync_to(stop_block).await;

    Ok(())
}

#[tokio::test]
async fn file_data_sources() {
    let RunnerTestRecipe {
        stores,
        subgraph_name,
        hash,
    } = RunnerTestRecipe::new("file-data-sources").await;

    let blocks = {
        let block_0 = genesis();
        let block_1 = empty_block(block_0.ptr(), test_ptr(1));
        let block_2 = empty_block(block_1.ptr(), test_ptr(2));
        let block_3 = empty_block(block_2.ptr(), test_ptr(3));
        let block_4 = empty_block(block_3.ptr(), test_ptr(4));
        let mut block_5 = empty_block(block_4.ptr(), test_ptr(5));
        push_test_log(&mut block_5, "createFile2");
        vec![block_0, block_1, block_2, block_3, block_4, block_5]
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
    let chain = chain(blocks.clone(), &stores, Some(Arc::new(adapter_selector))).await;
    let ctx = fixture::setup(subgraph_name.clone(), &hash, &stores, &chain, None, None).await;
    ctx.start_and_sync_to(test_ptr(1)).await;

    // CID of `file-data-sources/abis/Contract.abi` after being processed by graph-cli.
    let id = "QmQ2REmceVtzawp7yrnxLQXgNNCtFHEnig6fL9aqE1kcWq";
    let content_bytes = ctx.ipfs.cat_all(id, Duration::from_secs(10)).await.unwrap();
    let content = String::from_utf8(content_bytes.into()).unwrap();
    let query_res = ctx
        .query(&format!(r#"{{ ipfsFile(id: "{id}") {{ id, content }} }}"#,))
        .await
        .unwrap();

    assert_json_eq!(
        query_res,
        Some(object! { ipfsFile: object!{ id: id, content: content.clone() } })
    );

    // assert whether duplicate data sources are created.
    ctx.start_and_sync_to(test_ptr(2)).await;

    let store = ctx.store.cheap_clone();
    let writable = store
        .writable(ctx.logger.clone(), ctx.deployment.id)
        .await
        .unwrap();
    let datasources = writable.load_dynamic_data_sources(vec![]).await.unwrap();
    assert!(datasources.len() == 1);

    ctx.start_and_sync_to(test_ptr(3)).await;

    let query_res = ctx
        .query(&format!(r#"{{ ipfsFile1(id: "{id}") {{ id, content }} }}"#,))
        .await
        .unwrap();

    assert_json_eq!(
        query_res,
        Some(object! { ipfsFile1: object!{ id: id , content: content } })
    );

    ctx.start_and_sync_to(test_ptr(4)).await;
    let writable = ctx
        .store
        .clone()
        .writable(ctx.logger.clone(), ctx.deployment.id)
        .await
        .unwrap();
    let data_sources = writable.load_dynamic_data_sources(vec![]).await.unwrap();
    assert!(data_sources.len() == 2);

    let mut causality_region = CausalityRegion::ONCHAIN;
    for data_source in data_sources {
        assert!(data_source.done_at.is_some());
        assert!(data_source.causality_region == causality_region.next());
        causality_region = causality_region.next();
    }

    let stop_block = test_ptr(5);
    let err = ctx.start_and_sync_to_error(stop_block.clone()).await;
    let message = "entity type `IpfsFile1` is not on the 'entities' list for data source `File2`. \
                   Hint: Add `IpfsFile1` to the 'entities' list, which currently is: `IpfsFile`.\twasm backtrace:\t    0: 0x365d - <unknown>!src/mapping/handleFile1\t in handler `handleFile1` at block #5 ()".to_string();
    let expected_err = SubgraphError {
        subgraph_id: ctx.deployment.hash.clone(),
        message,
        block_ptr: Some(stop_block),
        handler: None,
        deterministic: false,
    };
    assert_eq!(err, expected_err);

    // Unfail the subgraph to test a conflict between an onchain and offchain entity
    {
        ctx.rewind(test_ptr(4));

        // Replace block number 5 with one that contains a different event
        let mut blocks = blocks.clone();
        blocks.pop();
        let block_5_1_ptr = test_ptr_reorged(5, 1);
        let mut block_5_1 = empty_block(test_ptr(4), block_5_1_ptr.clone());
        push_test_log(&mut block_5_1, "saveConflictingEntity");
        blocks.push(block_5_1);

        chain.set_block_stream(blocks);

        // Errors in the store pipeline can be observed by using the runner directly.
        let runner = ctx.runner(block_5_1_ptr.clone()).await;
        let err = runner
            .run()
            .await
            .err()
            .unwrap_or_else(|| panic!("subgraph ran successfully but an error was expected"));

        let message =
            "store error: conflicting key value violates exclusion constraint \"ipfs_file_id_block_range_excl\""
                .to_string();
        assert_eq!(err.to_string(), message);
    }

    // Unfail the subgraph to test a conflict between an onchain and offchain entity
    {
        // Replace block number 5 with one that contains a different event
        let mut blocks = blocks.clone();
        blocks.pop();
        let block_5_2_ptr = test_ptr_reorged(5, 2);
        let mut block_5_2 = empty_block(test_ptr(4), block_5_2_ptr.clone());
        push_test_log(&mut block_5_2, "createFile1");
        blocks.push(block_5_2);

        chain.set_block_stream(blocks);

        // Errors in the store pipeline can be observed by using the runner directly.
        let err = ctx
            .runner(block_5_2_ptr.clone())
            .await
            .run()
            .await
            .err()
            .unwrap_or_else(|| panic!("subgraph ran successfully but an error was expected"));

        let message =
            "store error: conflicting key value violates exclusion constraint \"ipfs_file_1_id_block_range_excl\""
                .to_string();
        assert_eq!(err.to_string(), message);
    }
}

#[tokio::test]
async fn template_static_filters_false_positives() {
    let RunnerTestRecipe {
        stores,
        subgraph_name,
        hash,
    } = RunnerTestRecipe::new("dynamic-data-source").await;

    let blocks = {
        let block_0 = genesis();
        let block_1 = empty_block(block_0.ptr(), test_ptr(1));
        let block_2 = empty_block(block_1.ptr(), test_ptr(2));
        vec![block_0, block_1, block_2]
    };
    let stop_block = test_ptr(1);
    let chain = chain(blocks, &stores, None).await;

    let mut env_vars = EnvVars::default();
    env_vars.experimental_static_filters = true;

    let ctx = fixture::setup(
        subgraph_name.clone(),
        &hash,
        &stores,
        &chain,
        None,
        Some(env_vars),
    )
    .await;
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
        poi.unwrap(),
        [
            172, 174, 50, 50, 108, 187, 89, 216, 16, 123, 40, 207, 250, 97, 247, 138, 180, 67, 20,
            5, 114, 187, 237, 104, 187, 122, 220, 9, 131, 67, 50, 237
        ],
    );
}

#[tokio::test]
async fn retry_create_ds() {
    let RunnerTestRecipe {
        stores,
        subgraph_name,
        hash,
    } = RunnerTestRecipe::new("data-source-revert2").await;

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
    let chain = chain(blocks, &stores, Some(triggers_adapter)).await;

    let mut env_vars = EnvVars::default();
    env_vars.subgraph_error_retry_ceil = Duration::from_secs(1);

    let ctx = fixture::setup(
        subgraph_name.clone(),
        &hash,
        &stores,
        &chain,
        None,
        Some(env_vars),
    )
    .await;

    let runner = ctx
        .runner(stop_block)
        .await
        .run_for_test(true)
        .await
        .unwrap();
    assert_eq!(runner.context().instance().hosts().len(), 2);
}

#[tokio::test]
async fn fatal_error() -> anyhow::Result<()> {
    let RunnerTestRecipe {
        stores,
        subgraph_name,
        hash,
    } = RunnerTestRecipe::new("fatal-error").await;

    let blocks = {
        let block_0 = genesis();
        let block_1 = empty_block(block_0.ptr(), test_ptr(1));
        let block_2 = empty_block(block_1.ptr(), test_ptr(2));
        let block_3 = empty_block(block_2.ptr(), test_ptr(3));
        vec![block_0, block_1, block_2, block_3]
    };

    let stop_block = blocks.last().unwrap().block.ptr();

    let chain = chain(blocks, &stores, None).await;
    let ctx = fixture::setup(subgraph_name.clone(), &hash, &stores, &chain, None, None).await;

    ctx.start_and_sync_to_error(stop_block).await;

    // Go through the indexing status API to also test it.
    let status = ctx.indexing_status().await;
    assert!(status.health == SubgraphHealth::Failed);
    assert!(status.entity_count == 1.into()); // Only PoI
    let err = status.fatal_error.unwrap();
    assert!(err.block.number == 3.into());
    assert!(err.deterministic);

    // Test that rewind unfails the subgraph.
    ctx.rewind(test_ptr(1));
    let status = ctx.indexing_status().await;
    assert!(status.health == SubgraphHealth::Healthy);
    assert!(status.fatal_error.is_none());

    Ok(())
}

async fn build_subgraph(dir: &str) -> DeploymentHash {
    build_subgraph_with_yarn_cmd(dir, "deploy:test").await
}

async fn build_subgraph_with_yarn_cmd(dir: &str, yarn_cmd: &str) -> DeploymentHash {
    // Test that IPFS is up.
    IpfsClient::localhost()
        .test()
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

    // Run `deploy` for the side effect of uploading to IPFS, the graph node url
    // is fake and the actual deploy call is meant to fail.
    let deploy_output = run_cmd(
        Command::new("yarn")
            .arg(yarn_cmd)
            .env("IPFS_URI", "http://127.0.0.1:5001")
            .env("GRAPH_NODE_ADMIN_URI", "http://localhost:0")
            .current_dir(dir),
    );

    // Hack to extract deployment id from `graph deploy` output.
    const ID_PREFIX: &str = "Build completed: ";
    let mut line = deploy_output
        .lines()
        .find(|line| line.contains(ID_PREFIX))
        .expect("found no matching line");
    if !line.starts_with(ID_PREFIX) {
        line = &line[5..line.len() - 5]; // workaround for colored output
    }
    DeploymentHash::new(line.trim_start_matches(ID_PREFIX)).unwrap()
}
