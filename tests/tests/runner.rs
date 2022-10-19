use std::sync::Arc;

use cid::Cid;
use graph::blockchain::{Block, BlockPtr};
use graph::env::EnvVars;
use graph::ipfs_client::CidFile;
use graph::object;
use graph::prelude::ethabi::ethereum_types::H256;
use graph::prelude::{CheapClone, SubgraphStore};
use graph::prelude::{SubgraphAssignmentProvider, SubgraphName};
use graph_tests::fixture::ethereum::{chain, empty_block, genesis};
use graph_tests::fixture::{self, stores, test_ptr};

#[tokio::test]
async fn data_source_revert() -> anyhow::Result<()> {
    let stores = stores("./integration-tests/config.simple.toml").await;

    let subgraph_name = SubgraphName::new("data-source-revert").unwrap();
    let hash = {
        let test_dir = format!("./integration-tests/{}", subgraph_name);
        fixture::build_subgraph(&test_dir).await
    };

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

    let chain = Arc::new(chain(blocks.clone(), &stores).await);
    let ctx = fixture::setup(
        subgraph_name.clone(),
        &hash,
        &stores,
        chain.clone(),
        None,
        None,
    )
    .await;

    let stop_block = test_ptr(2);
    ctx.start_and_sync_to(stop_block).await;
    ctx.provider.stop(ctx.deployment.clone()).await.unwrap();

    // Test loading data sources from DB.
    let stop_block = test_ptr(3);
    ctx.start_and_sync_to(stop_block).await;

    // Test grafted version
    let subgraph_name = SubgraphName::new("data-source-revert-grafted").unwrap();
    let hash = fixture::build_subgraph_with_yarn_cmd(
        "./integration-tests/data-source-revert",
        "deploy:test-grafted",
    )
    .await;
    let graft_block = Some(test_ptr(3));
    let ctx = fixture::setup(
        subgraph_name.clone(),
        &hash,
        &stores,
        chain,
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
    let subgraph_name = SubgraphName::new("typename").unwrap();

    let hash = {
        let test_dir = format!("./integration-tests/{}", subgraph_name);
        fixture::build_subgraph(&test_dir).await
    };

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

    let stores = stores("./integration-tests/config.simple.toml").await;
    let chain = Arc::new(chain(blocks, &stores).await);
    let ctx = fixture::setup(subgraph_name.clone(), &hash, &stores, chain, None, None).await;

    ctx.start_and_sync_to(stop_block).await;

    Ok(())
}

#[tokio::test]
async fn file_data_sources() {
    let stores = stores("./integration-tests/config.simple.toml").await;

    let subgraph_name = SubgraphName::new("file-data-sources").unwrap();
    let hash = {
        let test_dir = format!("./integration-tests/{}", subgraph_name);
        fixture::build_subgraph(&test_dir).await
    };

    let blocks = {
        let block_0 = genesis();
        let block_1 = empty_block(block_0.ptr(), test_ptr(1));
        let block_2 = empty_block(block_1.ptr(), test_ptr(2));
        let block_3 = empty_block(block_2.ptr(), test_ptr(3));
        let block_4 = empty_block(block_3.ptr(), test_ptr(4));
        vec![block_0, block_1, block_2, block_3, block_4]
    };
    let stop_block = test_ptr(1);
    let chain = Arc::new(chain(blocks, &stores).await);
    let ctx = fixture::setup(subgraph_name.clone(), &hash, &stores, chain, None, None).await;
    ctx.start_and_sync_to(stop_block).await;

    // CID QmVkvoPGi9jvvuxsHDVJDgzPEzagBaWSZRYoRDzU244HjZ is the file
    // `file-data-sources/abis/Contract.abi` after being processed by graph-cli.
    let id = format!(
        "0x{}",
        hex::encode(
            CidFile {
                cid: Cid::try_from("QmVkvoPGi9jvvuxsHDVJDgzPEzagBaWSZRYoRDzU244HjZ").unwrap(),
                path: None,
            }
            .to_bytes()
        )
    );

    let query_res = ctx
        .query(&format!(r#"{{ ipfsFile(id: "{id}") {{ id, content }} }}"#,))
        .await
        .unwrap();

    assert_eq!(
        query_res,
        Some(object! { ipfsFile: object!{ id: id.clone() , content: "[]" } })
    );

    // assert whether duplicate data sources are created.
    ctx.provider.stop(ctx.deployment.clone()).await.unwrap();
    let stop_block = test_ptr(2);
    ctx.start_and_sync_to(stop_block).await;

    let store = ctx.store.cheap_clone();
    let writable = store
        .writable(ctx.logger.clone(), ctx.deployment.id)
        .await
        .unwrap();
    let datasources = writable.load_dynamic_data_sources(vec![]).await.unwrap();
    assert!(datasources.len() == 1);

    ctx.provider.stop(ctx.deployment.clone()).await.unwrap();
    let stop_block = test_ptr(3);
    ctx.start_and_sync_to(stop_block).await;

    let query_res = ctx
        .query(&format!(r#"{{ ipfsFile1(id: "{id}") {{ id, content }} }}"#,))
        .await
        .unwrap();

    assert_eq!(
        query_res,
        Some(object! { ipfsFile1: object!{ id: id , content: "[]" } })
    );

    ctx.provider.stop(ctx.deployment.clone()).await.unwrap();
    let stop_block = test_ptr(4);
    ctx.start_and_sync_to(stop_block).await;
    let writable = ctx
        .store
        .clone()
        .writable(ctx.logger.clone(), ctx.deployment.id.clone())
        .await
        .unwrap();
    let data_sources = writable.load_dynamic_data_sources(vec![]).await.unwrap();
    assert!(data_sources.len() == 2);
    for data_source in data_sources {
        assert!(data_source.done_at.is_some())
    }
}

#[tokio::test]
async fn template_static_filters_false_positives() {
    let stores = stores("./integration-tests/config.simple.toml").await;

    let subgraph_name = SubgraphName::new("dynamic-data-source").unwrap();
    let hash = {
        let test_dir = format!("./integration-tests/{}", subgraph_name);
        fixture::build_subgraph(&test_dir).await
    };

    let blocks = {
        let block_0 = genesis();
        let block_1 = empty_block(block_0.ptr(), test_ptr(1));
        let block_2 = empty_block(block_1.ptr(), test_ptr(2));
        vec![block_0, block_1, block_2]
    };
    let stop_block = test_ptr(1);
    let chain = Arc::new(chain(blocks, &stores).await);

    let mut env_vars = EnvVars::default();
    env_vars.experimental_static_filters = true;

    let ctx = fixture::setup(
        subgraph_name.clone(),
        &hash,
        &stores,
        chain,
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
    // a change in the POI infrastructure.
    assert_eq!(
        poi.unwrap(),
        [
            196, 173, 167, 52, 226, 19, 154, 61, 189, 94, 19, 229, 18, 7, 0, 252, 234, 49, 110,
            179, 105, 64, 16, 46, 25, 194, 83, 94, 195, 225, 56, 252
        ],
    );
}
