use graph_chain_ethereum::chain::BlockFinality;
use graph_tests::fixture::{self, test_ptr};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use ethereum::trigger::{EthereumBlockTriggerType, EthereumTrigger};
use graph::blockchain::block_stream::BlockWithTriggers;
use graph::blockchain::{Block, BlockPtr};
use graph::prelude::ethabi::ethereum_types::{H256, U64};
use graph::prelude::{
    LightEthereumBlock, SubgraphAssignmentProvider, SubgraphName, SubgraphStore as _,
};
use graph_chain_ethereum::{self as ethereum, Chain};
use slog::{debug, info};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn data_source_revert() -> anyhow::Result<()> {
    let subgraph_name = SubgraphName::new("data-source-revert")
        .expect("Subgraph name must contain only a-z, A-Z, 0-9, '-' and '_'");

    let hash = {
        let test_dir = format!("./integration-tests/{}", subgraph_name);
        fixture::build_subgraph(&test_dir).await
    };

    let chain: Vec<BlockWithTriggers<Chain>> = {
        let start_block_hash = H256::from_low_u64_be(0);
        let start_block = BlockWithTriggers {
            block: BlockFinality::Final(Arc::new(LightEthereumBlock {
                hash: Some(start_block_hash),
                number: Some(U64::from(0)),
                ..Default::default()
            })),
            trigger_data: vec![],
        };
        let block_2 = BlockWithTriggers::<Chain> {
            block: BlockFinality::Final(Arc::new(LightEthereumBlock {
                hash: Some(H256::from_low_u64_be(1)),
                number: Some(U64::from(1)),
                parent_hash: start_block_hash,
                ..Default::default()
            })),
            trigger_data: vec![EthereumTrigger::Block(
                test_ptr(1),
                EthereumBlockTriggerType::Every,
            )],
        };
        let block_2_reorged_hash = H256::from_low_u64_be(12);
        let block_2_reorged = BlockWithTriggers::<Chain> {
            block: BlockFinality::Final(Arc::new(LightEthereumBlock {
                hash: Some(block_2_reorged_hash),
                number: Some(U64::from(1)),
                parent_hash: start_block_hash,
                ..Default::default()
            })),
            trigger_data: vec![EthereumTrigger::Block(
                BlockPtr {
                    hash: block_2_reorged_hash.into(),
                    number: 1,
                },
                EthereumBlockTriggerType::Every,
            )],
        };
        vec![start_block, block_2, block_2_reorged]
    };

    let stop_block = chain.last().unwrap().block.ptr();

    let ctx = fixture::setup(
        subgraph_name.clone(),
        &hash,
        "./integration-tests/config.simple.toml",
        chain,
    )
    .await;

    let provider = ctx.provider.clone();
    let store = ctx.store.clone();

    let logger = ctx.logger_factory.subgraph_logger(&ctx.deployment_locator);

    SubgraphAssignmentProvider::start(provider.as_ref(), ctx.deployment_locator.clone(), None)
        .await
        .expect("unabel to start subgraph");

    loop {
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let block_ptr = match store.least_block_ptr(&hash).await {
            Ok(Some(ptr)) => ptr,
            res => {
                info!(&logger, "{:?}", res);
                continue;
            }
        };

        debug!(&logger, "subgraph block: {:?}", block_ptr);

        if block_ptr == stop_block {
            info!(
                &logger,
                "subgraph now at block {}, reached stop block {}", block_ptr.number, stop_block
            );
            break;
        }

        if !store.is_healthy(&hash).await.unwrap() {
            return Err(anyhow!("subgraph failed unexpectedly"));
        }
    }

    assert!(store.is_healthy(&hash).await.unwrap());

    fixture::cleanup(&ctx.store, &subgraph_name, &hash);

    Ok(())
}
