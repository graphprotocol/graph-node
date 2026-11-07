// A proof of indexing for a block that pruning has already removed cannot be computed:
// the causality-region digests it is hashed from are gone. Returning a well-formed but
// wrong POI is worse than failing, because nothing downstream can tell the difference.

use graph::prelude::{DeploymentHash, SubgraphStore as _};
use test_store::*;

const SUBGRAPH_GQL: &str = "
    type User @entity {
        id: ID!,
        name: String
    }
";

/// The block pointer the POS tests use: a deterministic hash per number.
fn ptr(n: i32) -> graph::prelude::BlockPtr {
    let hash = graph::prelude::alloy::primitives::B256::from(
        graph::prelude::alloy::primitives::U256::from(n as u64),
    );
    graph::prelude::BlockPtr::new(hash.into(), n.into())
}

/// Put the subgraph at `head` and move its history window forward to `earliest`,
/// which is what the pruner leaves behind once it has passed the blocks below it.
async fn set_head_and_prune(deployment: &DeploymentHash, head: i32, earliest: i32) {
    let mut conn = PRIMARY_POOL.get().await.unwrap();
    let hash = deployment.as_str().to_string();
    let hash_bytes = format!("\\x{}", "11".repeat(32));
    let stmt = format!(
        "UPDATE subgraphs.deployment SET earliest_block_number = {earliest} WHERE subgraph = '{hash}';\
         UPDATE subgraphs.head SET block_number = {head}, block_hash = '{hash_bytes}' WHERE id = \
           (SELECT id FROM subgraphs.deployment WHERE subgraph = '{hash}');"
    );
    diesel_async::SimpleAsyncConnection::batch_execute(&mut conn, &stmt)
        .await
        .unwrap();
}

#[test]
fn proof_of_indexing_refuses_a_pruned_block() {
    run_test_sequentially(|store| async move {
        let id = DeploymentHash::new("prunedPoi").unwrap();
        create_test_subgraph(&id, SUBGRAPH_GQL).await;

        // The subgraph is at block 200; everything below 100 has been pruned away.
        set_head_and_prune(&id, 200, 100).await;

        let sg_store = store.subgraph_store();
        let indexer = None;

        // A retained block is unaffected.
        let retained = sg_store
            .get_proof_of_indexing(&id, &indexer, ptr(120))
            .await;
        assert!(
            retained.is_ok(),
            "a retained block must still produce a POI: {retained:?}"
        );

        // A pruned block must be an error, not a silently wrong hash.
        let pruned = sg_store
            .get_proof_of_indexing(&id, &indexer, ptr(50))
            .await;
        match pruned {
            Err(e) => {
                let msg = format!("{e}");
                assert!(
                    msg.contains("pruned") || msg.contains("earliest"),
                    "the error should say the block was pruned, got: {msg}"
                );
            }
            Ok(v) => panic!("a pruned block returned {v:?} instead of an error"),
        }
    })
}
