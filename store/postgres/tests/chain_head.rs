//! Test ChainStore implementation of Store, in particular, how
//! the chain head pointer gets updated in various situations

use diesel::prelude::*;
use diesel::RunQueryDsl;
use futures::future::{self, IntoFuture};
use lazy_static::lazy_static;
use std::fmt::Debug;
use std::sync::Arc;

use graph::components::store::{ChainStore, Store as _};
use graph::prelude::{serde_json, Future01CompatExt, SubgraphDeploymentId, TryFutureExt};
use graph_store_postgres::db_schema_for_tests as db_schema;
use graph_store_postgres::Store as DieselStore;

use test_store::block_store::{
    Chain, FakeBlock, BLOCK_FIVE, BLOCK_FOUR, BLOCK_ONE, BLOCK_ONE_NO_PARENT, BLOCK_ONE_SIBLING,
    BLOCK_THREE, BLOCK_THREE_NO_PARENT, BLOCK_TWO, BLOCK_TWO_NO_PARENT, GENESIS_BLOCK, NO_PARENT,
};
use test_store::*;

// The ancestor count we use for chain head updates. We keep this very small
// to make setting up the tests easier
const ANCESTOR_COUNT: u64 = 3;

/// Test harness for running database integration tests.
fn run_test<R, F>(chain: Chain, test: F)
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

    let _ = runtime
        .block_on(async {
            // Reset state before starting
            block_store::remove();

            // Seed database with test data
            block_store::insert(chain, NETWORK_NAME);

            // Run test
            test(store).into_future().compat()
        })
        .unwrap_or_else(|e| panic!("Failed to run ChainHead test: {:?}", e));
}

/// Check that `attempt_chain_head_update` works as expected on the given
/// chain. After writing the blocks in `chain` to the store, call
/// `attempt_chain_head_update` and check its result. Check that the new head
/// is the one indicated in `head_exp`. If `missing` is not `None`, check that
/// `attempt_chain_head_update` reports that block as missing
fn check_chain_head_update(
    chain: Chain,
    head_exp: Option<&'static FakeBlock>,
    missing: Option<&'static str>,
) {
    run_test(chain, move |store| -> Result<(), ()> {
        let missing_act: Vec<_> = store
            .attempt_chain_head_update(ANCESTOR_COUNT)
            .expect("attempt_chain_head_update failed")
            .iter()
            .map(|h| format!("{:x}", h))
            .collect();
        let missing_exp: Vec<_> = missing.into_iter().collect();
        assert_eq!(missing_exp, missing_act);

        let head_hash_exp = head_exp.map(|block| block.hash.clone());
        let head_hash_act = store
            .chain_head_ptr()
            .expect("chain_head_ptr failed")
            .map(|ebp| format!("{:x}", ebp.hash));
        assert_eq!(head_hash_exp, head_hash_act);
        Ok(())
    })
}

#[test]
fn genesis_only() {
    check_chain_head_update(vec![&*GENESIS_BLOCK], Some(&GENESIS_BLOCK), None);
}

#[test]
fn genesis_plus_one() {
    check_chain_head_update(vec![&*GENESIS_BLOCK, &*BLOCK_ONE], Some(&BLOCK_ONE), None);
}

#[test]
fn genesis_plus_two() {
    check_chain_head_update(
        vec![&*GENESIS_BLOCK, &*BLOCK_ONE, &*BLOCK_TWO],
        Some(&*BLOCK_TWO),
        None,
    );
}

#[test]
fn genesis_plus_one_with_sibling() {
    // Two valid blocks at the same height should give an error, but
    // we currently get one of them at random
    let chain = vec![&*GENESIS_BLOCK, &*BLOCK_ONE, &*BLOCK_ONE_SIBLING];
    check_chain_head_update(chain, Some(&*BLOCK_ONE), None);
}

#[test]
fn short_chain_missing_parent() {
    let chain = vec![&*BLOCK_ONE_NO_PARENT];
    check_chain_head_update(chain, None, Some(&NO_PARENT));
}

#[test]
fn long_chain() {
    let chain = vec![
        &*BLOCK_ONE,
        &*BLOCK_TWO,
        &*BLOCK_THREE,
        &*BLOCK_FOUR,
        &*BLOCK_FIVE,
    ];
    check_chain_head_update(chain, Some(&*BLOCK_FIVE), None);
}

#[test]
fn long_chain_missing_blocks_within_ancestor_count() {
    // BLOCK_THREE does not have a parent in the store
    let chain = vec![&*BLOCK_THREE, &*BLOCK_FOUR, &*BLOCK_FIVE];
    check_chain_head_update(chain, None, Some(&BLOCK_THREE.parent_hash));
}

#[test]
fn long_chain_missing_blocks_beyond_ancestor_count() {
    // We don't mind missing blocks ANCESTOR_COUNT many blocks out, in
    // this case BLOCK_ONE
    let chain = vec![&*BLOCK_TWO, &*BLOCK_THREE, &*BLOCK_FOUR, &*BLOCK_FIVE];
    check_chain_head_update(chain, Some(&*BLOCK_FIVE), None);
}

#[test]
fn long_chain_with_uncles() {
    let chain = vec![
        &*BLOCK_ONE,
        &*BLOCK_TWO,
        &*BLOCK_TWO_NO_PARENT,
        &*BLOCK_THREE,
        &*BLOCK_THREE_NO_PARENT,
        &*BLOCK_FOUR,
    ];
    check_chain_head_update(chain, Some(&*BLOCK_FOUR), None);
}

#[test]
fn block_number() {
    let chain = vec![&*GENESIS_BLOCK, &*BLOCK_ONE, &*BLOCK_TWO];
    let subgraph = SubgraphDeploymentId::new("nonExistentSubgraph").unwrap();
    run_test(chain, move |store| -> Result<(), ()> {
        let block = store
            .block_number(&subgraph, GENESIS_BLOCK.block_hash())
            .expect("Found genesis block");
        assert_eq!(Some(0), block);

        let block = store
            .block_number(&subgraph, BLOCK_ONE.block_hash())
            .expect("Found block 1");
        assert_eq!(Some(1), block);

        let block = store
            .block_number(&subgraph, BLOCK_THREE.block_hash())
            .expect("Looked for block 3");
        assert!(block.is_none());

        Ok(())
    })
}
