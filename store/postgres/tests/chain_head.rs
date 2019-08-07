//! Test ChainStore implementation of Store, in particular, how
//! the chain head pointer gets updated in various situations

use diesel::prelude::*;
use diesel::RunQueryDsl;
use futures::future::{self, IntoFuture};
use lazy_static::lazy_static;
use std::fmt::Debug;
use std::sync::Arc;

use graph::components::store::ChainStore;
use graph::prelude::serde_json;
use graph_store_postgres::db_schema_for_tests as db_schema;
use graph_store_postgres::Store as DieselStore;

use test_store::*;

// The ancestor count we use for chain head updates. We keep this very small
// to make setting up the tests easier
const ANCESTOR_COUNT: u64 = 3;

/// The parts of an Ethereum block that are interesting for these tests:
/// the block number, hash, and the hash of the parent block
#[derive(Clone, Debug, PartialEq)]
struct FakeBlock {
    number: u64,
    hash: String,
    parent_hash: String,
}

impl FakeBlock {
    fn make_child(&self, hash: &str) -> Self {
        FakeBlock {
            number: self.number + 1,
            hash: hash.to_owned(),
            parent_hash: self.hash.clone(),
        }
    }

    fn make_no_parent(number: u64, hash: &str) -> Self {
        FakeBlock {
            number,
            hash: hash.to_owned(),
            parent_hash: NO_PARENT.clone(),
        }
    }

    fn insert(&self, conn: &PgConnection) {
        use db_schema::ethereum_blocks as b;

        let data = serde_json::to_value(format!(
            "{{\"hash\":\"{}\", \"number\":{}}}",
            self.hash, self.number
        ))
        .expect("Failed to serialize block");

        let errmsg = format!("Failed to insert block {} ({})", self.number, self.hash);
        diesel::insert_into(b::table)
            .values((
                &b::number.eq(self.number as i64),
                &b::hash.eq(&self.hash),
                &b::parent_hash.eq(&self.parent_hash),
                &b::network_name.eq(NETWORK_NAME),
                &b::data.eq(data),
            ))
            .execute(conn)
            .expect(&errmsg);
    }
}

type Chain = Vec<&'static FakeBlock>;

lazy_static! {
    // Hash indicating 'no parent'
    static ref NO_PARENT: String =
        "0000000000000000000000000000000000000000000000000000000000000000".to_owned();
    // Genesis block
    static ref GENESIS_BLOCK: FakeBlock = FakeBlock {
        number: GENESIS_PTR.number,
        hash: format!("{:x}", GENESIS_PTR.hash),
        parent_hash: NO_PARENT.clone()
    };
    static ref BLOCK_ONE: FakeBlock = GENESIS_BLOCK
        .make_child("8511fa04b64657581e3f00e14543c1d522d5d7e771b54aa3060b662ade47da13");
    static ref BLOCK_ONE_SIBLING: FakeBlock =
        GENESIS_BLOCK.make_child("b98fb783b49de5652097a989414c767824dff7e7fd765a63b493772511db81c1");
    static ref BLOCK_ONE_NO_PARENT: FakeBlock = FakeBlock::make_no_parent(
        1,
        "7205bdfcf4521874cf38ce38c879ff967bf3a069941286bfe267109ad275a63d"
    );

    static ref BLOCK_TWO: FakeBlock = BLOCK_ONE.make_child("f8ccbd3877eb98c958614f395dd351211afb9abba187bfc1fb4ac414b099c4a6");
    static ref BLOCK_TWO_NO_PARENT: FakeBlock = FakeBlock::make_no_parent(2, "3b652b00bff5e168b1218ff47593d516123261c4487629c4175f642ee56113fe");
    static ref BLOCK_THREE: FakeBlock = BLOCK_TWO.make_child("7347afe69254df06729e123610b00b8b11f15cfae3241f9366fb113aec07489c");
    static ref BLOCK_THREE_NO_PARENT: FakeBlock = FakeBlock::make_no_parent(3, "fa9ebe3f74de4c56908b49f5c4044e85825f7350f3fa08a19151de82a82a7313");
    static ref BLOCK_FOUR: FakeBlock = BLOCK_THREE.make_child("7cce080f5a49c2997a6cc65fc1cee9910fd8fc3721b7010c0b5d0873e2ac785e");
    static ref BLOCK_FIVE: FakeBlock = BLOCK_FOUR.make_child("7b0ea919e258eb2b119eb32de56b85d12d50ac6a9f7c5909f843d6172c8ba196");
    static ref BLOCK_SIX_NO_PARENT: FakeBlock = FakeBlock::make_no_parent(6, "6b834521bb753c132fdcf0e1034803ed9068e324112f8750ba93580b393a986b");
}

/// Removes test data from the database behind the store.
fn remove_test_data() {
    let url = postgres_test_url();
    let conn = PgConnection::establish(url.as_str()).expect("Failed to connect to Postgres");

    diesel::delete(db_schema::ethereum_blocks::table)
        .execute(&conn)
        .expect("Failed to delete ethereum_blocks");
    diesel::delete(db_schema::ethereum_networks::table)
        .execute(&conn)
        .expect("Failed to delete ethereum_networks");
}

fn insert_test_data(_store: Arc<DieselStore>, chain: Chain) {
    let url = postgres_test_url();
    let conn = PgConnection::establish(url.as_str()).expect("Failed to connect to Postgres");

    use db_schema::ethereum_networks as n;
    let hash = format!("{:x}", GENESIS_PTR.hash);
    diesel::insert_into(n::table)
        .values((
            &n::name.eq(NETWORK_NAME),
            &n::genesis_block_hash.eq(&hash),
            &n::net_version.eq(NETWORK_VERSION),
        ))
        .execute(&conn)
        .expect("Failed to insert test network");

    for block in chain {
        block.insert(&conn);
    }
}

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

    runtime
        .block_on(future::lazy(move || {
            // Reset state before starting
            remove_test_data();

            // Seed database with test data
            insert_test_data(store.clone(), chain);

            // Run test
            test(store)
        }))
        .expect("Failed to run ChainHead test");
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
