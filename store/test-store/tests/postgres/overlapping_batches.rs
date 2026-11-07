//! Two writers on the same deployment can commit overlapping write batches.
//!
//! When a deployment is reassigned while the first write batch of the old
//! runner is still pending, the old runner commits that batch while stopping
//! and the new runner, which started from the same head, later commits a batch
//! covering the same blocks. `deployment::transact_block` only compares the
//! *last* block of a batch with the deployment head, so the second batch is
//! accepted and every entity written in the overlap ends up with two versions.
//!
//! These tests simulate two graph-node processes with two independent `Store`
//! instances (the global test store and a second one built with
//! `StoreBuilder`), so the per-process `writables` cache and the in-memory
//! check in `WritableStore` cannot see each other.
//!
//! `overlapping_batches_from_two_writers_are_rejected` FAILS on master: that
//! failure is the demonstration of the bug. The two controls pass.

use diesel::sql_types::{BigInt, Text};
use diesel_async::RunQueryDsl;
use graph::blockchain::BlockTime;
use graph::blockchain::block_stream::FirehoseCursor;
use graph::components::store::{DeploymentLocator, EntityCache, SeqGenerator, WritableStore};
use graph::data::query::QueryTarget;
use graph::data::subgraph::schema::DeploymentCreate;
use graph::data::subgraph::*;
use graph::prelude::alloy::primitives::B256;
use graph::schema::InputSchema;
use graph::semver::Version;
use graph::{entity, prelude::*};
use graph_node::config::{Config, Opt};
use graph_node::store_builder::StoreBuilder;
use graph_store_postgres::Store as DieselStore;
use graph_store_postgres::layout_for_tests::FAKE_NETWORK_SHARED;
use lazy_static::lazy_static;
use std::marker::PhantomData;
use test_store::*;

const SCHEMA_GQL: &str = "
    type Protocol @entity {
        id: ID!,
        value: Int!,
    }
";

lazy_static! {
    /// A second, independent `Store` on the same database: the "second process".
    static ref STORE_B: Arc<DieselStore> = build_store_b();
}

fn build_store_b() -> Arc<DieselStore> {
    // Make sure the global test store (chains, migrations) exists first
    let _ = STORE.clone();
    let url = std::env::var("THEGRAPH_STORE_POSTGRES_DIESEL_URL")
        .expect("THEGRAPH_STORE_POSTGRES_DIESEL_URL must be set");
    let opt = Opt {
        postgres_url: Some(url),
        store_connection_pool_size: 10,
        ..Default::default()
    };
    let config = Config::load(&LOGGER, &opt).expect("valid config");
    let registry = Arc::new(MetricsRegistry::mock());
    let node = NodeId::new("test_b").unwrap();
    // `StoreBuilder` blocks on its own futures; run it outside the test runtime
    std::thread::spawn(move || {
        graph::TEST_RUNTIME.handle().block_on(async {
            StoreBuilder::new(&LOGGER, &node, &config, None, registry)
                .await
                .network_store(vec![
                    NETWORK_NAME.to_string(),
                    FAKE_NETWORK_SHARED.to_string(),
                ])
                .await
        })
    })
    .join()
    .unwrap()
}

fn schema(version: &Version, hash: &DeploymentHash) -> InputSchema {
    InputSchema::parse(version, SCHEMA_GQL, hash.clone()).unwrap()
}

/// Create a fresh deployment with the given spec version. With
/// `release_like`, replace the debug-only exclusion constraint on
/// `(id, block_range)` with the plain gist index a release build creates.
async fn setup(
    name: &str,
    version: Version,
    release_like: bool,
) -> (DeploymentHash, DeploymentLocator) {
    remove_subgraphs().await;
    let hash = DeploymentHash::new(name).unwrap();
    let schema = schema(&version, &hash);
    let manifest = SubgraphManifest::<graph_chain_ethereum::Chain> {
        id: hash.clone(),
        spec_version: version,
        features: Default::default(),
        description: None,
        repository: None,
        schema: schema.clone(),
        data_sources: vec![],
        graft: None,
        templates: vec![],
        chain: PhantomData,
        indexer_hints: None,
    };
    let deployment = DeploymentCreate::new(String::new(), &manifest, None);
    let loc = SUBGRAPH_STORE
        .create_subgraph_deployment(
            SubgraphName::new(format!("test/{}", hash)).unwrap(),
            &schema,
            deployment,
            NODE_ID.clone(),
            NETWORK_NAME.to_string(),
            SubgraphVersionSwitchingMode::Instant,
        )
        .await
        .unwrap();

    if release_like {
        let ns = namespace(&hash).await;
        let mut conn = PRIMARY_POOL.get().await.unwrap();
        for stmt in [
            format!("alter table {ns}.protocol drop constraint protocol_id_block_range_excl"),
            format!(
                "create index protocol_id_block_range_excl \
                 on {ns}.protocol using gist (id, block_range)"
            ),
        ] {
            diesel::sql_query(stmt).execute(&mut conn).await.unwrap();
        }
    }
    (hash, loc)
}

async fn writable_a(loc: &DeploymentLocator) -> Arc<dyn WritableStore> {
    SUBGRAPH_STORE
        .cheap_clone()
        .writable(LOGGER.clone(), loc.id, Arc::new(Vec::new()))
        .await
        .unwrap()
}

async fn writable_b(loc: &DeploymentLocator) -> Arc<dyn WritableStore> {
    STORE_B
        .subgraph_store()
        .writable(LOGGER.clone(), loc.id, Arc::new(Vec::new()))
        .await
        .unwrap()
}

/// Process one block through `writable`: set `Protocol { id: "1", value: block }`
async fn transact_block(
    writable: &Arc<dyn WritableStore>,
    version: &Version,
    hash: &DeploymentHash,
    block: i32,
) -> Result<(), StoreError> {
    let schema = schema(version, hash);
    let protocol = schema.entity_type("Protocol").unwrap();
    // vid as `SeqGenerator` would assign it; ignored for `vid bigserial` (spec < 1.3.0)
    let data = entity! { schema => id: "1", value: block, vid: ((block as i64) << 32) + 100 };
    let ops = vec![EntityOperation::Set {
        key: protocol.parse_key("1").unwrap(),
        data,
    }];
    let ptr = BlockPtr::from((B256::from([block as u8; 32]), block as BlockNumber));
    let stopwatch = StopwatchMetrics::new(
        Logger::root(slog::Discard, o!()),
        hash.clone(),
        "transact",
        Arc::new(MetricsRegistry::mock()),
        writable.shard().to_string(),
    );
    let mut cache = EntityCache::new(Arc::new(writable.clone()), SeqGenerator::new(ptr.number));
    cache.append(ops);
    let mods = cache
        .as_modifications(ptr.number, &stopwatch)
        .await
        .unwrap()
        .modifications;
    writable
        .transact_block_operations(
            ptr.clone(),
            BlockTime::for_test(&ptr),
            FirehoseCursor::None,
            mods,
            &stopwatch,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            false,
            false,
        )
        .await
}

async fn transact_blocks(
    writable: &Arc<dyn WritableStore>,
    version: &Version,
    hash: &DeploymentHash,
    blocks: std::ops::RangeInclusive<i32>,
) {
    for block in blocks {
        transact_block(writable, version, hash, block)
            .await
            .expect("blocks are queued in memory");
    }
}

#[derive(diesel::QueryableByName)]
struct NameRow {
    #[diesel(sql_type = Text)]
    name: String,
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

async fn namespace(hash: &DeploymentHash) -> String {
    let mut conn = PRIMARY_POOL.get().await.unwrap();
    let rows: Vec<NameRow> = diesel::sql_query(format!(
        "select name from public.deployment_schemas where subgraph = '{hash}'"
    ))
    .load(&mut conn)
    .await
    .unwrap();
    rows[0].name.clone()
}

/// Number of rows for `Protocol` "1" whose block range is open at the top
async fn live_versions(hash: &DeploymentHash) -> i64 {
    let ns = namespace(hash).await;
    let mut conn = PRIMARY_POOL.get().await.unwrap();
    let rows: Vec<CountRow> = diesel::sql_query(format!(
        "select count(*) as n from {ns}.protocol where id = '1' and upper_inf(block_range)"
    ))
    .load(&mut conn)
    .await
    .unwrap();
    rows[0].n
}

/// How often `Protocol` "1" shows up in a list query at the subgraph head
async fn listed_at_head(hash: &DeploymentHash) -> usize {
    let q = "query { protocols(first: 100, orderBy: id) { id } }";
    let query = Query::new(q::parse_query(q).unwrap().into_static(), None, false);
    let target = QueryTarget::Deployment(hash.clone(), Default::default());
    let results = execute_subgraph_query(query, target).await;
    let data = graph::data::query::QueryResults::first(&results)
        .unwrap()
        .duplicate()
        .to_result()
        .unwrap()
        .unwrap();
    let r::Value::Object(obj) = data else {
        panic!("unexpected query result {data:?}")
    };
    let Some(r::Value::List(items)) = obj.get("protocols") else {
        panic!("unexpected query result {obj:?}")
    };
    items
        .iter()
        .filter(|item| match item {
            r::Value::Object(o) => o.get("id") == Some(&r::Value::String("1".to_string())),
            _ => false,
        })
        .count()
}

/// Control: one writer processing blocks 1..5 leaves one live version.
#[test]
fn single_writer_leaves_one_live_version() {
    let _ = STORE_B.clone();
    run_test_sequentially(|_store| async move {
        let version = Version::new(1, 2, 0);
        let (hash, loc) = setup("overlappingBatchesSingle", version.clone(), true).await;

        let a = writable_a(&loc).await;
        transact_blocks(&a, &version, &hash, 1..=5).await;
        a.flush().await.unwrap();

        assert_eq!(1, live_versions(&hash).await);
        assert_eq!(1, listed_at_head(&hash).await);
    })
}

/// Control: with spec 1.3.0, vids are deterministic (`block << 32 | seq`),
/// so the second writer's batch collides on the `vid` primary key. That check
/// fires while the entity modifications are applied, before the head comparison
/// runs, which is why this case is already covered on master.
#[test]
fn spec_1_3_overlapping_batch_hits_vid_pk() {
    let _ = STORE_B.clone();
    run_test_sequentially(|_store| async move {
        let version = Version::new(1, 3, 0);
        let (hash, loc) = setup("overlappingBatchesV13", version.clone(), true).await;

        // Both writers are obtained before anything is committed
        let a = writable_a(&loc).await;
        let b = writable_b(&loc).await;
        transact_blocks(&a, &version, &hash, 1..=3).await;
        transact_blocks(&b, &version, &hash, 1..=5).await;

        a.flush().await.unwrap();
        let err = b.flush().await.expect_err("overlapping batch must fail");
        assert!(
            err.to_string().contains("protocol_pkey"),
            "unexpected error: {err}"
        );
        assert_eq!(1, live_versions(&hash).await);
    })
}

/// Two writers on a spec 1.2.0 deployment (`vid bigserial`), without the
/// debug-only exclusion constraint, as in a release build. Writer A commits
/// blocks 1..3, then writer B commits blocks 1..5. B's batch overlaps blocks
/// that are already committed and should be refused.
///
/// On master this test FAILS: `deployment::transact_block` compares only the
/// last block of B's batch (5) with the head (3) and accepts it. The table
/// then holds two versions of `Protocol` "1" for blocks 1..3, two of them
/// live, and a list query at the head returns "1" twice.
#[test]
fn overlapping_batches_from_two_writers_are_rejected() {
    let _ = STORE_B.clone();
    run_test_sequentially(|_store| async move {
        let version = Version::new(1, 2, 0);
        let (hash, loc) = setup("overlappingBatchesV12", version.clone(), true).await;

        // Both writers are obtained before anything is committed
        let a = writable_a(&loc).await;
        let b = writable_b(&loc).await;
        transact_blocks(&a, &version, &hash, 1..=3).await;
        transact_blocks(&b, &version, &hash, 1..=5).await;

        a.flush().await.unwrap();
        let b_flush = b.flush().await;
        let live = live_versions(&hash).await;
        let listed = listed_at_head(&hash).await;

        assert!(
            b_flush.is_err() && live == 1,
            "overlapping batch from writer B was accepted: flush = {b_flush:?}, \
             live versions of Protocol \"1\" = {live}, \
             list query at head returns \"1\" {listed} times"
        );
    })
}
