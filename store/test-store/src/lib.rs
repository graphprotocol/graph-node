#[macro_use]
extern crate diesel;

use crate::tokio::runtime::{Builder, Runtime};
use diesel::PgConnection;
use graph::data::graphql::effort::LoadManager;
use graph::data::query::QueryResults;
use graph::data::query::QueryTarget;
use graph::data::subgraph::schema::SubgraphError;
use graph::log;
use graph::prelude::{QueryStoreManager as _, Store as _, *};
use graph::{components::store::EntityType, prelude::NodeId};
use graph_graphql::prelude::{
    execute_query, Query as PreparedQuery, QueryExecutionOptions, StoreResolver,
};
use graph_mock::MockMetricsRegistry;
use graph_node::config::{Config, Opt};
use graph_node::store_builder::StoreBuilder;
use graph_store_postgres::{connection_pool::ConnectionPool, Shard};
use graph_store_postgres::{DeploymentPlacer, NetworkStore};
use hex_literal::hex;
use lazy_static::lazy_static;
use std::env;
use std::sync::Mutex;
use std::time::Instant;
use web3::types::H256;

pub const NETWORK_NAME: &str = "fake_network";
pub const NETWORK_VERSION: &str = "graph test suite";

const CONN_POOL_SIZE: u32 = 20;

lazy_static! {
    pub static ref LOGGER: Logger = match env::var_os("GRAPH_LOG") {
        Some(_) => log::logger(false),
        None => Logger::root(slog::Discard, o!()),
    };
    pub static ref STORE_RUNTIME: Mutex<Runtime> = Mutex::new(
        Builder::new()
            .basic_scheduler()
            .enable_all()
            .build()
            .unwrap()
    );
    pub static ref LOAD_MANAGER: Arc<LoadManager> = Arc::new(LoadManager::new(
        &*LOGGER,
        Vec::new(),
        Arc::new(MockMetricsRegistry::new()),
        CONN_POOL_SIZE as usize
    ));
    static ref STORE_POOL_CONFIG: (Arc<NetworkStore>, ConnectionPool, Config) = build_store();
    static ref PRIMARY_POOL: ConnectionPool = STORE_POOL_CONFIG.1.clone();
    pub static ref STORE: Arc<NetworkStore> = STORE_POOL_CONFIG.0.clone();
    static ref CONFIG: Config = STORE_POOL_CONFIG.2.clone();
    pub static ref GENESIS_PTR: EthereumBlockPointer = (
        H256::from(hex!(
            "bd34884280958002c51d3f7b5f853e6febeba33de0f40d15b0363006533c924f"
        )),
        0u64
    )
        .into();
    pub static ref BLOCK_ONE: EthereumBlockPointer = (
        H256::from(hex!(
            "8511fa04b64657581e3f00e14543c1d522d5d7e771b54aa3060b662ade47da13"
        )),
        1u64
    )
        .into();
}

/// Run the `test` after performing `setup`. The result of `setup` is passed
/// into `test`. All tests using `run_test_sequentially` are run in sequence,
/// never in parallel. The `test` is passed a `Store`, but it is permissible
/// for tests to access the global `STORE` from this module, too.
pub fn run_test_sequentially<R, S, F, G>(setup: G, test: F)
where
    G: FnOnce() -> S + Send + 'static,
    F: FnOnce(Arc<NetworkStore>, S) -> R + Send + 'static,
    R: std::future::Future<Output = ()> + Send + 'static,
{
    let store = STORE.clone();

    // Lock regardless of poisoning. This also forces sequential test execution.
    let mut runtime = match STORE_RUNTIME.lock() {
        Ok(guard) => guard,
        Err(err) => err.into_inner(),
    };

    runtime.block_on(async {
        let state = setup();
        test(store, state).await
    })
}

/// Run a test with a connection into the primary database, not a full store
pub fn run_test_with_conn<F>(test: F)
where
    F: FnOnce(&PgConnection) -> (),
{
    let conn = PRIMARY_POOL
        .get()
        .expect("failed to get connection for primary database");

    let _runtime = match STORE_RUNTIME.lock() {
        Ok(guard) => guard,
        Err(err) => err.into_inner(),
    };

    test(&conn);
}

#[cfg(debug_assertions)]
pub fn remove_subgraphs() {
    STORE
        .delete_all_entities_for_test_use_only()
        .expect("deleting test entities succeeds");
}

#[cfg(debug_assertions)]
pub fn place(name: &str) -> Result<Option<(Shard, Vec<NodeId>)>, String> {
    CONFIG.deployment.place(name, NETWORK_NAME)
}

#[cfg(debug_assertions)]
fn create_subgraph(
    subgraph_id: &SubgraphDeploymentId,
    schema: &str,
    base: Option<(SubgraphDeploymentId, EthereumBlockPointer)>,
) -> Result<(), StoreError> {
    let schema = Schema::parse(schema, subgraph_id.clone()).unwrap();

    let manifest = SubgraphManifest {
        id: subgraph_id.clone(),
        location: String::new(),
        spec_version: "1".to_owned(),
        features: std::collections::BTreeSet::new(),
        description: None,
        repository: None,
        schema: schema.clone(),
        data_sources: vec![],
        graft: None,
        templates: vec![],
    };

    let deployment = SubgraphDeploymentEntity::new(&manifest, false, None).graft(base);
    let name = {
        let mut name = subgraph_id.to_string();
        name.truncate(32);
        SubgraphName::new(name).unwrap()
    };
    let node_id = NodeId::new("test").unwrap();
    STORE.create_deployment_replace(
        name,
        &schema,
        deployment,
        node_id,
        NETWORK_NAME.to_string(),
        SubgraphVersionSwitchingMode::Instant,
    )?;
    STORE.start_subgraph_deployment(&*LOGGER, &subgraph_id)
}

#[cfg(debug_assertions)]
pub fn create_test_subgraph(subgraph_id: &SubgraphDeploymentId, schema: &str) {
    create_subgraph(subgraph_id, schema, None).unwrap()
}

#[cfg(debug_assertions)]
pub fn create_grafted_subgraph(
    subgraph_id: &SubgraphDeploymentId,
    schema: &str,
    base_id: &str,
    base_block: EthereumBlockPointer,
) -> Result<(), StoreError> {
    let base = Some((SubgraphDeploymentId::new(base_id).unwrap(), base_block));
    create_subgraph(subgraph_id, schema, base)
}

pub fn transact_errors(
    store: &Arc<NetworkStore>,
    subgraph_id: SubgraphDeploymentId,
    block_ptr_to: EthereumBlockPointer,
    errs: Vec<SubgraphError>,
) -> Result<(), StoreError> {
    let metrics_registry = Arc::new(MockMetricsRegistry::new());
    let stopwatch_metrics = StopwatchMetrics::new(
        Logger::root(slog::Discard, o!()),
        subgraph_id.clone(),
        metrics_registry.clone(),
    );
    store.transact_block_operations(
        subgraph_id,
        block_ptr_to,
        Vec::new(),
        stopwatch_metrics,
        errs,
    )
}

/// Convenience to transact EntityOperation instead of EntityModification
pub fn transact_entity_operations(
    store: &Arc<NetworkStore>,
    subgraph_id: SubgraphDeploymentId,
    block_ptr_to: EthereumBlockPointer,
    ops: Vec<EntityOperation>,
) -> Result<(), StoreError> {
    let mut entity_cache = EntityCache::new(store.clone());
    entity_cache.append(ops);
    let mods = entity_cache
        .as_modifications(store.as_ref())
        .expect("failed to convert to modifications")
        .modifications;
    let metrics_registry = Arc::new(MockMetricsRegistry::new());
    let stopwatch_metrics = StopwatchMetrics::new(
        Logger::root(slog::Discard, o!()),
        subgraph_id.clone(),
        metrics_registry.clone(),
    );
    store.transact_block_operations(
        subgraph_id,
        block_ptr_to,
        mods,
        stopwatch_metrics,
        Vec::new(),
    )
}

pub fn insert_ens_name(hash: &str, name: &str) {
    use diesel::insert_into;
    use diesel::prelude::*;
    let conn = PRIMARY_POOL.get().unwrap();

    table! {
        ens_names(hash) {
            hash -> Varchar,
            name -> Varchar,
        }
    }

    insert_into(ens_names::table)
        .values((ens_names::hash.eq(hash), ens_names::name.eq(name)))
        .on_conflict_do_nothing()
        .execute(&conn)
        .unwrap();
}

pub fn insert_entities(
    subgraph_id: SubgraphDeploymentId,
    entities: Vec<(EntityType, Entity)>,
) -> Result<(), StoreError> {
    let insert_ops = entities
        .into_iter()
        .map(|(entity_type, data)| EntityOperation::Set {
            key: EntityKey {
                subgraph_id: subgraph_id.clone(),
                entity_type: entity_type.to_owned(),
                entity_id: data["id"].clone().as_string().unwrap(),
            },
            data,
        });

    transact_entity_operations(
        &STORE,
        subgraph_id.clone(),
        GENESIS_PTR.clone(),
        insert_ops.collect::<Vec<_>>(),
    )
    .map(|_| ())
}

/// Tap into store events sent when running `f` and return those events. This
/// intercepts `StoreEvent` when they are sent and therefore does not require
/// the delicate timing that actually listening to events in the database
/// requires. Of course, this does not test that events that are sent are
/// actually received by anything, but makes ensuring that the right events
/// get sent much more convenient than trying to receive them
#[cfg(debug_assertions)]
pub fn tap_store_events<F>(f: F) -> Vec<StoreEvent>
where
    F: FnOnce(),
{
    use graph_store_postgres::layout_for_tests::{EVENT_TAP, EVENT_TAP_ENABLED};

    EVENT_TAP.lock().unwrap().clear();
    *EVENT_TAP_ENABLED.lock().unwrap() = true;
    f();
    *EVENT_TAP_ENABLED.lock().unwrap() = false;
    EVENT_TAP.lock().unwrap().clone()
}

#[cfg(debug_assertions)]
pub mod block_store {
    use diesel::prelude::*;
    use diesel::PgConnection;
    use std::str::FromStr;

    use graph::prelude::{serde_json, web3::types::H256, EthereumBlockPointer};
    use graph_store_postgres::db_schema_for_tests as db_schema;
    use lazy_static::lazy_static;

    /// The parts of an Ethereum block that are interesting for these tests:
    /// the block number, hash, and the hash of the parent block
    #[derive(Clone, Debug, PartialEq)]
    pub struct FakeBlock {
        pub number: u64,
        pub hash: String,
        pub parent_hash: String,
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
                    &b::network_name.eq(super::NETWORK_NAME),
                    &b::data.eq(data),
                ))
                .execute(conn)
                .expect(&errmsg);
        }

        pub fn block_hash(&self) -> H256 {
            H256::from_str(self.hash.as_str()).expect("invalid block hash")
        }

        pub fn block_ptr(&self) -> EthereumBlockPointer {
            EthereumBlockPointer {
                number: self.number,
                hash: self.block_hash(),
            }
        }
    }

    pub type Chain = Vec<&'static FakeBlock>;

    lazy_static! {
        // Hash indicating 'no parent'
        pub static ref NO_PARENT: String =
            "0000000000000000000000000000000000000000000000000000000000000000".to_owned();
        // Genesis block
        pub static ref GENESIS_BLOCK: FakeBlock = FakeBlock {
            number: super::GENESIS_PTR.number,
            hash: format!("{:x}", super::GENESIS_PTR.hash),
            parent_hash: NO_PARENT.clone()
        };
        pub static ref BLOCK_ONE: FakeBlock = GENESIS_BLOCK
            .make_child("8511fa04b64657581e3f00e14543c1d522d5d7e771b54aa3060b662ade47da13");
        pub static ref BLOCK_ONE_SIBLING: FakeBlock =
            GENESIS_BLOCK.make_child("b98fb783b49de5652097a989414c767824dff7e7fd765a63b493772511db81c1");
        pub static ref BLOCK_ONE_NO_PARENT: FakeBlock = FakeBlock::make_no_parent(
            1,
            "7205bdfcf4521874cf38ce38c879ff967bf3a069941286bfe267109ad275a63d"
        );

        pub static ref BLOCK_TWO: FakeBlock = BLOCK_ONE.make_child("f8ccbd3877eb98c958614f395dd351211afb9abba187bfc1fb4ac414b099c4a6");
        pub static ref BLOCK_TWO_NO_PARENT: FakeBlock = FakeBlock::make_no_parent(2, "3b652b00bff5e168b1218ff47593d516123261c4487629c4175f642ee56113fe");
        pub static ref BLOCK_THREE: FakeBlock = BLOCK_TWO.make_child("7347afe69254df06729e123610b00b8b11f15cfae3241f9366fb113aec07489c");
        pub static ref BLOCK_THREE_NO_PARENT: FakeBlock = FakeBlock::make_no_parent(3, "fa9ebe3f74de4c56908b49f5c4044e85825f7350f3fa08a19151de82a82a7313");
        pub static ref BLOCK_FOUR: FakeBlock = BLOCK_THREE.make_child("7cce080f5a49c2997a6cc65fc1cee9910fd8fc3721b7010c0b5d0873e2ac785e");
        pub static ref BLOCK_FIVE: FakeBlock = BLOCK_FOUR.make_child("7b0ea919e258eb2b119eb32de56b85d12d50ac6a9f7c5909f843d6172c8ba196");
        pub static ref BLOCK_SIX_NO_PARENT: FakeBlock = FakeBlock::make_no_parent(6, "6b834521bb753c132fdcf0e1034803ed9068e324112f8750ba93580b393a986b");
    }

    /// Removes all networks and blocks from the database
    pub fn remove() {
        use db_schema::ethereum_blocks as b;
        use db_schema::ethereum_networks as n;

        let conn = super::PRIMARY_POOL
            .get()
            .expect("Failed to connect to Postgres");

        diesel::delete(b::table)
            .execute(&conn)
            .expect("Failed to delete ethereum_blocks");
        diesel::delete(n::table)
            .execute(&conn)
            .expect("Failed to delete ethereum_networks");
    }

    // Store the given chain as the blocks for the `network` and set the
    // network's genesis block to the hash of `GENESIS_BLOCK`
    pub fn insert(chain: Chain, network: &str) {
        let conn = super::PRIMARY_POOL
            .get()
            .expect("Failed to connect to Postgres");

        use db_schema::ethereum_networks as n;
        let hash = format!("{:x}", super::GENESIS_PTR.hash);
        diesel::insert_into(n::table)
            .values((
                &n::name.eq(network),
                &n::genesis_block_hash.eq(&hash),
                &n::net_version.eq(super::NETWORK_VERSION),
            ))
            .execute(&conn)
            .expect("Failed to insert test network");

        for block in chain {
            block.insert(&conn);
        }
    }
}

/// Run a GraphQL query against the `STORE`
pub fn execute_subgraph_query(query: Query, target: QueryTarget) -> QueryResults {
    execute_subgraph_query_with_complexity(query, target, None)
}

pub fn execute_subgraph_query_with_complexity(
    query: Query,
    target: QueryTarget,
    max_complexity: Option<u64>,
) -> QueryResults {
    execute_subgraph_query_internal(query, target, max_complexity, None)
}

pub fn execute_subgraph_query_with_deadline(
    query: Query,
    target: QueryTarget,
    deadline: Option<Instant>,
) -> QueryResults {
    execute_subgraph_query_internal(query, target, None, deadline)
}

/// Like `try!`, but we return the contents of an `Err`, not the
/// whole `Result`
#[macro_export]
macro_rules! return_err {
    ( $ expr : expr ) => {
        match $expr {
            Ok(v) => v,
            Err(e) => return e.into(),
        }
    };
}

fn execute_subgraph_query_internal(
    query: Query,
    target: QueryTarget,
    max_complexity: Option<u64>,
    deadline: Option<Instant>,
) -> QueryResults {
    let mut rt = tokio::runtime::Builder::new()
        .basic_scheduler()
        .enable_io()
        .enable_time()
        .build()
        .unwrap();
    let logger = Logger::root(slog::Discard, o!());
    let id = match target {
        QueryTarget::Deployment(id) => id,
        _ => unreachable!("tests do not use this"),
    };
    let schema = STORE.api_schema(&id).unwrap();
    let network = STORE.network_name(&id).unwrap();
    let query = return_err!(PreparedQuery::new(
        &logger,
        schema,
        network,
        query,
        max_complexity,
        100
    ));
    let mut result = QueryResults::empty();
    let deployment = query.schema.id().clone();
    let store = STORE.clone().query_store(deployment.into(), false).unwrap();
    for (bc, (selection_set, error_policy)) in return_err!(query.block_constraint()) {
        let logger = logger.clone();
        let resolver = return_err!(rt.block_on(StoreResolver::at_block(
            &logger,
            store.clone(),
            bc,
            error_policy,
            query.schema.id().clone()
        )));
        result.append(rt.block_on(execute_query(
            query.clone(),
            Some(selection_set),
            None,
            QueryExecutionOptions {
                resolver,
                deadline,
                load_manager: LOAD_MANAGER.clone(),
                max_first: std::u32::MAX,
                max_skip: std::u32::MAX,
            },
            false,
        )))
    }
    result
}

fn build_store() -> (Arc<NetworkStore>, ConnectionPool, Config) {
    let mut opt = Opt::default();
    let url = std::env::var_os("THEGRAPH_STORE_POSTGRES_DIESEL_URL").filter(|s| s.len() > 0);
    let file = std::env::var_os("GRAPH_NODE_TEST_CONFIG").filter(|s| s.len() > 0);
    if let Some(file) = file {
        let file = file.into_string().unwrap();
        opt.config = Some(file);
        if url.is_some() {
            eprintln!("WARNING: ignoring THEGRAPH_STORE_POSTGRES_DIESEL_URL because GRAPH_NODE_TEST_CONFIG is set");
        }
    } else if let Some(url) = url {
        let url = url.into_string().unwrap();
        opt.postgres_url = Some(url);
    } else {
        panic!("You can not set both THEGRAPH_STORE_POSTGRES_DIESEL_URL and GRAPH_NODE_TEST_CONFIG (see CONTRIBUTING.md)");
    }
    opt.store_connection_pool_size = CONN_POOL_SIZE;

    let config = Config::load(&*LOGGER, &opt).expect("config is not valid");
    let registry = Arc::new(MockMetricsRegistry::new());
    std::thread::spawn(move || {
        STORE_RUNTIME.lock().unwrap().block_on(async {
            let builder = StoreBuilder::new(&*LOGGER, &config, registry);
            let net_identifiers = EthereumNetworkIdentifier {
                net_version: NETWORK_VERSION.to_owned(),
                genesis_block_hash: GENESIS_PTR.hash,
            };

            (
                builder.network_store(NETWORK_NAME.to_string(), net_identifiers),
                builder.primary_pool(),
                config,
            )
        })
    })
    .join()
    .unwrap()
}

#[cfg(debug_assertions)]
pub fn primary_connection() -> graph_store_postgres::layout_for_tests::Connection {
    let conn = PRIMARY_POOL.get().unwrap();
    graph_store_postgres::layout_for_tests::Connection::new(conn)
}
