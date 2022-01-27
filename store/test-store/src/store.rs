use diesel::{self, PgConnection};
use graph::data::graphql::effort::LoadManager;
use graph::data::query::QueryResults;
use graph::data::query::QueryTarget;
use graph::data::subgraph::schema::SubgraphError;
use graph::log;
use graph::prelude::{QueryStoreManager as _, SubgraphStore as _, *};
use graph::semver::Version;
use graph::{
    blockchain::ChainIdentifier, components::store::DeploymentLocator,
    components::store::EntityType, components::store::StatusStore,
    components::store::StoredDynamicDataSource, data::subgraph::status, prelude::NodeId,
};
use graph_graphql::prelude::{
    execute_query, Query as PreparedQuery, QueryExecutionOptions, StoreResolver,
};
use graph_graphql::test_support::ResultSizeMetrics;
use graph_mock::MockMetricsRegistry;
use graph_node::config::{Config, Opt};
use graph_node::store_builder::StoreBuilder;
use graph_store_postgres::layout_for_tests::FAKE_NETWORK_SHARED;
use graph_store_postgres::{connection_pool::ConnectionPool, Shard, SubscriptionManager};
use graph_store_postgres::{
    BlockStore as DieselBlcokStore, DeploymentPlacer, SubgraphStore as DieselSubgraphStore,
    PRIMARY_SHARD,
};
use hex_literal::hex;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::time::Instant;
use std::{collections::BTreeSet, env};
use std::{marker::PhantomData, sync::Mutex};
use tokio::runtime::{Builder, Runtime};
use web3::types::H256;

pub const NETWORK_NAME: &str = "fake_network";
pub const NETWORK_VERSION: &str = "graph test suite";

pub use graph_store_postgres::Store;

const CONN_POOL_SIZE: u32 = 20;

lazy_static! {
    pub static ref LOGGER: Logger = match env::var_os("GRAPH_LOG") {
        Some(_) => log::logger(false),
        None => Logger::root(slog::Discard, o!()),
    };
    static ref SEQ_LOCK: Mutex<()> = Mutex::new(());
    pub static ref STORE_RUNTIME: Runtime =
        Builder::new_multi_thread().enable_all().build().unwrap();
    pub static ref METRICS_REGISTRY: Arc<MockMetricsRegistry> =
        Arc::new(MockMetricsRegistry::new());
    pub static ref LOAD_MANAGER: Arc<LoadManager> = Arc::new(LoadManager::new(
        &*LOGGER,
        Vec::new(),
        METRICS_REGISTRY.clone(),
    ));
    static ref STORE_POOL_CONFIG: (Arc<Store>, ConnectionPool, Config, Arc<SubscriptionManager>) =
        build_store();
    pub(crate) static ref PRIMARY_POOL: ConnectionPool = STORE_POOL_CONFIG.1.clone();
    pub static ref STORE: Arc<Store> = STORE_POOL_CONFIG.0.clone();
    static ref CONFIG: Config = STORE_POOL_CONFIG.2.clone();
    pub static ref SUBSCRIPTION_MANAGER: Arc<SubscriptionManager> = STORE_POOL_CONFIG.3.clone();
    pub static ref NODE_ID: NodeId = NodeId::new("test").unwrap();
    static ref SUBGRAPH_STORE: Arc<DieselSubgraphStore> = STORE.subgraph_store();
    static ref BLOCK_STORE: Arc<DieselBlcokStore> = STORE.block_store();
    pub static ref GENESIS_PTR: BlockPtr = (
        H256::from(hex!(
            "bd34884280958002c51d3f7b5f853e6febeba33de0f40d15b0363006533c924f"
        )),
        0u64
    )
        .into();
    pub static ref BLOCK_ONE: BlockPtr = (
        H256::from(hex!(
            "8511fa04b64657581e3f00e14543c1d522d5d7e771b54aa3060b662ade47da13"
        )),
        1u64
    )
        .into();
    pub static ref BLOCKS: [BlockPtr; 4] = {
        let two: BlockPtr = (
            H256::from(hex!(
                "b98fb783b49de5652097a989414c767824dff7e7fd765a63b493772511db81c1"
            )),
            2u64,
        )
            .into();
        let three: BlockPtr = (
            H256::from(hex!(
                "977c084229c72a0fa377cae304eda9099b6a2cb5d83b25cdf0f0969b69874255"
            )),
            3u64,
        )
            .into();
        [GENESIS_PTR.clone(), BLOCK_ONE.clone(), two, three]
    };
}

/// Run the `test` after performing `setup`. The result of `setup` is passed
/// into `test`. All tests using `run_test_sequentially` are run in sequence,
/// never in parallel. The `test` is passed a `Store`, but it is permissible
/// for tests to access the global `STORE` from this module, too.
pub fn run_test_sequentially<R, F>(test: F)
where
    F: FnOnce(Arc<Store>) -> R + Send + 'static,
    R: std::future::Future<Output = ()> + Send + 'static,
{
    // Lock regardless of poisoning. This also forces sequential test execution.
    let _lock = match SEQ_LOCK.lock() {
        Ok(guard) => guard,
        Err(err) => err.into_inner(),
    };

    STORE_RUNTIME.handle().block_on(async {
        let store = STORE.clone();
        test(store).await
    })
}

/// Run a test with a connection into the primary database, not a full store
pub fn run_test_with_conn<F>(test: F)
where
    F: FnOnce(&PgConnection) -> (),
{
    // Lock regardless of poisoning. This also forces sequential test execution.
    let _lock = match SEQ_LOCK.lock() {
        Ok(guard) => guard,
        Err(err) => err.into_inner(),
    };

    let conn = PRIMARY_POOL
        .get()
        .expect("failed to get connection for primary database");

    test(&conn);
}

pub fn remove_subgraphs() {
    SUBGRAPH_STORE
        .delete_all_entities_for_test_use_only()
        .expect("deleting test entities succeeds");
}

pub fn place(name: &str) -> Result<Option<(Vec<Shard>, Vec<NodeId>)>, String> {
    CONFIG.deployment.place(name, NETWORK_NAME)
}

pub fn create_subgraph(
    subgraph_id: &DeploymentHash,
    schema: &str,
    base: Option<(DeploymentHash, BlockPtr)>,
) -> Result<DeploymentLocator, StoreError> {
    let schema = Schema::parse(schema, subgraph_id.clone()).unwrap();

    let manifest = SubgraphManifest::<graph_chain_ethereum::Chain> {
        id: subgraph_id.clone(),
        spec_version: Version::new(1, 0, 0),
        features: BTreeSet::new(),
        description: Some(format!("manifest for {}", subgraph_id)),
        repository: Some(format!("repo for {}", subgraph_id)),
        schema: schema.clone(),
        data_sources: vec![],
        graft: None,
        templates: vec![],
        chain: PhantomData,
    };

    let deployment = SubgraphDeploymentEntity::new(&manifest, false, None).graft(base);
    let name = {
        let mut name = subgraph_id.to_string();
        name.truncate(32);
        SubgraphName::new(name).unwrap()
    };
    let deployment = SUBGRAPH_STORE.create_deployment_replace(
        name,
        &schema,
        deployment,
        NODE_ID.clone(),
        NETWORK_NAME.to_string(),
        SubgraphVersionSwitchingMode::Instant,
    )?;
    futures03::executor::block_on(
        SUBGRAPH_STORE
            .cheap_clone()
            .writable(LOGGER.clone(), deployment.id),
    )?
    .start_subgraph_deployment(&*LOGGER)?;
    Ok(deployment)
}

pub fn create_test_subgraph(subgraph_id: &DeploymentHash, schema: &str) -> DeploymentLocator {
    create_subgraph(subgraph_id, schema, None).unwrap()
}

pub fn remove_subgraph(id: &DeploymentHash) {
    let name = {
        let mut name = id.to_string();
        name.truncate(32);
        SubgraphName::new(name).unwrap()
    };
    SUBGRAPH_STORE.remove_subgraph(name).unwrap();
    for detail in SUBGRAPH_STORE.record_unused_deployments().unwrap() {
        SUBGRAPH_STORE.remove_deployment(detail.id).unwrap();
    }
}

pub async fn transact_errors(
    store: &Arc<Store>,
    deployment: &DeploymentLocator,
    block_ptr_to: BlockPtr,
    errs: Vec<SubgraphError>,
) -> Result<(), StoreError> {
    let metrics_registry = Arc::new(MockMetricsRegistry::new());
    let stopwatch_metrics = StopwatchMetrics::new(
        Logger::root(slog::Discard, o!()),
        deployment.hash.clone(),
        metrics_registry.clone(),
    );
    store
        .subgraph_store()
        .writable(LOGGER.clone(), deployment.id.clone())
        .await?
        .transact_block_operations(
            block_ptr_to,
            None,
            Vec::new(),
            stopwatch_metrics,
            Vec::new(),
            errs,
        )
}

/// Convenience to transact EntityOperation instead of EntityModification
pub fn transact_entity_operations(
    store: &Arc<DieselSubgraphStore>,
    deployment: &DeploymentLocator,
    block_ptr_to: BlockPtr,
    ops: Vec<EntityOperation>,
) -> Result<(), StoreError> {
    transact_entities_and_dynamic_data_sources(store, deployment.clone(), block_ptr_to, vec![], ops)
}

pub fn transact_entities_and_dynamic_data_sources(
    store: &Arc<DieselSubgraphStore>,
    deployment: DeploymentLocator,
    block_ptr_to: BlockPtr,
    data_sources: Vec<StoredDynamicDataSource>,
    ops: Vec<EntityOperation>,
) -> Result<(), StoreError> {
    let store =
        futures03::executor::block_on(store.cheap_clone().writable(LOGGER.clone(), deployment.id))?;
    let mut entity_cache = EntityCache::new(store.clone());
    entity_cache.append(ops);
    let mods = entity_cache
        .as_modifications()
        .expect("failed to convert to modifications")
        .modifications;
    let metrics_registry = Arc::new(MockMetricsRegistry::new());
    let stopwatch_metrics = StopwatchMetrics::new(
        Logger::root(slog::Discard, o!()),
        deployment.hash.clone(),
        metrics_registry.clone(),
    );
    store.transact_block_operations(
        block_ptr_to,
        None,
        mods,
        stopwatch_metrics,
        data_sources,
        Vec::new(),
    )
}

pub async fn revert_block(store: &Arc<Store>, deployment: &DeploymentLocator, ptr: &BlockPtr) {
    store
        .subgraph_store()
        .writable(LOGGER.clone(), deployment.id)
        .await
        .expect("can get writable")
        .revert_block_operations(ptr.clone(), None)
        .unwrap();
}

pub fn insert_ens_name(hash: &str, name: &str) {
    use diesel::insert_into;
    use diesel::prelude::*;
    use graph_store_postgres::command_support::catalog::ens_names;

    let conn = PRIMARY_POOL.get().unwrap();

    insert_into(ens_names::table)
        .values((ens_names::hash.eq(hash), ens_names::name.eq(name)))
        .on_conflict_do_nothing()
        .execute(&conn)
        .unwrap();
}

pub fn insert_entities(
    deployment: &DeploymentLocator,
    entities: Vec<(EntityType, Entity)>,
) -> Result<(), StoreError> {
    let insert_ops = entities
        .into_iter()
        .map(|(entity_type, data)| EntityOperation::Set {
            key: EntityKey {
                subgraph_id: deployment.hash.clone(),
                entity_type: entity_type.to_owned(),
                entity_id: data.get("id").unwrap().clone().as_string().unwrap(),
            },
            data,
        });

    transact_entity_operations(
        &*SUBGRAPH_STORE,
        deployment,
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
pub fn tap_store_events<F, R>(f: F) -> (R, Vec<StoreEvent>)
where
    F: FnOnce() -> R,
{
    use graph_store_postgres::layout_for_tests::{EVENT_TAP, EVENT_TAP_ENABLED};

    EVENT_TAP.lock().unwrap().clear();
    *EVENT_TAP_ENABLED.lock().unwrap() = true;
    let res = f();
    *EVENT_TAP_ENABLED.lock().unwrap() = false;
    (res, EVENT_TAP.lock().unwrap().clone())
}

/// Run a GraphQL query against the `STORE`
pub async fn execute_subgraph_query(query: Query, target: QueryTarget) -> QueryResults {
    execute_subgraph_query_with_complexity(query, target, None).await
}

pub async fn execute_subgraph_query_with_complexity(
    query: Query,
    target: QueryTarget,
    max_complexity: Option<u64>,
) -> QueryResults {
    execute_subgraph_query_internal(query, target, max_complexity, None).await
}

pub async fn execute_subgraph_query_with_deadline(
    query: Query,
    target: QueryTarget,
    deadline: Option<Instant>,
) -> QueryResults {
    execute_subgraph_query_internal(query, target, None, deadline).await
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

pub fn result_size_metrics() -> Arc<ResultSizeMetrics> {
    Arc::new(ResultSizeMetrics::make(METRICS_REGISTRY.clone()))
}

async fn execute_subgraph_query_internal(
    query: Query,
    target: QueryTarget,
    max_complexity: Option<u64>,
    deadline: Option<Instant>,
) -> QueryResults {
    let logger = Logger::root(slog::Discard, o!());
    let id = match target {
        QueryTarget::Deployment(id) => id,
        _ => unreachable!("tests do not use this"),
    };
    let schema = SUBGRAPH_STORE.api_schema(&id).unwrap();
    let status = StatusStore::status(
        STORE.as_ref(),
        status::Filter::Deployments(vec![id.to_string()]),
    )
    .unwrap();
    let network = Some(status[0].chains[0].network.clone());
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
    let store = STORE
        .clone()
        .query_store(deployment.into(), false)
        .await
        .unwrap();
    for (bc, (selection_set, error_policy)) in return_err!(query.block_constraint()) {
        let logger = logger.clone();
        let resolver = return_err!(
            StoreResolver::at_block(
                &logger,
                store.clone(),
                SUBSCRIPTION_MANAGER.clone(),
                bc,
                error_policy,
                query.schema.id().clone(),
                result_size_metrics()
            )
            .await
        );
        result.append(
            execute_query(
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
            )
            .await,
        )
    }
    result
}

pub async fn deployment_state(store: &Store, subgraph_id: &DeploymentHash) -> DeploymentState {
    store
        .query_store(QueryTarget::Deployment(subgraph_id.to_owned()), false)
        .await
        .expect("could get a query store")
        .deployment_state()
        .await
        .expect("can get deployment state")
}

pub fn store_is_sharded() -> bool {
    CONFIG.stores.len() > 1
}

pub fn all_shards() -> Vec<Shard> {
    CONFIG
        .stores
        .keys()
        .map(|shard| Shard::new(shard.clone()))
        .collect::<Result<Vec<_>, _>>()
        .expect("all configured shard names are valid")
}

fn build_store() -> (Arc<Store>, ConnectionPool, Config, Arc<SubscriptionManager>) {
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
        panic!("You must set either THEGRAPH_STORE_POSTGRES_DIESEL_URL or GRAPH_NODE_TEST_CONFIG (see ./CONTRIBUTING.md).");
    }
    opt.store_connection_pool_size = CONN_POOL_SIZE;

    let config = Config::load(&*LOGGER, &opt)
        .expect(&format!("config is not valid (file={:?})", &opt.config));
    let registry = Arc::new(MockMetricsRegistry::new());
    std::thread::spawn(move || {
        STORE_RUNTIME.handle().block_on(async {
            let builder = StoreBuilder::new(&*LOGGER, &*NODE_ID, &config, registry).await;
            let subscription_manager = builder.subscription_manager();
            let primary_pool = builder.primary_pool();

            let ident = ChainIdentifier {
                net_version: NETWORK_VERSION.to_owned(),
                genesis_block_hash: GENESIS_PTR.hash.clone(),
            };

            (
                builder.network_store(vec![
                    (NETWORK_NAME.to_string(), vec![ident.clone()]),
                    (FAKE_NETWORK_SHARED.to_string(), vec![ident]),
                ]),
                primary_pool,
                config,
                subscription_manager,
            )
        })
    })
    .join()
    .unwrap()
}

pub fn primary_connection() -> graph_store_postgres::layout_for_tests::Connection<'static> {
    let conn = PRIMARY_POOL.get().unwrap();
    graph_store_postgres::layout_for_tests::Connection::new(conn)
}

pub fn primary_mirror() -> graph_store_postgres::layout_for_tests::Mirror {
    let pool = PRIMARY_POOL.clone();
    let map = HashMap::from_iter(Some((PRIMARY_SHARD.clone(), pool)));
    graph_store_postgres::layout_for_tests::Mirror::new(&map)
}
