use crate::tokio::runtime::Runtime;
use graph::log;
use graph::prelude::{Store as _, *};
use graph_mock::MockMetricsRegistry;
use graph_store_postgres::connection_pool::create_connection_pool;
use graph_store_postgres::{Store, StoreConfig};
use hex_literal::hex;
use lazy_static::lazy_static;
use std::env;
use std::sync::Mutex;
use web3::types::H256;

pub fn postgres_test_url() -> String {
    std::env::var_os("THEGRAPH_STORE_POSTGRES_DIESEL_URL")
        .expect("The THEGRAPH_STORE_POSTGRES_DIESEL_URL environment variable is not set")
        .into_string()
        .unwrap()
}

pub const NETWORK_NAME: &str = "fake_network";
pub const NETWORK_VERSION: &str = "graph test suite";

lazy_static! {
    pub static ref LOGGER:Logger = match env::var_os("GRAPH_LOG") {
        Some(_) => log::logger(false),
        None => Logger::root(slog::Discard, o!()),
    };

    // Use this for tests that need subscriptions from `STORE`.
    pub static ref STORE_RUNTIME: Mutex<Runtime> = Mutex::new(Runtime::new().unwrap());

    // Create Store instance once for use with each of the tests.
    pub static ref STORE: Arc<Store> = {
        STORE_RUNTIME.lock().unwrap().block_on(future::lazy(|| -> Result<_, ()> {
            // Set up Store
            let logger = &*LOGGER;
            let postgres_url = postgres_test_url();
            let net_identifiers = EthereumNetworkIdentifier {
                net_version: NETWORK_VERSION.to_owned(),
                genesis_block_hash: GENESIS_PTR.hash,
            };
            let conn_pool_size: u32 = 10;
            let postgres_conn_pool = create_connection_pool(
                postgres_url.clone(),
                conn_pool_size,
                &logger,
            );
            Ok(Arc::new(Store::new(
                StoreConfig {
                    postgres_url,
                    network_name: NETWORK_NAME.to_owned(),
                },
                &logger,
                net_identifiers,
                postgres_conn_pool,
                Arc::new(MockMetricsRegistry::new()),
            )))
        })).expect("could not create Diesel Store instance for test suite")
    };

    pub static ref GENESIS_PTR: EthereumBlockPointer = (
        H256::from(hex!("bd34884280958002c51d3f7b5f853e6febeba33de0f40d15b0363006533c924f")),
        0u64
    ).into();

    pub static ref BLOCK_ONE: EthereumBlockPointer = (
        H256::from(hex!(
            "8511fa04b64657581e3f00e14543c1d522d5d7e771b54aa3060b662ade47da13"
        )),
        1u64
    ).into();
}

/// Convenience to transact EntityOperation instead of EntityModification
pub fn transact_entity_operations(
    store: &Arc<Store>,
    subgraph_id: SubgraphDeploymentId,
    block_ptr_to: EthereumBlockPointer,
    ops: Vec<EntityOperation>,
) -> Result<bool, StoreError> {
    let mut entity_cache = EntityCache::new();
    entity_cache.append(ops);
    let mods = entity_cache
        .as_modifications(store.as_ref())
        .expect("failed to convert to modifications");
    let metrics_registry = Arc::new(MockMetricsRegistry::new());
    let stopwatch_metrics = StopwatchMetrics::new(
        Logger::root(slog::Discard, o!()),
        subgraph_id.clone(),
        metrics_registry.clone(),
    );
    store.transact_block_operations(subgraph_id, block_ptr_to, mods, stopwatch_metrics)
}
