use crate::tokio::runtime::Runtime;
use graph::log;
#[allow(unused_imports)]
use graph::prelude::{Store as _, *};
use graph::web3::types::H256;
use graph_store_postgres::{Store, StoreConfig};
use lazy_static::lazy_static;
use std::env;
use std::sync::Mutex;

pub fn postgres_test_url() -> String {
    std::env::var_os("THEGRAPH_STORE_POSTGRES_DIESEL_URL")
        .expect("The THEGRAPH_STORE_POSTGRES_DIESEL_URL environment variable is not set")
        .into_string()
        .unwrap()
}

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
                net_version: "graph test suite".to_owned(),
                genesis_block_hash: GENESIS_PTR.hash,
            };
            let network_name = "fake_network".to_owned();

            Ok(Arc::new(Store::new(
                StoreConfig {
                    postgres_url,
                    network_name,
                    start_block: 0u64,
                    conn_pool_size: 10,
                },
                &logger,
                net_identifiers,
            )))
        })).expect("could not create Diesel Store instance for test suite")
    };

    pub static ref GENESIS_PTR: EthereumBlockPointer = (
        H256::from("0xbd34884280958002c51d3f7b5f853e6febeba33de0f40d15b0363006533c924f"),
        0u64
    ).into();
}
