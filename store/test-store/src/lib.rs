use crate::tokio::runtime::Runtime;
use graph::log;
#[allow(unused_imports)]
use graph::prelude::{Store as _, *};
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

            Ok(Arc::new(Store::new(
                StoreConfig {
                    postgres_url,
                    network_name: NETWORK_NAME.to_owned(),
                    start_block: 0u64,
                    conn_pool_size: 10,
                },
                &logger,
                net_identifiers,
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

pub fn make_history_event(
    block_ptr: &EthereumBlockPointer,
    subgraph: &SubgraphDeploymentId,
) -> HistoryEvent {
    let source = EventSource::EthereumBlock(block_ptr.to_owned());
    HistoryEvent {
        id: 0,
        subgraph: subgraph.clone(),
        source,
    }
}
