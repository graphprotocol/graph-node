use std::sync::{Arc, RwLock};
use url::Url;

use graph::prelude::{info, CheapClone, EthereumNetworkIdentifier, Logger, MovingStats};
use graph_core::MetricsRegistry;
use graph_store_postgres::connection_pool::{create_connection_pool, ConnectionPool};
use graph_store_postgres::{
    ChainHeadUpdateListener as PostgresChainHeadUpdateListener, ChainStore as DieselChainStore,
    NetworkStore as DieselNetworkStore, Store as DieselStore, SubscriptionManager,
};

/// Replace the host portion of `url` and return a new URL with `host`
/// as the host portion
///
/// Panics if `url` is not a valid URL (which won't happen in our case since
/// we would have paniced before getting here as `url` is the connection for
/// the primary Postgres instance)
fn replace_host(url: &str, host: &str) -> String {
    let mut url = match Url::parse(url) {
        Ok(url) => url,
        Err(_) => panic!("Invalid Postgres URL {}", url),
    };
    if let Err(e) = url.set_host(Some(host)) {
        panic!("Invalid Postgres url {}: {}", url, e.to_string());
    }
    url.into_string()
}

pub struct StoreBuilder {
    store: Arc<DieselStore>,
    conn_pool: ConnectionPool,
    chain_head_update_listener: Arc<PostgresChainHeadUpdateListener>,
}

impl StoreBuilder {
    pub fn new(
        logger: &Logger,
        postgres_url: &str,
        pool_size: u32,
        read_replicas: Vec<String>,
        replica_weights: Vec<usize>,
        wait_stats: Arc<RwLock<MovingStats>>,
        registry: Arc<MetricsRegistry>,
    ) -> Self {
        // Minimum of two connections needed for the pool in order for the Store to bootstrap
        if pool_size <= 1 {
            panic!("--store-connection-pool-size/STORE_CONNECTION_POOL_SIZE must be > 1")
        }

        let conn_pool = create_connection_pool(
            "main",
            postgres_url.to_owned(),
            pool_size,
            &logger,
            registry.cheap_clone(),
            wait_stats.cheap_clone(),
        );

        let read_only_conn_pools: Vec<_> = read_replicas
            .into_iter()
            .enumerate()
            .map(|(i, host)| {
                info!(&logger, "Connecting to Postgres read replica at {}", host);
                let url = replace_host(postgres_url, &host);
                create_connection_pool(
                    &format!("replica{}", i),
                    url,
                    pool_size,
                    &logger,
                    registry.cheap_clone(),
                    wait_stats.cheap_clone(),
                )
            })
            .collect();

        let subscriptions = Arc::new(SubscriptionManager::new(
            logger.clone(),
            postgres_url.to_owned(),
        ));

        let store = Arc::new(DieselStore::new(
            logger,
            subscriptions.clone(),
            conn_pool.clone(),
            read_only_conn_pools.clone(),
            replica_weights.clone(),
            registry.clone(),
        ));

        let chain_head_update_listener = Arc::new(PostgresChainHeadUpdateListener::new(
            &logger,
            registry.clone(),
            postgres_url.to_owned(),
        ));

        Self {
            store,
            conn_pool,
            chain_head_update_listener,
        }
    }

    /// Return a store that includes a `ChainStore` for the given network
    pub fn network_store(
        &self,
        network_name: String,
        network_identifier: EthereumNetworkIdentifier,
    ) -> Arc<DieselNetworkStore> {
        let chain_store = DieselChainStore::new(
            network_name,
            network_identifier,
            self.chain_head_update_listener.clone(),
            self.conn_pool.clone(),
        );
        Arc::new(DieselNetworkStore::new(self.store.clone(), chain_store))
    }

    /// Return the store for subgraph and other storage; this store can
    /// handle everything besides being a `ChainStore`
    pub fn store(&self) -> Arc<DieselStore> {
        self.store.cheap_clone()
    }
}
