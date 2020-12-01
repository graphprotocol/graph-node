use std::{collections::HashMap, sync::Arc};

use graph::prelude::MetricsRegistry;
use graph::{
    prelude::{info, CheapClone, EthereumNetworkIdentifier, Logger},
    util::security::SafeDisplay,
};
use graph_store_postgres::connection_pool::ConnectionPool;
use graph_store_postgres::{
    ChainHeadUpdateListener as PostgresChainHeadUpdateListener, ChainStore as DieselChainStore,
    NetworkStore as DieselNetworkStore, ShardedStore, Store as DieselStore, SubscriptionManager,
    PRIMARY_SHARD,
};

use crate::config::Config;

pub struct StoreBuilder {
    store: Arc<ShardedStore>,
    conn_pool: ConnectionPool,
    chain_head_update_listener: Arc<PostgresChainHeadUpdateListener>,
}

impl StoreBuilder {
    pub fn new(logger: &Logger, config: &Config, registry: Arc<dyn MetricsRegistry>) -> Self {
        let primary = config.primary_store();

        let conn_pool = Self::main_pool(logger, config, registry.cheap_clone());

        let (read_only_conn_pools, weights) =
            Self::replica_pools(logger, config, registry.cheap_clone());

        let subscriptions = Arc::new(SubscriptionManager::new(
            logger.clone(),
            primary.connection.to_owned(),
        ));

        let primary_store = Arc::new(DieselStore::new(
            logger,
            subscriptions.clone(),
            conn_pool.clone(),
            read_only_conn_pools.clone(),
            weights,
            registry.cheap_clone(),
        ));
        let mut store_map = HashMap::new();
        store_map.insert(PRIMARY_SHARD.clone(), primary_store);
        let store = Arc::new(ShardedStore::new(
            store_map,
            Arc::new(config.deployment.clone()),
        ));

        let chain_head_update_listener = Arc::new(PostgresChainHeadUpdateListener::new(
            &logger,
            registry.clone(),
            primary.connection.to_owned(),
        ));

        Self {
            store,
            conn_pool,
            chain_head_update_listener,
        }
    }

    /// Create a connection pool for the main database
    pub fn main_pool(
        logger: &Logger,
        config: &Config,
        registry: Arc<dyn MetricsRegistry>,
    ) -> ConnectionPool {
        let primary = config.primary_store();

        info!(
            logger,
            "Connecting to Postgres (primary)";
            "url" => SafeDisplay(primary.connection.as_str()),
            "conn_pool_size" => primary.pool_size,
            "weight" => primary.weight
        );
        ConnectionPool::create(
            "main",
            primary.connection.to_owned(),
            primary.pool_size,
            &logger,
            registry.cheap_clone(),
        )
    }

    /// Create connection pools for each of the replicas
    pub fn replica_pools(
        logger: &Logger,
        config: &Config,
        registry: Arc<dyn MetricsRegistry>,
    ) -> (Vec<ConnectionPool>, Vec<usize>) {
        let primary = config.primary_store();
        let mut weights: Vec<_> = vec![primary.weight];
        (
            primary
                .replicas
                .values()
                .enumerate()
                .map(|(i, replica)| {
                    info!(
                        &logger,
                        "Connecting to Postgres (read replica {})", i+1;
                        "url" => SafeDisplay(replica.connection.as_str()),
                        "weight" => replica.weight
                    );
                    weights.push(replica.weight);
                    ConnectionPool::create(
                        &format!("replica{}", i),
                        replica.connection.clone(),
                        replica.pool_size,
                        &logger,
                        registry.cheap_clone(),
                    )
                })
                .collect(),
            weights,
        )
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
        Arc::new(DieselNetworkStore::new(
            self.store.clone(),
            Arc::new(chain_store),
        ))
    }

    /// Return the store for subgraph and other storage; this store can
    /// handle everything besides being a `ChainStore`
    pub fn store(&self) -> Arc<ShardedStore> {
        self.store.cheap_clone()
    }

    // This is used in the test-store, but rustc keeps complaining that it
    // is not used
    #[cfg(debug_assertions)]
    #[allow(dead_code)]
    pub fn primary_pool(&self) -> ConnectionPool {
        self.conn_pool.clone()
    }
}
