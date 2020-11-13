use std::{collections::HashMap, sync::Arc};

use graph::{
    prelude::{info, CheapClone, EthereumNetworkIdentifier, Logger, PRIMARY_SHARD},
    util::security::SafeDisplay,
};
use graph_core::MetricsRegistry;
use graph_store_postgres::connection_pool::ConnectionPool;
use graph_store_postgres::{
    ChainHeadUpdateListener as PostgresChainHeadUpdateListener, ChainStore as DieselChainStore,
    NetworkStore as DieselNetworkStore, ShardedStore, Store as DieselStore, SubscriptionManager,
};

use crate::config::Config;

pub struct StoreBuilder {
    store: Arc<ShardedStore>,
    conn_pool: ConnectionPool,
    chain_head_update_listener: Arc<PostgresChainHeadUpdateListener>,
}

impl StoreBuilder {
    pub fn new(logger: &Logger, config: &Config, registry: Arc<MetricsRegistry>) -> Self {
        let primary = config.primary_store();

        info!(
            logger,
            "Connecting to Postgres (primary)";
            "url" => SafeDisplay(primary.connection.as_str()),
            "conn_pool_size" => primary.pool_size,
            "weight" => primary.weight
        );
        let conn_pool = ConnectionPool::create(
            "main",
            primary.connection.to_owned(),
            primary.pool_size,
            &logger,
            registry.cheap_clone(),
        );

        let mut weights: Vec<_> = vec![primary.weight];
        let read_only_conn_pools: Vec<_> = primary
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
            .collect();

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
            registry.clone(),
        ));
        let mut store_map = HashMap::new();
        store_map.insert(PRIMARY_SHARD.to_string(), primary_store);
        let store = Arc::new(ShardedStore::new(store_map));

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
    pub fn store(&self) -> Arc<ShardedStore> {
        self.store.cheap_clone()
    }
}
