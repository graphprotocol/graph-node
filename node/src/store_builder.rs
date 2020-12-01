use std::iter::FromIterator;
use std::{collections::HashMap, sync::Arc};

use graph::prelude::{o, MetricsRegistry};
use graph::{
    prelude::{info, CheapClone, EthereumNetworkIdentifier, Logger},
    util::security::SafeDisplay,
};
use graph_store_postgres::connection_pool::ConnectionPool;
use graph_store_postgres::{
    ChainHeadUpdateListener as PostgresChainHeadUpdateListener, ChainStore as DieselChainStore,
    NetworkStore as DieselNetworkStore, Shard as ShardName, ShardedStore, Store as DieselStore,
    SubscriptionManager, PRIMARY_SHARD,
};

use crate::config::{Config, Shard};

pub struct StoreBuilder {
    store: Arc<ShardedStore>,
    primary_pool: ConnectionPool,
    chain_head_update_listener: Arc<PostgresChainHeadUpdateListener>,
}

impl StoreBuilder {
    pub fn new(logger: &Logger, config: &Config, registry: Arc<dyn MetricsRegistry>) -> Self {
        let primary = config.primary_store();

        let subscriptions = Arc::new(SubscriptionManager::new(
            logger.cheap_clone(),
            primary.connection.to_owned(),
        ));

        let shards: Vec<_> = config
            .stores
            .iter()
            .map(|(name, shard)| {
                Self::make_shard(
                    logger,
                    name,
                    shard,
                    subscriptions.cheap_clone(),
                    registry.cheap_clone(),
                )
            })
            .collect();

        let primary_pool = shards
            .iter()
            .find(|(name, _, _)| name.as_str() == PRIMARY_SHARD.as_str())
            .unwrap()
            .2
            .clone();

        let shard_map =
            HashMap::from_iter(shards.into_iter().map(|(name, shard, _)| (name, shard)));

        let store = Arc::new(ShardedStore::new(
            shard_map,
            Arc::new(config.deployment.clone()),
        ));

        let chain_head_update_listener = Arc::new(PostgresChainHeadUpdateListener::new(
            &logger,
            registry.cheap_clone(),
            primary.connection.to_owned(),
        ));

        Self {
            store,
            primary_pool,
            chain_head_update_listener,
        }
    }

    fn make_shard(
        logger: &Logger,
        name: &str,
        shard: &Shard,
        subscriptions: Arc<SubscriptionManager>,
        registry: Arc<dyn MetricsRegistry>,
    ) -> (ShardName, Arc<DieselStore>, ConnectionPool) {
        let logger = logger.new(o!("shard" => name.to_string()));
        let conn_pool = Self::main_pool(&logger, name, shard, registry.cheap_clone());

        let (read_only_conn_pools, weights) =
            Self::replica_pools(&logger, name, shard, registry.cheap_clone());

        let shard = Arc::new(DieselStore::new(
            &logger,
            subscriptions.cheap_clone(),
            conn_pool.clone(),
            read_only_conn_pools.clone(),
            weights,
            registry.cheap_clone(),
        ));
        let name = ShardName::new(name.to_string()).expect("shard names have been validated");
        (name, shard, conn_pool)
    }

    /// Create a connection pool for the main database of hte primary shard
    /// without connecting to all the other configured databases
    pub fn main_pool(
        logger: &Logger,
        name: &str,
        shard: &Shard,
        registry: Arc<dyn MetricsRegistry>,
    ) -> ConnectionPool {
        let logger = logger.new(o!("pool" => "main"));
        info!(
            logger,
            "Connecting to Postgres";
            "url" => SafeDisplay(shard.connection.as_str()),
            "conn_pool_size" => shard.pool_size,
            "weight" => shard.weight
        );
        ConnectionPool::create(
            name,
            "main",
            shard.connection.to_owned(),
            shard.pool_size,
            &logger,
            registry.cheap_clone(),
        )
    }

    /// Create connection pools for each of the replicas
    fn replica_pools(
        logger: &Logger,
        name: &str,
        shard: &Shard,
        registry: Arc<dyn MetricsRegistry>,
    ) -> (Vec<ConnectionPool>, Vec<usize>) {
        let mut weights: Vec<_> = vec![shard.weight];
        (
            shard
                .replicas
                .values()
                .enumerate()
                .map(|(i, replica)| {
                    let pool = &format!("replica{}", i + 1);
                    let logger = logger.new(o!("pool" => pool.clone()));
                    info!(
                        &logger,
                        "Connecting to Postgres (read replica {})", i+1;
                        "url" => SafeDisplay(replica.connection.as_str()),
                        "weight" => replica.weight
                    );
                    weights.push(replica.weight);
                    ConnectionPool::create(
                        name,
                        pool,
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
            self.chain_head_update_listener.cheap_clone(),
            self.primary_pool.clone(),
        );
        Arc::new(DieselNetworkStore::new(
            self.store.cheap_clone(),
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
        self.primary_pool.clone()
    }
}
