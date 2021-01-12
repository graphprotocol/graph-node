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
    NetworkStore as DieselNetworkStore, Shard as ShardName, Store as DieselStore, SubgraphStore,
    SubscriptionManager, PRIMARY_SHARD,
};

use crate::config::{Config, Shard};

pub struct StoreBuilder {
    store: Arc<SubgraphStore>,
    primary_pool: ConnectionPool,
    chain_head_update_listener: Arc<PostgresChainHeadUpdateListener>,
    subscription_manager: Arc<SubscriptionManager>,
}

impl StoreBuilder {
    pub fn new(logger: &Logger, config: &Config, registry: Arc<dyn MetricsRegistry>) -> Self {
        let primary = config.primary_store();

        let subscription_manager = Arc::new(SubscriptionManager::new(
            logger.cheap_clone(),
            primary.connection.to_owned(),
        ));

        let (store, primary_pool) =
            Self::make_sharded_store_and_primary_pool(logger, config, registry.cheap_clone());

        let chain_head_update_listener = Arc::new(PostgresChainHeadUpdateListener::new(
            &logger,
            registry.cheap_clone(),
            primary.connection.to_owned(),
        ));

        Self {
            store,
            primary_pool,
            chain_head_update_listener,
            subscription_manager,
        }
    }

    /// Make a `ShardedStore` across all configured shards, and also return
    /// the connection pool for the primary shard
    fn make_sharded_store_and_primary_pool(
        logger: &Logger,
        config: &Config,
        registry: Arc<dyn MetricsRegistry>,
    ) -> (Arc<SubgraphStore>, ConnectionPool) {
        let shards: Vec<_> = config
            .stores
            .iter()
            .map(|(name, shard)| Self::make_shard(logger, name, shard, registry.cheap_clone()))
            .collect();

        let primary_pool = shards
            .iter()
            .find(|(name, _, _)| name.as_str() == PRIMARY_SHARD.as_str())
            .unwrap()
            .2
            .clone();

        let shard_map =
            HashMap::from_iter(shards.into_iter().map(|(name, shard, _)| (name, shard)));

        let store = Arc::new(SubgraphStore::new(
            shard_map,
            Arc::new(config.deployment.clone()),
        ));

        (store, primary_pool)
    }

    // Somehow, rustc gets this wrong; the function is used in
    // `manager::make_store`
    #[allow(dead_code)]
    pub fn make_sharded_store(
        logger: &Logger,
        config: &Config,
        registry: Arc<dyn MetricsRegistry>,
    ) -> Arc<SubgraphStore> {
        Self::make_sharded_store_and_primary_pool(logger, config, registry).0
    }

    fn make_shard(
        logger: &Logger,
        name: &str,
        shard: &Shard,
        registry: Arc<dyn MetricsRegistry>,
    ) -> (ShardName, Arc<DieselStore>, ConnectionPool) {
        let logger = logger.new(o!("shard" => name.to_string()));
        let conn_pool = Self::main_pool(&logger, name, shard, registry.cheap_clone());

        let (read_only_conn_pools, weights) =
            Self::replica_pools(&logger, name, shard, registry.cheap_clone());

        let shard = Arc::new(DieselStore::new(
            &logger,
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
    pub fn store(&self) -> Arc<SubgraphStore> {
        self.store.cheap_clone()
    }

    pub fn subscription_manager(&self) -> Arc<SubscriptionManager> {
        self.subscription_manager.cheap_clone()
    }

    // This is used in the test-store, but rustc keeps complaining that it
    // is not used
    #[cfg(debug_assertions)]
    #[allow(dead_code)]
    pub fn primary_pool(&self) -> ConnectionPool {
        self.primary_pool.clone()
    }
}
