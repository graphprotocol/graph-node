use std::collections::HashMap;
use std::sync::Arc;

use super::config::{self, Config as Cfg};
use super::store_builder::StoreBuilder;
use super::utils::PanicSubscriptionManager;
use graph::endpoint::EndpointMetrics;
use graph::{data::graphql::effort::LoadManager, prometheus::Registry};
use graph::{
    prelude::{Logger, MetricsRegistry, NodeId},
    url::Url,
};

use graph::prelude::anyhow::{self};
use graph_chain_ethereum::{EthereumAdapter, EthereumNetworks};
use graph_graphql::prelude::GraphQlRunner;
use graph_store_postgres::connection_pool::{ConnectionPool, PoolCoordinator};
use graph_store_postgres::{
    BlockStore, ChainStore, NotificationSender, Shard, Store, SubgraphStore, SubscriptionManager,
    PRIMARY_SHARD,
};

use super::chain::create_all_ethereum_networks;

const VERSION_LABEL_KEY: &str = "version";

/// Utilities to interact mostly with the store and build the parts of the
/// store we need for specific commands
pub struct GraphmanContext {
    pub logger: Logger,
    pub node_id: NodeId,
    pub config: Cfg,
    pub ipfs_url: Vec<String>,
    pub fork_base: Option<Url>,
    pub registry: Arc<MetricsRegistry>,
    pub prometheus_registry: Arc<Registry>,
}

impl GraphmanContext {
    pub fn new(
        logger: Logger,
        node_id: NodeId,
        config: Cfg,
        ipfs_url: Vec<String>,
        fork_base: Option<Url>,
        version_label: Option<String>,
    ) -> Self {
        let prometheus_registry = Arc::new(
            Registry::new_custom(
                None,
                version_label.map(|label| {
                    let mut m = HashMap::<String, String>::new();
                    m.insert(VERSION_LABEL_KEY.into(), label);
                    m
                }),
            )
            .expect("unable to build prometheus registry"),
        );

        let registry = Arc::new(MetricsRegistry::new(
            logger.clone(),
            prometheus_registry.clone(),
        ));

        Self {
            logger,
            node_id,
            config,
            ipfs_url,
            fork_base,
            registry,
            prometheus_registry,
        }
    }

    pub fn metrics_registry(&self) -> Arc<MetricsRegistry> {
        self.registry.clone()
    }

    pub fn config(&self) -> Cfg {
        self.config.clone()
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id.clone()
    }

    pub fn notification_sender(&self) -> Arc<NotificationSender> {
        Arc::new(NotificationSender::new(self.registry.clone()))
    }

    pub fn primary_pool(self) -> ConnectionPool {
        let primary = self.config.primary_store();
        let coord = Arc::new(PoolCoordinator::new(Arc::new(vec![])));
        let pool = StoreBuilder::main_pool(
            &self.logger,
            &self.node_id,
            PRIMARY_SHARD.as_str(),
            primary,
            self.metrics_registry(),
            coord,
        );
        pool.skip_setup();
        pool
    }

    pub fn subgraph_store(self) -> Arc<SubgraphStore> {
        self.store_and_pools().0.subgraph_store()
    }

    pub fn subscription_manager(&self) -> Arc<SubscriptionManager> {
        let primary = self.config.primary_store();

        Arc::new(SubscriptionManager::new(
            self.logger.clone(),
            primary.connection.clone(),
            self.registry.clone(),
        ))
    }

    pub fn primary_and_subscription_manager(self) -> (ConnectionPool, Arc<SubscriptionManager>) {
        let mgr = self.subscription_manager();
        let primary_pool = self.primary_pool();

        (primary_pool, mgr)
    }

    pub fn store(self) -> Arc<Store> {
        let (store, _) = self.store_and_pools();
        store
    }

    pub fn pools(self) -> HashMap<Shard, ConnectionPool> {
        let (_, pools) = self.store_and_pools();
        pools
    }

    pub async fn store_builder(&self) -> StoreBuilder {
        StoreBuilder::new(
            &self.logger,
            &self.node_id,
            &self.config,
            self.fork_base.clone(),
            self.registry.clone(),
        )
        .await
    }

    pub fn store_and_pools(self) -> (Arc<Store>, HashMap<Shard, ConnectionPool>) {
        let (subgraph_store, pools, _) = StoreBuilder::make_subgraph_store_and_pools(
            &self.logger,
            &self.node_id,
            &self.config,
            self.fork_base,
            self.registry.clone(),
        );

        for pool in pools.values() {
            pool.skip_setup();
        }

        let store = StoreBuilder::make_store(
            &self.logger,
            pools.clone(),
            subgraph_store,
            HashMap::default(),
            vec![],
            self.registry,
        );

        (store, pools)
    }

    pub fn store_and_primary(self) -> (Arc<Store>, ConnectionPool) {
        let (store, pools) = self.store_and_pools();
        let primary = pools.get(&*PRIMARY_SHARD).expect("there is a primary pool");
        (store, primary.clone())
    }

    pub fn block_store_and_primary_pool(self) -> (Arc<BlockStore>, ConnectionPool) {
        let (store, pools) = self.store_and_pools();

        let primary = pools.get(&*PRIMARY_SHARD).unwrap();
        (store.block_store(), primary.clone())
    }

    pub fn graphql_runner(self) -> Arc<GraphQlRunner<Store, PanicSubscriptionManager>> {
        let logger = self.logger.clone();
        let registry = self.registry.clone();

        let store = self.store();

        let subscription_manager = Arc::new(PanicSubscriptionManager);
        let load_manager = Arc::new(LoadManager::new(&logger, vec![], registry.clone()));

        Arc::new(GraphQlRunner::new(
            &logger,
            store,
            subscription_manager,
            load_manager,
            registry,
        ))
    }

    pub async fn ethereum_networks(&self) -> anyhow::Result<EthereumNetworks> {
        let logger = self.logger.clone();
        let registry = self.metrics_registry();
        let metrics = Arc::new(EndpointMetrics::mock());
        create_all_ethereum_networks(logger, registry, &self.config, metrics).await
    }

    pub fn chain_store(self, chain_name: &str) -> anyhow::Result<Arc<ChainStore>> {
        use graph::components::store::BlockStore;
        self.store()
            .block_store()
            .chain_store(chain_name)
            .ok_or_else(|| anyhow::anyhow!("Could not find a network named '{}'", chain_name))
    }

    pub async fn chain_store_and_adapter(
        self,
        chain_name: &str,
    ) -> anyhow::Result<(Arc<ChainStore>, Arc<EthereumAdapter>)> {
        let ethereum_networks = self.ethereum_networks().await?;
        let chain_store = self.chain_store(chain_name)?;
        let ethereum_adapter = ethereum_networks
            .networks
            .get(chain_name)
            .and_then(|adapters| adapters.cheapest())
            .ok_or(anyhow::anyhow!(
                "Failed to obtain an Ethereum adapter for chain '{}'",
                chain_name
            ))?;
        Ok((chain_store, ethereum_adapter))
    }
}
