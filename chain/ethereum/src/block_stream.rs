use graph::{
    blockchain::Blockchain,
    components::{ethereum::ChainHeadUpdateListener, store::DeploymentLocator},
};
use graph::{
    blockchain::{
        block_stream::BlockStreamMetrics, BlockStream, BlockchainMap, TriggerFilter as _,
    },
    prelude::*,
};

use crate::network::EthereumNetworks;

lazy_static! {
    /// Maximum number of blocks to request in each chunk.
    static ref MAX_BLOCK_RANGE_SIZE: BlockNumber = std::env::var("GRAPH_ETHEREUM_MAX_BLOCK_RANGE_SIZE")
        .unwrap_or("2000".into())
        .parse::<BlockNumber>()
        .expect("invalid GRAPH_ETHEREUM_MAX_BLOCK_RANGE_SIZE");

    /// Ideal number of triggers in a range. The range size will adapt to try to meet this.
    static ref TARGET_TRIGGERS_PER_BLOCK_RANGE: u64 = std::env::var("GRAPH_ETHEREUM_TARGET_TRIGGERS_PER_BLOCK_RANGE")
        .unwrap_or("100".into())
        .parse::<u64>()
        .expect("invalid GRAPH_ETHEREUM_TARGET_TRIGGERS_PER_BLOCK_RANGE");
}

pub struct BlockStreamBuilder<C> {
    subgraph_store: Arc<dyn SubgraphStore>,
    chains: Arc<BlockchainMap<C>>,
    chain_head_update_listener: Arc<dyn ChainHeadUpdateListener>,
    eth_networks: EthereumNetworks,
    node_id: NodeId,
    reorg_threshold: BlockNumber,
    metrics_registry: Arc<dyn MetricsRegistry>,
}

impl<C> Clone for BlockStreamBuilder<C> {
    fn clone(&self) -> Self {
        BlockStreamBuilder {
            subgraph_store: self.subgraph_store.clone(),
            chains: self.chains.clone(),
            chain_head_update_listener: self.chain_head_update_listener.clone(),
            eth_networks: self.eth_networks.clone(),
            node_id: self.node_id.clone(),
            reorg_threshold: self.reorg_threshold,
            metrics_registry: self.metrics_registry.clone(),
        }
    }
}

impl<C: Blockchain> BlockStreamBuilder<C> {
    pub fn new(
        subgraph_store: Arc<dyn SubgraphStore>,
        chains: Arc<BlockchainMap<C>>,
        chain_head_update_listener: Arc<dyn ChainHeadUpdateListener>,
        eth_networks: EthereumNetworks,
        node_id: NodeId,
        reorg_threshold: BlockNumber,
        metrics_registry: Arc<dyn MetricsRegistry>,
    ) -> Self {
        BlockStreamBuilder {
            subgraph_store,
            chains,
            chain_head_update_listener,
            eth_networks,
            node_id,
            reorg_threshold,
            metrics_registry,
        }
    }

    pub fn build(
        &self,
        logger: Logger,
        deployment: DeploymentLocator,
        network_name: String,
        start_blocks: Vec<BlockNumber>,
        filter: C::TriggerFilter,
        metrics: Arc<BlockStreamMetrics>,
    ) -> BlockStream<C> {
        let logger = logger.new(o!(
            "component" => "BlockStream",
        ));

        let chain = self
            .chains
            .get(&network_name)
            .expect(&format!("no chain configured for network {}", network_name));

        let chain_store = chain.chain_store();

        let chain_head_update_stream = self
            .chain_head_update_listener
            .subscribe(network_name.clone());

        let requirements = filter.node_capabilities();

        let triggers_adapter = chain
            .triggers_adapter(&deployment, &requirements)
            .expect(&format!(
                "no adapter for network {} with capabilities {}",
                network_name, requirements
            ));
        // Create the actual subgraph-specific block stream
        BlockStream::new(
            self.subgraph_store
                .writable(&deployment)
                .expect(&format!("no store for deployment `{}`", deployment.hash)),
            chain_store,
            chain_head_update_stream,
            triggers_adapter,
            self.node_id.clone(),
            deployment.hash,
            filter,
            start_blocks,
            self.reorg_threshold,
            logger,
            metrics,
            *MAX_BLOCK_RANGE_SIZE,
            *TARGET_TRIGGERS_PER_BLOCK_RANGE,
        )
    }
}
