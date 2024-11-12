use ethereum::{
    network::{EthereumNetworkAdapter, EthereumNetworkAdapters},
    BlockIngestor,
};
use graph::components::network_provider::ChainName;
use graph::components::network_provider::NetworkDetails;
use graph::components::network_provider::ProviderCheck;
use graph::components::network_provider::ProviderCheckStrategy;
use graph::components::network_provider::ProviderManager;
use graph::{
    anyhow::{self, bail},
    blockchain::{Blockchain, BlockchainKind, BlockchainMap, ChainIdentifier},
    cheap_clone::CheapClone,
    components::metrics::MetricsRegistry,
    endpoint::EndpointMetrics,
    env::EnvVars,
    firehose::{FirehoseEndpoint, FirehoseEndpoints},
    futures03::future::TryFutureExt,
    itertools::Itertools,
    log::factory::LoggerFactory,
    prelude::{
        anyhow::{anyhow, Result},
        info, Logger, NodeId,
    },
    slog::{o, warn, Discard},
};
use graph_chain_ethereum as ethereum;
use graph_store_postgres::{BlockStore, ChainHeadUpdateListener};

use std::{any::Any, cmp::Ordering, sync::Arc, time::Duration};

use crate::chain::{
    create_all_ethereum_networks, create_firehose_networks, create_substreams_networks,
    networks_as_chains,
};

#[derive(Debug, Clone)]
pub struct EthAdapterConfig {
    pub chain_id: ChainName,
    pub adapters: Vec<EthereumNetworkAdapter>,
    pub call_only: Vec<EthereumNetworkAdapter>,
    // polling interval is set per chain so if set all adapter configuration will have
    // the same value.
    pub polling_interval: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct FirehoseAdapterConfig {
    pub chain_id: ChainName,
    pub kind: BlockchainKind,
    pub adapters: Vec<Arc<FirehoseEndpoint>>,
}

#[derive(Debug, Clone)]
pub enum AdapterConfiguration {
    Rpc(EthAdapterConfig),
    Firehose(FirehoseAdapterConfig),
    Substreams(FirehoseAdapterConfig),
}

impl AdapterConfiguration {
    pub fn blockchain_kind(&self) -> &BlockchainKind {
        match self {
            AdapterConfiguration::Rpc(_) => &BlockchainKind::Ethereum,
            AdapterConfiguration::Firehose(fh) | AdapterConfiguration::Substreams(fh) => &fh.kind,
        }
    }
    pub fn chain_id(&self) -> &ChainName {
        match self {
            AdapterConfiguration::Rpc(EthAdapterConfig { chain_id, .. })
            | AdapterConfiguration::Firehose(FirehoseAdapterConfig { chain_id, .. })
            | AdapterConfiguration::Substreams(FirehoseAdapterConfig { chain_id, .. }) => chain_id,
        }
    }

    pub fn as_rpc(&self) -> Option<&EthAdapterConfig> {
        match self {
            AdapterConfiguration::Rpc(rpc) => Some(rpc),
            _ => None,
        }
    }

    pub fn as_firehose(&self) -> Option<&FirehoseAdapterConfig> {
        match self {
            AdapterConfiguration::Firehose(fh) => Some(fh),
            _ => None,
        }
    }

    pub fn is_firehose(&self) -> bool {
        self.as_firehose().is_none()
    }

    pub fn as_substreams(&self) -> Option<&FirehoseAdapterConfig> {
        match self {
            AdapterConfiguration::Substreams(fh) => Some(fh),
            _ => None,
        }
    }

    pub fn is_substreams(&self) -> bool {
        self.as_substreams().is_none()
    }
}

pub struct Networks {
    pub adapters: Vec<AdapterConfiguration>,
    pub rpc_provider_manager: ProviderManager<EthereumNetworkAdapter>,
    pub firehose_provider_manager: ProviderManager<Arc<FirehoseEndpoint>>,
    pub substreams_provider_manager: ProviderManager<Arc<FirehoseEndpoint>>,
}

impl Networks {
    // noop is important for query_nodes as it shortcuts a lot of the process.
    fn noop() -> Self {
        Self {
            adapters: vec![],
            rpc_provider_manager: ProviderManager::new(
                Logger::root(Discard, o!()),
                vec![].into_iter(),
                ProviderCheckStrategy::MarkAsValid,
            ),
            firehose_provider_manager: ProviderManager::new(
                Logger::root(Discard, o!()),
                vec![].into_iter(),
                ProviderCheckStrategy::MarkAsValid,
            ),
            substreams_provider_manager: ProviderManager::new(
                Logger::root(Discard, o!()),
                vec![].into_iter(),
                ProviderCheckStrategy::MarkAsValid,
            ),
        }
    }

    pub async fn chain_identifier(
        &self,
        logger: &Logger,
        chain_id: &ChainName,
    ) -> Result<ChainIdentifier> {
        async fn get_identifier<T: NetworkDetails + Clone>(
            pm: ProviderManager<T>,
            logger: &Logger,
            chain_id: &ChainName,
            provider_type: &str,
        ) -> Result<ChainIdentifier> {
            for adapter in pm.providers_unchecked(chain_id) {
                match adapter.chain_identifier().await {
                    Ok(ident) => return Ok(ident),
                    Err(err) => {
                        warn!(
                        logger,
                        "unable to get chain identification from {} provider {} for chain {}, err: {}",
                        provider_type,
                        adapter.provider_name(),
                        chain_id,
                        err.to_string(),
                    );
                    }
                }
            }

            bail!("no working adapters for chain {}", chain_id);
        }

        get_identifier(self.rpc_provider_manager.clone(), logger, chain_id, "rpc")
            .or_else(|_| {
                get_identifier(
                    self.firehose_provider_manager.clone(),
                    logger,
                    chain_id,
                    "firehose",
                )
            })
            .or_else(|_| {
                get_identifier(
                    self.substreams_provider_manager.clone(),
                    logger,
                    chain_id,
                    "substreams",
                )
            })
            .await
    }

    pub async fn from_config(
        logger: Logger,
        config: &crate::config::Config,
        registry: Arc<MetricsRegistry>,
        endpoint_metrics: Arc<EndpointMetrics>,
        provider_checks: &[Arc<dyn ProviderCheck>],
    ) -> Result<Networks> {
        if config.query_only(&config.node) {
            return Ok(Networks::noop());
        }

        let eth = create_all_ethereum_networks(
            logger.cheap_clone(),
            registry,
            &config,
            endpoint_metrics.cheap_clone(),
        )
        .await?;
        let firehose = create_firehose_networks(
            logger.cheap_clone(),
            &config,
            endpoint_metrics.cheap_clone(),
        );
        let substreams =
            create_substreams_networks(logger.cheap_clone(), &config, endpoint_metrics);
        let adapters: Vec<_> = eth
            .into_iter()
            .chain(firehose.into_iter())
            .chain(substreams.into_iter())
            .collect();

        Ok(Networks::new(&logger, adapters, provider_checks))
    }

    fn new(
        logger: &Logger,
        adapters: Vec<AdapterConfiguration>,
        provider_checks: &[Arc<dyn ProviderCheck>],
    ) -> Self {
        let adapters2 = adapters.clone();
        let eth_adapters = adapters.iter().flat_map(|a| a.as_rpc()).cloned().map(
            |EthAdapterConfig {
                 chain_id,
                 mut adapters,
                 call_only: _,
                 polling_interval: _,
             }| {
                adapters.sort_by(|a, b| {
                    a.capabilities
                        .partial_cmp(&b.capabilities)
                        .unwrap_or(Ordering::Equal)
                });

                (chain_id, adapters)
            },
        );

        let firehose_adapters = adapters
            .iter()
            .flat_map(|a| a.as_firehose())
            .cloned()
            .map(
                |FirehoseAdapterConfig {
                     chain_id,
                     kind: _,
                     adapters,
                 }| { (chain_id, adapters) },
            )
            .collect_vec();

        let substreams_adapters = adapters
            .iter()
            .flat_map(|a| a.as_substreams())
            .cloned()
            .map(
                |FirehoseAdapterConfig {
                     chain_id,
                     kind: _,
                     adapters,
                 }| { (chain_id, adapters) },
            )
            .collect_vec();

        let s = Self {
            adapters: adapters2,
            rpc_provider_manager: ProviderManager::new(
                logger.clone(),
                eth_adapters,
                ProviderCheckStrategy::RequireAll(provider_checks),
            ),
            firehose_provider_manager: ProviderManager::new(
                logger.clone(),
                firehose_adapters
                    .into_iter()
                    .map(|(chain_id, endpoints)| (chain_id, endpoints)),
                ProviderCheckStrategy::RequireAll(provider_checks),
            ),
            substreams_provider_manager: ProviderManager::new(
                logger.clone(),
                substreams_adapters
                    .into_iter()
                    .map(|(chain_id, endpoints)| (chain_id, endpoints)),
                ProviderCheckStrategy::RequireAll(provider_checks),
            ),
        };

        s
    }

    pub async fn block_ingestors(
        logger: &Logger,
        blockchain_map: &Arc<BlockchainMap>,
    ) -> anyhow::Result<Vec<Box<dyn BlockIngestor>>> {
        async fn block_ingestor<C: Blockchain>(
            logger: &Logger,
            chain_id: &ChainName,
            chain: &Arc<dyn Any + Send + Sync>,
            ingestors: &mut Vec<Box<dyn BlockIngestor>>,
        ) -> anyhow::Result<()> {
            let chain: Arc<C> = chain.cheap_clone().downcast().map_err(|_| {
                anyhow!("unable to downcast, wrong type for blockchain {}", C::KIND)
            })?;

            let logger = logger.new(o!("network_name" => chain_id.to_string()));

            match chain.block_ingestor().await {
                Ok(ingestor) => {
                    info!(&logger, "Creating block ingestor");
                    ingestors.push(ingestor)
                }
                Err(err) => graph::slog::error!(
                    &logger,
                    "unable to create block_ingestor for {}: {}",
                    chain_id,
                    err.to_string()
                ),
            }

            Ok(())
        }

        let mut res = vec![];
        for ((kind, id), chain) in blockchain_map.iter() {
            match kind {
                BlockchainKind::Arweave => {
                    block_ingestor::<graph_chain_arweave::Chain>(logger, id, chain, &mut res)
                        .await?
                }
                BlockchainKind::Ethereum => {
                    block_ingestor::<graph_chain_ethereum::Chain>(logger, id, chain, &mut res)
                        .await?
                }
                BlockchainKind::Near => {
                    block_ingestor::<graph_chain_near::Chain>(logger, id, chain, &mut res).await?
                }
                BlockchainKind::Cosmos => {
                    block_ingestor::<graph_chain_cosmos::Chain>(logger, id, chain, &mut res).await?
                }
                BlockchainKind::Substreams => {}
                BlockchainKind::Starknet => {
                    block_ingestor::<graph_chain_starknet::Chain>(logger, id, chain, &mut res)
                        .await?
                }
            }
        }

        // substreams networks that also have other types of chain(rpc or firehose), will have
        // block ingestors already running.
        let visited: Vec<_> = res.iter().map(|b| b.network_name()).collect();

        for ((_, id), chain) in blockchain_map
            .iter()
            .filter(|((kind, id), _)| BlockchainKind::Substreams.eq(&kind) && !visited.contains(id))
        {
            block_ingestor::<graph_chain_substreams::Chain>(logger, id, chain, &mut res).await?
        }

        Ok(res)
    }

    pub async fn blockchain_map(
        &self,
        config: &Arc<EnvVars>,
        node_id: &NodeId,
        logger: &Logger,
        store: Arc<BlockStore>,
        logger_factory: &LoggerFactory,
        metrics_registry: Arc<MetricsRegistry>,
        chain_head_update_listener: Arc<ChainHeadUpdateListener>,
    ) -> BlockchainMap {
        let mut bm = BlockchainMap::new();

        networks_as_chains(
            config,
            &mut bm,
            node_id,
            logger,
            self,
            store,
            logger_factory,
            metrics_registry,
            chain_head_update_listener,
        )
        .await;

        bm
    }

    pub fn firehose_endpoints(&self, chain_id: ChainName) -> FirehoseEndpoints {
        FirehoseEndpoints::new(chain_id, self.firehose_provider_manager.clone())
    }

    pub fn substreams_endpoints(&self, chain_id: ChainName) -> FirehoseEndpoints {
        FirehoseEndpoints::new(chain_id, self.substreams_provider_manager.clone())
    }

    pub fn ethereum_rpcs(&self, chain_id: ChainName) -> EthereumNetworkAdapters {
        let eth_adapters = self
            .adapters
            .iter()
            .filter(|a| a.chain_id().eq(&chain_id))
            .flat_map(|a| a.as_rpc())
            .flat_map(|eth_c| eth_c.call_only.clone())
            .collect_vec();

        EthereumNetworkAdapters::new(
            chain_id,
            self.rpc_provider_manager.clone(),
            eth_adapters,
            None,
        )
    }
}
