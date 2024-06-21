use crate::config::{Config, ProviderDetails};
use crate::network_setup::{
    AdapterConfiguration, EthAdapterConfig, FirehoseAdapterConfig, Networks,
};
use ethereum::chain::{
    EthereumAdapterSelector, EthereumBlockRefetcher, EthereumRuntimeAdapterBuilder,
    EthereumStreamBuilder,
};
use ethereum::network::EthereumNetworkAdapter;
use ethereum::ProviderEthRpcMetrics;
use graph::anyhow::bail;
use graph::blockchain::client::ChainClient;
use graph::blockchain::{
    BasicBlockchainBuilder, Blockchain, BlockchainBuilder as _, BlockchainKind, BlockchainMap,
    ChainIdentifier,
};
use graph::cheap_clone::CheapClone;
use graph::components::adapter::ChainId;
use graph::components::store::{BlockStore as _, ChainStore};
use graph::data::store::NodeId;
use graph::endpoint::EndpointMetrics;
use graph::env::{EnvVars, ENV_VARS};
use graph::firehose::{
    FirehoseEndpoint, FirehoseGenesisDecoder, GenesisDecoder, SubgraphLimit,
    SubstreamsGenesisDecoder,
};
use graph::futures03::future::try_join_all;
use graph::futures03::TryFutureExt;
use graph::ipfs_client::IpfsClient;
use graph::itertools::Itertools;
use graph::log::factory::LoggerFactory;
use graph::prelude::anyhow;
use graph::prelude::MetricsRegistry;
use graph::slog::{debug, error, info, o, Logger};
use graph::url::Url;
use graph::util::security::SafeDisplay;
use graph_chain_ethereum::{self as ethereum, Transport};
use graph_store_postgres::{BlockStore, ChainHeadUpdateListener};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

// The status of a provider that we learned from connecting to it
#[derive(PartialEq)]
pub enum ProviderNetworkStatus {
    Broken {
        chain_id: String,
        provider: String,
    },
    Version {
        chain_id: String,
        ident: ChainIdentifier,
    },
}

pub fn create_ipfs_clients(logger: &Logger, ipfs_addresses: &Vec<String>) -> Vec<IpfsClient> {
    // Parse the IPFS URL from the `--ipfs` command line argument
    let ipfs_addresses: Vec<_> = ipfs_addresses
        .iter()
        .map(|uri| {
            if uri.starts_with("http://") || uri.starts_with("https://") {
                String::from(uri)
            } else {
                format!("http://{}", uri)
            }
        })
        .collect();

    ipfs_addresses
        .into_iter()
        .map(|ipfs_address| {
            info!(
                logger,
                "Trying IPFS node at: {}",
                SafeDisplay(&ipfs_address)
            );

            let ipfs_client = match IpfsClient::new(&ipfs_address) {
                Ok(ipfs_client) => ipfs_client,
                Err(e) => {
                    error!(
                        logger,
                        "Failed to create IPFS client for `{}`: {}",
                        SafeDisplay(&ipfs_address),
                        e
                    );
                    panic!("Could not connect to IPFS");
                }
            };

            // Test the IPFS client by getting the version from the IPFS daemon
            let ipfs_test = ipfs_client.cheap_clone();
            let ipfs_ok_logger = logger.clone();
            let ipfs_err_logger = logger.clone();
            let ipfs_address_for_ok = ipfs_address.clone();
            let ipfs_address_for_err = ipfs_address;
            graph::spawn(async move {
                ipfs_test
                    .test()
                    .map_err(move |e| {
                        error!(
                            ipfs_err_logger,
                            "Is there an IPFS node running at \"{}\"?",
                            SafeDisplay(ipfs_address_for_err),
                        );
                        panic!("Failed to connect to IPFS: {}", e);
                    })
                    .map_ok(move |_| {
                        info!(
                            ipfs_ok_logger,
                            "Successfully connected to IPFS node at: {}",
                            SafeDisplay(ipfs_address_for_ok)
                        );
                    })
                    .await
            });

            ipfs_client
        })
        .collect()
}

pub fn create_substreams_networks(
    logger: Logger,
    config: &Config,
    endpoint_metrics: Arc<EndpointMetrics>,
) -> Vec<AdapterConfiguration> {
    debug!(
        logger,
        "Creating firehose networks [{} chains, ingestor {}]",
        config.chains.chains.len(),
        config.chains.ingestor,
    );

    let mut networks_by_kind: BTreeMap<(BlockchainKind, ChainId), Vec<Arc<FirehoseEndpoint>>> =
        BTreeMap::new();

    for (name, chain) in &config.chains.chains {
        let name: ChainId = name.as_str().into();
        for provider in &chain.providers {
            if let ProviderDetails::Substreams(ref firehose) = provider.details {
                info!(
                    logger,
                    "Configuring substreams endpoint";
                    "provider" => &provider.label,
                    "network" => &name.to_string(),
                );

                let parsed_networks = networks_by_kind
                    .entry((chain.protocol, name.clone()))
                    .or_insert_with(Vec::new);

                for _ in 0..firehose.conn_pool_size {
                    parsed_networks.push(Arc::new(FirehoseEndpoint::new(
                        // This label needs to be the original label so that the metrics
                        // can be deduped.
                        &provider.label,
                        &firehose.url,
                        firehose.token.clone(),
                        firehose.key.clone(),
                        firehose.filters_enabled(),
                        firehose.compression_enabled(),
                        SubgraphLimit::Unlimited,
                        endpoint_metrics.clone(),
                        Box::new(SubstreamsGenesisDecoder {}),
                    )));
                }
            }
        }
    }

    networks_by_kind
        .into_iter()
        .map(|((kind, chain_id), endpoints)| {
            AdapterConfiguration::Substreams(FirehoseAdapterConfig {
                chain_id,
                kind,
                adapters: endpoints.into(),
            })
        })
        .collect()
}

pub fn create_firehose_networks(
    logger: Logger,
    config: &Config,
    endpoint_metrics: Arc<EndpointMetrics>,
) -> Vec<AdapterConfiguration> {
    debug!(
        logger,
        "Creating firehose networks [{} chains, ingestor {}]",
        config.chains.chains.len(),
        config.chains.ingestor,
    );

    let mut networks_by_kind: BTreeMap<(BlockchainKind, ChainId), Vec<Arc<FirehoseEndpoint>>> =
        BTreeMap::new();

    for (name, chain) in &config.chains.chains {
        let name: ChainId = name.as_str().into();
        for provider in &chain.providers {
            let logger = logger.cheap_clone();
            if let ProviderDetails::Firehose(ref firehose) = provider.details {
                info!(
                    &logger,
                    "Configuring firehose endpoint";
                    "provider" => &provider.label,
                    "network" => &name.to_string(),
                );

                let parsed_networks = networks_by_kind
                    .entry((chain.protocol, name.clone()))
                    .or_insert_with(Vec::new);

                let decoder: Box<dyn GenesisDecoder> = match chain.protocol {
                    BlockchainKind::Arweave => {
                        FirehoseGenesisDecoder::<graph_chain_arweave::Block>::new(logger)
                    }
                    BlockchainKind::Ethereum => {
                        FirehoseGenesisDecoder::<graph_chain_ethereum::codec::Block>::new(logger)
                    }
                    BlockchainKind::Near => {
                        FirehoseGenesisDecoder::<graph_chain_near::HeaderOnlyBlock>::new(logger)
                    }
                    BlockchainKind::Cosmos => {
                        FirehoseGenesisDecoder::<graph_chain_cosmos::Block>::new(logger)
                    }
                    BlockchainKind::Substreams => {
                        unreachable!("Substreams configuration should not be handled here");
                    }
                    BlockchainKind::Starknet => {
                        FirehoseGenesisDecoder::<graph_chain_starknet::Block>::new(logger)
                    }
                };

                // Create n FirehoseEndpoints where n is the size of the pool. If a
                // subgraph limit is defined for this endpoint then each endpoint
                // instance will have their own subgraph limit.
                // eg: pool_size = 3 and sg_limit 2 will result in 3 separate instances
                // of FirehoseEndpoint and each of those instance can be used in 2 different
                // SubgraphInstances.
                for _ in 0..firehose.conn_pool_size {
                    parsed_networks.push(Arc::new(FirehoseEndpoint::new(
                        // This label needs to be the original label so that the metrics
                        // can be deduped.
                        &provider.label,
                        &firehose.url,
                        firehose.token.clone(),
                        firehose.key.clone(),
                        firehose.filters_enabled(),
                        firehose.compression_enabled(),
                        firehose.limit_for(&config.node),
                        endpoint_metrics.cheap_clone(),
                        decoder.box_clone(),
                    )));
                }
            }
        }
    }

    networks_by_kind
        .into_iter()
        .map(|((kind, chain_id), endpoints)| {
            AdapterConfiguration::Firehose(FirehoseAdapterConfig {
                chain_id,
                kind,
                adapters: endpoints.into(),
            })
        })
        .collect()
}

/// Parses all Ethereum connection strings and returns their network names and
/// `EthereumAdapter`.
pub async fn create_all_ethereum_networks(
    logger: Logger,
    registry: Arc<MetricsRegistry>,
    config: &Config,
    endpoint_metrics: Arc<EndpointMetrics>,
) -> anyhow::Result<Vec<AdapterConfiguration>> {
    let eth_rpc_metrics = Arc::new(ProviderEthRpcMetrics::new(registry));
    let eth_networks_futures = config
        .chains
        .chains
        .iter()
        .filter(|(_, chain)| chain.protocol == BlockchainKind::Ethereum)
        .map(|(name, _)| {
            create_ethereum_networks_for_chain(
                &logger,
                eth_rpc_metrics.clone(),
                config,
                name,
                endpoint_metrics.cheap_clone(),
            )
        });

    Ok(try_join_all(eth_networks_futures).await?)
}

/// Parses a single Ethereum connection string and returns its network name and `EthereumAdapter`.
pub async fn create_ethereum_networks_for_chain(
    logger: &Logger,
    eth_rpc_metrics: Arc<ProviderEthRpcMetrics>,
    config: &Config,
    network_name: &str,
    endpoint_metrics: Arc<EndpointMetrics>,
) -> anyhow::Result<AdapterConfiguration> {
    let chain = config
        .chains
        .chains
        .get(network_name)
        .ok_or_else(|| anyhow!("unknown network {}", network_name))?;
    let mut adapters = vec![];
    let mut call_only_adapters = vec![];

    for provider in &chain.providers {
        let (web3, call_only) = match &provider.details {
            ProviderDetails::Web3Call(web3) => (web3, true),
            ProviderDetails::Web3(web3) => (web3, false),
            _ => {
                continue;
            }
        };

        let capabilities = web3.node_capabilities();
        if call_only && !capabilities.archive {
            bail!("Ethereum call-only adapters require archive features to be enabled");
        }

        let logger = logger.new(o!("provider" => provider.label.clone()));
        info!(
            logger,
            "Creating transport";
            "url" => &web3.url,
            "capabilities" => capabilities
        );

        use crate::config::Transport::*;

        let transport = match web3.transport {
            Rpc => Transport::new_rpc(
                Url::parse(&web3.url)?,
                web3.headers.clone(),
                endpoint_metrics.cheap_clone(),
                &provider.label,
            ),
            Ipc => Transport::new_ipc(&web3.url).await,
            Ws => Transport::new_ws(&web3.url).await,
        };

        let supports_eip_1898 = !web3.features.contains("no_eip1898");
        let adapter = EthereumNetworkAdapter::new(
            endpoint_metrics.cheap_clone(),
            capabilities,
            Arc::new(
                graph_chain_ethereum::EthereumAdapter::new(
                    logger,
                    provider.label.clone(),
                    transport,
                    eth_rpc_metrics.clone(),
                    supports_eip_1898,
                    call_only,
                )
                .await,
            ),
            web3.limit_for(&config.node),
        );

        if call_only {
            call_only_adapters.push(adapter);
        } else {
            adapters.push(adapter);
        }
    }

    adapters.sort_by(|a, b| {
        a.capabilities
            .partial_cmp(&b.capabilities)
            // We can't define a total ordering over node capabilities,
            // so incomparable items are considered equal and end up
            // near each other.
            .unwrap_or(Ordering::Equal)
    });

    Ok(AdapterConfiguration::Rpc(EthAdapterConfig {
        chain_id: network_name.into(),
        adapters,
        call_only: call_only_adapters,
        polling_interval: Some(chain.polling_interval),
    }))
}

/// Networks as chains will create the necessary chains from the adapter information.
/// There are two major cases that are handled currently:
/// Deep integration chains (explicitly defined on the graph-node like Ethereum, Near, etc):
///  - These can have adapter of any type. Adapters of firehose and rpc types are used by the Chain implementation, aka deep integration
///  - The substreams adapters will trigger the creation of a Substreams chain, the priority for the block ingestor setup depends on the chain, if enabled at all.
/// Substreams Chain(chains the graph-node knows nothing about and are only accessible through substreams):
///  - This chain type is more generic and can only have adapters of substreams type.
///  - Substreams chain are created as a "secondary" chain for deep integrations but in that case the block ingestor should be run by the main/deep integration chain.
///  - These chains will use SubstreamsBlockIngestor by default.
pub async fn networks_as_chains(
    config: &Arc<EnvVars>,
    blockchain_map: &mut BlockchainMap,
    node_id: &NodeId,
    logger: &Logger,
    networks: &Networks,
    store: Arc<BlockStore>,
    logger_factory: &LoggerFactory,
    metrics_registry: Arc<MetricsRegistry>,
    chain_head_update_listener: Arc<ChainHeadUpdateListener>,
) {
    let adapters = networks
        .adapters
        .iter()
        .sorted_by_key(|a| a.chain_id())
        .chunk_by(|a| a.chain_id())
        .into_iter()
        .map(|(chain_id, adapters)| (chain_id, adapters.into_iter().collect_vec()))
        .collect_vec();

    let chains = adapters.into_iter().map(|(chain_id, adapters)| {
        let adapters: Vec<&AdapterConfiguration> = adapters.into_iter().collect();
        let kind = adapters
            .first()
            .map(|a| a.blockchain_kind())
            .expect("validation should have checked we have at least one provider");
        (chain_id, adapters, kind)
    });

    for (chain_id, adapters, kind) in chains.into_iter() {
        let chain_store = match store.chain_store(chain_id) {
            Some(c) => c,
            None => {
                let ident = networks
                    .chain_identifier(&logger, chain_id)
                    .await
                    .expect("must be able to get chain identity to create a store");
                store
                    .create_chain_store(chain_id, ident)
                    .expect("must be able to create store if one is not yet setup for the chain")
            }
        };

        async fn add_substreams<C: Blockchain>(
            networks: &Networks,
            config: &Arc<EnvVars>,
            chain_id: ChainId,
            blockchain_map: &mut BlockchainMap,
            logger_factory: LoggerFactory,
            chain_store: Arc<dyn ChainStore>,
            metrics_registry: Arc<MetricsRegistry>,
        ) {
            let substreams_endpoints = networks.substreams_endpoints(chain_id.clone());
            if substreams_endpoints.len() == 0 {
                return;
            }

            blockchain_map.insert::<graph_chain_substreams::Chain>(
                chain_id.clone(),
                Arc::new(
                    BasicBlockchainBuilder {
                        logger_factory: logger_factory.clone(),
                        name: chain_id.clone(),
                        chain_store,
                        metrics_registry: metrics_registry.clone(),
                        firehose_endpoints: substreams_endpoints,
                    }
                    .build(config)
                    .await,
                ),
            );
        }

        match kind {
            BlockchainKind::Arweave => {
                let firehose_endpoints = networks.firehose_endpoints(chain_id.clone());

                blockchain_map.insert::<graph_chain_arweave::Chain>(
                    chain_id.clone(),
                    Arc::new(
                        BasicBlockchainBuilder {
                            logger_factory: logger_factory.clone(),
                            name: chain_id.clone(),
                            chain_store: chain_store.cheap_clone(),
                            firehose_endpoints,
                            metrics_registry: metrics_registry.clone(),
                        }
                        .build(config)
                        .await,
                    ),
                );

                add_substreams::<graph_chain_arweave::Chain>(
                    networks,
                    config,
                    chain_id.clone(),
                    blockchain_map,
                    logger_factory.clone(),
                    chain_store,
                    metrics_registry.clone(),
                )
                .await;
            }
            BlockchainKind::Ethereum => {
                // polling interval is set per chain so if set all adapter configuration will have
                // the same value.
                let polling_interval = adapters
                    .first()
                    .and_then(|a| a.as_rpc().and_then(|a| a.polling_interval))
                    .unwrap_or(config.ingestor_polling_interval);

                let firehose_endpoints = networks.firehose_endpoints(chain_id.clone());
                let eth_adapters = networks.ethereum_rpcs(chain_id.clone());

                let cc = if firehose_endpoints.len() > 0 {
                    ChainClient::<graph_chain_ethereum::Chain>::new_firehose(firehose_endpoints)
                } else {
                    ChainClient::<graph_chain_ethereum::Chain>::new_rpc(eth_adapters.clone())
                };

                let client = Arc::new(cc);
                let adapter_selector = EthereumAdapterSelector::new(
                    logger_factory.clone(),
                    client.clone(),
                    metrics_registry.clone(),
                    chain_store.clone(),
                );

                let call_cache = chain_store.cheap_clone();

                let chain = ethereum::Chain::new(
                    logger_factory.clone(),
                    chain_id.clone(),
                    node_id.clone(),
                    metrics_registry.clone(),
                    chain_store.cheap_clone(),
                    call_cache,
                    client,
                    chain_head_update_listener.clone(),
                    Arc::new(EthereumStreamBuilder {}),
                    Arc::new(EthereumBlockRefetcher {}),
                    Arc::new(adapter_selector),
                    Arc::new(EthereumRuntimeAdapterBuilder {}),
                    Arc::new(eth_adapters.clone()),
                    ENV_VARS.reorg_threshold,
                    polling_interval,
                    true,
                );

                blockchain_map
                    .insert::<graph_chain_ethereum::Chain>(chain_id.clone(), Arc::new(chain));

                add_substreams::<graph_chain_ethereum::Chain>(
                    networks,
                    config,
                    chain_id.clone(),
                    blockchain_map,
                    logger_factory.clone(),
                    chain_store,
                    metrics_registry.clone(),
                )
                .await;
            }
            BlockchainKind::Near => {
                let firehose_endpoints = networks.firehose_endpoints(chain_id.clone());
                blockchain_map.insert::<graph_chain_near::Chain>(
                    chain_id.clone(),
                    Arc::new(
                        BasicBlockchainBuilder {
                            logger_factory: logger_factory.clone(),
                            name: chain_id.clone(),
                            chain_store: chain_store.cheap_clone(),
                            firehose_endpoints,
                            metrics_registry: metrics_registry.clone(),
                        }
                        .build(config)
                        .await,
                    ),
                );

                add_substreams::<graph_chain_near::Chain>(
                    networks,
                    config,
                    chain_id.clone(),
                    blockchain_map,
                    logger_factory.clone(),
                    chain_store,
                    metrics_registry.clone(),
                )
                .await;
            }
            BlockchainKind::Cosmos => {
                let firehose_endpoints = networks.firehose_endpoints(chain_id.clone());
                blockchain_map.insert::<graph_chain_cosmos::Chain>(
                    chain_id.clone(),
                    Arc::new(
                        BasicBlockchainBuilder {
                            logger_factory: logger_factory.clone(),
                            name: chain_id.clone(),
                            chain_store: chain_store.cheap_clone(),
                            firehose_endpoints,
                            metrics_registry: metrics_registry.clone(),
                        }
                        .build(config)
                        .await,
                    ),
                );
                add_substreams::<graph_chain_cosmos::Chain>(
                    networks,
                    config,
                    chain_id.clone(),
                    blockchain_map,
                    logger_factory.clone(),
                    chain_store,
                    metrics_registry.clone(),
                )
                .await;
            }
            BlockchainKind::Starknet => {
                let firehose_endpoints = networks.firehose_endpoints(chain_id.clone());
                blockchain_map.insert::<graph_chain_starknet::Chain>(
                    chain_id.clone(),
                    Arc::new(
                        BasicBlockchainBuilder {
                            logger_factory: logger_factory.clone(),
                            name: chain_id.clone(),
                            chain_store: chain_store.cheap_clone(),
                            firehose_endpoints,
                            metrics_registry: metrics_registry.clone(),
                        }
                        .build(config)
                        .await,
                    ),
                );
                add_substreams::<graph_chain_starknet::Chain>(
                    networks,
                    config,
                    chain_id.clone(),
                    blockchain_map,
                    logger_factory.clone(),
                    chain_store,
                    metrics_registry.clone(),
                )
                .await;
            }
            BlockchainKind::Substreams => {
                let substreams_endpoints = networks.substreams_endpoints(chain_id.clone());
                blockchain_map.insert::<graph_chain_substreams::Chain>(
                    chain_id.clone(),
                    Arc::new(
                        BasicBlockchainBuilder {
                            logger_factory: logger_factory.clone(),
                            name: chain_id.clone(),
                            chain_store,
                            metrics_registry: metrics_registry.clone(),
                            firehose_endpoints: substreams_endpoints,
                        }
                        .build(config)
                        .await,
                    ),
                );
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::config::{Config, Opt};
    use crate::network_setup::{AdapterConfiguration, Networks};
    use graph::components::adapter::{ChainId, MockIdentValidator};
    use graph::endpoint::EndpointMetrics;
    use graph::log::logger;
    use graph::prelude::{tokio, MetricsRegistry};
    use graph_chain_ethereum::NodeCapabilities;
    use std::sync::Arc;

    #[tokio::test]
    async fn correctly_parse_ethereum_networks() {
        let logger = logger(true);

        let network_args = vec![
            "mainnet:traces:http://localhost:8545/".to_string(),
            "goerli:archive:http://localhost:8546/".to_string(),
        ];

        let opt = Opt {
            postgres_url: Some("not needed".to_string()),
            config: None,
            store_connection_pool_size: 5,
            postgres_secondary_hosts: vec![],
            postgres_host_weights: vec![],
            disable_block_ingestor: true,
            node_id: "default".to_string(),
            ethereum_rpc: network_args,
            ethereum_ws: vec![],
            ethereum_ipc: vec![],
            unsafe_config: false,
        };

        let metrics = Arc::new(EndpointMetrics::mock());
        let config = Config::load(&logger, &opt).expect("can create config");
        let metrics_registry = Arc::new(MetricsRegistry::mock());
        let ident_validator = Arc::new(MockIdentValidator);

        let networks =
            Networks::from_config(logger, &config, metrics_registry, metrics, ident_validator)
                .await
                .expect("can parse config");
        let mut network_names = networks
            .adapters
            .iter()
            .map(|a| a.chain_id())
            .collect::<Vec<&ChainId>>();
        network_names.sort();

        let traces = NodeCapabilities {
            archive: false,
            traces: true,
        };
        let archive = NodeCapabilities {
            archive: true,
            traces: false,
        };

        let mainnet: Vec<&AdapterConfiguration> = networks
            .adapters
            .iter()
            .filter(|a| a.chain_id().as_str().eq("mainnet"))
            .collect();
        assert_eq!(mainnet.len(), 1);
        let mainnet = mainnet.first().unwrap().as_rpc().unwrap();
        assert_eq!(mainnet.adapters.len(), 1);
        let mainnet = mainnet.adapters.first().unwrap();
        assert_eq!(mainnet.capabilities, traces);

        let goerli: Vec<&AdapterConfiguration> = networks
            .adapters
            .iter()
            .filter(|a| a.chain_id().as_str().eq("goerli"))
            .collect();
        assert_eq!(goerli.len(), 1);
        let goerli = goerli.first().unwrap().as_rpc().unwrap();
        assert_eq!(goerli.adapters.len(), 1);
        let goerli = goerli.adapters.first().unwrap();
        assert_eq!(goerli.capabilities, archive);
    }
}
