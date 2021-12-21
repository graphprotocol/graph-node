use crate::config::{Config, ProviderDetails};
use ethereum::{EthereumNetworks, ProviderEthRpcMetrics};
use futures::future::join_all;
use futures::TryFutureExt;
use graph::anyhow::Error;
use graph::blockchain::{Block as BlockchainBlock, BlockchainKind, ChainIdentifier};
use graph::cheap_clone::CheapClone;
use graph::firehose::endpoints::{FirehoseEndpoint, FirehoseNetworks};
use graph::ipfs_client::IpfsClient;
use graph::prelude::{anyhow, tokio, BlockNumber};
use graph::prelude::{prost, MetricsRegistry as MetricsRegistryTrait};
use graph::slog::{debug, error, info, o, Logger};
use graph::util::security::SafeDisplay;
use graph_chain_ethereum::{self as ethereum, EthereumAdapterTrait, Transport};
use graph_core::MetricsRegistry;
use lazy_static::lazy_static;
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

// The status of a provider that we learned from connecting to it
#[derive(PartialEq)]
enum ProviderNetworkStatus {
    Broken {
        network: String,
        provider: String,
    },
    Version {
        network: String,
        ident: ChainIdentifier,
    },
}

/// How long we will hold up node startup to get the net version and genesis
/// hash from the client. If we can't get it within that time, we'll try and
/// continue regardless.
const NET_VERSION_WAIT_TIME: Duration = Duration::from_secs(30);

lazy_static! {
    // Default to an Ethereum reorg threshold to 50 blocks
    pub static ref REORG_THRESHOLD: BlockNumber = env::var("ETHEREUM_REORG_THRESHOLD")
        .ok()
        .map(|s| BlockNumber::from_str(&s)
            .unwrap_or_else(|_| panic!("failed to parse env var ETHEREUM_REORG_THRESHOLD")))
        .unwrap_or(50);

    // Default to an ancestor count of 50 blocks
    pub static ref ANCESTOR_COUNT: BlockNumber = env::var("ETHEREUM_ANCESTOR_COUNT")
        .ok()
        .map(|s| BlockNumber::from_str(&s)
             .unwrap_or_else(|_| panic!("failed to parse env var ETHEREUM_ANCESTOR_COUNT")))
        .unwrap_or(50);
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
            let ipfs_address_for_err = ipfs_address.clone();
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

/// Parses an Ethereum connection string and returns the network name and Ethereum adapter.
pub async fn create_ethereum_networks(
    logger: Logger,
    registry: Arc<MetricsRegistry>,
    config: &Config,
) -> Result<EthereumNetworks, anyhow::Error> {
    let eth_rpc_metrics = Arc::new(ProviderEthRpcMetrics::new(registry));
    let mut parsed_networks = EthereumNetworks::new();
    for (name, chain) in &config.chains.chains {
        if chain.protocol != BlockchainKind::Ethereum {
            continue;
        }

        for provider in &chain.providers {
            if let ProviderDetails::Web3(web3) = &provider.details {
                let capabilities = web3.node_capabilities();

                let logger = logger.new(o!("provider" => provider.label.clone()));
                info!(
                    logger,
                    "Creating transport";
                    "url" => &web3.url,
                    "capabilities" => capabilities
                );

                use crate::config::Transport::*;

                let transport = match web3.transport {
                    Rpc => Transport::new_rpc(&web3.url, web3.headers.clone()),
                    Ipc => Transport::new_ipc(&web3.url).await,
                    Ws => Transport::new_ws(&web3.url).await,
                };

                let supports_eip_1898 = !web3.features.contains("no_eip1898");

                parsed_networks.insert(
                    name.to_string(),
                    capabilities,
                    Arc::new(
                        graph_chain_ethereum::EthereumAdapter::new(
                            logger,
                            provider.label.clone(),
                            &web3.url,
                            transport,
                            eth_rpc_metrics.clone(),
                            supports_eip_1898,
                        )
                        .await,
                    ),
                );
            }
        }
    }
    parsed_networks.sort();
    Ok(parsed_networks)
}

pub async fn create_firehose_networks(
    logger: Logger,
    _registry: Arc<dyn MetricsRegistryTrait>,
    config: &Config,
) -> Result<BTreeMap<BlockchainKind, FirehoseNetworks>, anyhow::Error> {
    debug!(
        logger,
        "Creating firehose networks [{} chains, ingestor {}]",
        config.chains.chains.len(),
        config.chains.ingestor,
    );

    let mut networks_by_kind = BTreeMap::new();

    for (name, chain) in &config.chains.chains {
        for provider in &chain.providers {
            if let ProviderDetails::Firehose(ref firehose) = provider.details {
                let logger = logger.new(o!("provider" => provider.label.clone()));
                info!(
                    logger,
                    "Creating firehose endpoint";
                    "url" => &firehose.url,
                );

                let endpoint = FirehoseEndpoint::new(
                    logger,
                    &provider.label,
                    &firehose.url,
                    firehose.token.clone(),
                )
                .await?;

                let parsed_networks = networks_by_kind
                    .entry(chain.protocol)
                    .or_insert_with(|| FirehoseNetworks::new());
                parsed_networks.insert(name.to_string(), Arc::new(endpoint));
            }
        }
    }

    Ok(networks_by_kind)
}

/// Try to connect to all the providers in `eth_networks` and get their net
/// version and genesis block. Return the same `eth_networks` and the
/// retrieved net identifiers grouped by network name. Remove all providers
/// for which trying to connect resulted in an error from the returned
/// `EthereumNetworks`, since it's likely pointless to try and connect to
/// them. If the connection attempt to a provider times out after
/// `NET_VERSION_WAIT_TIME`, keep the provider, but don't report a
/// version for it.
pub async fn connect_ethereum_networks(
    logger: &Logger,
    mut eth_networks: EthereumNetworks,
) -> (EthereumNetworks, Vec<(String, Vec<ChainIdentifier>)>) {
    // This has one entry for each provider, and therefore multiple entries
    // for each network
    let statuses = join_all(
        eth_networks
            .flatten()
            .into_iter()
            .map(|(network_name, capabilities, eth_adapter)| {
                (network_name, capabilities, eth_adapter, logger.clone())
            })
            .map(|(network, capabilities, eth_adapter, logger)| async move {
                let logger = logger.new(o!("provider" => eth_adapter.provider().to_string()));
                info!(
                    logger, "Connecting to Ethereum to get network identifier";
                    "capabilities" => &capabilities
                );
                match tokio::time::timeout(NET_VERSION_WAIT_TIME, eth_adapter.net_identifiers())
                    .await
                    .map_err(Error::from)
                {
                    // An `Err` means a timeout, an `Ok(Err)` means some other error (maybe a typo
                    // on the URL)
                    Ok(Err(e)) | Err(e) => {
                        error!(logger, "Connection to provider failed. Not using this provider";
                                       "error" =>  e.to_string());
                        ProviderNetworkStatus::Broken {
                            network,
                            provider: eth_adapter.provider().to_string(),
                        }
                    }
                    Ok(Ok(ident)) => {
                        info!(
                            logger,
                            "Connected to Ethereum";
                            "network_version" => &ident.net_version,
                            "capabilities" => &capabilities
                        );
                        ProviderNetworkStatus::Version { network, ident }
                    }
                }
            }),
    )
    .await;

    // Group identifiers by network name
    let idents: HashMap<String, Vec<ChainIdentifier>> =
        statuses
            .into_iter()
            .fold(HashMap::new(), |mut networks, status| {
                match status {
                    ProviderNetworkStatus::Broken { network, provider } => {
                        eth_networks.remove(&network, &provider)
                    }
                    ProviderNetworkStatus::Version { network, ident } => {
                        networks.entry(network.to_string()).or_default().push(ident)
                    }
                }
                networks
            });
    let idents: Vec<_> = idents.into_iter().collect();
    (eth_networks, idents)
}

/// Try to connect to all the providers in `firehose_networks` and get their net
/// version and genesis block. Return the same `eth_networks` and the
/// retrieved net identifiers grouped by network name. Remove all providers
/// for which trying to connect resulted in an error from the returned
/// `EthereumNetworks`, since it's likely pointless to try and connect to
/// them. If the connection attempt to a provider times out after
/// `NET_VERSION_WAIT_TIME`, keep the provider, but don't report a
/// version for it.
pub async fn connect_firehose_networks<M>(
    logger: &Logger,
    mut firehose_networks: FirehoseNetworks,
) -> (FirehoseNetworks, Vec<(String, Vec<ChainIdentifier>)>)
where
    M: prost::Message + BlockchainBlock + Default + 'static,
{
    // This has one entry for each provider, and therefore multiple entries
    // for each network
    let statuses = join_all(
        firehose_networks
            .flatten()
            .into_iter()
            .map(|(network_name, endpoint)| (network_name, endpoint, logger.clone()))
            .map(|(network, endpoint, logger)| async move {
                let logger = logger.new(o!("provider" => endpoint.provider.to_string()));
                info!(
                    logger, "Connecting to Firehose to get network identifier";
                    "url" => &endpoint.uri,
                );
                match tokio::time::timeout(
                    NET_VERSION_WAIT_TIME,
                    endpoint.genesis_block_ptr::<M>(&logger),
                )
                .await
                .map_err(Error::from)
                {
                    // An `Err` means a timeout, an `Ok(Err)` means some other error (maybe a typo
                    // on the URL)
                    Ok(Err(e)) | Err(e) => {
                        error!(logger, "Connection to provider failed. Not using this provider";
                                       "error" =>  e.to_string());
                        ProviderNetworkStatus::Broken {
                            network,
                            provider: endpoint.provider.to_string(),
                        }
                    }
                    Ok(Ok(ptr)) => {
                        info!(
                            logger,
                            "Connected to Firehose";
                            "uri" => &endpoint.uri,
                            "genesis_block" => format_args!("{}", &ptr),
                        );

                        let ident = ChainIdentifier {
                            net_version: "0".to_string(),
                            genesis_block_hash: ptr.hash,
                        };

                        ProviderNetworkStatus::Version { network, ident }
                    }
                }
            }),
    )
    .await;

    // Group identifiers by network name
    let idents: HashMap<String, Vec<ChainIdentifier>> =
        statuses
            .into_iter()
            .fold(HashMap::new(), |mut networks, status| {
                match status {
                    ProviderNetworkStatus::Broken { network, provider } => {
                        firehose_networks.remove(&network, &provider)
                    }
                    ProviderNetworkStatus::Version { network, ident } => {
                        networks.entry(network.to_string()).or_default().push(ident)
                    }
                }
                networks
            });
    let idents: Vec<_> = idents.into_iter().collect();
    (firehose_networks, idents)
}

#[cfg(test)]
mod test {
    use crate::chain::create_ethereum_networks;
    use crate::config::{Config, Opt};
    use graph::log::logger;
    use graph::prelude::tokio;
    use graph::prometheus::Registry;
    use graph_chain_ethereum::NodeCapabilities;
    use graph_core::MetricsRegistry;
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

        let config = Config::load(&logger, &opt).expect("can create config");
        let prometheus_registry = Arc::new(Registry::new());
        let metrics_registry = Arc::new(MetricsRegistry::new(
            logger.clone(),
            prometheus_registry.clone(),
        ));

        let ethereum_networks = create_ethereum_networks(logger, metrics_registry, &config)
            .await
            .expect("Correctly parse Ethereum network args");
        let mut network_names = ethereum_networks.networks.keys().collect::<Vec<&String>>();
        network_names.sort();

        let traces = NodeCapabilities {
            archive: false,
            traces: true,
        };
        let archive = NodeCapabilities {
            archive: true,
            traces: false,
        };
        let has_mainnet_with_traces = ethereum_networks
            .adapter_with_capabilities("mainnet".to_string(), &traces)
            .is_ok();
        let has_goerli_with_archive = ethereum_networks
            .adapter_with_capabilities("goerli".to_string(), &archive)
            .is_ok();
        let has_mainnet_with_archive = ethereum_networks
            .adapter_with_capabilities("mainnet".to_string(), &archive)
            .is_ok();
        let has_goerli_with_traces = ethereum_networks
            .adapter_with_capabilities("goerli".to_string(), &traces)
            .is_ok();

        assert_eq!(has_mainnet_with_traces, true);
        assert_eq!(has_goerli_with_archive, true);
        assert_eq!(has_mainnet_with_archive, false);
        assert_eq!(has_goerli_with_traces, false);

        let goerli_capability = ethereum_networks
            .networks
            .get("goerli")
            .unwrap()
            .adapters
            .iter()
            .next()
            .unwrap()
            .capabilities;
        let mainnet_capability = ethereum_networks
            .networks
            .get("mainnet")
            .unwrap()
            .adapters
            .iter()
            .next()
            .unwrap()
            .capabilities;
        assert_eq!(
            network_names,
            vec![&"goerli".to_string(), &"mainnet".to_string()]
        );
        assert_eq!(goerli_capability, archive);
        assert_eq!(mainnet_capability, traces);
    }
}
