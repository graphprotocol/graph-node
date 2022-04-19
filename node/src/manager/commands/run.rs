use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use crate::config::{Config, ProviderDetails};
use crate::manager::PanicSubscriptionManager;
use crate::store_builder::StoreBuilder;
use crate::MetricsContext;
use ethereum::chain::{EthereumAdapterSelector, EthereumStreamBuilder};
use ethereum::{EthereumNetworks, ProviderEthRpcMetrics, RuntimeAdapter as EthereumRuntimeAdapter};
use futures::future::join_all;
use futures::TryFutureExt;
use graph::anyhow::{bail, format_err, Error};
use graph::blockchain::{BlockchainKind, BlockchainMap, ChainIdentifier};
use graph::cheap_clone::CheapClone;
use graph::components::store::{BlockStore as _, DeploymentLocator};
use graph::env::EnvVars;
use graph::firehose::{FirehoseEndpoint, FirehoseEndpoints, FirehoseNetworks};
use graph::ipfs_client::IpfsClient;
use graph::prelude::MetricsRegistry as MetricsRegistryTrait;
use graph::prelude::{
    anyhow, tokio, BlockNumber, DeploymentHash, LoggerFactory, NodeId, SubgraphAssignmentProvider,
    SubgraphName, SubgraphRegistrar, SubgraphStore, SubgraphVersionSwitchingMode, ENV_VARS,
};
use graph::slog::{debug, error, info, o, Logger};
use graph::util::security::SafeDisplay;
use graph_chain_ethereum::{self as ethereum, EthereumAdapterTrait, Transport};
use graph_core::{
    LinkResolver, MetricsRegistry, SubgraphAssignmentProvider as IpfsSubgraphAssignmentProvider,
    SubgraphInstanceManager, SubgraphRegistrar as IpfsSubgraphRegistrar,
};
use url::Url;

fn locate(store: &dyn SubgraphStore, hash: &str) -> Result<DeploymentLocator, anyhow::Error> {
    let mut locators = store.locators(&hash)?;
    match locators.len() {
        0 => bail!("could not find subgraph {hash} we just created"),
        1 => Ok(locators.pop().unwrap()),
        n => bail!("there are {n} subgraphs with hash {hash}"),
    }
}

pub async fn run(
    logger: Logger,
    store_builder: StoreBuilder,
    network_name: String,
    ipfs_url: Vec<String>,
    config: Config,
    metrics_ctx: MetricsContext,
    node_id: NodeId,
    subgraph: String,
    stop_block: BlockNumber,
) -> Result<(), anyhow::Error> {
    println!(
        "Run command: starting subgraph => {}, stop_block = {}",
        subgraph, stop_block
    );

    let metrics_registry = metrics_ctx.registry.clone();
    let logger_factory = LoggerFactory::new(logger.clone(), None);

    // FIXME: Hard-coded IPFS config, take it from config file instead?
    let ipfs_clients: Vec<_> = create_ipfs_clients(&logger, &ipfs_url);

    // Convert the clients into a link resolver. Since we want to get past
    // possible temporary DNS failures, make the resolver retry
    let link_resolver = Arc::new(LinkResolver::new(
        ipfs_clients,
        Arc::new(EnvVars::default()),
    ));

    let eth_networks = create_ethereum_networks(logger.clone(), metrics_registry.clone(), &config)
        .await
        .expect("Failed to parse Ethereum networks");
    let firehose_networks_by_kind =
        create_firehose_networks(logger.clone(), metrics_registry.clone(), &config)
            .await
            .expect("Failed to parse Firehose endpoints");
    let firehose_networks = firehose_networks_by_kind.get(&BlockchainKind::Ethereum);
    let firehose_endpoints = firehose_networks
        .and_then(|v| v.networks.get(&network_name))
        .map_or_else(|| FirehoseEndpoints::new(), |v| v.clone());

    let eth_adapters = match eth_networks.networks.get(&network_name) {
        Some(adapters) => adapters.clone(),
        None => {
            return Err(format_err!(
                "No ethereum adapters found, but required in this state of graphman run command"
            ))
        }
    };

    let eth_adapters2 = eth_adapters.clone();

    let (_, ethereum_idents) = connect_ethereum_networks(&logger, eth_networks).await;
    // let (near_networks, near_idents) = connect_firehose_networks::<NearFirehoseHeaderOnlyBlock>(
    //     &logger,
    //     firehose_networks_by_kind
    //         .remove(&BlockchainKind::Near)
    //         .unwrap_or_else(|| FirehoseNetworks::new()),
    // )
    // .await;

    let chain_head_update_listener = store_builder.chain_head_update_listener();
    let network_identifiers = ethereum_idents.into_iter().collect();
    let network_store = store_builder.network_store(network_identifiers);

    let subgraph_store = network_store.subgraph_store();
    let chain_store = network_store
        .block_store()
        .chain_store(network_name.as_ref())
        .expect(format!("No chain store for {}", &network_name).as_ref());

    let chain = ethereum::Chain::new(
        logger_factory.clone(),
        network_name.clone(),
        node_id.clone(),
        metrics_registry.clone(),
        chain_store.cheap_clone(),
        chain_store.cheap_clone(),
        firehose_endpoints.clone(),
        eth_adapters.clone(),
        chain_head_update_listener,
        Arc::new(EthereumStreamBuilder {}),
        Arc::new(EthereumAdapterSelector::new(
            logger_factory.clone(),
            Arc::new(eth_adapters),
            Arc::new(firehose_endpoints.clone()),
            metrics_registry.clone(),
            chain_store.cheap_clone(),
        )),
        Arc::new(EthereumRuntimeAdapter {
            call_cache: chain_store.cheap_clone(),
            eth_adapters: Arc::new(eth_adapters2),
        }),
        ethereum::ENV_VARS.reorg_threshold,
        // We assume the tested chain is always ingestible for now
        true,
    );

    let mut blockchain_map = BlockchainMap::new();
    blockchain_map.insert(network_name.clone(), Arc::new(chain));

    let static_filters = ENV_VARS.experimental_static_filters;

    let blockchain_map = Arc::new(blockchain_map);
    let subgraph_instance_manager = SubgraphInstanceManager::new(
        &logger_factory,
        subgraph_store.clone(),
        blockchain_map.clone(),
        metrics_registry.clone(),
        link_resolver.cheap_clone(),
        static_filters,
    );

    // Create IPFS-based subgraph provider
    let subgraph_provider = Arc::new(IpfsSubgraphAssignmentProvider::new(
        &logger_factory,
        link_resolver.cheap_clone(),
        subgraph_instance_manager,
    ));

    let panicking_subscription_manager = Arc::new(PanicSubscriptionManager {});

    let subgraph_registrar = Arc::new(IpfsSubgraphRegistrar::new(
        &logger_factory,
        link_resolver.cheap_clone(),
        subgraph_provider.clone(),
        subgraph_store.clone(),
        panicking_subscription_manager,
        blockchain_map,
        node_id.clone(),
        SubgraphVersionSwitchingMode::Instant,
    ));

    let (name, hash) = if subgraph.contains(':') {
        let mut split = subgraph.split(':');
        (split.next().unwrap(), split.next().unwrap().to_owned())
    } else {
        ("cli", subgraph)
    };

    let subgraph_name = SubgraphName::new(name)
        .expect("Subgraph name must contain only a-z, A-Z, 0-9, '-' and '_'");
    let subgraph_hash =
        DeploymentHash::new(hash.clone()).expect("Subgraph hash must be a valid IPFS hash");

    info!(&logger, "Creating subgraph {}", name);
    let create_result =
        SubgraphRegistrar::create_subgraph(subgraph_registrar.as_ref(), subgraph_name.clone())
            .await?;

    info!(
        &logger,
        "Looking up subgraph deployment {} (Deployment hash => {}, id => {})",
        name,
        subgraph_hash,
        create_result.id,
    );

    SubgraphRegistrar::create_subgraph_version(
        subgraph_registrar.as_ref(),
        subgraph_name.clone(),
        subgraph_hash.clone(),
        node_id.clone(),
        None,
        None,
    )
    .await?;

    let locator = locate(subgraph_store.as_ref(), &hash)?;

    SubgraphAssignmentProvider::start(subgraph_provider.as_ref(), locator, Some(stop_block))
        .await?;

    loop {
        tokio::time::sleep(Duration::from_millis(1000)).await;

        let block_ptr = subgraph_store
            .least_block_ptr(&subgraph_hash)
            .await
            .unwrap()
            .unwrap();

        debug!(&logger, "subgraph block: {:?}", block_ptr);

        if block_ptr.number >= stop_block {
            info!(
                &logger,
                "subgraph now at block {}, reached stop block {}", block_ptr.number, stop_block
            );
            break;
        }
    }

    // FIXME: wait for instance manager to stop.
    // If we remove the subgraph first, it will panic on:
    // 1504c9d8-36e4-45bb-b4f2-71cf58789ed9
    tokio::time::sleep(Duration::from_millis(4000)).await;

    info!(&logger, "Removing subgraph {}", name);
    subgraph_store.clone().remove_subgraph(subgraph_name)?;

    if let Some(host) = metrics_ctx.prometheus_host {
        let mfs = metrics_ctx.prometheus.gather();
        let job_name = match metrics_ctx.job_name {
            Some(name) => name,
            None => "graphman run".into(),
        };

        tokio::task::spawn_blocking(move || {
            prometheus::push_metrics(&job_name, HashMap::new(), &host, mfs, None)
        })
        .await??;
    }

    Ok(())
}

// Stuff copied directly moslty from `main.rs`
//
// FIXME: Share that with `main.rs` stuff

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

fn create_ipfs_clients(logger: &Logger, ipfs_addresses: &Vec<String>) -> Vec<IpfsClient> {
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
async fn create_ethereum_networks(
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
                    Rpc => Transport::new_rpc(Url::parse(&web3.url)?, web3.headers.clone()),
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

async fn create_firehose_networks(
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
                    firehose.filters_enabled(),
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
async fn connect_ethereum_networks(
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
