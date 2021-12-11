use ethereum::{EthereumNetworks, ProviderEthRpcMetrics};
use futures::future::join_all;
use git_testament::{git_testament, render_testament};
use graph::blockchain::firehose_block_ingestor::FirehoseBlockIngestor;
use graph::firehose::endpoints::{FirehoseEndpoint, FirehoseNetworkEndpoints, FirehoseNetworks};
use graph::{ipfs_client::IpfsClient, prometheus::Registry};
use lazy_static::lazy_static;
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic;
use std::time::Duration;
use std::{collections::HashMap, env};
use structopt::StructOpt;
use tokio::sync::mpsc;

use graph::blockchain::block_ingestor::BlockIngestor;
use graph::blockchain::{
    Block as BlockchainBlock, Blockchain, BlockchainKind, BlockchainMap, ChainIdentifier,
};
use graph::components::store::BlockStore;
use graph::data::graphql::effort::LoadManager;
use graph::log::logger;
use graph::prelude::{IndexNodeServer as _, JsonRpcServer as _, *};
use graph::util::security::SafeDisplay;
use graph_chain_ethereum::{self as ethereum, EthereumAdapterTrait, Transport};
use graph_chain_near::{self as near, HeaderOnlyBlock as NearFirehoseHeaderOnlyBlock};
use graph_core::{
    LinkResolver, MetricsRegistry, SubgraphAssignmentProvider as IpfsSubgraphAssignmentProvider,
    SubgraphInstanceManager, SubgraphRegistrar as IpfsSubgraphRegistrar,
};
use graph_graphql::prelude::GraphQlRunner;
use graph_server_http::GraphQLServer as GraphQLQueryServer;
use graph_server_index_node::IndexNodeServer;
use graph_server_json_rpc::JsonRpcServer;
use graph_server_metrics::PrometheusMetricsServer;
use graph_server_websocket::SubscriptionServer as GraphQLSubscriptionServer;
use graph_store_postgres::{register_jobs as register_store_jobs, ChainHeadUpdateListener, Store};

mod config;
mod opt;
mod store_builder;

use config::Config;
use store_builder::StoreBuilder;

use crate::config::ProviderDetails;

lazy_static! {
    // Default to an Ethereum reorg threshold to 50 blocks
    static ref REORG_THRESHOLD: BlockNumber = env::var("ETHEREUM_REORG_THRESHOLD")
        .ok()
        .map(|s| BlockNumber::from_str(&s)
            .unwrap_or_else(|_| panic!("failed to parse env var ETHEREUM_REORG_THRESHOLD")))
        .unwrap_or(50);

    // Default to an ancestor count of 50 blocks
    static ref ANCESTOR_COUNT: BlockNumber = env::var("ETHEREUM_ANCESTOR_COUNT")
        .ok()
        .map(|s| BlockNumber::from_str(&s)
             .unwrap_or_else(|_| panic!("failed to parse env var ETHEREUM_ANCESTOR_COUNT")))
        .unwrap_or(50);
}

/// How long we will hold up node startup to get the net version and genesis
/// hash from the client. If we can't get it within that time, we'll try and
/// continue regardless.
const NET_VERSION_WAIT_TIME: Duration = Duration::from_secs(30);

git_testament!(TESTAMENT);

fn read_expensive_queries() -> Result<Vec<Arc<q::Document>>, std::io::Error> {
    // A file with a list of expensive queries, one query per line
    // Attempts to run these queries will return a
    // QueryExecutionError::TooExpensive to clients
    const EXPENSIVE_QUERIES: &str = "/etc/graph-node/expensive-queries.txt";
    let path = Path::new(EXPENSIVE_QUERIES);
    let mut queries = Vec::new();
    if path.exists() {
        let file = std::fs::File::open(path)?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = line?;
            let query = graphql_parser::parse_query(&line)
                .map_err(|e| {
                    let msg = format!(
                        "invalid GraphQL query in {}: {}\n{}",
                        EXPENSIVE_QUERIES,
                        e.to_string(),
                        line
                    );
                    std::io::Error::new(std::io::ErrorKind::InvalidData, msg)
                })?
                .into_static();
            queries.push(Arc::new(query));
        }
    }
    Ok(queries)
}

#[tokio::main]
async fn main() {
    env_logger::init();

    // Allow configuring fail points on debug builds. Used for integration tests.
    #[cfg(debug_assertions)]
    std::mem::forget(fail::FailScenario::setup());

    let opt = opt::Opt::from_args();

    // Set up logger
    let logger = logger(opt.debug);

    // Log version information
    info!(
        logger,
        "Graph Node version: {}",
        render_testament!(TESTAMENT)
    );

    if opt.unsafe_config {
        warn!(logger, "allowing unsafe configurations");
        graph::env::UNSAFE_CONFIG.store(true, atomic::Ordering::SeqCst);
    }

    let config = match Config::load(&logger, &opt.clone().into()) {
        Err(e) => {
            eprintln!("configuration error: {}", e);
            std::process::exit(1);
        }
        Ok(config) => config,
    };
    if opt.check_config {
        match config.to_json() {
            Ok(txt) => println!("{}", txt),
            Err(e) => eprintln!("error serializing config: {}", e),
        }
        eprintln!("Successfully validated configuration");
        std::process::exit(0);
    }

    let node_id =
        NodeId::new(opt.node_id.clone()).expect("Node ID must contain only a-z, A-Z, 0-9, and '_'");
    let query_only = config.query_only(&node_id);

    // Obtain subgraph related command-line arguments
    let subgraph = opt.subgraph.clone();

    // Obtain ports to use for the GraphQL server(s)
    let http_port = opt.http_port;
    let ws_port = opt.ws_port;

    // Obtain JSON-RPC server port
    let json_rpc_port = opt.admin_port;

    // Obtain index node server port
    let index_node_port = opt.index_node_port;

    // Obtain metrics server port
    let metrics_port = opt.metrics_port;

    info!(logger, "Starting up");

    // Optionally, identify the Elasticsearch logging configuration
    let elastic_config = opt
        .elasticsearch_url
        .clone()
        .map(|endpoint| ElasticLoggingConfig {
            endpoint: endpoint.clone(),
            username: opt.elasticsearch_user.clone(),
            password: opt.elasticsearch_password.clone(),
        });

    // Create a component and subgraph logger factory
    let logger_factory = LoggerFactory::new(logger.clone(), elastic_config);

    // Try to create IPFS clients for each URL specified in `--ipfs`
    let ipfs_clients: Vec<_> = create_ipfs_clients(&logger, &opt.ipfs);

    // Convert the clients into a link resolver. Since we want to get past
    // possible temporary DNS failures, make the resolver retry
    let link_resolver = Arc::new(LinkResolver::from(ipfs_clients));

    // Set up Prometheus registry
    let prometheus_registry = Arc::new(Registry::new());
    let metrics_registry = Arc::new(MetricsRegistry::new(
        logger.clone(),
        prometheus_registry.clone(),
    ));
    let mut metrics_server =
        PrometheusMetricsServer::new(&logger_factory, prometheus_registry.clone());

    // Ethereum clients; query nodes ignore all ethereum clients and never
    // connect to them directly
    let eth_networks = if query_only {
        EthereumNetworks::new()
    } else {
        create_ethereum_networks(logger.clone(), metrics_registry.clone(), config.clone())
            .await
            .expect("Failed to parse Ethereum networks")
    };

    let mut firehose_networks_by_kind = if query_only {
        BTreeMap::new()
    } else {
        create_firehose_networks(logger.clone(), metrics_registry.clone(), &config)
            .await
            .expect("Failed to parse Firehose networks")
    };

    let graphql_metrics_registry = metrics_registry.clone();

    let contention_logger = logger.clone();

    let expensive_queries = read_expensive_queries().unwrap();

    let store_builder =
        StoreBuilder::new(&logger, &node_id, &config, metrics_registry.cheap_clone()).await;

    let launch_services = |logger: Logger| async move {
        let subscription_manager = store_builder.subscription_manager();
        let chain_head_update_listener = store_builder.chain_head_update_listener();
        let primary_pool = store_builder.primary_pool();

        // To support the ethereum block ingestor, ethereum networks are referenced both by the
        // `blockchain_map` and `ethereum_chains`. Future chains should be referred to only in
        // `blockchain_map`.
        let mut blockchain_map = BlockchainMap::new();

        let (eth_networks, ethereum_idents) = connect_networks(&logger, eth_networks).await;
        let (near_networks, near_idents) =
            connect_firehose_networks::<NearFirehoseHeaderOnlyBlock>(
                &logger,
                firehose_networks_by_kind
                    .remove(&BlockchainKind::Near)
                    .unwrap_or_else(|| FirehoseNetworks::new()),
            )
            .await;

        let network_identifiers = ethereum_idents.into_iter().chain(near_idents).collect();
        let network_store = store_builder.network_store(network_identifiers);

        let ethereum_chains = ethereum_networks_as_chains(
            &mut blockchain_map,
            &logger,
            node_id.clone(),
            metrics_registry.clone(),
            firehose_networks_by_kind.get(&BlockchainKind::Ethereum),
            &eth_networks,
            network_store.as_ref(),
            chain_head_update_listener,
            &logger_factory,
        );

        let near_chains = near_networks_as_chains(
            &mut blockchain_map,
            &logger,
            &near_networks,
            network_store.as_ref(),
            &logger_factory,
        );

        let blockchain_map = Arc::new(blockchain_map);

        let load_manager = Arc::new(LoadManager::new(
            &logger,
            expensive_queries,
            metrics_registry.clone(),
        ));
        let graphql_runner = Arc::new(GraphQlRunner::new(
            &logger,
            network_store.clone(),
            subscription_manager.clone(),
            load_manager,
            metrics_registry.clone(),
        ));
        let mut graphql_server = GraphQLQueryServer::new(
            &logger_factory,
            graphql_metrics_registry,
            graphql_runner.clone(),
            node_id.clone(),
        );
        let subscription_server =
            GraphQLSubscriptionServer::new(&logger, graphql_runner.clone(), network_store.clone());

        let mut index_node_server = IndexNodeServer::new(
            &logger_factory,
            graphql_runner.clone(),
            network_store.clone(),
            link_resolver.clone(),
            network_store.subgraph_store().clone(),
        );

        if !opt.disable_block_ingestor {
            if ethereum_chains.len() > 0 {
                let block_polling_interval = Duration::from_millis(opt.ethereum_polling_interval);

                start_block_ingestor(&logger, block_polling_interval, ethereum_chains);
            }

            start_firehose_block_ingestor::<_, NearFirehoseHeaderOnlyBlock>(
                &logger,
                &network_store,
                near_chains,
            );

            // Start a task runner
            let mut job_runner = graph::util::jobs::Runner::new(&logger);
            register_store_jobs(
                &mut job_runner,
                network_store.clone(),
                primary_pool,
                metrics_registry.clone(),
            );
            graph::spawn_blocking(job_runner.start());
        }

        let subgraph_instance_manager = SubgraphInstanceManager::new(
            &logger_factory,
            network_store.subgraph_store(),
            blockchain_map.cheap_clone(),
            metrics_registry.clone(),
            link_resolver.cheap_clone(),
        );

        // Create IPFS-based subgraph provider
        let subgraph_provider = IpfsSubgraphAssignmentProvider::new(
            &logger_factory,
            link_resolver.cheap_clone(),
            subgraph_instance_manager,
        );

        // Check version switching mode environment variable
        let version_switching_mode = SubgraphVersionSwitchingMode::parse(
            env::var_os("EXPERIMENTAL_SUBGRAPH_VERSION_SWITCHING_MODE")
                .unwrap_or_else(|| "instant".into())
                .to_str()
                .expect("invalid version switching mode"),
        );

        // Create named subgraph provider for resolving subgraph name->ID mappings
        let subgraph_registrar = Arc::new(IpfsSubgraphRegistrar::new(
            &logger_factory,
            link_resolver.cheap_clone(),
            Arc::new(subgraph_provider),
            network_store.subgraph_store(),
            subscription_manager,
            blockchain_map,
            node_id.clone(),
            version_switching_mode,
        ));
        graph::spawn(
            subgraph_registrar
                .start()
                .map_err(|e| panic!("failed to initialize subgraph provider {}", e))
                .compat(),
        );

        // Start admin JSON-RPC server.
        let json_rpc_server = JsonRpcServer::serve(
            json_rpc_port,
            http_port,
            ws_port,
            subgraph_registrar.clone(),
            node_id.clone(),
            logger.clone(),
        )
        .expect("failed to start JSON-RPC admin server");

        // Let the server run forever.
        std::mem::forget(json_rpc_server);

        // Add the CLI subgraph with a REST request to the admin server.
        if let Some(subgraph) = subgraph {
            let (name, hash) = if subgraph.contains(':') {
                let mut split = subgraph.split(':');
                (split.next().unwrap(), split.next().unwrap().to_owned())
            } else {
                ("cli", subgraph)
            };

            let name = SubgraphName::new(name)
                .expect("Subgraph name must contain only a-z, A-Z, 0-9, '-' and '_'");
            let subgraph_id =
                DeploymentHash::new(hash).expect("Subgraph hash must be a valid IPFS hash");

            graph::spawn(
                async move {
                    subgraph_registrar.create_subgraph(name.clone()).await?;
                    subgraph_registrar
                        .create_subgraph_version(name, subgraph_id, node_id)
                        .await
                }
                .map_err(|e| panic!("Failed to deploy subgraph from `--subgraph` flag: {}", e)),
            );
        }

        // Serve GraphQL queries over HTTP
        graph::spawn(
            graphql_server
                .serve(http_port, ws_port)
                .expect("Failed to start GraphQL query server")
                .compat(),
        );

        // Serve GraphQL subscriptions over WebSockets
        graph::spawn(subscription_server.serve(ws_port));

        // Run the index node server
        graph::spawn(
            index_node_server
                .serve(index_node_port)
                .expect("Failed to start index node server")
                .compat(),
        );

        graph::spawn(
            metrics_server
                .serve(metrics_port)
                .expect("Failed to start metrics server")
                .compat(),
        );
    };

    graph::spawn(launch_services(logger.clone()));

    // Periodically check for contention in the tokio threadpool. First spawn a
    // task that simply responds to "ping" requests. Then spawn a separate
    // thread to periodically ping it and check responsiveness.
    let (ping_send, mut ping_receive) = mpsc::channel::<crossbeam_channel::Sender<()>>(1);
    graph::spawn(async move {
        while let Some(pong_send) = ping_receive.recv().await {
            let _ = pong_send.clone().send(());
        }
        panic!("ping sender dropped");
    });
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(1));
        let (pong_send, pong_receive) = crossbeam_channel::bounded(1);
        if futures::executor::block_on(ping_send.clone().send(pong_send)).is_err() {
            debug!(contention_logger, "Shutting down contention checker thread");
            break;
        }
        let mut timeout = Duration::from_millis(10);
        while pong_receive.recv_timeout(timeout)
            == Err(crossbeam_channel::RecvTimeoutError::Timeout)
        {
            debug!(contention_logger, "Possible contention in tokio threadpool";
                                     "timeout_ms" => timeout.as_millis(),
                                     "code" => LogCode::TokioContention);
            if timeout < Duration::from_secs(10) {
                timeout *= 10;
            } else if std::env::var_os("GRAPH_KILL_IF_UNRESPONSIVE").is_some() {
                // The node is unresponsive, kill it in hopes it will be restarted.
                crit!(contention_logger, "Node is unresponsive, killing process");
                std::process::abort()
            }
        }
    });

    futures::future::pending::<()>().await;
}

/// Parses an Ethereum connection string and returns the network name and Ethereum adapter.
async fn create_ethereum_networks(
    logger: Logger,
    registry: Arc<MetricsRegistry>,
    config: Config,
) -> Result<EthereumNetworks, anyhow::Error> {
    let eth_rpc_metrics = Arc::new(ProviderEthRpcMetrics::new(registry));
    let mut parsed_networks = EthereumNetworks::new();
    for (name, chain) in config.chains.chains {
        if chain.protocol != BlockchainKind::Ethereum {
            continue;
        }

        for provider in chain.providers {
            if let ProviderDetails::Web3(web3) = provider.details {
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
                    Rpc => Transport::new_rpc(&web3.url, web3.headers),
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
                            provider.label,
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
    _registry: Arc<MetricsRegistry>,
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

/// Try to connect to all the providers in `eth_networks` and get their net
/// version and genesis block. Return the same `eth_networks` and the
/// retrieved net identifiers grouped by network name. Remove all providers
/// for which trying to connect resulted in an error from the returned
/// `EthereumNetworks`, since it's likely pointless to try and connect to
/// them. If the connection attempt to a provider times out after
/// `NET_VERSION_WAIT_TIME`, keep the provider, but don't report a
/// version for it.
async fn connect_networks(
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
async fn connect_firehose_networks<M>(
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

/// Return the hashmap of ethereum chains and also add them to `blockchain_map`.
fn ethereum_networks_as_chains(
    blockchain_map: &mut BlockchainMap,
    logger: &Logger,
    node_id: NodeId,
    registry: Arc<MetricsRegistry>,
    firehose_networks: Option<&FirehoseNetworks>,
    eth_networks: &EthereumNetworks,
    store: &Store,
    chain_head_update_listener: Arc<ChainHeadUpdateListener>,
    logger_factory: &LoggerFactory,
) -> HashMap<String, Arc<ethereum::Chain>> {
    let chains: Vec<_> = eth_networks
        .networks
        .iter()
        .filter_map(|(network_name, eth_adapters)| {
            store
                .block_store()
                .chain_store(network_name)
                .map(|chain_store| {
                    let is_ingestible = chain_store.is_ingestible();
                    (network_name, eth_adapters, chain_store, is_ingestible)
                })
                .or_else(|| {
                    error!(
                        logger,
                        "No store configured for Ethereum chain {}; ignoring this chain",
                        network_name
                    );
                    None
                })
        })
        .map(|(network_name, eth_adapters, chain_store, is_ingestible)| {
            let firehose_endpoints = firehose_networks.and_then(|v| v.networks.get(network_name));

            let chain = ethereum::Chain::new(
                logger_factory.clone(),
                network_name.clone(),
                node_id.clone(),
                registry.clone(),
                chain_store.cheap_clone(),
                chain_store,
                store.subgraph_store(),
                firehose_endpoints.map_or_else(|| FirehoseNetworkEndpoints::new(), |v| v.clone()),
                eth_adapters.clone(),
                chain_head_update_listener.clone(),
                *ANCESTOR_COUNT,
                *REORG_THRESHOLD,
                is_ingestible,
            );
            (network_name.clone(), Arc::new(chain))
        })
        .collect();

    for (network_name, chain) in chains.iter().cloned() {
        blockchain_map.insert::<graph_chain_ethereum::Chain>(network_name, chain)
    }

    HashMap::from_iter(chains)
}

/// Return the hashmap of NEAR chains and also add them to `blockchain_map`.
fn near_networks_as_chains(
    blockchain_map: &mut BlockchainMap,
    logger: &Logger,
    firehose_networks: &FirehoseNetworks,
    store: &Store,
    logger_factory: &LoggerFactory,
) -> HashMap<String, FirehoseChain<near::Chain>> {
    let chains: Vec<_> = firehose_networks
        .networks
        .iter()
        .filter_map(|(network_name, firehose_endpoints)| {
            store
                .block_store()
                .chain_store(network_name)
                .map(|chain_store| (network_name, chain_store, firehose_endpoints))
                .or_else(|| {
                    error!(
                        logger,
                        "No store configured for NEAR chain {}; ignoring this chain", network_name
                    );
                    None
                })
        })
        .map(|(network_name, chain_store, firehose_endpoints)| {
            (
                network_name.clone(),
                FirehoseChain {
                    chain: Arc::new(near::Chain::new(
                        logger_factory.clone(),
                        network_name.clone(),
                        chain_store,
                        store.subgraph_store(),
                        firehose_endpoints.clone(),
                    )),
                    firehose_endpoints: firehose_endpoints.clone(),
                },
            )
        })
        .collect();

    for (network_name, firehose_chain) in chains.iter() {
        blockchain_map
            .insert::<graph_chain_near::Chain>(network_name.clone(), firehose_chain.chain.clone())
    }

    HashMap::from_iter(chains)
}

fn start_block_ingestor(
    logger: &Logger,
    block_polling_interval: Duration,
    chains: HashMap<String, Arc<ethereum::Chain>>,
) {
    // BlockIngestor must be configured to keep at least REORG_THRESHOLD ancestors,
    // otherwise BlockStream will not work properly.
    // BlockStream expects the blocks after the reorg threshold to be present in the
    // database.
    assert!(*ANCESTOR_COUNT >= *REORG_THRESHOLD);

    info!(
        logger,
        "Starting block ingestors with {} chains [{}]",
        chains.len(),
        chains
            .keys()
            .map(|v| v.clone())
            .collect::<Vec<String>>()
            .join(", ")
    );

    // Create Ethereum block ingestors and spawn a thread to run each
    chains
        .iter()
        .filter(|(network_name, chain)| {
            if !chain.is_ingestible {
                error!(logger, "Not starting block ingestor (chain is defective)"; "network_name" => &network_name);
            }
            chain.is_ingestible
        })
        .for_each(|(network_name, chain)| {
            info!(
                logger,
                "Starting block ingestor for network";
                "network_name" => &network_name
            );

            let block_ingestor = BlockIngestor::<ethereum::Chain>::new(
                chain.ingestor_adapter(),
                block_polling_interval,
            )
            .expect("failed to create Ethereum block ingestor");

            // Run the Ethereum block ingestor in the background
            graph::spawn(block_ingestor.into_polling_stream());
        });
}

#[derive(Clone)]
struct FirehoseChain<C: Blockchain> {
    chain: Arc<C>,
    firehose_endpoints: FirehoseNetworkEndpoints,
}

fn start_firehose_block_ingestor<C, M>(
    logger: &Logger,
    store: &Store,
    chains: HashMap<String, FirehoseChain<C>>,
) where
    C: Blockchain,
    M: prost::Message + BlockchainBlock + Default + 'static,
{
    info!(
        logger,
        "Starting firehose block ingestors with {} chains [{}]",
        chains.len(),
        chains
            .keys()
            .map(|v| v.clone())
            .collect::<Vec<String>>()
            .join(", ")
    );

    // Create Firehose block ingestors and spawn a thread to run each
    chains
        .iter()
        .for_each(|(network_name, chain)| {
            info!(
                logger,
                "Starting firehose block ingestor for network";
                "network_name" => &network_name
            );

            let endpoint = chain
                .firehose_endpoints
                .random()
                .expect("One Firehose endpoint should exist at that execution point");

            match store.block_store().chain_store(network_name.as_ref()) {
                Some(s) => {
                    let block_ingestor = FirehoseBlockIngestor::<M>::new(
                        s,
                        endpoint.clone(),
                        logger.new(o!("component" => "FirehoseBlockIngestor", "provider" => endpoint.provider.clone())),
                    );

                    // Run the Firehose block ingestor in the background
                    graph::spawn(block_ingestor.run());
                },
                None => {
                    error!(logger, "Not starting firehose block ingestor (no chain store available)"; "network_name" => &network_name);
                }
            }
        });
}
#[cfg(test)]
mod test {
    use super::create_ethereum_networks;
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

        let ethereum_networks = create_ethereum_networks(logger, metrics_registry, config.clone())
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
