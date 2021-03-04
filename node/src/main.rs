use git_testament::{git_testament, render_testament};
use graph::prometheus::Registry;
use ipfs_api::IpfsClient;
use lazy_static::lazy_static;
use std::env;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;
use structopt::StructOpt;
use tokio::sync::mpsc;

use graph::components::forward;
use graph::components::{
    ethereum::{EthereumNetworks, NodeCapabilities},
    store::BlockStore,
};
use graph::data::graphql::effort::LoadManager;
use graph::log::logger;
use graph::prelude::{IndexNodeServer as _, JsonRpcServer as _, *};
use graph::util::security::SafeDisplay;
use graph_chain_arweave::adapter::ArweaveAdapter;
use graph_chain_ethereum::{network_indexer, BlockIngestor, BlockStreamBuilder, Transport};
use graph_core::{
    three_box::ThreeBoxAdapter, LinkResolver, MetricsRegistry,
    SubgraphAssignmentProvider as IpfsSubgraphAssignmentProvider, SubgraphInstanceManager,
    SubgraphRegistrar as IpfsSubgraphRegistrar,
};
use graph_graphql::prelude::GraphQlRunner;
use graph_runtime_wasm::RuntimeHostBuilder as WASMRuntimeHostBuilder;
use graph_server_http::GraphQLServer as GraphQLQueryServer;
use graph_server_index_node::IndexNodeServer;
use graph_server_json_rpc::JsonRpcServer;
use graph_server_metrics::PrometheusMetricsServer;
use graph_server_websocket::SubscriptionServer as GraphQLSubscriptionServer;
use graph_store_postgres::BlockStore as DieselBlockStore;

mod config;
mod opt;
mod store_builder;

use config::Config;
use store_builder::StoreBuilder;

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

// Saturating the blocking threads can cause all sorts of issues, so set a large maximum.
// Ideally we'd use semaphores to not use more blocking threads than DB connections,
// but for now this is necessary.
#[tokio::main(max_threads = 2000)]
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

    // Obtain STORE_CONNECTION_POOL_SIZE setting
    let store_conn_pool_size: u32 = opt.store_connection_pool_size;

    let arweave_adapter = Arc::new(ArweaveAdapter::new(opt.arweave_api.clone()));

    let three_box_adapter = Arc::new(ThreeBoxAdapter::new(opt.three_box_api.clone()));

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

    // Convert the client into a link resolver
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
        create_ethereum_networks(logger.clone(), metrics_registry.clone(), &config)
            .await
            .expect("Failed to parse Ethereum networks")
    };

    let graphql_metrics_registry = metrics_registry.clone();

    let stores_logger = logger.clone();
    let stores_error_logger = logger.clone();
    let stores_eth_networks = eth_networks.clone();
    let contention_logger = logger.clone();

    let expensive_queries = read_expensive_queries().unwrap();

    let store_builder =
        StoreBuilder::new(&logger, &node_id, &config, metrics_registry.cheap_clone());

    graph::spawn(
        futures::stream::FuturesOrdered::from_iter(stores_eth_networks.flatten().into_iter().map(
            |(network_name, capabilities, eth_adapter)| {
                info!(
                    logger, "Connecting to Ethereum...";
                    "network" => &network_name,
                    "capabilities" => &capabilities
                );
                eth_adapter
                    .net_identifiers(&logger)
                    .map(move |network_identifier| (network_name, capabilities, network_identifier))
                    .compat()
            },
        ))
        .compat()
        .map_err(move |e| {
            error!(stores_error_logger, "Was a valid Ethereum node provided?");
            panic!("Failed to connect to Ethereum node: {}", e);
        })
        .map(move |(network_name, capabilities, network_identifier)| {
            info!(
                stores_logger,
                "Connected to Ethereum";
                "network" => &network_name,
                "network_version" => &network_identifier.net_version,
                "capabilities" => &capabilities
            );
            (network_name, network_identifier)
        })
        .collect()
        .and_then(move |networks| {
            let subscription_manager = store_builder.subscription_manager();
            let network_store = store_builder.network_store(networks);
            let load_manager = Arc::new(LoadManager::new(
                &logger,
                expensive_queries,
                metrics_registry.clone(),
                store_conn_pool_size as usize,
            ));
            let graphql_runner = Arc::new(GraphQlRunner::new(
                &logger,
                network_store.clone(),
                subscription_manager.clone(),
                load_manager,
            ));
            let mut graphql_server = GraphQLQueryServer::new(
                &logger_factory,
                graphql_metrics_registry,
                graphql_runner.clone(),
                node_id.clone(),
            );
            let subscription_server = GraphQLSubscriptionServer::new(
                &logger,
                graphql_runner.clone(),
                network_store.subgraph_store(),
            );

            let mut index_node_server = IndexNodeServer::new(
                &logger_factory,
                graphql_runner.clone(),
                network_store.clone(),
            );

            // Spawn Ethereum network indexers for all networks that are to be indexed
            opt.network_subgraphs
                .into_iter()
                .filter(|network_subgraph| network_subgraph.starts_with("ethereum/"))
                .for_each(|network_subgraph| {
                    let network_name = network_subgraph.replace("ethereum/", "");
                    let mut indexer = network_indexer::NetworkIndexer::new(
                        &logger,
                        eth_networks
                            .adapter_with_capabilities(
                                network_name.clone(),
                                &NodeCapabilities {
                                    archive: false,
                                    traces: false,
                                },
                            )
                            .expect(&*format!("adapter for network, {}", network_name))
                            .clone(),
                        network_store.subgraph_store(),
                        metrics_registry.clone(),
                        format!("network/{}", network_subgraph).into(),
                        None,
                        network_name,
                    );
                    graph::spawn(
                        indexer
                            .take_event_stream()
                            .unwrap()
                            .for_each(|_| {
                                // For now we simply ignore these events; we may later use them
                                // to drive subgraph indexing
                                Ok(())
                            })
                            .compat(),
                    );
                });

            if !opt.disable_block_ingestor {
                let block_polling_interval = Duration::from_millis(opt.ethereum_polling_interval);

                start_block_ingestor(
                    &logger,
                    block_polling_interval,
                    &eth_networks,
                    network_store.block_store(),
                    &logger_factory,
                );
            }

            let block_stream_builder = BlockStreamBuilder::new(
                network_store.subgraph_store(),
                network_store.block_store(),
                eth_networks.clone(),
                node_id.clone(),
                *REORG_THRESHOLD,
                metrics_registry.clone(),
            );
            let runtime_host_builder = WASMRuntimeHostBuilder::new(
                eth_networks.clone(),
                link_resolver.clone(),
                network_store.subgraph_store(),
                network_store.block_store(),
                arweave_adapter,
                three_box_adapter,
            );

            let subgraph_instance_manager = SubgraphInstanceManager::new(
                &logger_factory,
                network_store.subgraph_store(),
                network_store.block_store(),
                eth_networks.clone(),
                runtime_host_builder,
                block_stream_builder,
                metrics_registry.clone(),
            );

            // Create IPFS-based subgraph provider
            let mut subgraph_provider = IpfsSubgraphAssignmentProvider::new(
                &logger_factory,
                link_resolver.clone(),
                network_store.subgraph_store(),
            );

            // Forward subgraph events from the subgraph provider to the subgraph instance manager
            graph::spawn(
                forward(&mut subgraph_provider, &subgraph_instance_manager)
                    .unwrap()
                    .compat(),
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
                link_resolver,
                Arc::new(subgraph_provider),
                network_store.subgraph_store(),
                subscription_manager,
                network_store.block_store(),
                eth_networks.clone(),
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
                let subgraph_id = SubgraphDeploymentId::new(hash)
                    .expect("Subgraph hash must be a valid IPFS hash");

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

            future::ok(())
        })
        .compat(),
    );

    // Periodically check for contention in the tokio threadpool. First spawn a
    // task that simply responds to "ping" requests. Then spawn a separate
    // thread to periodically ping it and check responsiveness.
    let (ping_send, ping_receive) = mpsc::channel::<crossbeam_channel::Sender<()>>(1);
    graph::spawn(ping_receive.for_each(move |pong_send| async move {
        let _ = pong_send.clone().send(());
    }));
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
    config: &Config,
) -> Result<EthereumNetworks, anyhow::Error> {
    let eth_rpc_metrics = Arc::new(ProviderEthRpcMetrics::new(registry));
    let mut parsed_networks = EthereumNetworks::new();
    for (name, chain) in &config.chains.chains {
        for provider in &chain.providers {
            let capabilities = provider.node_capabilities();

            info!(
                logger,
                "Creating transport";
                "network" => &name,
                "url" => &provider.url,
                "capabilities" => capabilities
            );

            use crate::config::Transport::*;

            let (transport_event_loop, transport) = match provider.transport {
                Rpc => Transport::new_rpc(&provider.url),
                Ipc => Transport::new_ipc(&provider.url),
                Ws => Transport::new_ws(&provider.url),
            };

            // If we drop the event loop the transport will stop working.
            // For now it's fine to just leak it.
            std::mem::forget(transport_event_loop);

            parsed_networks.insert(
                name.to_string(),
                capabilities,
                Arc::new(
                    graph_chain_ethereum::EthereumAdapter::new(
                        &provider.url,
                        transport,
                        eth_rpc_metrics.clone(),
                    )
                    .await,
                ) as Arc<dyn EthereumAdapter>,
            );
        }
    }
    Ok(parsed_networks)
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

            let ipfs_client = match IpfsClient::new_from_uri(&ipfs_address) {
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
            let ipfs_test = ipfs_client.clone();
            let ipfs_ok_logger = logger.clone();
            let ipfs_err_logger = logger.clone();
            let ipfs_address_for_ok = ipfs_address.clone();
            let ipfs_address_for_err = ipfs_address.clone();
            graph::spawn(async move {
                ipfs_test
                    .version()
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

fn start_block_ingestor(
    logger: &Logger,
    block_polling_interval: Duration,
    eth_networks: &EthereumNetworks,
    block_store: Arc<DieselBlockStore>,
    logger_factory: &LoggerFactory,
) {
    // BlockIngestor must be configured to keep at least REORG_THRESHOLD ancestors,
    // otherwise BlockStream will not work properly.
    // BlockStream expects the blocks after the reorg threshold to be present in the
    // database.
    assert!(*ANCESTOR_COUNT >= *REORG_THRESHOLD);

    info!(logger, "Starting block ingestors");

    // Create Ethereum block ingestors and spawn a thread to run each
    eth_networks
        .networks
        .iter()
        .for_each(|(network_name, eth_adapters)| {
            info!(
                logger,
                "Starting block ingestor for network";
                "network_name" => &network_name
            );
            let eth_adapter = eth_adapters.cheapest().unwrap(); //Safe to unwrap since it cannot be empty
            let block_ingestor = BlockIngestor::new(
                block_store
                    .chain_store(network_name)
                    .expect("network with name"),
                eth_adapter.clone(),
                *ANCESTOR_COUNT,
                network_name.to_string(),
                logger_factory,
                block_polling_interval,
            )
            .expect("failed to create Ethereum block ingestor");

            // Run the Ethereum block ingestor in the background
            graph::spawn(block_ingestor.into_polling_stream());
        });
}

#[cfg(test)]
mod test {
    use super::create_ethereum_networks;
    use crate::config::{Config, Opt};
    use graph::components::ethereum::NodeCapabilities;
    use graph::log::logger;
    use graph::prelude::tokio;
    use graph::prometheus::Registry;
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
