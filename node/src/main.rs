use ethereum::chain::{EthereumAdapterSelector, EthereumStreamBuilder};
use ethereum::{
    BlockIngestor as EthereumBlockIngestor, EthereumAdapterTrait, EthereumNetworks, RuntimeAdapter,
};
use git_testament::{git_testament, render_testament};
use graph::blockchain::firehose_block_ingestor::FirehoseBlockIngestor;
use graph::blockchain::{Block as BlockchainBlock, Blockchain, BlockchainKind, BlockchainMap};
use graph::components::store::BlockStore;
use graph::data::graphql::effort::LoadManager;
use graph::env::EnvVars;
use graph::firehose::{FirehoseEndpoints, FirehoseNetworks};
use graph::log::logger;
use graph::prelude::{IndexNodeServer as _, JsonRpcServer as _, *};
use graph::prometheus::Registry;
use graph::url::Url;
use graph_chain_arweave::{self as arweave, Block as ArweaveBlock};
use graph_chain_cosmos::{self as cosmos, Block as CosmosFirehoseBlock};
use graph_chain_ethereum as ethereum;
use graph_chain_near::{self as near, HeaderOnlyBlock as NearFirehoseHeaderOnlyBlock};
use graph_core::{
    LinkResolver, MetricsRegistry, SubgraphAssignmentProvider as IpfsSubgraphAssignmentProvider,
    SubgraphInstanceManager, SubgraphRegistrar as IpfsSubgraphRegistrar,
};
use graph_graphql::prelude::GraphQlRunner;
use graph_node::chain::{
    connect_ethereum_networks, connect_firehose_networks, create_ethereum_networks,
    create_firehose_networks, create_ipfs_clients,
};
use graph_node::config::Config;
use graph_node::opt;
use graph_node::store_builder::StoreBuilder;
use graph_server_http::GraphQLServer as GraphQLQueryServer;
use graph_server_index_node::IndexNodeServer;
use graph_server_json_rpc::JsonRpcServer;
use graph_server_metrics::PrometheusMetricsServer;
use graph_server_websocket::SubscriptionServer as GraphQLSubscriptionServer;
use graph_store_postgres::{register_jobs as register_store_jobs, ChainHeadUpdateListener, Store};
use near::NearStreamBuilder;
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::atomic;
use std::time::Duration;
use std::{collections::HashMap, env};
use structopt::StructOpt;
use tokio::sync::mpsc;

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

    if !graph_server_index_node::PoiProtection::from_env(&ENV_VARS).is_active() {
        warn!(
            logger,
            "GRAPH_POI_ACCESS_TOKEN not set; might leak POIs to the public via GraphQL"
        );
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

    // Obtain the fork base URL
    let fork_base = match &opt.fork_base {
        Some(url) => {
            // Make sure the endpoint ends with a terminating slash.
            let url = if !url.ends_with("/") {
                let mut url = url.clone();
                url.push('/');
                Url::parse(&url)
            } else {
                Url::parse(url)
            };

            Some(url.expect("Failed to parse the fork base URL"))
        }
        None => {
            warn!(
                logger,
                "No fork base URL specified, subgraph forking is disabled"
            );
            None
        }
    };

    info!(logger, "Starting up");

    // Optionally, identify the Elasticsearch logging configuration
    let elastic_config = opt
        .elasticsearch_url
        .clone()
        .map(|endpoint| ElasticLoggingConfig {
            endpoint: endpoint.clone(),
            username: opt.elasticsearch_user.clone(),
            password: opt.elasticsearch_password.clone(),
            client: reqwest::Client::new(),
        });

    // Create a component and subgraph logger factory
    let logger_factory = LoggerFactory::new(logger.clone(), elastic_config);

    // Try to create IPFS clients for each URL specified in `--ipfs`
    let ipfs_clients: Vec<_> = create_ipfs_clients(&logger, &opt.ipfs);

    // Convert the clients into a link resolver. Since we want to get past
    // possible temporary DNS failures, make the resolver retry
    let link_resolver = Arc::new(LinkResolver::new(
        ipfs_clients,
        Arc::new(EnvVars::default()),
    ));

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

    let store_builder = StoreBuilder::new(
        &logger,
        &node_id,
        &config,
        fork_base,
        metrics_registry.cheap_clone(),
    )
    .await;

    let launch_services = |logger: Logger| async move {
        let subscription_manager = store_builder.subscription_manager();
        let chain_head_update_listener = store_builder.chain_head_update_listener();
        let primary_pool = store_builder.primary_pool();

        // To support the ethereum block ingestor, ethereum networks are referenced both by the
        // `blockchain_map` and `ethereum_chains`. Future chains should be referred to only in
        // `blockchain_map`.
        let mut blockchain_map = BlockchainMap::new();

        let (arweave_networks, arweave_idents) = connect_firehose_networks::<ArweaveBlock>(
            &logger,
            firehose_networks_by_kind
                .remove(&BlockchainKind::Arweave)
                .unwrap_or_else(|| FirehoseNetworks::new()),
        )
        .await;

        let (eth_networks, ethereum_idents) =
            connect_ethereum_networks(&logger, eth_networks).await;

        let (near_networks, near_idents) =
            connect_firehose_networks::<NearFirehoseHeaderOnlyBlock>(
                &logger,
                firehose_networks_by_kind
                    .remove(&BlockchainKind::Near)
                    .unwrap_or_else(|| FirehoseNetworks::new()),
            )
            .await;

        let (cosmos_networks, cosmos_idents) = connect_firehose_networks::<CosmosFirehoseBlock>(
            &logger,
            firehose_networks_by_kind
                .remove(&BlockchainKind::Cosmos)
                .unwrap_or_else(|| FirehoseNetworks::new()),
        )
        .await;

        let network_identifiers = ethereum_idents
            .into_iter()
            .chain(arweave_idents)
            .chain(near_idents)
            .chain(cosmos_idents)
            .collect();

        let network_store = store_builder.network_store(network_identifiers);

        let arweave_chains = arweave_networks_as_chains(
            &mut blockchain_map,
            &logger,
            &arweave_networks,
            network_store.as_ref(),
            &logger_factory,
            metrics_registry.clone(),
        );

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
            metrics_registry.clone(),
        );

        let cosmos_chains = cosmos_networks_as_chains(
            &mut blockchain_map,
            &logger,
            &cosmos_networks,
            network_store.as_ref(),
            &logger_factory,
            metrics_registry.clone(),
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
            blockchain_map.clone(),
            graphql_runner.clone(),
            network_store.clone(),
            link_resolver.clone(),
        );

        if !opt.disable_block_ingestor {
            if ethereum_chains.len() > 0 {
                let block_polling_interval = Duration::from_millis(opt.ethereum_polling_interval);

                start_block_ingestor(
                    &logger,
                    &logger_factory,
                    block_polling_interval,
                    ethereum_chains,
                );
            }

            start_firehose_block_ingestor::<_, ArweaveBlock>(
                &logger,
                &network_store,
                arweave_chains,
            );

            start_firehose_block_ingestor::<_, NearFirehoseHeaderOnlyBlock>(
                &logger,
                &network_store,
                near_chains,
            );
            start_firehose_block_ingestor::<_, CosmosFirehoseBlock>(
                &logger,
                &network_store,
                cosmos_chains,
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
        let static_filters = ENV_VARS.experimental_static_filters;

        let subgraph_instance_manager = SubgraphInstanceManager::new(
            &logger_factory,
            network_store.subgraph_store(),
            blockchain_map.cheap_clone(),
            metrics_registry.clone(),
            link_resolver.clone(),
            static_filters,
        );

        // Create IPFS-based subgraph provider
        let subgraph_provider = IpfsSubgraphAssignmentProvider::new(
            &logger_factory,
            link_resolver.clone(),
            subgraph_instance_manager,
        );

        // Check version switching mode environment variable
        let version_switching_mode = ENV_VARS.subgraph_version_switching_mode;

        // Create named subgraph provider for resolving subgraph name->ID mappings
        let subgraph_registrar = Arc::new(IpfsSubgraphRegistrar::new(
            &logger_factory,
            link_resolver,
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
            let debug_fork = opt
                .debug_fork
                .map(DeploymentHash::new)
                .map(|h| h.expect("Debug fork hash must be a valid IPFS hash"));
            let start_block = opt
                .start_block
                .map(|block| {
                    let mut split = block.split(":");
                    (
                        // BlockHash
                        split.next().unwrap().to_owned(),
                        // BlockNumber
                        split.next().unwrap().parse::<i64>().unwrap(),
                    )
                })
                .map(|(hash, number)| BlockPtr::try_from((hash.as_str(), number)))
                .map(Result::unwrap);

            graph::spawn(
                async move {
                    subgraph_registrar.create_subgraph(name.clone()).await?;
                    subgraph_registrar
                        .create_subgraph_version(
                            name,
                            subgraph_id,
                            node_id,
                            debug_fork,
                            start_block,
                        )
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
            } else if ENV_VARS.kill_if_unresponsive {
                // The node is unresponsive, kill it in hopes it will be restarted.
                crit!(contention_logger, "Node is unresponsive, killing process");
                std::process::abort()
            }
        }
    });

    futures::future::pending::<()>().await;
}

/// Return the hashmap of Arweave chains and also add them to `blockchain_map`.
fn arweave_networks_as_chains(
    blockchain_map: &mut BlockchainMap,
    logger: &Logger,
    firehose_networks: &FirehoseNetworks,
    store: &Store,
    logger_factory: &LoggerFactory,
    metrics_registry: Arc<MetricsRegistry>,
) -> HashMap<String, FirehoseChain<arweave::Chain>> {
    let chains: Vec<_> = firehose_networks
        .networks
        .iter()
        .filter_map(|(chain_id, endpoints)| {
            store
                .block_store()
                .chain_store(chain_id)
                .map(|chain_store| (chain_id, chain_store, endpoints))
                .or_else(|| {
                    error!(
                        logger,
                        "No store configured for Arweave chain {}; ignoring this chain", chain_id
                    );
                    None
                })
        })
        .map(|(chain_id, chain_store, endpoints)| {
            (
                chain_id.clone(),
                FirehoseChain {
                    chain: Arc::new(arweave::Chain::new(
                        logger_factory.clone(),
                        chain_id.clone(),
                        chain_store,
                        endpoints.clone(),
                        metrics_registry.clone(),
                    )),
                    firehose_endpoints: endpoints.clone(),
                },
            )
        })
        .collect();

    for (chain_id, firehose_chain) in chains.iter() {
        blockchain_map.insert::<arweave::Chain>(chain_id.clone(), firehose_chain.chain.clone())
    }

    HashMap::from_iter(chains)
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

            let adapter_selector = EthereumAdapterSelector::new(
                logger_factory.clone(),
                Arc::new(eth_adapters.clone()),
                Arc::new(
                    firehose_endpoints
                        .map(|fe| fe.clone())
                        .unwrap_or(FirehoseEndpoints::new()),
                ),
                registry.clone(),
                chain_store.clone(),
            );

            let runtime_adapter = Arc::new(RuntimeAdapter {
                eth_adapters: Arc::new(eth_adapters.clone()),
                call_cache: chain_store.cheap_clone(),
            });

            let chain = ethereum::Chain::new(
                logger_factory.clone(),
                network_name.clone(),
                node_id.clone(),
                registry.clone(),
                chain_store.cheap_clone(),
                chain_store,
                firehose_endpoints.map_or_else(|| FirehoseEndpoints::new(), |v| v.clone()),
                eth_adapters.clone(),
                chain_head_update_listener.clone(),
                Arc::new(EthereumStreamBuilder {}),
                Arc::new(adapter_selector),
                runtime_adapter,
                ethereum::ENV_VARS.reorg_threshold,
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

fn cosmos_networks_as_chains(
    blockchain_map: &mut BlockchainMap,
    logger: &Logger,
    firehose_networks: &FirehoseNetworks,
    store: &Store,
    logger_factory: &LoggerFactory,
    metrics_registry: Arc<MetricsRegistry>,
) -> HashMap<String, FirehoseChain<cosmos::Chain>> {
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
                        "No store configured for Cosmos chain {}; ignoring this chain",
                        network_name
                    );
                    None
                })
        })
        .map(|(network_name, chain_store, firehose_endpoints)| {
            (
                network_name.clone(),
                FirehoseChain {
                    chain: Arc::new(cosmos::Chain::new(
                        logger_factory.clone(),
                        network_name.clone(),
                        chain_store,
                        firehose_endpoints.clone(),
                        metrics_registry.clone(),
                    )),
                    firehose_endpoints: firehose_endpoints.clone(),
                },
            )
        })
        .collect();

    for (network_name, firehose_chain) in chains.iter() {
        blockchain_map.insert::<cosmos::Chain>(network_name.clone(), firehose_chain.chain.clone())
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
    metrics_registry: Arc<MetricsRegistry>,
) -> HashMap<String, FirehoseChain<near::Chain>> {
    let chains: Vec<_> = firehose_networks
        .networks
        .iter()
        .filter_map(|(chain_id, endpoints)| {
            store
                .block_store()
                .chain_store(chain_id)
                .map(|chain_store| (chain_id, chain_store, endpoints))
                .or_else(|| {
                    error!(
                        logger,
                        "No store configured for NEAR chain {}; ignoring this chain", chain_id
                    );
                    None
                })
        })
        .map(|(chain_id, chain_store, endpoints)| {
            (
                chain_id.clone(),
                FirehoseChain {
                    chain: Arc::new(near::Chain::new(
                        logger_factory.clone(),
                        chain_id.clone(),
                        chain_store,
                        endpoints.clone(),
                        metrics_registry.clone(),
                        Arc::new(NearStreamBuilder {}),
                    )),
                    firehose_endpoints: endpoints.clone(),
                },
            )
        })
        .collect();

    for (chain_id, firehose_chain) in chains.iter() {
        blockchain_map
            .insert::<graph_chain_near::Chain>(chain_id.clone(), firehose_chain.chain.clone())
    }

    HashMap::from_iter(chains)
}

fn start_block_ingestor(
    logger: &Logger,
    logger_factory: &LoggerFactory,
    block_polling_interval: Duration,
    chains: HashMap<String, Arc<ethereum::Chain>>,
) {
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

            let eth_adapter = chain.cheapest_adapter();
                let logger = logger_factory
                    .component_logger(
                        "BlockIngestor",
                        Some(ComponentLoggerConfig {
                            elastic: Some(ElasticComponentLoggerConfig {
                                index: String::from("block-ingestor-logs"),
                            }),
                        }),
                    )
                    .new(o!("provider" => eth_adapter.provider().to_string()));

            // The block ingestor must be configured to keep at least REORG_THRESHOLD ancestors,
            // because the json-rpc BlockStream expects blocks after the reorg threshold to be
            // present in the DB.
            let block_ingestor = EthereumBlockIngestor::new(
                logger,
                ethereum::ENV_VARS.reorg_threshold,
                eth_adapter,
                chain.chain_store(),
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
    firehose_endpoints: FirehoseEndpoints,
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
