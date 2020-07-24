use clap::{App, Arg};
use git_testament::{git_testament, render_testament};
use ipfs_api::IpfsClient;
use lazy_static::lazy_static;
use prometheus::Registry;
use std::collections::HashMap;
use std::env;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::str::FromStr;
use std::sync::RwLock;
use std::time::Duration;
use tokio::sync::mpsc;
use url::Url;

use graph::components::ethereum::{EthereumNetworks, NodeCapabilities};
use graph::components::forward;
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
use graph_store_postgres::connection_pool::create_connection_pool;
use graph_store_postgres::{
    ChainHeadUpdateListener as PostgresChainHeadUpdateListener, Store as DieselStore, StoreConfig,
    SubscriptionManager,
};
use graphql_parser::query as q;

lazy_static! {
    // Default to an Ethereum reorg threshold to 50 blocks
    static ref REORG_THRESHOLD: u64 = env::var("ETHEREUM_REORG_THRESHOLD")
        .ok()
        .map(|s| u64::from_str(&s)
            .unwrap_or_else(|_| panic!("failed to parse env var ETHEREUM_REORG_THRESHOLD")))
        .unwrap_or(50);

    // Default to an ancestor count of 50 blocks
    static ref ANCESTOR_COUNT: u64 = env::var("ETHEREUM_ANCESTOR_COUNT")
        .ok()
        .map(|s| u64::from_str(&s)
             .unwrap_or_else(|_| panic!("failed to parse env var ETHEREUM_ANCESTOR_COUNT")))
        .unwrap_or(50);
}

git_testament!(TESTAMENT);

#[derive(Debug, Clone)]
enum ConnectionType {
    IPC,
    RPC,
    WS,
}

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
            let query = graphql_parser::parse_query(&line).map_err(|e| {
                let msg = format!(
                    "invalid GraphQL query in {}: {}\n{}",
                    EXPENSIVE_QUERIES,
                    e.to_string(),
                    line
                );
                std::io::Error::new(std::io::ErrorKind::InvalidData, msg)
            })?;
            queries.push(Arc::new(query));
        }
    }
    Ok(queries)
}

/// Replace the host portion of `url` and return a new URL with `host`
/// as the host portion
///
/// Panics if `url` is not a valid URL (which won't happen in our case since
/// we would have paniced before getting here as `url` is the connection for
/// the primary Postgres instance)
fn replace_host(url: &str, host: &str) -> String {
    let mut url = match Url::parse(url) {
        Ok(url) => url,
        Err(_) => panic!("Invalid Postgres URL {}", url),
    };
    if let Err(e) = url.set_host(Some(host)) {
        panic!("Invalid Postgres url {}: {}", url, e.to_string());
    }
    url.into_string()
}

// Saturating the blocking threads can cause all sorts of issues, so set a large maximum.
// Ideally we'd use semaphores to not use more blocking threads than DB connections,
// but for now this is necessary.
#[tokio::main(max_threads = 2000)]
async fn main() {
    env_logger::init();

    // Setup CLI using Clap, provide general info and capture postgres url
    let matches = App::new("graph-node")
        .version(render_testament!(TESTAMENT).as_str())
        .author("Graph Protocol, Inc.")
        .about("Scalable queries for a decentralized future")
        .arg(
            Arg::with_name("subgraph")
                .takes_value(true)
                .long("subgraph")
                .value_name("[NAME:]IPFS_HASH")
                .env("SUBGRAPH")
                .help("name and IPFS hash of the subgraph manifest"),
        )
        .arg(
            Arg::with_name("postgres-url")
                .takes_value(true)
                .required(true)
                .long("postgres-url")
                .value_name("URL")
                .env("POSTGRES_URL")
                .help("Location of the Postgres database used for storing entities"),
        )
        .arg(
            Arg::with_name("postgres-secondary-hosts")
                .multiple(true)
                .use_delimiter(true)
                .long("postgres-secondary-hosts")
                .value_name("URL,")
                .env("GRAPH_POSTGRES_SECONDARY_HOSTS")
                .help(
                    "Comma-separated list of host names/IP's for read-only Postgres replicas, \
                       which will share the load with the primary server",
                ),
        )
        .arg(
            Arg::with_name("postgres-host-weights")
                .multiple(true)
                .use_delimiter(true)
                .long("postgres-host-weights")
                .value_name("WEIGHT,")
                .env("GRAPH_POSTGRES_HOST_WEIGHTS")
                .help(
                    "Comma-separated list of relative weights for selecting the main database \
                and secondary databases. The list is in the order MAIN,REPLICA1,REPLICA2,...\
                A host will receive approximately WEIGHT/SUM(WEIGHTS) fraction of total queries. \
                Defaults to weight 1 for each host",
                ),
        )
        .arg(
            Arg::with_name("ethereum-rpc")
                .takes_value(true)
                .multiple(true)
                .min_values(0)
                .required_unless_one(&["ethereum-ws", "ethereum-ipc"])
                .conflicts_with_all(&["ethereum-ws", "ethereum-ipc"])
                .long("ethereum-rpc")
                .value_name("NETWORK_NAME:URL")
                .env("ETHEREUM_RPC")
                .help(
                    "Ethereum network name (e.g. 'mainnet') and \
                     Ethereum RPC URL, separated by a ':'",
                ),
        )
        .arg(
            Arg::with_name("ethereum-ws")
                .takes_value(true)
                .multiple(true)
                .min_values(0)
                .required_unless_one(&["ethereum-rpc", "ethereum-ipc"])
                .conflicts_with_all(&["ethereum-rpc", "ethereum-ipc"])
                .long("ethereum-ws")
                .value_name("NETWORK_NAME:URL")
                .env("ETHEREUM_WS")
                .help(
                    "Ethereum network name (e.g. 'mainnet') and \
                     Ethereum WebSocket URL, separated by a ':'",
                ),
        )
        .arg(
            Arg::with_name("ethereum-ipc")
                .takes_value(true)
                .multiple(true)
                .min_values(0)
                .required_unless_one(&["ethereum-rpc", "ethereum-ws"])
                .conflicts_with_all(&["ethereum-rpc", "ethereum-ws"])
                .long("ethereum-ipc")
                .value_name("NETWORK_NAME:FILE")
                .env("ETHEREUM_IPC")
                .help(
                    "Ethereum network name (e.g. 'mainnet') and \
                     Ethereum IPC pipe, separated by a ':'",
                ),
        )
        .arg(
            Arg::with_name("ipfs")
                .takes_value(true)
                .required(true)
                .long("ipfs")
                .multiple(true)
                .value_name("HOST:PORT")
                .env("IPFS")
                .help("HTTP addresses of IPFS nodes"),
        )
        .arg(
            Arg::with_name("http-port")
                .default_value("8000")
                .long("http-port")
                .value_name("PORT")
                .help("Port for the GraphQL HTTP server"),
        )
        .arg(
            Arg::with_name("index-node-port")
                .default_value("8030")
                .long("index-node-port")
                .value_name("PORT")
                .help("Port for the index node server"),
        )
        .arg(
            Arg::with_name("ws-port")
                .default_value("8001")
                .long("ws-port")
                .value_name("PORT")
                .help("Port for the GraphQL WebSocket server"),
        )
        .arg(
            Arg::with_name("admin-port")
                .default_value("8020")
                .long("admin-port")
                .value_name("PORT")
                .help("Port for the JSON-RPC admin server"),
        )
        .arg(
            Arg::with_name("metrics-port")
                .default_value("8040")
                .long("metrics-port")
                .value_name("PORT")
                .help("Port for the Prometheus metrics server"),
        )
        .arg(
            Arg::with_name("node-id")
                .default_value("default")
                .long("node-id")
                .value_name("NODE_ID")
                .env("GRAPH_NODE_ID")
                .help("a unique identifier for this node"),
        )
        .arg(
            Arg::with_name("debug")
                .long("debug")
                .help("Enable debug logging"),
        )
        .arg(
            Arg::with_name("elasticsearch-url")
                .long("elasticsearch-url")
                .value_name("URL")
                .env("ELASTICSEARCH_URL")
                .help("Elasticsearch service to write subgraph logs to"),
        )
        .arg(
            Arg::with_name("elasticsearch-user")
                .long("elasticsearch-user")
                .value_name("USER")
                .env("ELASTICSEARCH_USER")
                .help("User to use for Elasticsearch logging"),
        )
        .arg(
            Arg::with_name("elasticsearch-password")
                .long("elasticsearch-password")
                .value_name("PASSWORD")
                .env("ELASTICSEARCH_PASSWORD")
                .hide_env_values(true)
                .help("Password to use for Elasticsearch logging"),
        )
        .arg(
            Arg::with_name("ethereum-polling-interval")
                .long("ethereum-polling-interval")
                .value_name("MILLISECONDS")
                .default_value("1000")
                .env("ETHEREUM_POLLING_INTERVAL")
                .help("How often to poll the Ethereum node for new blocks"),
        )
        .arg(
            Arg::with_name("disable-block-ingestor")
                .long("disable-block-ingestor")
                .value_name("DISABLE_BLOCK_INGESTOR")
                .env("DISABLE_BLOCK_INGESTOR")
                .default_value("false")
                .help("Ensures that the block ingestor component does not execute"),
        )
        .arg(
            Arg::with_name("store-connection-pool-size")
                .long("store-connection-pool-size")
                .value_name("STORE_CONNECTION_POOL_SIZE")
                .default_value("10")
                .env("STORE_CONNECTION_POOL_SIZE")
                .help("Limits the number of connections in the store's connection pool"),
        )
        .arg(
            Arg::with_name("network-subgraphs")
                .takes_value(true)
                .multiple(true)
                .min_values(1)
                .long("network-subgraphs")
                .value_name("NETWORK_NAME")
                .help(
                    "One or more network names to index using built-in subgraphs \
                     (e.g. 'ethereum/mainnet').",
                ),
        )
        .arg(
            Arg::with_name("arweave-api")
                .default_value("https://arweave.net/")
                .long("arweave-api")
                .value_name("URL")
                .help("HTTP endpoint of an Arweave gateway"),
        )
        .arg(
            Arg::with_name("3box-api")
                .default_value("https://ipfs.3box.io/")
                .long("3box-api")
                .value_name("URL")
                .help("HTTP endpoint for 3box profiles"),
        )
        .get_matches();

    // Set up logger
    let logger = logger(matches.is_present("debug"));

    // Log version information
    info!(
        logger,
        "Graph Node version: {}",
        render_testament!(TESTAMENT)
    );

    // Safe to unwrap because a value is required by CLI
    let postgres_url = matches.value_of("postgres-url").unwrap().to_string();

    let node_id = NodeId::new(matches.value_of("node-id").unwrap())
        .expect("Node ID must contain only a-z, A-Z, 0-9, and '_'");

    // Obtain subgraph related command-line arguments
    let subgraph = matches.value_of("subgraph").map(|s| s.to_owned());

    // Obtain the Ethereum parameters
    let ethereum_rpc = matches.values_of("ethereum-rpc");
    let ethereum_ipc = matches.values_of("ethereum-ipc");
    let ethereum_ws = matches.values_of("ethereum-ws");

    let block_polling_interval = Duration::from_millis(
        matches
            .value_of("ethereum-polling-interval")
            .unwrap()
            .parse()
            .expect("Ethereum polling interval must be a nonnegative integer"),
    );

    // Obtain ports to use for the GraphQL server(s)
    let http_port = matches
        .value_of("http-port")
        .unwrap()
        .parse()
        .expect("invalid GraphQL HTTP server port");
    let ws_port = matches
        .value_of("ws-port")
        .unwrap()
        .parse()
        .expect("invalid GraphQL WebSocket server port");

    // Obtain JSON-RPC server port
    let json_rpc_port = matches
        .value_of("admin-port")
        .unwrap()
        .parse()
        .expect("invalid admin port");

    // Obtain index node server port
    let index_node_port = matches
        .value_of("index-node-port")
        .unwrap()
        .parse()
        .expect("invalid index node server port");

    // Obtain metrics server port
    let metrics_port = matches
        .value_of("metrics-port")
        .unwrap()
        .parse()
        .expect("invalid metrics port");

    // Obtain DISABLE_BLOCK_INGESTOR setting
    let disable_block_ingestor: bool = matches
        .value_of("disable-block-ingestor")
        .unwrap()
        .parse()
        .expect("invalid --disable-block-ingestor/DISABLE_BLOCK_INGESTOR value");

    // Obtain STORE_CONNECTION_POOL_SIZE setting
    let store_conn_pool_size: u32 = matches
        .value_of("store-connection-pool-size")
        .unwrap()
        .parse()
        .expect("invalid --store-connection-pool-size/STORE_CONNECTION_POOL_SIZE value");

    // Minimum of two connections needed for the pool in order for the Store to bootstrap
    if store_conn_pool_size <= 1 {
        panic!("--store-connection-pool-size/STORE_CONNECTION_POOL_SIZE must be > 1")
    }

    let arweave_adapter = Arc::new(ArweaveAdapter::new(
        matches.value_of("arweave-api").unwrap().to_string(),
    ));

    let three_box_adapter = Arc::new(ThreeBoxAdapter::new(
        matches.value_of("3box-api").unwrap().to_string(),
    ));

    let pg_read_replicas: Vec<_> = matches
        .values_of("postgres-secondary-hosts")
        .into_iter()
        .flatten()
        .collect();
    let pg_host_weights: Vec<_> = matches
        .values_of("postgres-host-weights")
        .into_iter()
        .flatten()
        .map(|s| s.parse::<usize>())
        .collect::<Result<_, _>>()
        .expect("--postgres-host-weights must be a comma-separated list of integers");

    info!(logger, "Starting up");

    // Parse the IPFS URL from the `--ipfs` command line argument
    let ipfs_addresses: Vec<_> = matches
        .values_of("ipfs")
        .expect("At least one IPFS node is required")
        .map(|uri| {
            if uri.starts_with("http://") || uri.starts_with("https://") {
                String::from(uri)
            } else {
                format!("http://{}", uri)
            }
        })
        .collect();

    // Optionally, identify the Elasticsearch logging configuration
    let elastic_config =
        matches
            .value_of("elasticsearch-url")
            .map(|endpoint| ElasticLoggingConfig {
                endpoint: endpoint.into(),
                username: matches.value_of("elasticsearch-user").map(|s| s.into()),
                password: matches.value_of("elasticsearch-password").map(|s| s.into()),
            });

    // Create a component and subgraph logger factory
    let logger_factory = LoggerFactory::new(logger.clone(), elastic_config);

    // Try to create IPFS clients for each URL
    let ipfs_clients: Vec<_> = ipfs_addresses
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
        .collect();

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

    // Ethereum clients
    let mut eth_networks = EthereumNetworks::new();

    for (connection_type, values) in [
        (ConnectionType::RPC, ethereum_rpc),
        (ConnectionType::IPC, ethereum_ipc),
        (ConnectionType::WS, ethereum_ws),
    ]
    .iter()
    .cloned()
    .filter(|(_, values)| values.is_some())
    {
        let networks = parse_ethereum_networks(
            logger.clone(),
            values.unwrap(),
            connection_type,
            metrics_registry.clone(),
        )
        .await
        .expect("Failed to parse Ethereum networks");

        eth_networks.extend(networks);
    }
    eth_networks.sort();
    let eth_networks = eth_networks;

    // Set up Store
    info!(
        logger,
        "Connecting to Postgres";
        "url" => SafeDisplay(postgres_url.as_str()),
        "conn_pool_size" => store_conn_pool_size,
    );

    let connection_pool_registry = metrics_registry.clone();
    let stores_metrics_registry = metrics_registry.clone();
    let graphql_metrics_registry = metrics_registry.clone();

    let stores_logger = logger.clone();
    let stores_error_logger = logger.clone();
    let stores_eth_networks = eth_networks.clone();
    let contention_logger = logger.clone();
    let wait_stats = Arc::new(RwLock::new(MovingStats::default()));

    let postgres_conn_pool = create_connection_pool(
        "main",
        postgres_url.clone(),
        store_conn_pool_size,
        &logger,
        connection_pool_registry.cheap_clone(),
        wait_stats.cheap_clone(),
    );

    let read_only_conn_pools: Vec<_> = pg_read_replicas
        .into_iter()
        .enumerate()
        .map(|(i, host)| {
            info!(&logger, "Connecting to Postgres read replica at {}", host);
            let url = replace_host(&postgres_url, host);
            create_connection_pool(
                &format!("replica{}", i),
                url,
                store_conn_pool_size,
                &logger,
                connection_pool_registry.cheap_clone(),
                wait_stats.cheap_clone(),
            )
        })
        .collect();

    let chain_head_update_listener = Arc::new(PostgresChainHeadUpdateListener::new(
        &logger,
        stores_metrics_registry.clone(),
        postgres_url.clone(),
    ));

    let subscriptions = Arc::new(SubscriptionManager::new(
        logger.clone(),
        postgres_url.clone(),
    ));

    let expensive_queries = read_expensive_queries().unwrap();

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
            (
                network_name.to_string(),
                Arc::new(DieselStore::new(
                    StoreConfig {
                        postgres_url: postgres_url.clone(),
                        network_name: network_name.to_string(),
                    },
                    &stores_logger,
                    network_identifier,
                    chain_head_update_listener.clone(),
                    subscriptions.clone(),
                    postgres_conn_pool.clone(),
                    read_only_conn_pools.clone(),
                    pg_host_weights.clone(),
                    stores_metrics_registry.clone(),
                )),
            )
        })
        .collect()
        .map(|stores| HashMap::from_iter(stores.into_iter()))
        .and_then(move |stores| {
            let generic_store = stores.values().next().expect("error creating stores");

            let load_manager = Arc::new(LoadManager::new(
                &logger,
                wait_stats.clone(),
                expensive_queries,
                metrics_registry.clone(),
                store_conn_pool_size as usize,
            ));
            let graphql_runner = Arc::new(GraphQlRunner::new(
                &logger,
                generic_store.clone(),
                load_manager,
            ));
            let mut graphql_server = GraphQLQueryServer::new(
                &logger_factory,
                graphql_metrics_registry,
                graphql_runner.clone(),
                generic_store.clone(),
                node_id.clone(),
            );
            let subscription_server = GraphQLSubscriptionServer::new(
                &logger,
                graphql_runner.clone(),
                generic_store.clone(),
            );

            let mut index_node_server = IndexNodeServer::new(
                &logger_factory,
                graphql_runner.clone(),
                generic_store.clone(),
                node_id.clone(),
            );

            // Spawn Ethereum network indexers for all networks that are to be indexed
            if let Some(network_subgraphs) = matches.values_of("network-subgraphs") {
                network_subgraphs
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
                            stores
                                .get(&network_name)
                                .expect("store for network")
                                .clone(),
                            metrics_registry.clone(),
                            format!("network/{}", network_subgraph).into(),
                            None,
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
                    })
            };

            if !disable_block_ingestor {
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
                            stores.get(network_name).expect("network with name").clone(),
                            eth_adapter.clone(),
                            *ANCESTOR_COUNT,
                            network_name.to_string(),
                            &logger_factory,
                            block_polling_interval,
                        )
                        .expect("failed to create Ethereum block ingestor");

                        // Run the Ethereum block ingestor in the background
                        graph::spawn(block_ingestor.into_polling_stream());
                    });
            }

            let block_stream_builder = BlockStreamBuilder::new(
                generic_store.clone(),
                stores.clone(),
                eth_networks.clone(),
                node_id.clone(),
                *REORG_THRESHOLD,
                metrics_registry.clone(),
            );
            let runtime_host_builder = WASMRuntimeHostBuilder::new(
                eth_networks.clone(),
                link_resolver.clone(),
                stores.clone(),
                arweave_adapter,
                three_box_adapter,
            );

            let subgraph_instance_manager = SubgraphInstanceManager::new(
                &logger_factory,
                stores.clone(),
                eth_networks.clone(),
                runtime_host_builder,
                block_stream_builder,
                metrics_registry.clone(),
                graphql_runner.cheap_clone(),
            );

            // Create IPFS-based subgraph provider
            let mut subgraph_provider = IpfsSubgraphAssignmentProvider::new(
                &logger_factory,
                link_resolver.clone(),
                generic_store.clone(),
                graphql_runner.clone(),
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
                generic_store.clone(),
                stores,
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
async fn parse_ethereum_networks(
    logger: Logger,
    networks: clap::Values<'_>,
    connection_type: ConnectionType,
    registry: Arc<MetricsRegistry>,
) -> Result<EthereumNetworks, anyhow::Error> {
    let eth_rpc_metrics = Arc::new(ProviderEthRpcMetrics::new(registry));
    let mut parsed_networks = EthereumNetworks::new();
    for network_arg in networks {
        if network_arg.starts_with("wss://")
            || network_arg.starts_with("http://")
            || network_arg.starts_with("https://")
        {
            return Err(anyhow::anyhow!(
                "Is your Ethereum node string missing a network name? \
                     Try 'mainnet:' + the Ethereum node URL."
            ));
        } else {
            // Parse string (format is "NETWORK_NAME:NETWORK_CAPABILITIES:URL" OR
            // "NETWORK_NAME::URL" which will default to NETWORK_CAPABILITIES="archive,traces")
            let split_at = network_arg.find(':').ok_or_else(|| {
                return anyhow::anyhow!(
                    "A network name must be provided alongside the \
                         Ethereum node location. Try e.g. 'mainnet:URL'."
                );
            })?;

            let (name, rest_with_delim) = network_arg.split_at(split_at);
            let rest = &rest_with_delim[1..];
            if name.is_empty() {
                return Err(anyhow::anyhow!(
                    "Ethereum network name cannot be an empty string"
                ));
            }

            let url_split_at = rest.find(":").ok_or_else(|| {
                return anyhow::anyhow!(
                    "A network name must be provided alongside the \
                         Ethereum node location. Try e.g. 'mainnet:URL'."
                );
            })?;

            let (capabilities_str, url_str) = rest.split_at(url_split_at);
            let (url, capabilities) =
                if vec!["http", "https", "ws", "wss"].contains(&capabilities_str) {
                    (
                        rest,
                        NodeCapabilities {
                            archive: true,
                            traces: true,
                        },
                    )
                } else {
                    (&url_str[1..], capabilities_str.parse()?)
                };

            if rest.is_empty() {
                return Err(anyhow::anyhow!(
                    "Ethereum node URL cannot be an empty string"
                ));
            }

            info!(
                logger,
                "Creating transport";
                "network" => &name,
                "url" => &url,
                "capabilities" => capabilities
            );

            let (transport_event_loop, transport) = match connection_type {
                ConnectionType::RPC => Transport::new_rpc(url),
                ConnectionType::IPC => Transport::new_ipc(url),
                ConnectionType::WS => Transport::new_ws(url),
            };

            // If we drop the event loop the transport will stop working.
            // For now it's fine to just leak it.
            std::mem::forget(transport_event_loop);

            parsed_networks.insert(
                name.to_string(),
                capabilities,
                Arc::new(
                    graph_chain_ethereum::EthereumAdapter::new(
                        url,
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

#[cfg(test)]
mod test {
    use super::parse_ethereum_networks;
    use crate::ConnectionType;
    use clap::{App, Arg};
    use graph::components::ethereum::NodeCapabilities;
    use graph::log::logger;
    use graph::prelude::tokio;
    use graph_core::MetricsRegistry;
    use prometheus::Registry;
    use std::sync::Arc;

    #[tokio::test]
    async fn correctly_parse_ethereum_networks() {
        let logger = logger(true);
        let m = App::new("graph-node")
            .arg(
                Arg::with_name("ethereum-rpc")
                    .long("ethereum-rpc")
                    .multiple(true)
                    .takes_value(true),
            )
            .get_matches_from(vec![
                "graph-node",
                "--ethereum-rpc",
                "mainnet:traces:http://localhost:8545/",
                "goerli:archive:http://localhost:8546/",
            ]);

        let network_args = m.values_of("ethereum-rpc").unwrap();

        let prometheus_registry = Arc::new(Registry::new());
        let metrics_registry = Arc::new(MetricsRegistry::new(
            logger.clone(),
            prometheus_registry.clone(),
        ));

        let ethereum_networks =
            parse_ethereum_networks(logger, network_args, ConnectionType::RPC, metrics_registry)
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
