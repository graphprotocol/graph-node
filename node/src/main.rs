use std::collections::{HashMap, HashSet};
use std::env;
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::mpsc;

use clap::{App, Arg};
use git_testament::{git_testament, render_testament};
use lazy_static::lazy_static;
use prometheus::Registry;

use graph::log::logger;
use graph::prelude::{
    GraphQLServer as _, IndexNodeServer as _, JsonRpcServer as _, NetworkRegistry as _,
    NetworkStoreFactory as _, *,
};
use graph::util::security::SafeDisplay;
use graph_chain_arweave::adapter::ArweaveAdapter;
use graph_chain_ethereum as ethereum;
use graph_core::{
    three_box::ThreeBoxAdapter, LinkResolver, MetricsRegistry, NetworkRegistry,
    SubgraphAssignmentProvider as IpfsSubgraphAssignmentProvider,
    SubgraphRegistrar as IpfsSubgraphRegistrar,
};
use graph_server_http::GraphQLServer as GraphQLQueryServer;
use graph_server_index_node::IndexNodeServer;
use graph_server_json_rpc::JsonRpcServer;
use graph_server_metrics::PrometheusMetricsServer;
use graph_server_websocket::SubscriptionServer as GraphQLSubscriptionServer;
use graph_store_postgres::connection_pool::create_connection_pool;
use graph_store_postgres::{NetworkStoreFactory, NetworkStoreFactoryOptions};

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

#[tokio::main]
async fn main() {
    env_logger::init();

    // Setup CLI using Clap, provide general info and capture postgres url
    let matches = App::new("graph-node")
        .version(render_testament!(TESTAMENT).as_str())
        .author("Graph Protocol, Inc.")
        .about("Scalable queries for a decentralized future")
        .arg(
            Arg::with_name("postgres-url")
                .takes_value(true)
                .required(true)
                .long("postgres-url")
                .value_name("URL")
                .help("Location of the Postgres database used for storing entities"),
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
                .help(
                    "Ethereum network name (e.g. 'mainnet') and \
                     Ethereum RPC URL, separated by a ':'",
                ),
        )
        .arg(
            Arg::with_name("ipfs")
                .takes_value(true)
                .required(true)
                .long("ipfs")
                .multiple(true)
                .value_name("HOST:PORT")
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
                .help("A unique identifier for this node"),
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
                .value_name("NETWORK_INSTANCE_NAME")
                .help(
                    "One or more network instance names to index using built-in subgraphs \
                     (e.g. ethereum/mainnet).",
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

    // Obtain Ethereum chains to connect to
    let ethereum_rpc = matches.values_of("ethereum-rpc");

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

    // Identify whether block ingestion should be enabled (default) or disabled
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

    info!(logger, "Starting up");

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

    // Set up Prometheus registry
    let prometheus_registry = Arc::new(Registry::new());
    let metrics_registry = Arc::new(MetricsRegistry::new(
        logger.clone(),
        prometheus_registry.clone(),
    ));
    let mut metrics_server =
        PrometheusMetricsServer::new(&logger_factory, prometheus_registry.clone());

    info!(
        logger,
        "Connecting to Postgres";
        "conn_pool_size" => store_conn_pool_size,
        "url" => SafeDisplay(postgres_url.as_str()),
    );

    // Connect to Postgres
    let postgres_conn_pool = create_connection_pool(
        postgres_url.as_str(),
        store_conn_pool_size,
        &logger,
        metrics_registry.clone(),
    );

    info!(
        logger,
        "Connected to Postgres";
        "url" => SafeDisplay(postgres_url.as_str())
    );

    // Create a factory for network stores
    let mut store_factory = NetworkStoreFactory::new(NetworkStoreFactoryOptions {
        logger: logger.clone(),
        url: postgres_url,
        conn_pool: postgres_conn_pool,
        metrics_registry: metrics_registry.clone(),
    });

    // Parse the IPFS URLs
    let ipfs_urls: Vec<_> = matches
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

    // Connect to IPFS nodes
    let link_resolver = Arc::new(LinkResolver::from_urls(&logger, ipfs_urls).await);

    // Parse network subgraphs into network instance IDs
    let network_subgraphs: HashSet<NetworkInstanceId> = matches
        .values_of("network-subgraphs")
        .map_or(Default::default(), |values| {
            values
                .map(|s| {
                    NetworkInstanceId::try_from(s).expect("Failed to parse --network-subgraphs")
                })
                .collect()
        });

    // Establish registry with all networks that the node was configured to index
    let mut network_registry = NetworkRegistry::new();

    // Add selected Ethereum chains to the network registry
    if let Some(descriptors) = ethereum_rpc.clone() {
        for s in descriptors {
            let descriptor =
                ethereum::chain::parse_descriptor(s).expect("failed to parse Ethereum descriptor");

            info!(
                logger,
                "Connecting to Ethereum chain";
                "url" => SafeDisplay(&descriptor.url),
                "name" => &descriptor.id.name,
            );

            let conn = ethereum::chain::connect(ethereum::chain::ConnectOptions {
                descriptor,
                logger: &logger,
                metrics_registry: metrics_registry.clone(),
            })
            .await
            .expect("failed to connect to Ethereum chain");

            info!(
                 logger,
                 "Successfully connected to Ethereum chain";
                 "url" => SafeDisplay(&conn.url),
                 "name" => &conn.id.name,
                 "version" => &conn.version,
                 "genesis_block" => format!("{}", &conn.genesis_block),
            );

            let store = store_factory
                .blockchain_store(&conn.id)
                .await
                .expect("failed to create store for Ethereum chain");

            let chain_indexing = network_subgraphs.contains(&conn.id);

            let chain = ethereum::chain::Chain::new(ethereum::chain::ChainOptions {
                conn,
                logger_factory: &logger_factory,
                store,
                metrics_registry: metrics_registry.clone(),
                link_resolver: link_resolver.clone(),
                arweave_adapter: arweave_adapter.clone(),
                three_box_adapter: three_box_adapter.clone(),
                block_ingestion: !disable_block_ingestor,
                chain_indexing,
            })
            .await
            .expect("failed to connect to Ethereum chain");

            // Add the chain to the registry
            network_registry.register_instance(Box::new(chain));
        }
    }

    // Seal the network registry
    let network_registry = Arc::new(network_registry);

    // Check tokio contention in the background
    monitor_tokio_contention(logger.clone());

    // Obtain selected Ethereum chains
    let ethereum_chains = network_registry.instances("ethereum");

    // Obtain stores for these chains
    let mut stores = HashMap::new();
    for chain in ethereum_chains.iter() {
        stores.insert(
            chain.id().name.clone(),
            store_factory
                .blockchain_store(chain.id())
                .await
                .expect("failed to get store for Ethereum chain"),
        );
    }

    // Use one of the stores (doesn't matter which one) for GraphQL
    // queries, subscriptions etc.
    let generic_store = stores.values().next().expect("error creating stores");

    let graphql_runner = Arc::new(graph_core::GraphQlRunner::new(
        &logger,
        generic_store.clone(),
    ));

    // Serve GraphQL queries over HTTP
    let mut graphql_server = GraphQLQueryServer::new(
        &logger_factory,
        metrics_registry.clone(),
        graphql_runner.clone(),
        generic_store.clone(),
        node_id.clone(),
    );
    graph::spawn(
        graphql_server
            .serve(http_port, ws_port)
            .expect("Failed to start GraphQL query server")
            .compat(),
    );

    let subscription_server =
        GraphQLSubscriptionServer::new(&logger, graphql_runner.clone(), generic_store.clone());
    let mut index_node_server = IndexNodeServer::new(
        &logger_factory,
        graphql_runner.clone(),
        generic_store.clone(),
        node_id.clone(),
    );

    let ethereum_adapters = HashMap::from_iter(ethereum_chains.iter().map(|chain| {
        (
            chain.id().name.clone(),
            chain.compat_ethereum_adapter().unwrap(),
        )
    }));

    // Create subgraph provider
    let provider = IpfsSubgraphAssignmentProvider::new(
        &logger_factory,
        link_resolver.clone(),
        network_registry.clone(),
        generic_store.clone(),
        graphql_runner.clone(),
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
        Arc::new(provider),
        generic_store.clone(),
        stores,
        ethereum_adapters.clone(),
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

    futures::future::pending::<()>().await;
}

fn monitor_tokio_contention(logger: Logger) {
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
            debug!(logger, "Shutting down contention checker thread");
            break;
        }
        let mut timeout = Duration::from_millis(10);
        while pong_receive.recv_timeout(timeout)
            == Err(crossbeam_channel::RecvTimeoutError::Timeout)
        {
            debug!(logger, "Possible contention in tokio threadpool";
                                     "timeout_ms" => timeout.as_millis(),
                                     "code" => LogCode::TokioContention);
            if timeout < Duration::from_secs(10) {
                timeout *= 10;
            } else if std::env::var_os("GRAPH_KILL_IF_UNRESPONSIVE").is_some() {
                // The node is unresponsive, kill it in hopes it will be restarted.
                crit!(logger, "Node is unresponsive, killing process");
                std::process::abort()
            }
        }
    });
}
