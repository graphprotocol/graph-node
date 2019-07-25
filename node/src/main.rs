extern crate clap;
extern crate env_logger;
extern crate futures;
extern crate itertools;
#[macro_use]
extern crate sentry;
extern crate graph;
extern crate graph_core;
extern crate graph_datasource_ethereum;
extern crate graph_runtime_wasm;
extern crate graph_server_http;
extern crate graph_server_json_rpc;
extern crate graph_server_websocket;
extern crate graph_store_postgres;
extern crate http;
extern crate ipfs_api;
extern crate lazy_static;
extern crate url;

use clap::{App, Arg};
use futures::sync::mpsc;
use git_testament::{git_testament, render_testament};
use ipfs_api::IpfsClient;
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::env;
use std::str::FromStr;
use std::time::Duration;

use graph::components::forward;
use graph::log::logger;
use graph::prelude::{JsonRpcServer as _, *};
use graph::tokio_executor;
use graph::tokio_timer;
use graph::tokio_timer::timer::Timer;
use graph::util::security::SafeDisplay;
use graph_core::{
    LinkResolver, SubgraphAssignmentProvider as IpfsSubgraphAssignmentProvider,
    SubgraphInstanceManager, SubgraphRegistrar as IpfsSubgraphRegistrar,
};
use graph_datasource_ethereum::{BlockStreamBuilder, Transport};
use graph_runtime_wasm::RuntimeHostBuilder as WASMRuntimeHostBuilder;
use graph_server_http::GraphQLServer as GraphQLQueryServer;
use graph_server_json_rpc::JsonRpcServer;
use graph_server_websocket::SubscriptionServer as GraphQLSubscriptionServer;
use graph_store_postgres::{Store as DieselStore, StoreConfig};

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

    // Start block to use when indexing Ethereum.
    static ref ETHEREUM_START_BLOCK: u64 = env::var("ETHEREUM_START_BLOCK")
        .ok()
        .map(|s| u64::from_str(&s)
             .unwrap_or_else(|_| panic!("failed to parse env var ETHEREUM_START_BLOCK")))
        .unwrap_or(0);
}

git_testament!(TESTAMENT);

#[derive(Debug, Clone)]
enum ConnectionType {
    IPC,
    RPC,
    WS,
}

fn main() {
    use std::sync::Mutex;
    use tokio::runtime;

    // Create components for tokio context: multi-threaded runtime, executor
    // context on the runtime, and Timer handle.
    //
    // Configure the runtime to shutdown after a panic.
    let runtime: Arc<Mutex<Option<runtime::Runtime>>> = Arc::new(Mutex::new(None));
    let handler_runtime = runtime.clone();
    *runtime.lock().unwrap() = Some(
        runtime::Builder::new()
            .core_threads(100)
            .panic_handler(move |_| {
                let runtime = handler_runtime.clone();
                std::thread::spawn(move || {
                    if let Some(runtime) = runtime.lock().unwrap().take() {
                        // Try to cleanly shutdown the runtime, but
                        // unconditionally exit after a while.
                        std::thread::spawn(|| {
                            std::thread::sleep(Duration::from_millis(3000));
                            std::process::exit(1);
                        });
                        runtime
                            .shutdown_now()
                            .wait()
                            .expect("Failed to shutdown Tokio Runtime");
                        println!("Runtime cleaned up and shutdown successfully");
                    }
                });
            })
            .build()
            .unwrap(),
    );

    let mut executor = runtime.lock().unwrap().as_ref().unwrap().executor();
    let mut enter = tokio_executor::enter()
        .expect("Failed to enter runtime executor, multiple executors at once");
    let timer = Timer::default();
    let timer_handle = timer.handle();

    // Setup runtime context with defaults and run the main application
    tokio_executor::with_default(&mut executor, &mut enter, |enter| {
        tokio_timer::with_default(&timer_handle, enter, |enter| {
            enter
                .block_on(future::lazy(|| async_main()))
                .expect("Failed to run main function");
        })
    });
}

fn async_main() -> impl Future<Item = (), Error = ()> + Send + 'static {
    env_logger::init();
    // Setup CLI using Clap, provide general info and capture postgres url
    let matches = App::new("graph-node")
        .version("0.1.0")
        .author("Graph Protocol, Inc.")
        .about("Scalable queries for a decentralized future")
        .arg(
            Arg::with_name("subgraph")
                .takes_value(true)
                .long("subgraph")
                .value_name("[NAME:]IPFS_HASH")
                .help("name and IPFS hash of the subgraph manifest"),
        )
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
            Arg::with_name("ethereum-ws")
                .takes_value(true)
                .multiple(true)
                .min_values(0)
                .required_unless_one(&["ethereum-rpc", "ethereum-ipc"])
                .conflicts_with_all(&["ethereum-rpc", "ethereum-ipc"])
                .long("ethereum-ws")
                .value_name("NETWORK_NAME:URL")
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
                .value_name("HOST:PORT")
                .help("HTTP address of an IPFS node"),
        )
        .arg(
            Arg::with_name("http-port")
                .default_value("8000")
                .long("http-port")
                .value_name("PORT")
                .help("Port for the GraphQL HTTP server"),
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
            Arg::with_name("node-id")
                .default_value("default")
                .long("node-id")
                .value_name("NODE_ID")
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
                .default_value("500")
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
                .help("Limits the maximum number of connections in the Store's connection pool"),
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
        .expect("Invalid GraphQL HTTP server port");
    let ws_port = matches
        .value_of("ws-port")
        .unwrap()
        .parse()
        .expect("Invalid GraphQL WebSocket server port");

    // Obtain JSON-RPC server port
    let json_rpc_port = matches
        .value_of("admin-port")
        .unwrap()
        .parse()
        .expect("invalid admin port");

    debug!(logger, "Setting up Sentry");

    // Obtain DISABLE_BLOCK_INGESTOR setting
    let disable_block_ingestor: bool = matches
        .value_of("disable-block-ingestor")
        .unwrap()
        .parse()
        .expect("Invalid disable block ingestor value");

    // Obtain STORE_CONNECTION_POOL_SIZE setting
    let store_conn_pool_size = matches
        .value_of("store-connection-pool-size")
        .unwrap()
        .parse()
        .expect("Invalid store connection pool size value");

    // Minimum of two connections needed for the pool in order for the Store to bootstrap
    assert!(store_conn_pool_size > 1);

    // Set up Sentry, with release tracking and panic handling;
    // fall back to an empty URL, which will result in no errors being reported
    let sentry_url = env::var_os("THEGRAPH_SENTRY_URL").unwrap_or_else(|| "".into());
    let _sentry = sentry::init((
        sentry_url,
        sentry::ClientOptions {
            release: sentry_crate_release!(),
            ..Default::default()
        },
    ));
    sentry::integrations::panic::register_panic_handler();
    info!(logger, "Starting up");

    // Parse the IPFS URL from the `--ipfs` command line argument
    let ipfs_address = matches
        .value_of("ipfs")
        .map(|uri| {
            if uri.starts_with("http://") || uri.starts_with("https://") {
                String::from(uri)
            } else {
                format!("http://{}", uri)
            }
        })
        .unwrap()
        .to_owned();

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

    info!(
        logger,
        "Trying IPFS node at: {}",
        SafeDisplay(&ipfs_address)
    );

    // Try to create an IPFS client for this URL
    let ipfs_client = match IpfsClient::new_from_uri(ipfs_address.as_ref()) {
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
    let ipfs_test = ipfs_client.version();
    let ipfs_ok_logger = logger.clone();
    let ipfs_err_logger = logger.clone();
    let ipfs_address_for_ok = ipfs_address.clone();
    let ipfs_address_for_err = ipfs_address.clone();
    tokio::spawn(
        ipfs_test
            .map_err(move |e| {
                error!(
                    ipfs_err_logger,
                    "Is there an IPFS node running at \"{}\"?",
                    SafeDisplay(ipfs_address_for_err),
                );
                panic!("Failed to connect to IPFS: {}", e);
            })
            .map(move |_| {
                info!(
                    ipfs_ok_logger,
                    "Successfully connected to IPFS node at: {}",
                    SafeDisplay(ipfs_address_for_ok)
                );
            }),
    );

    // Convert the client into a link resolver
    let link_resolver = Arc::new(LinkResolver::from(ipfs_client));

    let eth_adapters = [
        (ConnectionType::RPC, ethereum_rpc),
        (ConnectionType::IPC, ethereum_ipc),
        (ConnectionType::WS, ethereum_ws),
    ]
    .iter()
    .cloned()
    .filter(|(_, values)| values.is_some())
    .fold(HashMap::new(), |adapters, (connection_type, values)| {
        match parse_ethereum_networks_and_nodes(logger.clone(), values.unwrap(), connection_type) {
            Ok(adapter) => adapters.into_iter().chain(adapter).collect(),
            Err(e) => {
                panic!(
                    "Failed to parse Ethereum networks and create Ethereum adapters: {}",
                    e
                );
            }
        }
    });

    // Warn if the start block is != genesis
    if *ETHEREUM_START_BLOCK > 0 {
        warn!(
            logger,
            "Using {} as the block to start indexing at. \
             This may cause subgraphs to be only indexed partially",
            *ETHEREUM_START_BLOCK,
        );
    }

    // Set up Store
    info!(
        logger,
        "Connecting to Postgres";
        "url" => SafeDisplay(postgres_url.as_str()),
        "conn_pool_size" => store_conn_pool_size,
    );

    let stores: HashMap<String, Arc<DieselStore>> = eth_adapters
        .iter()
        .map(|(network_name, eth_adapter)| {
            info!(
                logger, "Connecting to Ethereum...";
                "network" => &network_name,
            );
            match eth_adapter.net_identifiers(&logger).wait() {
                Ok(network_identifier) => {
                    info!(
                    logger,
                    "Connected to Ethereum";
                    "network" => &network_name,
                    "network_version" => &network_identifier.net_version,
                    );
                    (
                        network_name.to_string(),
                        Arc::new(DieselStore::new(
                            StoreConfig {
                                postgres_url: postgres_url.clone(),
                                network_name: network_name.to_string(),
                                start_block: *ETHEREUM_START_BLOCK,
                                conn_pool_size: store_conn_pool_size,
                            },
                            &logger,
                            network_identifier,
                        )),
                    )
                }
                Err(e) => {
                    error!(logger, "Was a valid Ethereum node provided?");
                    panic!("Failed to connect to Ethereum node: {}", e);
                }
            }
        })
        .collect();

    let generic_store = stores.values().next().expect("error creating stores");

    let graphql_runner = Arc::new(graph_core::GraphQlRunner::new(
        &logger,
        generic_store.clone(),
    ));
    let mut graphql_server = GraphQLQueryServer::new(
        &logger_factory,
        graphql_runner.clone(),
        generic_store.clone(),
        node_id.clone(),
    );
    let mut subscription_server =
        GraphQLSubscriptionServer::new(&logger, graphql_runner.clone(), generic_store.clone());

    if !disable_block_ingestor {
        // BlockIngestor must be configured to keep at least REORG_THRESHOLD ancestors,
        // otherwise BlockStream will not work properly.
        // BlockStream expects the blocks after the reorg threshold to be present in the
        // database.
        assert!(*ANCESTOR_COUNT >= *REORG_THRESHOLD);

        info!(logger, "Starting block ingestor");

        // Create Ethereum block ingestors and spawn a thread to run each
        eth_adapters.iter().for_each(|(network_name, eth_adapter)| {
            let block_ingestor = graph_datasource_ethereum::BlockIngestor::new(
                stores.get(network_name).expect("network with name").clone(),
                eth_adapter.clone(),
                *ANCESTOR_COUNT,
                network_name.to_string(),
                &logger_factory,
                block_polling_interval,
            )
            .expect("failed to create Ethereum block ingestor");

            // Run the Ethereum block ingestor in the background
            tokio::spawn(block_ingestor.into_polling_stream());
        });
    }

    let block_stream_builder = BlockStreamBuilder::new(
        generic_store.clone(),
        stores.clone(),
        eth_adapters.clone(),
        node_id.clone(),
        *REORG_THRESHOLD,
    );
    let runtime_host_builder =
        WASMRuntimeHostBuilder::new(eth_adapters.clone(), link_resolver.clone(), stores.clone());

    let subgraph_instance_manager = SubgraphInstanceManager::new(
        &logger_factory,
        stores.clone(),
        runtime_host_builder,
        block_stream_builder,
    );

    // Create IPFS-based subgraph provider
    let mut subgraph_provider = IpfsSubgraphAssignmentProvider::new(
        &logger_factory,
        link_resolver.clone(),
        generic_store.clone(),
        graphql_runner.clone(),
    );

    // Forward subgraph events from the subgraph provider to the subgraph instance manager
    tokio::spawn(forward(&mut subgraph_provider, &subgraph_instance_manager).unwrap());

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
        node_id.clone(),
        version_switching_mode,
    ));
    tokio::spawn(
        subgraph_registrar
            .start()
            .then(|start_result| Ok(start_result.expect("failed to initialize subgraph provider"))),
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
            SubgraphDeploymentId::new(hash).expect("Subgraph hash must be a valid IPFS hash");

        tokio::spawn(
            subgraph_registrar
                .create_subgraph(name.clone())
                .then(
                    |result| Ok(result.expect("Failed to create subgraph from `--subgraph` flag")),
                )
                .and_then(move |_| {
                    subgraph_registrar.create_subgraph_version(name, subgraph_id, node_id)
                })
                .then(|result| {
                    Ok(result.expect("Failed to deploy subgraph from `--subgraph` flag"))
                }),
        );
    }

    // Serve GraphQL queries over HTTP
    tokio::spawn(
        graphql_server
            .serve(http_port, ws_port)
            .expect("Failed to start GraphQL query server"),
    );

    // Serve GraphQL subscriptions over WebSockets
    tokio::spawn(
        subscription_server
            .serve(ws_port)
            .expect("Failed to start GraphQL subscription server"),
    );

    // Periodically check for contention in the tokio threadpool. First spawn a
    // task that simply responds to "ping" requests. Then spawn a separate
    // thread to periodically ping it and check responsiveness.
    let (ping_send, ping_receive) = mpsc::channel::<crossbeam_channel::Sender<()>>(1);
    tokio::spawn(
        ping_receive
            .for_each(move |pong_send| pong_send.clone().send(()).map(|_| ()).map_err(|_| ())),
    );
    let contention_logger = logger.clone();
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(1));
        let (pong_send, pong_receive) = crossbeam_channel::bounded(1);
        if ping_send.clone().send(pong_send).wait().is_err() {
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
            }
        }
    });

    future::empty()
}

/// Parses an Ethereum connection string and returns the network name and Ethereum adapter.
fn parse_ethereum_networks_and_nodes(
    logger: Logger,
    networks: clap::Values,
    connection_type: ConnectionType,
) -> Result<
    HashMap<
        String,
        Arc<graph_datasource_ethereum::EthereumAdapter<graph_datasource_ethereum::Transport>>,
    >,
    Error,
> {
    networks
        .map(|network| {
            if network.starts_with("wss://")
                || network.starts_with("http://")
                || network.starts_with("https://")
            {
                return Err(format_err!(
                    "Is your Ethereum node string missing a network name? \
                     Try 'mainnet:' + the Ethereum node URL."
                ));
            } else {
                // Parse string (format is "NETWORK_NAME:URL")
                let split_at = network.find(':').ok_or_else(|| {
                    return format_err!(
                        "A network name must be provided alongside the \
                         Ethereum node location. Try e.g. 'mainnet:URL'."
                    );
                })?;

                let (name, loc_with_delim) = network.split_at(split_at);
                let loc = &loc_with_delim[1..];

                if name.is_empty() {
                    return Err(format_err!(
                        "Ethereum network name cannot be an empty string"
                    ));
                }

                if loc.is_empty() {
                    return Err(format_err!("Ethereum node URL cannot be an empty string"));
                }

                info!(
                    logger,
                    "Creating transport";
                    "network" => &name,
                    "url" => &loc,
                );

                let (transport_event_loop, transport) = match connection_type {
                    ConnectionType::RPC => Transport::new_rpc(loc),
                    ConnectionType::IPC => Transport::new_ipc(loc),
                    ConnectionType::WS => Transport::new_ws(loc),
                };

                // If we drop the event loop the transport will stop working.
                // For now it's fine to just leak it.
                std::mem::forget(transport_event_loop);

                Ok((
                    name.to_string(),
                    Arc::new(graph_datasource_ethereum::EthereumAdapter::new(
                        transport,
                        *ETHEREUM_START_BLOCK,
                    )),
                ))
            }
        })
        .collect()
}
