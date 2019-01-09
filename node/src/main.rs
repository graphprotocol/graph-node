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
extern crate url;

use clap::{App, Arg};
use futures::sync::oneshot;
use ipfs_api::IpfsClient;
use itertools::FoldWhile::{Continue, Done};
use itertools::Itertools;
use std::env;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

use graph::components::forward;
use graph::prelude::{JsonRpcServer as JsonRpcServerTrait, *};
use graph::tokio_executor;
use graph::tokio_timer;
use graph::tokio_timer::timer::Timer;
use graph::util::log::{guarded_logger, logger, register_panic_hook};
use graph_core::{
    ElasticLoggingConfig, SubgraphDeploymentProvider as IpfsSubgraphDeploymentProvider,
    SubgraphInstanceManager, SubgraphRegistrar as IpfsSubgraphRegistrar,
};
use graph_datasource_ethereum::{BlockStreamBuilder, Transport};
use graph_runtime_wasm::RuntimeHostBuilder as WASMRuntimeHostBuilder;
use graph_server_http::GraphQLServer as GraphQLQueryServer;
use graph_server_json_rpc::JsonRpcServer;
use graph_server_websocket::SubscriptionServer as GraphQLSubscriptionServer;
use graph_store_postgres::{Store as DieselStore, StoreConfig};

fn main() {
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    // Register guarded panic logger which ensures logs flush on shutdown
    let (panic_logger, _panic_guard) = guarded_logger();
    register_panic_hook(panic_logger, shutdown_sender);

    // Create components for tokio context: multi-threaded runtime,
    // executor context on the runtime, and Timer handle.
    let runtime = tokio::runtime::Runtime::new().expect("Failed to create runtime");
    let mut executor = runtime.executor();
    let mut enter = tokio_executor::enter()
        .expect("Failed to enter runtime executor, multiple executors at once");
    let timer = Timer::default();
    let timer_handle = timer.handle();

    // Shutdown the runtime after a panic
    std::thread::spawn(|| {
        let shutdown_logger = logger(false);
        shutdown_receiver
            .wait()
            .map(|_| {
                let _ = runtime
                    .shutdown_now()
                    .wait()
                    .expect("Failed to shutdown Tokio Runtime");
                info!(
                    shutdown_logger,
                    "Runtime cleaned up and shutdown successfully"
                );
            })
            .expect("Runtime shutdown process did not finish");
    });

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
        .get_matches();

    // Set up logger
    let logger = logger(matches.is_present("debug"));

    // Safe to unwrap because a value is required by CLI
    let postgres_url = matches.value_of("postgres-url").unwrap().to_string();

    let node_id = NodeId::new(matches.value_of("node-id").unwrap())
        .expect("Node ID must contain only a-z, A-Z, 0-9, and '_'");

    // Obtain subgraph related command-line arguments
    let subgraph = matches.value_of("subgraph").map(|s| s.to_owned());

    // Obtain the Ethereum parameters
    let ethereum_rpc = matches.value_of("ethereum-rpc");
    let ethereum_ipc = matches.value_of("ethereum-ipc");
    let ethereum_ws = matches.value_of("ethereum-ws");

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

    debug!(logger, "Setting up Sentry");

    // Set up Sentry, with release tracking and panic handling;
    // fall back to an empty URL, which will result in no errors being reported
    let sentry_url = env::var_os("THEGRAPH_SENTRY_URL")
        .or_else(|| Some("".into()))
        .unwrap();
    let _sentry = sentry::init((
        sentry_url,
        sentry::ClientOptions {
            release: sentry_crate_release!(),
            ..Default::default()
        },
    ));
    sentry::integrations::panic::register_panic_handler();
    info!(logger, "Starting up");

    // Try to create an IPFS client for one of the resolved IPFS addresses
    let ipfs_address = matches.value_of("ipfs").unwrap();
    let (ipfs_client, ipfs_address) = match ipfs_address
        // Resolve the IPFS address into socket addresses
        .to_socket_addrs()
        .unwrap_or_else(|e| panic!("failed to resolve IPFS address {}: {}", ipfs_address, e))
        // Try to create an IPFS client for one of these addresses; collect
        // errors in case we can't create a client for any of them
        .fold_while(Err(vec![]), |result, address| {
            info!(logger, "Trying IPFS node at: {}", address);

            match IpfsClient::new(&format!("{}", address.ip()), address.port()) {
                Ok(client) => Done(Ok((Arc::new(client), address))),
                Err(e) => Continue(result.map_err(|mut errors| {
                    errors.push((address, e));
                    errors
                })),
            }
        })
        .into_inner()
    {
        Ok((client, address)) => (client, address),
        Err(errors) => {
            for (address, e) in errors.iter() {
                error!(
                    logger, "Failed to create IPFS client for address: {}", address;
                    "error" => format!("{}", e),
                )
            }
            panic!("Could not connect to IPFS");
        }
    };

    // Test the IPFS client by getting the version from the IPFS daemon
    let ipfs_test = ipfs_client.version();
    let ipfs_ok_logger = logger.clone();
    let ipfs_err_logger = logger.clone();
    tokio::spawn(
        ipfs_test
            .map_err(move |e| {
                error!(
                    ipfs_err_logger,
                    "Is there an IPFS node running at \"{}\"?", ipfs_address
                );
                panic!("Failed to connect to IPFS: {}", e);
            })
            .map(move |_| {
                info!(
                    ipfs_ok_logger,
                    "Successfully connected to IPFS node at: {}", ipfs_address
                );
            }),
    );

    // Parse the Ethereum URL
    let (ethereum_network_name, ethereum_node_url) = parse_ethereum_network_and_node(
        [ethereum_ipc, ethereum_rpc, ethereum_ws]
            .into_iter()
            .filter_map(|x| x.to_owned())
            .next()
            .expect("one of --ethereum-ipc, --ethereum-rpc or --ethereum-ws must be provided"),
    )
    .expect("failed to parse Ethereum connection string");

    // Set up Ethereum transport
    let (transport_event_loop, transport) = ethereum_ipc
        .map(|_| Transport::new_ipc(ethereum_node_url))
        .or_else(|| ethereum_ws.map(|_| Transport::new_ws(ethereum_node_url)))
        .or_else(|| ethereum_rpc.map(|_| Transport::new_rpc(ethereum_node_url)))
        .expect("One of --ethereum-ipc, --ethereum-rpc or --ethereum-ws must be provided");

    // If we drop the event loop the transport will stop working.
    // For now it's fine to just leak it.
    std::mem::forget(transport_event_loop);

    // Create Ethereum adapter
    let ethereum = Arc::new(graph_datasource_ethereum::EthereumAdapter::new(
        transport.clone(),
    ));

    // Ask Ethereum node for network identifiers
    info!(
        logger, "Connecting to Ethereum...";
        "network" => &ethereum_network_name,
        "node" => &ethereum_node_url,
    );
    let eth_net_identifiers = match ethereum.net_identifiers(&logger).wait() {
        Ok(net) => {
            info!(
                logger, "Connected to Ethereum";
                "network" => &ethereum_network_name,
                "node" => &ethereum_node_url,
            );
            net
        }
        Err(e) => {
            error!(logger, "Was a valid Ethereum node provided?");
            panic!("Failed to connect to Ethereum node: {}", e);
        }
    };

    // Set up Store
    info!(logger, "Connecting to Postgres"; "url" => &postgres_url);
    let store = Arc::new(DieselStore::new(
        StoreConfig {
            postgres_url,
            network_name: ethereum_network_name.to_owned(),
        },
        &logger,
        eth_net_identifiers,
    ));
    let graphql_runner = Arc::new(graph_core::GraphQlRunner::new(&logger, store.clone()));
    let mut graphql_server = GraphQLQueryServer::new(
        &logger,
        graphql_runner.clone(),
        store.clone(),
        node_id.clone(),
    );
    let mut subscription_server =
        GraphQLSubscriptionServer::new(&logger, graphql_runner.clone(), store.clone());

    // Create Ethereum block ingestor
    let block_ingestor = graph_datasource_ethereum::BlockIngestor::new(
        store.clone(),
        transport.clone(),
        50, // ancestor count, which we could make configuable
        logger.clone(),
        block_polling_interval,
    )
    .expect("failed to create Ethereum block ingestor");

    // Run the Ethereum block ingestor in the background
    tokio::spawn(block_ingestor.into_polling_stream());

    // Prepare a block stream builder for subgraphs
    let block_stream_builder = BlockStreamBuilder::new(
        store.clone(),
        store.clone(),
        ethereum.clone(),
        node_id.clone(),
    );

    // Optionally, identify the Elasticsearch logging configuration
    let elastic_config =
        matches
            .value_of("elasticsearch-url")
            .map(|endpoint| ElasticLoggingConfig {
                endpoint: endpoint.into(),
                username: matches.value_of("elasticsearch-user").map(|s| s.into()),
                password: matches.value_of("elasticsearch-password").map(|s| s.into()),
            });

    // Prepare for hosting WASM runtimes and managing subgraph instances
    let runtime_host_builder =
        WASMRuntimeHostBuilder::new(ethereum.clone(), ipfs_client.clone(), store.clone());
    let subgraph_instance_manager = SubgraphInstanceManager::new(
        &logger,
        store.clone(),
        runtime_host_builder,
        block_stream_builder,
        elastic_config,
    );

    // Create IPFS-based subgraph provider
    let mut subgraph_provider =
        IpfsSubgraphDeploymentProvider::new(logger.clone(), ipfs_client.clone(), store.clone());

    // Forward subgraph events from the subgraph provider to the subgraph instance manager
    tokio::spawn(forward(&mut subgraph_provider, &subgraph_instance_manager).unwrap());

    // Create named subgraph provider for resolving subgraph name->ID mappings
    let subgraph_registrar = Arc::new(IpfsSubgraphRegistrar::new(
        logger.clone(),
        ipfs_client,
        Arc::new(subgraph_provider),
        store.clone(),
        store.clone(),
        node_id.clone(),
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
        let subgraph_id = SubgraphId::new(hash).expect("Subgraph hash must be a valid IPFS hash");

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

    future::empty()
}

/// Parses an Ethereum connection string and returns the network name and Ethereum node.
fn parse_ethereum_network_and_node(s: &str) -> Result<(&str, &str), Error> {
    // Check for common Ethereum node mistakes
    if s.starts_with("wss://") || s.starts_with("http://") || s.starts_with("https://") {
        return Err(format_err!(
            "Is your Ethereum node string missing a network name? \
             Try 'mainnet:' + the Ethereum node URL."
        ));
    }

    // Parse string (format is "NETWORK_NAME:URL")
    let split_at = s.find(':').ok_or_else(|| {
        format_err!(
            "A network name must be provided alongside the \
             Ethereum node location. Try e.g. 'mainnet:URL'."
        )
    })?;
    let (name, loc_with_delim) = s.split_at(split_at);
    let loc = &loc_with_delim[1..];

    if name.is_empty() {
        return Err(format_err!(
            "Ethereum network name cannot be an empty string"
        ));
    }

    if loc.is_empty() {
        return Err(format_err!("Ethereum node URL cannot be an empty string"));
    }

    Ok((name, loc))
}
