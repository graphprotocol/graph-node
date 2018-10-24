extern crate clap;
extern crate env_logger;
#[macro_use]
extern crate failure;
extern crate futures;
extern crate itertools;
extern crate reqwest;
#[macro_use]
extern crate sentry;
extern crate graph;
extern crate graph_core;
extern crate graph_datasource_ethereum;
extern crate graph_mock;
extern crate graph_runtime_wasm;
extern crate graph_server_http;
extern crate graph_server_json_rpc;
extern crate graph_server_websocket;
extern crate graph_store_postgres;
extern crate http;
extern crate ipfs_api;
extern crate url;

use clap::{App, Arg};
use ipfs_api::IpfsClient;
use itertools::FoldWhile::{Continue, Done};
use itertools::Itertools;
use reqwest::Client;
use std::env;
use std::net::ToSocketAddrs;
use std::time::Duration;
use url::Url;

use graph::components::forward;
use graph::prelude::{JsonRpcServer as JsonRpcServerTrait, *};
use graph::util::log::{guarded_logger, logger, register_panic_hook};
use graph_core::{SubgraphInstanceManager, SubgraphProvider as IpfsSubgraphProvider};
use graph_datasource_ethereum::{BlockStreamBuilder, Transport};
use graph_runtime_wasm::RuntimeHostBuilder as WASMRuntimeHostBuilder;
use graph_server_http::GraphQLServer as GraphQLQueryServer;
use graph_server_json_rpc::{subgraph_deploy_request, JsonRpcServer};
use graph_server_websocket::SubscriptionServer as GraphQLSubscriptionServer;
use graph_store_postgres::{Store as DieselStore, StoreConfig};

fn main() {
    let (panic_logger, _panic_guard) = guarded_logger();
    register_panic_hook(panic_logger);
    tokio::run(future::lazy(|| async_main()))
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
        ).arg(
            Arg::with_name("postgres-url")
                .takes_value(true)
                .required(true)
                .long("postgres-url")
                .value_name("URL")
                .help("Location of the Postgres database used for storing entities"),
        ).arg(
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
        ).arg(
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
        ).arg(
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
        ).arg(
            Arg::with_name("ipfs")
                .takes_value(true)
                .required(true)
                .long("ipfs")
                .value_name("HOST:PORT")
                .help("HTTP address of an IPFS node"),
        ).arg(
            Arg::with_name("admin-port")
                .default_value("8020")
                .long("admin-port")
                .value_name("PORT")
                .help("port for the admin JSON-RPC server"),
        ).arg(
            Arg::with_name("debug")
                .long("debug")
                .help("Enable debug logging"),
        ).get_matches();

    // Set up logger
    let logger = logger(matches.is_present("debug"));

    // Safe to unwrap because a value is required by CLI
    let postgres_url = matches.value_of("postgres-url").unwrap().to_string();

    // Obtain subgraph related command-line arguments
    let subgraph = matches.value_of("subgraph");

    // Obtain the Ethereum parameters
    let ethereum_rpc = matches.value_of("ethereum-rpc");
    let ethereum_ipc = matches.value_of("ethereum-ipc");
    let ethereum_ws = matches.value_of("ethereum-ws");

    // Parse rpc port
    let json_rpc_port = matches
        .value_of("admin-port")
        .unwrap()
        .parse()
        .expect("invalid admin port");

    debug!(logger, "Setting up Sentry");

    // Set up Sentry, with release tracking and panic handling;
    // fall back to an empty URL, which will result in no errors being reported
    let sentry_url = env::var_os("THEGRAPH_SENTRY_URL")
        .or(Some("".into()))
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
        .expect(&format!("failed to parse IPFS address: {}", ipfs_address))
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
        }).into_inner()
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
            }).map(move |_| {
                info!(
                    ipfs_ok_logger,
                    "Successfully connected to IPFS node at: {}", ipfs_address
                );
            }),
    );

    let mut subgraph_provider = IpfsSubgraphProvider::new(logger.clone(), ipfs_client.clone());

    // Parse the Ethereum URL
    let (ethereum_network_name, ethereum_node_url) = parse_ethereum_network_and_node(
        [ethereum_ipc, ethereum_rpc, ethereum_ws]
            .into_iter()
            .filter_map(|x| x.to_owned())
            .next()
            .expect("one of --ethereum-ipc, --ethereum-rpc or --ethereum-ws must be provided"),
    ).expect("failed to parse Ethereum connection string");

    // Set up Ethereum transport
    let (transport_event_loop, transport) = ethereum_ipc
        .map(|_| Transport::new_ipc(ethereum_node_url))
        .or(ethereum_ws.map(|_| Transport::new_ws(ethereum_node_url)))
        .or(ethereum_rpc.map(|_| Transport::new_rpc(ethereum_node_url)))
        .expect("One of --ethereum-ipc, --ethereum-rpc or --ethereum-ws must be provided");

    // If we drop the event loop the transport will stop working.
    // For now it's fine to just leak it.
    std::mem::forget(transport_event_loop);

    // Create Ethereum adapter
    let ethereum = Arc::new(graph_datasource_ethereum::EthereumAdapter::new(
        &logger,
        graph_datasource_ethereum::EthereumAdapterConfig {
            transport: transport.clone(),
        },
    ));

    // Ask Ethereum node for network identifiers
    info!(
        logger, "Connecting to Ethereum...";
        "network" => &ethereum_network_name,
        "node" => &ethereum_node_url,
    );
    let eth_net_identifiers = match ethereum.net_identifiers().wait() {
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
            url: postgres_url,
            network_name: ethereum_network_name.to_owned(),
        },
        &logger,
        eth_net_identifiers,
    ));
    let graphql_runner = Arc::new(graph_core::GraphQlRunner::new(&logger, store.clone()));
    let mut graphql_server = GraphQLQueryServer::new(&logger, graphql_runner.clone());
    let mut subscription_server = GraphQLSubscriptionServer::new(&logger, graphql_runner.clone());

    // Create Ethereum block ingestor
    let block_ingestor = graph_datasource_ethereum::BlockIngestor::new(
        store.clone(),
        transport.clone(),
        50, // ancestor count, which we could make configuable
        logger.clone(),
        Duration::from_millis(500), // polling interval, which we could make configurable
    ).expect("failed to create Ethereum block ingestor");

    // Run the Ethereum block ingestor in the background
    tokio::spawn(block_ingestor.into_polling_stream());

    // Prepare a block stream builder for subgraphs
    let block_stream_builder =
        BlockStreamBuilder::new(store.clone(), store.clone(), ethereum.clone());

    // Prepare for hosting WASM runtimes and managing subgraph instances
    let runtime_host_builder =
        WASMRuntimeHostBuilder::new(&logger, ethereum.clone(), ipfs_client, store.clone());
    let subgraph_instance_manager = SubgraphInstanceManager::new(
        &logger,
        store.clone(),
        runtime_host_builder,
        block_stream_builder,
    );

    // Forward subgraph events from the subgraph provider to the subgraph instance manager
    tokio::spawn(forward(&mut subgraph_provider, &subgraph_instance_manager).unwrap());

    // Forward schema events from the subgraph provider to the GraphQL server.
    let graphql_server_logger = logger.clone();
    tokio::spawn(
        subgraph_provider
            .take_event_stream()
            .unwrap()
            .forward(subscription_server.event_sink().fanout(
                graphql_server.schema_event_sink().sink_map_err(move |e| {
                    error!(graphql_server_logger, "Error forwarding schema event {}", e);
                }),
            )).and_then(|_| Ok(())),
    );

    // Start admin JSON-RPC server.
    let json_rpc_server =
        JsonRpcServer::serve(json_rpc_port, Arc::new(subgraph_provider), logger.clone())
            .expect("Failed to start admin server");

    // Let the server run forever.
    std::mem::forget(json_rpc_server);

    // Add the CLI subgraph with a REST request to the admin server.
    if let Some(subgraph) = subgraph {
        let (name, hash) = if subgraph.contains(':') {
            let mut split = subgraph.split(':');
            (split.next().unwrap(), split.next().unwrap())
        } else {
            ("cli", subgraph)
        };

        let mut url = Url::parse("http://localhost").unwrap();
        url.set_port(Some(json_rpc_port))
            .expect("invalid admin port");
        let raw_response = Client::new()
            .post(url.clone())
            .json(&subgraph_deploy_request(
                name.to_owned(),
                hash.to_owned(),
                "1".to_owned(),
            )).send()
            .expect("failed to make `subgraph_deploy` request");

        graph_server_json_rpc::parse_response(
            raw_response
                .error_for_status()
                .and_then(|mut res| res.json())
                .expect("`subgraph_deploy` request error"),
        ).expect("`subgraph_deploy` server error");
    }

    // Serve GraphQL queries over HTTP. We will listen on port 8000.
    tokio::spawn(
        graphql_server
            .serve(8000)
            .expect("Failed to start GraphQL query server"),
    );

    // Serve GraphQL subscriptions over WebSockets. We will listen on port 8001.
    tokio::spawn(
        subscription_server
            .serve(8001)
            .expect("Failed to start GraphQL subscription server"),
    );

    future::empty()
}

/// Parses an Ethereum connection string and returns the network name and Ethereum node.
fn parse_ethereum_network_and_node<'a>(s: &'a str) -> Result<(&'a str, &'a str), Error> {
    // Check for common Ethereum node mistakes
    if s.starts_with("wss://") || s.starts_with("http://") || s.starts_with("https://") {
        return Err(format_err!(
            "Is your Ethereum node string missing a network name? \
             Try 'mainnet:' + the Ethereum node URL."
        ));
    }

    // Parse string (format is "NETWORK_NAME:URL")
    let split_at = s.find(':').ok_or(format_err!(
        "A network name must be provided alongside the \
         Ethereum node location. Try e.g. 'mainnet:URL'."
    ))?;
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
