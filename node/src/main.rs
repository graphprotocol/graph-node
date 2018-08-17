extern crate clap;
extern crate env_logger;
extern crate futures;
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
extern crate graph_store_postgres;
extern crate ipfs_api;
extern crate url;

use clap::{App, Arg};
use ipfs_api::IpfsClient;
use reqwest::Client;
use std::env;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Mutex;
use url::Url;

use graph::components::forward;
use graph::prelude::{JsonRpcServer as JsonRpcServerTrait, *};
use graph::util::log::logger;
use graph_core::SubgraphProvider as IpfsSubgraphProvider;
use graph_datasource_ethereum::Transport;
use graph_runtime_wasm::RuntimeHostBuilder as WASMRuntimeHostBuilder;
use graph_server_http::GraphQLServer as HyperGraphQLServer;
use graph_server_json_rpc::{subgraph_add_request, JsonRpcServer};
use graph_store_postgres::{Store as DieselStore, StoreConfig};

fn main() {
    // Run `async_main` inside the context of an executor.
    tokio::run(future::lazy(|| async_main()))
}

fn async_main() -> impl Future<Item = (), Error = ()> + Send + 'static {
    env_logger::init();
    let logger = logger();

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
                .value_name("URL")
                .help("Ethereum RPC endpoint"),
        )
        .arg(
            Arg::with_name("ethereum-ws")
                .takes_value(true)
                .required_unless_one(&["ethereum-rpc", "ethereum-ipc"])
                .conflicts_with_all(&["ethereum-rpc", "ethereum-ipc"])
                .long("ethereum-ws")
                .value_name("URL")
                .help("Ethereum WebSocket endpoint"),
        )
        .arg(
            Arg::with_name("ethereum-ipc")
                .takes_value(true)
                .required_unless_one(&["ethereum-rpc", "ethereum-ws"])
                .conflicts_with_all(&["ethereum-rpc", "ethereum-ws"])
                .long("ethereum-ipc")
                .value_name("FILE")
                .help("Ethereum IPC pipe"),
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
            Arg::with_name("admin-port")
                .default_value("8020")
                .long("admin-port")
                .value_name("PORT")
                .help("port for the admin JSON-RPC server"),
        )
        .get_matches();

    // Safe to unwrap because a value is required by CLI
    let postgres_url = matches.value_of("postgres-url").unwrap().to_string();

    // Obtain subgraph related command-line arguments
    let subgraph = matches.value_of("subgraph");

    // Obtain the Ethereum RPC/WS/IPC transport locations
    let ethereum_rpc = matches.value_of("ethereum-rpc");
    let ethereum_ipc = matches.value_of("ethereum-ipc");
    let ethereum_ws = matches.value_of("ethereum-ws");

    let ipfs_socket_addr = SocketAddr::from_str(matches.value_of("ipfs").unwrap())
        .expect("could not parse IPFS address, expected format is host:port");

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

    // Create system components
    let resolver = Arc::new(
        IpfsClient::new(
            &format!("{}", ipfs_socket_addr.ip()),
            ipfs_socket_addr.port(),
        ).expect("Failed to start IPFS client"),
    );
    let mut subgraph_provider = IpfsSubgraphProvider::new(logger.clone(), resolver.clone());

    let store = DieselStore::new(StoreConfig { url: postgres_url }, &logger);
    let protected_store = Arc::new(Mutex::new(store));
    let mut graphql_server = HyperGraphQLServer::new(&logger);

    // Create Ethereum adapter
    let (transport_event_loop, transport) = ethereum_ipc
        .map(Transport::new_ipc)
        .or(ethereum_ws.map(Transport::new_ws))
        .or(ethereum_rpc.map(Transport::new_rpc))
        .expect("One of --ethereum-ipc, --ethereum-ws or --ethereum-rpc must be provided");

    // If we drop the event loop the transport will stop working. For now it's
    // fine to just leak it.
    std::mem::forget(transport_event_loop);

    let ethereum_watcher = graph_datasource_ethereum::EthereumAdapter::new(
        graph_datasource_ethereum::EthereumAdapterConfig { transport },
    );
    let runtime_host_builder =
        WASMRuntimeHostBuilder::new(&logger, Arc::new(Mutex::new(ethereum_watcher)), resolver);
    let runtime_manager =
        graph_core::RuntimeManager::new(&logger, protected_store.clone(), runtime_host_builder);

    // Forward subgraph events from the subgraph provider to the runtime manager
    tokio::spawn(forward(&mut subgraph_provider, &runtime_manager).unwrap());

    // Forward schema events from the subgraph provider to the GraphQL server.
    let schema_event_logger = logger.clone();
    tokio::spawn(
        subgraph_provider
            .take_event_stream()
            .unwrap()
            .forward(graphql_server.schema_event_sink().sink_map_err(move |e| {
                error!(schema_event_logger, "Error forwarding schema event {}", e);
            }))
            .and_then(|_| Ok(())),
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
            .json(&subgraph_add_request(
                name.to_owned(),
                hash.to_owned(),
                "1".to_owned(),
            ))
            .send()
            .expect("failed to make `subgraph_add` request");

        graph_server_json_rpc::parse_response(
            raw_response
                .error_for_status()
                .and_then(|mut res| res.json())
                .expect("`subgraph_add` request error"),
        ).expect("`subgraph_add` server error");
    }

    // Forward incoming queries from the GraphQL server to the query runner
    let mut query_runner = graph_core::QueryRunner::new(&logger, protected_store.clone());
    let query_stream = graphql_server.query_stream().unwrap();
    tokio::spawn(
        query_stream
            .forward(query_runner.query_sink().sink_map_err(|e| {
                panic!("Failed to send query to query runner: {:?}", e);
            }))
            .and_then(|_| Ok(())),
    );

    // Serve GraphQL server over HTTP. We will listen on port 8000.
    let http_server = graphql_server
        .serve(8000)
        .expect("Failed to start GraphQL server");
    http_server
}
