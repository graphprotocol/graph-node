extern crate clap;
extern crate env_logger;
extern crate futures;
extern crate graph_node;
#[macro_use]
extern crate sentry;
#[macro_use]
extern crate slog;
extern crate graph;
extern crate graph_core;
extern crate graph_datasource_ethereum;
extern crate graph_mock;
extern crate graph_runtime_wasm;
extern crate graph_server_http;
extern crate graph_store_postgres;
extern crate ipfs_api;

use clap::{App, Arg};
use ipfs_api::IpfsClient;
use std::env;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Mutex;

use graph::components::forward;
use graph::prelude::*;
use graph::util::log::logger;
use graph_datasource_ethereum::Transport;
use graph_node::SubgraphProvider as IpfsSubgraphProvider;
use graph_runtime_wasm::RuntimeHostBuilder as WASMRuntimeHostBuilder;
use graph_server_http::GraphQLServer as HyperGraphQLServer;
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
                .required(true)
                .long("subgraph")
                .value_name("IPFS_HASH")
                .help("IPFS hash of the subgraph manifest"),
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
        .get_matches();

    // Safe to unwrap because a value is required by CLI
    let postgres_url = matches.value_of("postgres-url").unwrap().to_string();

    // Obtain subgraph related command-line arguments
    let subgraph_hash = matches.value_of("subgraph").unwrap();

    // Obtain the Ethereum RPC/WS/IPC transport locations
    let ethereum_rpc = matches.value_of("ethereum-rpc");
    let ethereum_ipc = matches.value_of("ethereum-ipc");
    let ethereum_ws = matches.value_of("ethereum-ws");

    let ipfs_socket_addr = SocketAddr::from_str(matches.value_of("ipfs").unwrap())
        .expect("could not parse IPFS address, expected format is host:port");

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
    let (mut subgraph_provider, subgraph_provider_events) = IpfsSubgraphProvider::new(
        logger.clone(),
        &format!("/ipfs/{}", subgraph_hash.clone()),
        resolver.clone(),
    );
    tokio::spawn(subgraph_provider_events.map_err(|_| ()));

    let mut schema_provider = graph_core::SchemaProvider::new(&logger);
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

    // Forward schema events from the subgraph provider to the schema provider
    tokio::spawn(forward(&mut subgraph_provider, &schema_provider).unwrap());

    // Forward schema events from the schema provider to the store and GraphQL server
    let schema_stream = schema_provider.take_event_stream().unwrap();
    tokio::spawn(
        schema_stream
            .forward(
                protected_store
                    .lock()
                    .unwrap()
                    .schema_provider_event_sink()
                    .fanout(graphql_server.schema_provider_event_sink())
                    .sink_map_err(|e| {
                        panic!("Failed to send event to store and server: {:?}", e);
                    }),
            )
            .and_then(|_| Ok(())),
    );

    // Forward store events to the GraphQL server
    {
        let store_stream = protected_store.lock().unwrap().event_stream().unwrap();
        tokio::spawn(
            store_stream
                .forward(graphql_server.store_event_sink().sink_map_err(|e| {
                    panic!("Failed to send store event to the GraphQL server: {:?}", e);
                }))
                .and_then(|_| Ok(())),
        );
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

    // Serve GraphQL server over HTTP
    let http_server = graphql_server
        .serve()
        .expect("Failed to start GraphQL server");
    http_server
}
