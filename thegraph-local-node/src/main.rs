extern crate clap;
extern crate env_logger;
extern crate futures;
extern crate thegraph_local_node;
#[macro_use]
extern crate sentry;
#[macro_use]
extern crate slog;
extern crate ipfs_api;
extern crate thegraph;
extern crate thegraph_core;
extern crate thegraph_ethereum;
extern crate thegraph_hyper;
extern crate thegraph_mock;
extern crate thegraph_runtime;
extern crate thegraph_store_postgres_diesel;
extern crate tokio;
extern crate tokio_core;

use clap::{App, Arg};
use ipfs_api::IpfsClient;
use sentry::integrations::panic::register_panic_handler;
use std::env;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tokio::prelude::*;
use tokio_core::reactor::Core;

use thegraph::components::forward;
use thegraph::components::EventProducer;
use thegraph::prelude::*;
use thegraph::util::log::logger;
use thegraph_ethereum::Transport;
use thegraph_hyper::GraphQLServer as HyperGraphQLServer;
use thegraph_local_node::DataSourceProvider as IpfsDataSourceProvider;
use thegraph_runtime::RuntimeHostBuilder as WASMRuntimeHostBuilder;
use thegraph_store_postgres_diesel::{Store as DieselStore, StoreConfig};

fn main() {
    env_logger::init();
    let mut core = Core::new().unwrap();
    let logger = logger();

    // Setup CLI using Clap, provide general info and capture postgres url
    let matches = App::new("thegraph-local-node")
        .version("0.1.0")
        .author("Graph Protocol, Inc.")
        .about("Scalable queries for a decentralized future (local node)")
        .arg(
            Arg::with_name("data-source")
                .takes_value(true)
                .required(true)
                .long("data-source")
                .value_name("IPFS_HASH")
                .help("IPFS hash of the data source definition file"),
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

    // Obtain data source related command-line arguments
    let data_source_hash = matches.value_of("data-source").unwrap();

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
    register_panic_handler();

    info!(logger, "Starting up");

    // Create system components
    let runtime = core.handle();
    let resolver = IpfsClient::new(
        &format!("{}", ipfs_socket_addr.ip()),
        ipfs_socket_addr.port(),
    ).expect("Failed to start IPFS client");
    let mut data_source_provider = core.run(IpfsDataSourceProvider::new(
        logger.clone(),
        runtime,
        &format!("/ipfs/{}", data_source_hash.clone()),
        &resolver,
    )).expect("Failed to initialize data source provider");
    let mut schema_provider = thegraph_core::SchemaProvider::new(&logger, core.handle());
    let store = DieselStore::new(StoreConfig { url: postgres_url }, &logger, core.handle());
    let protected_store = Arc::new(Mutex::new(store));
    let mut graphql_server = HyperGraphQLServer::new(&logger, core.handle());

    // Create Ethereum adapter
    let (_transport_event_loop, transport) = ethereum_ipc
        .map(Transport::new_ipc)
        .or(ethereum_ws.map(Transport::new_ws))
        .or(ethereum_rpc.map(Transport::new_rpc))
        .expect("One of --ethereum-ipc, --ethereum-ws or --ethereum-rpc must be provided");
    let ethereum_watcher = thegraph_ethereum::EthereumAdapter::new(
        core.handle(),
        thegraph_ethereum::EthereumAdapterConfig { transport },
    );
    let runtime_host_builder = WASMRuntimeHostBuilder::new(
        &logger,
        core.handle(),
        Arc::new(Mutex::new(ethereum_watcher)),
    );
    let runtime_manager = thegraph_core::RuntimeManager::new(
        &logger,
        core.handle(),
        protected_store.clone(),
        runtime_host_builder,
    );

    // Forward data source events from the data source provider to the runtime manager
    core.handle()
        .spawn(forward(&mut data_source_provider, &runtime_manager).unwrap());

    // Forward schema events from the data source provider to the schema provider
    core.handle()
        .spawn(forward(&mut data_source_provider, &schema_provider).unwrap());

    // Forward schema events from the schema provider to the store and GraphQL server
    let schema_stream = schema_provider.take_event_stream().unwrap();
    core.handle().spawn({
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
            .and_then(|_| Ok(()))
    });

    // Forward store events to the GraphQL server
    {
        let store_stream = protected_store.lock().unwrap().event_stream().unwrap();
        core.handle().spawn({
            store_stream
                .forward(graphql_server.store_event_sink().sink_map_err(|e| {
                    panic!("Failed to send store event to the GraphQL server: {:?}", e);
                }))
                .and_then(|_| Ok(()))
        });
    }

    // Forward incoming queries from the GraphQL server to the query runner
    let mut query_runner =
        thegraph_core::QueryRunner::new(&logger, core.handle(), protected_store.clone());
    let query_stream = graphql_server.query_stream().unwrap();
    core.handle().spawn({
        query_stream
            .forward(query_runner.query_sink().sink_map_err(|e| {
                panic!("Failed to send query to query runner: {:?}", e);
            }))
            .and_then(|_| Ok(()))
    });

    // Serve GraphQL server over HTTP
    let http_server = graphql_server
        .serve()
        .expect("Failed to start GraphQL server");
    core.run(http_server).unwrap();
}
