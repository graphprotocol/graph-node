extern crate clap;
extern crate futures;
extern crate thegraph_local_node;
#[macro_use]
extern crate sentry;
#[macro_use]
extern crate slog;
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
use sentry::integrations::panic::register_panic_handler;
use std::env;
use std::sync::{Arc, Mutex};
use tokio::prelude::*;
use tokio_core::reactor::Core;

use thegraph::components::data_sources::RuntimeHostEvent;
use thegraph::components::{EventConsumer, EventProducer};
use thegraph::prelude::*;
use thegraph::util::log::logger;
use thegraph_hyper::GraphQLServer as HyperGraphQLServer;
use thegraph_runtime::{RuntimeHost as WASMRuntimeHost, RuntimeHostConfig};
use thegraph_store_postgres_diesel::{Store as DieselStore, StoreConfig};

use thegraph_local_node::LocalDataSourceProvider;

fn main() {
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
                .value_name("FILE")
                .help("Path to the data source definition file"),
        )
        .arg(
            Arg::with_name("postgres-url")
                .takes_value(true)
                .required(true)
                .long("postgres-url")
                .value_name("URL")
                .help("Location of the Postgres database used for storing entities"),
        )
        .get_matches();

    // Safe to unwrap because a value is required by CLI
    let postgres_url = matches.value_of("postgres-url").unwrap().to_string();

    // Obtain data source related command-line arguments
    let data_source_definition_file = matches.value_of("data-source").unwrap();

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
    let mut data_source_provider =
        LocalDataSourceProvider::new(&logger, core.handle(), data_source_definition_file.clone());
    let mut schema_provider = thegraph_core::SchemaProvider::new(&logger, core.handle());
    let mut store = DieselStore::new(StoreConfig { url: postgres_url }, &logger, core.handle());
    let mut graphql_server = HyperGraphQLServer::new(&logger, core.handle());
    let ethereum_watcher = Arc::new(Mutex::new(thegraph_ethereum::EthereumWatcher::new()));
    let runtime_manager = thegraph_core::RuntimeManager::new(&logger, core.handle());

    // TODO: This should be turned into a runtime host builder that we pass to the
    // runtime manager. The builder would be a common trait in `thegraph`, with a
    // RuntimeHostBuilder implementation in `thegraph-runtime`.
    // Create runtime host and connect it to Ethereum
    let mut data_source_runtime_host = WASMRuntimeHost::new(
        &logger,
        core.handle(),
        ethereum_watcher.clone(),
        RuntimeHostConfig {
            data_source_definition: data_source_definition_file.to_string(),
        },
    );

    // Forward data source events from the data source provider to the runtime manager
    let data_source_stream = data_source_provider.event_stream().unwrap();
    let runtime_manager_sink = runtime_manager.event_sink();
    core.handle().spawn({
        data_source_stream
            .forward(runtime_manager_sink.sink_map_err(|e| {
                panic!(
                    "Failed to send data source event to the runtime manager: {:?}",
                    e
                );
            }))
            .and_then(|_| Ok(()))
    });

    // Forward schema events from the data source provider to the schema provider
    let schema_stream = data_source_provider.schema_event_stream().unwrap();
    let schema_sink = schema_provider.event_sink();
    core.handle().spawn({
        schema_stream
            .forward(schema_sink.sink_map_err(|e| {
                panic!("Failed to send schema event to schema provider: {:?}", e);
            }))
            .and_then(|_| Ok(()))
    });

    // Forward schema events from the schema provider to the store and GraphQL server
    let schema_stream = schema_provider.take_event_stream().unwrap();
    core.handle().spawn({
        schema_stream
            .forward(
                store
                    .schema_provider_event_sink()
                    .fanout(graphql_server.schema_provider_event_sink())
                    .sink_map_err(|e| {
                        panic!("Failed to send event to store and server: {:?}", e);
                    }),
            )
            .and_then(|_| Ok(()))
    });

    // Forward store events to the GraphQL server
    let store_stream = store.event_stream().unwrap();
    core.handle().spawn({
        store_stream
            .forward(graphql_server.store_event_sink().sink_map_err(|e| {
                panic!("Failed to send store event to the GraphQL server: {:?}", e);
            }))
            .and_then(|_| Ok(()))
    });

    // Obtain a protected version of the store
    let protected_store = Arc::new(Mutex::new(store));

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

    // Connect the runtime host to the store
    core.handle().spawn({
        data_source_runtime_host
            .event_stream()
            .unwrap()
            .for_each(move |event| {
                let mut store = protected_store.lock().unwrap();

                match event {
                    RuntimeHostEvent::EntityCreated(_, k, entity) => {
                        store
                            .set(k, entity)
                            .expect("Failed to set entity in the store");
                    }
                    RuntimeHostEvent::EntityChanged(_, k, entity) => {
                        store
                            .set(k, entity)
                            .expect("Failed to set entity in the store");
                    }
                    RuntimeHostEvent::EntityRemoved(_, k) => {
                        store
                            .delete(k)
                            .expect("Failed to remove entity from the store");
                    }
                };
                Ok(())
            })
            .and_then(|_| Ok(()))
    });

    // Start the runtime host
    data_source_runtime_host.start();

    // Serve GraphQL server over HTTP
    let http_server = graphql_server
        .serve()
        .expect("Failed to start GraphQL server");
    core.run(http_server).unwrap();

    // Stop the runtime host
    data_source_runtime_host.stop();
}
