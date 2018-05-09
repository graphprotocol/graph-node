extern crate futures;
#[macro_use]
extern crate sentry;
#[macro_use]
extern crate slog;
extern crate thegraph;
extern crate thegraph_hyper;
extern crate tokio;
extern crate tokio_core;

use sentry::integrations::panic::register_panic_handler;
use std::env;
use thegraph::common::util::log::logger;
use thegraph::prelude::*;
use thegraph::mock;
use thegraph_hyper::HyperGraphQLServer;
use tokio::prelude::*;
use tokio_core::reactor::Core;

fn main() {
    let mut core = Core::new().unwrap();

    let logger = logger();

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
    let mut data_source_provider = mock::MockDataSourceProvider::new(&logger);
    let mut schema_provider = mock::MockSchemaProvider::new(&logger);
    let mut store = mock::MockStore::new(&logger);
    let mut graphql_server = HyperGraphQLServer::new(&logger, core.handle());

    // Forward schema events from the data source provider to the schema provider
    let schema_stream = data_source_provider.schema_event_stream().unwrap();
    let schema_sink = schema_provider.schema_event_sink();
    core.handle().spawn({
        schema_stream
            .forward(schema_sink.sink_map_err(|e| {
                panic!("Failed to send schema event to schema provider: {:?}", e);
            }))
            .and_then(|_| Ok(()))
    });

    // Forward schema events from the schema provider to the store and GraphQL server
    let schema_stream = schema_provider.event_stream().unwrap();
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

    // Forward incoming queries from the GraphQL server to the query runner
    let mut query_runner = mock::MockQueryRunner::new(&logger, store);
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
