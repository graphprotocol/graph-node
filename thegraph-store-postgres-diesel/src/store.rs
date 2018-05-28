use diesel::pg::PgConnection;
use diesel::prelude::*;
use migrations_internals::run_pending_migrations;
use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use slog;
use tokio_core::reactor::Handle;

use thegraph::components::schema::SchemaProviderEvent;
use thegraph::components::store::{*, Store as StoreTrait};
use thegraph::data::store::*;
use thegraph::util::stream::StreamError;
use thegraph::util::log::logger;

/// Creates the "entities" table if it doesn't already exist
fn run_all_migrations(conn: &PgConnection) {
    let logger = logger();
    match run_pending_migrations(conn) {
        Ok(_) => info!(logger, "All pending postgres schema migrations successfully completed"),
        Err(e) => panic!("Error with postgres schema setup: {:?}", e),
    }
}

/// Configuration for the Diesel/Postgres store.
pub struct StoreConfig {
    pub url: String,
}

/// A Store based on Diesel and Postgres.
pub struct Store {
    event_sink: Option<Sender<StoreEvent>>,
    logger: slog::Logger,
    runtime: Handle,
    schema_provider_event_sink: Sender<SchemaProviderEvent>,
    _config: StoreConfig,
    _conn: PgConnection,
}

impl Store {
    pub fn new(config: StoreConfig, logger: &slog::Logger, runtime: Handle) -> Self {
        // Create a channel for handling incoming schema provider events
        let (sink, stream) = channel(100);

        // Connect to Postgres
        let conn =
            PgConnection::establish(config.url.as_str()).expect("Failed to connect to Postgres");

        info!(logger, "Connected to Postgres"; "url" => &config.url);

        // Create the entities table (if necessary)
        run_all_migrations(&conn);

        // Create the store
        let mut store = Store {
            logger: logger.new(o!("component" => "Store")),
            event_sink: None,
            schema_provider_event_sink: sink,
            runtime,
            _config: config,
            _conn: conn,
        };

        // Spawn a task that handles incoming schema provider events
        store.handle_schema_provider_events(stream);

        // Return the store
        store
    }

    /// Handles incoming schema provider events.
    fn handle_schema_provider_events(&mut self, stream: Receiver<SchemaProviderEvent>) {
        let logger = self.logger.clone();
        self.runtime.spawn(stream.for_each(move |event| {
            info!(logger, "Received schema provider event: {:?}", event);
            Ok(())
        }));
    }
}

impl StoreTrait for Store {
    fn get(&self, _key: StoreKey) -> Result<Entity, ()> {
        unimplemented!()
    }

    fn set(&mut self, _key: StoreKey, _entity: Entity) -> Result<(), ()> {
        unimplemented!()
    }

    fn delete(&mut self, _key: StoreKey) -> Result<(), ()> {
        unimplemented!()
    }

    fn find(&self, _query: StoreQuery) -> Result<Vec<Entity>, ()> {
        unimplemented!()
    }

    fn schema_provider_event_sink(&mut self) -> Sender<SchemaProviderEvent> {
        self.schema_provider_event_sink.clone()
    }

    fn event_stream(&mut self) -> Result<Receiver<StoreEvent>, StreamError> {
        // If possible, create a new channel for streaming store events
        match self.event_sink {
            Some(_) => Err(StreamError::AlreadyCreated),
            None => {
                let (sink, stream) = channel(100);
                self.event_sink = Some(sink);
                Ok(stream)
            }
        }
    }
}
