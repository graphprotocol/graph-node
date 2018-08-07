use futures::future;
use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use hyper;
use hyper::Server;

use slog;
use std::error::Error;
use std::fmt;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::{Arc, Mutex};

use graph::components::schema::SchemaProviderEvent;
use graph::components::store::StoreEvent;
use graph::data::query::Query;
use graph::data::schema::Schema;
use graph::prelude::GraphQLServer as GraphQLServerTrait;
use graph::tokio;
use graph::util::stream::StreamError;

use service::GraphQLService;

/// Errors that may occur when starting the server.
#[derive(Debug)]
pub enum GraphQLServeError {
    OrphanError,
    BindError(hyper::Error),
}

impl Error for GraphQLServeError {
    fn description(&self) -> &str {
        "Failed to start the server"
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}

impl fmt::Display for GraphQLServeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OrphanError: No component set up to handle the queries")
    }
}

impl From<hyper::Error> for GraphQLServeError {
    fn from(err: hyper::Error) -> Self {
        GraphQLServeError::BindError(err)
    }
}

/// A GraphQL server based on Hyper.
pub struct GraphQLServer {
    logger: slog::Logger,
    query_sink: Option<Sender<Query>>,
    schema_provider_event_sink: Sender<SchemaProviderEvent>,
    store_event_sink: Sender<StoreEvent>,
    schema: Arc<Mutex<Option<Schema>>>,
}

impl GraphQLServer {
    /// Creates a new GraphQL server.
    pub fn new(logger: &slog::Logger) -> Self {
        // Create channels for handling incoming events from the schema provider and the store
        let (store_sink, store_stream) = channel(100);
        let (schema_provider_sink, schema_provider_stream) = channel(100);

        // Create a new GraphQL server
        let mut server = GraphQLServer {
            logger: logger.new(o!("component" => "GraphQLServer")),
            query_sink: None,
            schema_provider_event_sink: schema_provider_sink,
            store_event_sink: store_sink,
            schema: Arc::new(Mutex::new(None)),
        };

        // Spawn tasks to handle incoming events from the schema provider and store
        server.handle_schema_provider_events(schema_provider_stream);
        server.handle_store_events(store_stream);

        // Return the new server
        server
    }

    /// Handle incoming events from the schema provider
    fn handle_schema_provider_events(&mut self, stream: Receiver<SchemaProviderEvent>) {
        let logger = self.logger.clone();
        let schema = self.schema.clone();

        tokio::spawn(stream.for_each(move |event| {
            info!(logger, "Received schema provider event");

            let SchemaProviderEvent::SchemaChanged(new_schema) = event;
            let mut schema = schema.lock().unwrap();
            *schema = new_schema;

            Ok(())
        }));
    }

    // Handle incoming events from the store
    fn handle_store_events(&mut self, stream: Receiver<StoreEvent>) {
        let logger = self.logger.clone();

        tokio::spawn(stream.for_each(move |event| {
            info!(logger, "Received store event"; "event" => format!("{:?}",  event));
            Ok(())
        }));
    }
}

impl GraphQLServerTrait for GraphQLServer {
    type ServeError = GraphQLServeError;

    fn schema_provider_event_sink(&mut self) -> Sender<SchemaProviderEvent> {
        self.schema_provider_event_sink.clone()
    }

    fn store_event_sink(&mut self) -> Sender<StoreEvent> {
        self.store_event_sink.clone()
    }

    fn query_stream(&mut self) -> Result<Receiver<Query>, StreamError> {
        // If possible, create a new channel for streaming incoming queries
        match self.query_sink {
            Some(_) => Err(StreamError::AlreadyCreated),
            None => {
                let (sink, stream) = channel(100);
                self.query_sink = Some(sink);
                Ok(stream)
            }
        }
    }

    fn serve(
        &mut self,
        port: u16,
    ) -> Result<Box<Future<Item = (), Error = ()> + Send>, Self::ServeError> {
        let logger = self.logger.clone();

        let addr = SocketAddrV4::new(Ipv4Addr::new(0, 0, 0, 0), port);

        // Only launch the GraphQL server if there is a component that will handle incoming queries
        let query_sink = self
            .query_sink
            .as_ref()
            .ok_or(GraphQLServeError::OrphanError)?;

        // On every incoming request, launch a new GraphQL service that writes
        // incoming queries to the query sink.
        let query_sink = query_sink.clone();
        let schema = self.schema.clone();
        let new_service = move || {
            let service = GraphQLService::new(schema.clone(), query_sink.clone());
            future::ok::<GraphQLService, hyper::Error>(service)
        };

        // Create a task to run the server and handle HTTP requests
        let task = Server::try_bind(&addr.into())?
            .serve(new_service)
            .map_err(move |e| error!(logger, "Server error"; "error" => format!("{}", e)));

        Ok(Box::new(task))
    }
}
