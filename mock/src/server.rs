use futures::prelude::*;
use futures::stream;
use futures::sync::mpsc::{channel, Receiver, Sender};
use futures::sync::oneshot;
use graphql_parser;
use slog;
use std::error::Error;
use std::fmt;
use std::sync::{Arc, Mutex};
use tokio;

use graph::components::schema::SchemaProviderEvent;
use graph::components::store::StoreEvent;
use graph::prelude::*;
use graph::util::stream::StreamError;

#[derive(Debug)]
pub struct MockServeError;

impl Error for MockServeError {
    fn description(&self) -> &str {
        "Mock serve error"
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}

impl fmt::Display for MockServeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Mock serve error")
    }
}

/// A mock `GraphQLServer`.
pub struct MockGraphQLServer {
    logger: slog::Logger,
    query_sink: Option<Sender<Query>>,
    schema_provider_event_sink: Sender<SchemaProviderEvent>,
    store_event_sink: Sender<StoreEvent>,
    schema: Arc<Mutex<Option<Schema>>>,
}

impl MockGraphQLServer {
    /// Creates a new mock `GraphQLServer`.
    pub fn new(logger: &slog::Logger) -> Self {
        // Create channels for handling incoming events from the schema provider and the store
        let (store_sink, store_stream) = channel(100);
        let (schema_provider_sink, schema_provider_stream) = channel(100);

        // Create a new mock GraphQL server
        let mut server = MockGraphQLServer {
            logger: logger.new(o!("component" => "MockGraphQLServer")),
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
            info!(logger, "Received schema provider event"; "event" => format!("{:?}", event));
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
            info!(logger, "Received store event"; "event" => format!("{:?}", event));
            Ok(())
        }));
    }
}

impl GraphQLServer for MockGraphQLServer {
    type ServeError = MockServeError;

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

    fn serve(&mut self) -> Result<Box<Future<Item = (), Error = ()> + Send>, Self::ServeError> {
        // Only launch the GraphQL server if there is a component that will handle incoming queries
        let query_sink = self.query_sink.clone().ok_or_else(|| MockServeError)?;
        let schema = self.schema.clone();

        // Generate mock query requests
        let requests = (0..5)
            .map(|_| {
                let schema = schema.lock().unwrap();
                let (sink, stream) = oneshot::channel();
                (
                    stream,
                    Query {
                        schema: schema.clone().unwrap(),
                        document: graphql_parser::parse_query("{ allUsers { name }}").unwrap(),
                        variables: None,
                        result_sender: sink,
                    },
                )
            })
            .collect::<Vec<(oneshot::Receiver<QueryResult>, Query)>>();

        println!("Requests: {:?}", requests);

        let logger = self.logger.clone();

        // Create task to generate mock queries
        Ok(Box::new(stream::iter_ok(requests).for_each(
            move |(receiver, query)| {
                query_sink
                    .clone()
                    .send(query)
                    .wait()
                    .expect("Failed to forward mock query");

                let logger = logger.clone();
                receiver.then(move |result| {
                    info!(logger, "Send query result to client: {:?}", result);
                    Ok(())
                })
            },
        )))
    }
}
