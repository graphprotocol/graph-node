use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use slog;
use std::sync::{Arc, Mutex};
use tokio_core::reactor::Handle;

use thegraph::components::schema::SchemaProviderEvent;
use thegraph::components::store::*;
use thegraph::prelude::*;
use thegraph::util::stream::StreamError;

/// A mock `Store`.
pub struct MockStore {
    logger: slog::Logger,
    event_sink: Option<Sender<StoreEvent>>,
    schema_provider_event_sink: Sender<SchemaProviderEvent>,
    request_sink: Sender<StoreRequest>,
    runtime: Handle,
    entities: Arc<Mutex<Vec<Entity>>>,
}

impl MockStore {
    /// Creates a new mock `Store`.
    pub fn new(logger: &slog::Logger, runtime: Handle) -> Self {
        // Create a channel for handling incoming schema provider events
        let (schema_provider_sink, schema_provider_stream) = channel(100);

        // Create a channel for handling store requests
        let (request_sink, request_stream) = channel(100);

        // Create a few test entities
        let mut entities = vec![];
        for (i, name) in ["Joe", "Jeff", "Linda"].iter().enumerate() {
            let mut entity = Entity::new();
            entity.insert("id".to_string(), Value::String(i.to_string()));
            entity.insert("name".to_string(), Value::String(name.to_string()));
            entities.push(entity);
        }

        // Create a new mock store
        let mut store = MockStore {
            logger: logger.new(o!("component" => "MockStore")),
            event_sink: None,
            schema_provider_event_sink: schema_provider_sink,
            request_sink: request_sink,
            runtime,
            entities: Arc::new(Mutex::new(entities)),
        };

        // Spawn a task that handles incoming schema provider events
        store.handle_schema_provider_events(schema_provider_stream);

        // Spawn a task that handles store requests
        store.handle_requests(request_stream);

        // Return the new store
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

    /// Handles store requests.
    fn handle_requests(&mut self, stream: Receiver<StoreRequest>) {
        let logger = self.logger.clone();
        let entities = self.entities.clone();
        self.runtime.spawn(stream.for_each(move |request| {
            info!(logger, "Received store request"; "request" => format!("{:?}", request));
            let entities = entities.lock().unwrap();
            match request {
                StoreRequest::Get(key, sender) => {
                    sender.send(Self::get(&*entities, key)).unwrap();
                }
                StoreRequest::Set(key, entity, sender) => {
                    sender.send(Self::set(&*entities, key, entity)).unwrap();
                }
                StoreRequest::Delete(key, sender) => {
                    sender.send(Self::delete(&*entities, key)).unwrap();
                }
                StoreRequest::Find(query, sender) => {
                    sender.send(Self::find(&*entities, query)).unwrap();
                }
            };
            Ok(())
        }));
    }

    /// Generates a bunch of mock store events.
    fn generate_mock_events(&self) {
        info!(self.logger, "Generate mock events");

        let sink = self.event_sink.clone().unwrap();
        let entities = self.entities.lock().unwrap();
        for entity in entities.iter() {
            sink.clone()
                .send(StoreEvent::EntityAdded(entity.clone()))
                .wait()
                .unwrap();
        }
    }

    fn get(entities: &Vec<Entity>, key: StoreKey) -> StoreGetResponse {
        if key.entity == "User" {
            entities
                .iter()
                .find(|entity| {
                    let id = entity.get("id").unwrap();
                    match id {
                        &Value::String(ref s) => s == &key.id,
                        _ => false,
                    }
                })
                .map(|entity| entity.clone())
                .ok_or(())
        } else {
            unimplemented!()
        }
    }

    fn set(_entities: &Vec<Entity>, _key: StoreKey, _entity: Entity) -> StoreSetResponse {
        unimplemented!();
    }

    fn delete(_entities: &Vec<Entity>, _key: StoreKey) -> StoreDeleteResponse {
        unimplemented!();
    }

    fn find(entities: &Vec<Entity>, _query: StoreQuery) -> StoreFindResponse {
        Ok(entities.clone())
    }
}

impl Store for MockStore {
    fn request_sink(&mut self) -> Sender<StoreRequest> {
        self.request_sink.clone()
    }

    fn schema_provider_event_sink(&mut self) -> Sender<SchemaProviderEvent> {
        self.schema_provider_event_sink.clone()
    }

    fn event_stream(&mut self) -> Result<Receiver<StoreEvent>, StreamError> {
        // If possible, create a new channel for streaming store events
        let result = match self.event_sink {
            Some(_) => Err(StreamError::AlreadyCreated),
            None => {
                let (sink, stream) = channel(100);
                self.event_sink = Some(sink);
                Ok(stream)
            }
        };

        self.generate_mock_events();
        result
    }
}
