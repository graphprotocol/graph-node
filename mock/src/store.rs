use futures::sync::mpsc::{channel, Receiver, Sender};

use graph::components::store::*;
use graph::prelude::*;

/// A mock `Store`.
pub struct MockStore {
    logger: slog::Logger,
    event_sink: Option<Sender<StoreEvent>>,
    schema_provider_event_sink: Sender<SchemaProviderEvent>,
    entities: Vec<Entity>,
}

impl MockStore {
    /// Creates a new mock `Store`.
    pub fn new(logger: &slog::Logger) -> Self {
        // Create a channel for handling incoming schema provider events
        let (sink, stream) = channel(100);

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
            schema_provider_event_sink: sink,
            entities,
        };

        // Spawn a task that handles incoming schema provider events
        store.handle_schema_provider_events(stream);

        // Return the new store
        store
    }

    /// Handles incoming schema provider events.
    fn handle_schema_provider_events(&mut self, stream: Receiver<SchemaProviderEvent>) {
        let logger = self.logger.clone();
        tokio::spawn(stream.for_each(move |event| {
            info!(logger, "Received schema provider event: {:?}", event);
            Ok(())
        }));
    }

    /// Generates a bunch of mock store events.
    fn generate_mock_events(&self) {
        info!(self.logger, "Generate mock events");

        let sink = self.event_sink.clone().unwrap();
        for entity in self.entities.iter() {
            sink.clone()
                .send(StoreEvent::EntityAdded(entity.clone()))
                .wait()
                .unwrap();
        }
    }
}

impl BasicStore for MockStore {
    fn get(&self, key: StoreKey) -> Result<Entity, ()> {
        if key.entity == "User" {
            self.entities
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

    fn set(&mut self, _key: StoreKey, _entity: Entity, _source: EventSource) -> Result<(), ()> {
        unimplemented!();
    }

    fn delete(&mut self, _key: StoreKey, _source: EventSource) -> Result<(), ()> {
        unimplemented!();
    }

    fn find(&self, _query: StoreQuery) -> Result<Vec<Entity>, ()> {
        Ok(self.entities.clone())
    }
}

impl Store for MockStore {
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

pub struct FakeStore;

impl BasicStore for FakeStore {
    fn get(&self, _: StoreKey) -> Result<Entity, ()> {
        panic!("called FakeStore")
    }

    fn set(&mut self, _: StoreKey, _: Entity, _source: EventSource) -> Result<(), ()> {
        panic!("called FakeStore")
    }

    fn delete(&mut self, _: StoreKey, _source: EventSource) -> Result<(), ()> {
        panic!("called FakeStore")
    }

    fn find(&self, _: StoreQuery) -> Result<Vec<Entity>, ()> {
        panic!("called FakeStore")
    }
}

impl Store for FakeStore {
    fn schema_provider_event_sink(&mut self) -> Sender<SchemaProviderEvent> {
        panic!("called FakeStore")
    }

    fn event_stream(&mut self) -> Result<Receiver<StoreEvent>, StreamError> {
        panic!("called FakeStore")
    }
}
