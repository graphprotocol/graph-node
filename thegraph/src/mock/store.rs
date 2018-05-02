use tokio;
use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver, Sender};
use common::store::*;
use common::schema::SchemaProviderEvent;
use common::util::stream::StreamError;

/// A mock [Store](../common/store/trait.Store.html).
pub struct MockStore {
    event_sink: Option<Sender<StoreEvent>>,
    schema_provider_event_sink: Sender<SchemaProviderEvent>,
}

impl MockStore {
    /// Creates a new mock [Store](../common/store/trait.Store.html).
    pub fn new() -> Self {
        // Create a channel for handling incoming schema provider events
        let (sink, stream) = channel(100);

        // Create a new mock store
        let mut store = MockStore {
            event_sink: None,
            schema_provider_event_sink: sink,
        };

        // Spawn a task that handles incoming schema provider events
        store.handle_schema_provider_events(stream);

        // Return the new store
        store
    }

    /// Handles incoming schema provider events.
    fn handle_schema_provider_events(&mut self, stream: Receiver<SchemaProviderEvent>) {
        tokio::spawn(stream.for_each(|event| {
            println!("Store: Received schema provider event: {:?}", event);
            Ok(())
        }));
    }

    /// Generates a bunch of mock store events.
    fn generate_mock_events(&self) {
        let sink = self.event_sink.clone().unwrap();
        sink.clone()
            .send(StoreEvent::EntityAdded("Entity 1"))
            .wait()
            .unwrap();
        sink.clone()
            .send(StoreEvent::EntityAdded("Entity 2"))
            .wait()
            .unwrap();
        sink.clone()
            .send(StoreEvent::EntityChanged("Entity 1"))
            .wait()
            .unwrap();
        sink.clone()
            .send(StoreEvent::EntityRemoved("Entity 2"))
            .wait()
            .unwrap();
    }
}

impl Store for MockStore {
    fn get(&self, _key: StoreKey) -> Result<Entity, ()> {
        unimplemented!();
    }

    fn set(&mut self, _key: StoreKey, _entity: Entity) -> Result<(), ()> {
        unimplemented!();
    }

    fn delete(&mut self, _key: StoreKey) -> Result<(), ()> {
        unimplemented!();
    }

    fn find(&self, _query: StoreQuery) -> Result<Vec<Entity>, ()> {
        unimplemented!();
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
