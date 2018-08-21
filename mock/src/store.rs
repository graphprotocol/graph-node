use futures::sync::mpsc::{channel, Receiver, Sender};

use graph::components::store::*;
use graph::prelude::*;

/// A mock `Store`.
pub struct MockStore {
    logger: slog::Logger,
    entities: Vec<Entity>,
}

impl MockStore {
    /// Creates a new mock `Store`.
    pub fn new(logger: &slog::Logger) -> Self {
        // Create a few test entities
        let mut entities = vec![];
        for (i, name) in ["Joe", "Jeff", "Linda"].iter().enumerate() {
            let mut entity = Entity::new();
            entity.insert("id".to_string(), Value::String(i.to_string()));
            entity.insert("name".to_string(), Value::String(name.to_string()));
            entities.push(entity);
        }

        MockStore {
            logger: logger.new(o!("component" => "MockStore")),
            entities,
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
    fn subscribe(
        &mut self,
        _subgraph: String,
        _entities: Vec<String>,
    ) -> (String, Box<Stream<Item = EntityChange, Error = ()> + Send>) {
        unimplemented!();
    }

    fn unsubscribe(&mut self, _id: String) {
        unimplemented!();
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
    fn subscribe(
        &mut self,
        _subgraph: String,
        _entities: Vec<String>,
    ) -> (String, Box<Stream<Item = EntityChange, Error = ()> + Send>) {
        unimplemented!();
    }

    fn unsubscribe(&mut self, _id: String) {
        unimplemented!();
    }
}
