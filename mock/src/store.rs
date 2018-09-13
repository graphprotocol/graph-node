use graph::components::store::*;
use graph::prelude::*;
use graph::web3::types::{Block, Transaction, H256};

/// A mock `Store`.
pub struct MockStore {
    entities: Vec<Entity>,
}

impl MockStore {
    /// Creates a new mock `Store`.
    pub fn new() -> Self {
        // Create a few test entities
        let mut entities = vec![];
        for (i, name) in ["Joe", "Jeff", "Linda"].iter().enumerate() {
            let mut entity = Entity::new();
            entity.insert("id".to_string(), Value::String(i.to_string()));
            entity.insert("name".to_string(), Value::String(name.to_string()));
            entities.push(entity);
        }

        MockStore { entities }
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
                }).map(|entity| entity.clone())
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

impl BlockStore for MockStore {
    fn add_network_if_missing(&self, _: &str, _: &str, _: H256) -> Result<(), Error> {
        unimplemented!()
    }

    fn upsert_blocks<'a, B>(
        &self,
        _: &str,
        _: B,
    ) -> Box<Future<Item = (), Error = Error> + Send + 'a>
    where
        B: Stream<Item = Block<Transaction>, Error = Error> + Send + 'a,
    {
        unimplemented!()
    }

    fn attempt_chain_head_update(&self, _: &str, _: u64) -> Result<Vec<H256>, Error> {
        unimplemented!()
    }
}

impl Store for MockStore {
    fn subscribe(&mut self, _entities: Vec<SubgraphEntityPair>) -> EntityChangeStream {
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

impl BlockStore for FakeStore {
    fn add_network_if_missing(&self, _: &str, _: &str, _: H256) -> Result<(), Error> {
        panic!("called FakeStore")
    }

    fn upsert_blocks<'a, B>(
        &self,
        _: &str,
        _: B,
    ) -> Box<Future<Item = (), Error = Error> + Send + 'a>
    where
        B: Stream<Item = Block<Transaction>, Error = Error> + Send + 'a,
    {
        panic!("called FakeStore")
    }

    fn attempt_chain_head_update(&self, _: &str, _: u64) -> Result<Vec<H256>, Error> {
        panic!("called FakeStore")
    }
}

impl Store for FakeStore {
    fn subscribe(&mut self, _entities: Vec<SubgraphEntityPair>) -> EntityChangeStream {
        unimplemented!();
    }
}
