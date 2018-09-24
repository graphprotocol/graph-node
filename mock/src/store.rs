use failure::*;

use graph::components::store::*;
use graph::prelude::*;
use graph::web3::types::{Block, Transaction, H256};

/// A mock `ChainHeadUpdateListener`
pub struct MockChainHeadUpdateListener {}

impl ChainHeadUpdateListener for MockChainHeadUpdateListener {
    fn start(&mut self) {}
}

impl EventProducer<ChainHeadUpdate> for MockChainHeadUpdateListener {
    fn take_event_stream(
        &mut self,
    ) -> Option<Box<Stream<Item = ChainHeadUpdate, Error = ()> + Send>> {
        unimplemented!();
    }
}

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

impl Store for MockStore {
    fn get(&self, key: StoreKey) -> Result<Entity, Error> {
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
                .ok_or(format_err!("Failed to get entity from mock store"))
        } else {
            unimplemented!()
        }
    }

    fn find(&self, _query: StoreQuery) -> Result<Vec<Entity>, ()> {
        Ok(self.entities.clone())
    }

    fn add_subgraph_if_missing(&self, _: SubgraphId, _: EthereumBlockPointer) -> Result<(), Error> {
        unimplemented!();
    }

    fn block_ptr(&self, _: SubgraphId) -> Result<EthereumBlockPointer, Error> {
        unimplemented!();
    }

    fn set_block_ptr_with_no_changes(
        &self,
        _: SubgraphId,
        _: EthereumBlockPointer,
        _: EthereumBlockPointer,
    ) -> Result<(), Error> {
        unimplemented!();
    }

    fn transact_block_operations(
        &self,
        _: &str,
        _: EthereumBlockPointer,
        _: EthereumBlockPointer,
        _: Vec<EntityOperation>,
    ) -> Result<(), Error> {
        unimplemented!();
    }

    fn revert_block_operations(
        &self,
        _: &str,
        _: EthereumBlockPointer,
        _: EthereumBlockPointer,
    ) -> Result<(), Error> {
        unimplemented!();
    }

    fn subscribe(&self, _: Vec<SubgraphEntityPair>) -> EntityChangeStream {
        unimplemented!();
    }
}

impl ChainStore for MockStore {
    type ChainHeadUpdateListener = MockChainHeadUpdateListener;

    fn upsert_blocks<'a, B: Stream<Item = Block<Transaction>, Error = Error> + Send + 'a>(
        &self,
        _: B,
    ) -> Box<Future<Item = (), Error = Error> + Send + 'a> {
        unimplemented!();
    }

    fn attempt_chain_head_update(&self, _: u64) -> Result<Vec<H256>, Error> {
        unimplemented!();
    }

    fn chain_head_updates(&self) -> Self::ChainHeadUpdateListener {
        unimplemented!();
    }

    fn chain_head_ptr(&self) -> Result<Option<EthereumBlockPointer>, Error> {
        unimplemented!();
    }

    fn block(&self, _: H256) -> Result<Option<Block<Transaction>>, Error> {
        unimplemented!();
    }

    fn ancestor_block(
        &self,
        _: EthereumBlockPointer,
        _: u64,
    ) -> Result<Option<Block<Transaction>>, Error> {
        unimplemented!();
    }
}

pub struct FakeStore;

impl Store for FakeStore {
    fn get(&self, _: StoreKey) -> Result<Entity, Error> {
        unimplemented!();
    }

    fn find(&self, _: StoreQuery) -> Result<Vec<Entity>, ()> {
        unimplemented!();
    }

    fn add_subgraph_if_missing(&self, _: SubgraphId, _: EthereumBlockPointer) -> Result<(), Error> {
        unimplemented!();
    }

    fn block_ptr(&self, _: SubgraphId) -> Result<EthereumBlockPointer, Error> {
        unimplemented!();
    }

    fn set_block_ptr_with_no_changes(
        &self,
        _: SubgraphId,
        _: EthereumBlockPointer,
        _: EthereumBlockPointer,
    ) -> Result<(), Error> {
        unimplemented!();
    }

    fn transact_block_operations(
        &self,
        _: &str,
        _: EthereumBlockPointer,
        _: EthereumBlockPointer,
        _: Vec<EntityOperation>,
    ) -> Result<(), Error> {
        unimplemented!();
    }

    fn revert_block_operations(
        &self,
        _: &str,
        _: EthereumBlockPointer,
        _: EthereumBlockPointer,
    ) -> Result<(), Error> {
        unimplemented!();
    }

    fn subscribe(&self, _: Vec<SubgraphEntityPair>) -> EntityChangeStream {
        unimplemented!();
    }
}

impl ChainStore for FakeStore {
    type ChainHeadUpdateListener = MockChainHeadUpdateListener;

    fn upsert_blocks<'a, B: Stream<Item = Block<Transaction>, Error = Error> + Send + 'a>(
        &self,
        _: B,
    ) -> Box<Future<Item = (), Error = Error> + Send + 'a> {
        unimplemented!();
    }

    fn attempt_chain_head_update(&self, _: u64) -> Result<Vec<H256>, Error> {
        unimplemented!();
    }

    fn chain_head_updates(&self) -> Self::ChainHeadUpdateListener {
        unimplemented!();
    }

    fn chain_head_ptr(&self) -> Result<Option<EthereumBlockPointer>, Error> {
        unimplemented!();
    }

    fn block(&self, _: H256) -> Result<Option<Block<Transaction>>, Error> {
        unimplemented!();
    }

    fn ancestor_block(
        &self,
        _: EthereumBlockPointer,
        _: u64,
    ) -> Result<Option<Block<Transaction>>, Error> {
        unimplemented!();
    }
}
