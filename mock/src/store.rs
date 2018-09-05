use web3::types::Block;
use web3::types::H256;
use web3::types::Transaction;

use graph::components::store::*;
use graph::prelude::*;

/// A mock `Store`.
#[derive(Clone)]
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
    fn add_subgraph(&self, _: SubgraphId) -> Result<(), Error> {
        unimplemented!()
    }

    fn block_ptr(&self, _subgraph_id: SubgraphId) -> Result<EthereumBlockPointer, Error> {
        unimplemented!()
    }

    fn set_block_ptr_with_no_changes(
        &self,
        _subgraph_id: SubgraphId,
        _from: EthereumBlockPointer,
        _to: EthereumBlockPointer,
    ) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn revert_block(
        &self,
        _subgraph_id: SubgraphId,
        _block: Block<Transaction>,
    ) -> Result<(), Error> {
        unimplemented!()
    }

    fn get(&self, key: StoreKey, _block_ptr: EthereumBlockPointer) -> Result<Entity, StoreError> {
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
                .ok_or(StoreError::Database(format_err!("entity not found")))
        } else {
            unimplemented!()
        }
    }

    fn find(
        &self,
        _query: StoreQuery,
        _block_ptr: EthereumBlockPointer,
    ) -> Result<Vec<Entity>, StoreError> {
        Ok(self.entities.clone())
    }

    fn commit_transaction(
        &self,
        _subgraph_id: SubgraphId,
        _tx_ops: Vec<StoreOp>,
        _block: Block<Transaction>,
    ) -> Result<(), StoreError> {
        unimplemented!()
    }
}

impl BlockStore for MockStore {
    fn upsert_blocks<'a, B>(
        &self,
        _: B,
    ) -> Box<Future<Item = (), Error = Error> + Send + 'a>
    where
        B: Stream<Item = Block<Transaction>, Error = Error> + Send + 'a,
    {
        unimplemented!()
    }

    fn attempt_head_update(&self, _ancestor_count: u64) -> Result<Vec<H256>, Error> {
        unimplemented!()
    }

    fn head_block_ptr(&self) -> Result<Option<EthereumBlockPointer>, Error> {
        unimplemented!()
    }

    fn block(&self, _block_hash: H256) -> Result<Option<Block<Transaction>>, Error> {
        unimplemented!()
    }

    fn ancestor_block(
        &self,
        _block_ptr: EthereumBlockPointer,
        _offset: u64,
    ) -> Result<Option<Block<Transaction>>, Error> {
        unimplemented!()
    }
}

impl Store for MockStore {
    fn subscribe(&mut self, _entities: Vec<SubgraphEntityPair>) -> EntityChangeStream {
        unimplemented!();
    }
}

#[derive(Clone)]
pub struct FakeStore;

impl BasicStore for FakeStore {
    fn add_subgraph(&self, _: SubgraphId) -> Result<(), Error> {
        panic!("called FakeStore")
    }

    fn block_ptr(&self, _subgraph_id: SubgraphId) -> Result<EthereumBlockPointer, Error> {
        panic!("called FakeStore")
    }

    fn set_block_ptr_with_no_changes(
        &self,
        _subgraph_id: SubgraphId,
        _from: EthereumBlockPointer,
        _to: EthereumBlockPointer,
    ) -> Result<(), StoreError> {
        panic!("called FakeStore")
    }

    fn revert_block(
        &self,
        _subgraph_id: SubgraphId,
        _block: Block<Transaction>,
    ) -> Result<(), Error> {
        panic!("called FakeStore")
    }

    fn get(&self, _key: StoreKey, _block_ptr: EthereumBlockPointer) -> Result<Entity, StoreError> {
        panic!("called FakeStore")
    }

    fn find(
        &self,
        _query: StoreQuery,
        _block_ptr: EthereumBlockPointer,
    ) -> Result<Vec<Entity>, StoreError> {
        panic!("called FakeStore")
    }

    fn commit_transaction(
        &self,
        _subgraph_id: SubgraphId,
        _tx_ops: Vec<StoreOp>,
        _block: Block<Transaction>,
    ) -> Result<(), StoreError> {
        panic!("called FakeStore")
    }
}

impl BlockStore for FakeStore {
    fn upsert_blocks<'a, B>(
        &self,
        _: B,
    ) -> Box<Future<Item = (), Error = Error> + Send + 'a>
    where
        B: Stream<Item = Block<Transaction>, Error = Error> + Send + 'a,
    {
        panic!("called FakeStore")
    }

    fn attempt_head_update(&self, _ancestor_count: u64) -> Result<Vec<H256>, Error> {
        panic!("called FakeStore")
    }

    fn head_block_ptr(&self) -> Result<Option<EthereumBlockPointer>, Error> {
        panic!("called FakeStore")
    }

    fn block(&self, _block_hash: H256) -> Result<Option<Block<Transaction>>, Error> {
        panic!("called FakeStore")
    }

    fn ancestor_block(
        &self,
        _block_ptr: EthereumBlockPointer,
        _offset: u64,
    ) -> Result<Option<Block<Transaction>>, Error> {
        panic!("called FakeStore")
    }
}

impl Store for FakeStore {
    fn subscribe(&mut self, _entities: Vec<SubgraphEntityPair>) -> EntityChangeStream {
        unimplemented!();
    }
}
