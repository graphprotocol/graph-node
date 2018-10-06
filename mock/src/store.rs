use failure::*;

use graph::components::store::*;
use graph::prelude::*;
use graph::web3::types::H256;

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
    fn get(&self, key: StoreKey) -> Result<Entity, QueryExecutionError> {
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
                .ok_or(QueryExecutionError::ResolveEntitiesError(String::from(
                    "Mock store error",
                )))
        } else {
            unimplemented!()
        }
    }

    fn find(&self, _query: StoreQuery) -> Result<Vec<Entity>, QueryExecutionError> {
        Ok(self.entities.clone())
    }

    fn authorize_subgraph_name(&self, _: String, _: String) -> Result<(), Error> {
        unimplemented!();
    }

    fn check_subgraph_name_access_token(&self, _: String, _: String) -> Result<bool, Error> {
        unimplemented!();
    }

    fn read_all_subgraph_names(&self) -> Result<Vec<(String, Option<SubgraphId>)>, Error> {
        unimplemented!();
    }

    fn read_subgraph_name(&self, _: String) -> Result<Option<Option<SubgraphId>>, Error> {
        unimplemented!();
    }

    fn write_subgraph_name(&self, _: String, _: Option<SubgraphId>) -> Result<(), Error> {
        unimplemented!();
    }

    fn find_subgraph_names_by_id(&self, _: SubgraphId) -> Result<Vec<String>, Error> {
        unimplemented!();
    }

    fn delete_subgraph_name(&self, _: String) -> Result<(), Error> {
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
        _: SubgraphId,
        _: EthereumBlockPointer,
        _: EthereumBlockPointer,
        _: Vec<EntityOperation>,
    ) -> Result<(), Error> {
        unimplemented!();
    }

    fn revert_block_operations(
        &self,
        _: SubgraphId,
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

    fn genesis_block_ptr(&self) -> Result<EthereumBlockPointer, Error> {
        unimplemented!();
    }

    fn upsert_blocks<'a, B: Stream<Item = EthereumBlock, Error = Error> + Send + 'a>(
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

    fn block(&self, _: H256) -> Result<Option<EthereumBlock>, Error> {
        unimplemented!();
    }

    fn ancestor_block(
        &self,
        _: EthereumBlockPointer,
        _: u64,
    ) -> Result<Option<EthereumBlock>, Error> {
        unimplemented!();
    }
}

pub struct FakeStore;

impl Store for FakeStore {
    fn get(&self, _: StoreKey) -> Result<Entity, QueryExecutionError> {
        unimplemented!();
    }

    fn find(&self, _: StoreQuery) -> Result<Vec<Entity>, QueryExecutionError> {
        unimplemented!();
    }

    fn authorize_subgraph_name(&self, _: String, _: String) -> Result<(), Error> {
        unimplemented!();
    }

    fn check_subgraph_name_access_token(&self, _: String, _: String) -> Result<bool, Error> {
        unimplemented!();
    }

    fn read_all_subgraph_names(&self) -> Result<Vec<(String, Option<SubgraphId>)>, Error> {
        unimplemented!();
    }

    fn read_subgraph_name(&self, _: String) -> Result<Option<Option<SubgraphId>>, Error> {
        unimplemented!();
    }

    fn write_subgraph_name(&self, _: String, _: Option<SubgraphId>) -> Result<(), Error> {
        unimplemented!();
    }

    fn find_subgraph_names_by_id(&self, _: SubgraphId) -> Result<Vec<String>, Error> {
        unimplemented!();
    }

    fn delete_subgraph_name(&self, _: String) -> Result<(), Error> {
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
        _: SubgraphId,
        _: EthereumBlockPointer,
        _: EthereumBlockPointer,
        _: Vec<EntityOperation>,
    ) -> Result<(), Error> {
        unimplemented!();
    }

    fn revert_block_operations(
        &self,
        _: SubgraphId,
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

    fn genesis_block_ptr(&self) -> Result<EthereumBlockPointer, Error> {
        unimplemented!();
    }

    fn upsert_blocks<'a, B: Stream<Item = EthereumBlock, Error = Error> + Send + 'a>(
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

    fn block(&self, _: H256) -> Result<Option<EthereumBlock>, Error> {
        unimplemented!();
    }

    fn ancestor_block(
        &self,
        _: EthereumBlockPointer,
        _: u64,
    ) -> Result<Option<EthereumBlock>, Error> {
        unimplemented!();
    }
}
