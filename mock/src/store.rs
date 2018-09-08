use std::cell::RefCell;
use std::time::Duration;
use std::time::Instant;
use web3::types::*;

use graph::components::store::*;
use graph::prelude::*;

use graph::tokio::timer::Interval;

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
    fn add_subgraph_if_missing(&self, _: SubgraphId) -> Result<(), Error> {
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
    ) -> Result<(), StoreError> {
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
        _ptr_update: bool,
    ) -> Result<(), StoreError> {
        unimplemented!()
    }
}

impl BlockStore for MockStore {
    fn upsert_blocks<'a, B>(&self, _: B) -> Box<Future<Item = (), Error = Error> + Send + 'a>
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

    fn head_block_updates(&self) -> Box<Stream<Item = HeadBlockUpdateEvent, Error = Error> + Send> {
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
    fn subscribe(&self, _entities: Vec<SubgraphEntityPair>) -> EntityChangeStream {
        unimplemented!();
    }
}

#[derive(Clone)]
pub struct FakeStore {
    pub block_ptr: RefCell<EthereumBlockPointer>,
}

impl FakeStore {
    pub fn new() -> FakeStore {
        let genesis_block_hash = "d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3".parse().unwrap();
        let genesis_block_ptr = (genesis_block_hash, 0u64).into();
        FakeStore {
            block_ptr: RefCell::new(genesis_block_ptr),
        }
    }
}

impl BasicStore for FakeStore {
    fn add_subgraph_if_missing(&self, _: SubgraphId) -> Result<(), Error> {
        Ok(())
    }

    fn block_ptr(&self, _subgraph_id: SubgraphId) -> Result<EthereumBlockPointer, Error> {
        Ok(self.block_ptr.borrow().clone())
    }

    fn set_block_ptr_with_no_changes(
        &self,
        _subgraph_id: SubgraphId,
        from: EthereumBlockPointer,
        to: EthereumBlockPointer,
    ) -> Result<(), StoreError> {
        assert_eq!(*self.block_ptr.borrow(), from);
        self.block_ptr.replace(to);
        Ok(())
    }

    fn revert_block(
        &self,
        _subgraph_id: SubgraphId,
        _block: Block<Transaction>,
    ) -> Result<(), StoreError> {
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
        _ptr_update: bool,
    ) -> Result<(), StoreError> {
        panic!("called FakeStore")
    }
}

impl BlockStore for FakeStore {
    fn upsert_blocks<'a, B>(&self, _: B) -> Box<Future<Item = (), Error = Error> + Send + 'a>
    where
        B: Stream<Item = Block<Transaction>, Error = Error> + Send + 'a,
    {
        panic!("called FakeStore")
    }

    fn attempt_head_update(&self, _ancestor_count: u64) -> Result<Vec<H256>, Error> {
        panic!("called FakeStore")
    }

    fn head_block_ptr(&self) -> Result<Option<EthereumBlockPointer>, Error> {
        Ok(Some((H256::zero(), 1u64).into()))
    }

    fn head_block_updates(&self) -> Box<Stream<Item = HeadBlockUpdateEvent, Error = Error> + Send> {
        // Emit a bunch of fake head block updates to the same block
        Box::new(
            Interval::new(Instant::now(), Duration::from_secs(1))
                .then(|_| {
                    Ok(HeadBlockUpdateEvent {
                        block_ptr: (H256::zero(), 1u64).into()
                    })
                })
        )
    }

    fn block(&self, block_hash: H256) -> Result<Option<Block<Transaction>>, Error> {
        let genesis_block_hash = "d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3".parse().unwrap();
        if block_hash == H256::zero() {
            Ok(Some(Block {
                hash: Some(H256::zero()),
                parent_hash: genesis_block_hash,
                uncles_hash: H256::default(),
                author: H160::default(),
                state_root: H256::default(),
                transactions_root: H256::default(),
                receipts_root: H256::default(),
                number: Some(U128::from(1)),
                gas_used: U256::from(100),
                gas_limit: U256::from(1000),
                extra_data: Bytes(String::from("0x00").into_bytes()),
                logs_bloom: H2048::default(),
                timestamp: U256::from(100000),
                difficulty: U256::from(10),
                total_difficulty: U256::from(100),
                seal_fields: vec![],
                uncles: Vec::<H256>::default(),
                transactions: vec![],
                size: Some(U256::from(10000)),
            }))
        } else {
            panic!("called FakeStore")
        }
    }

    fn ancestor_block(
        &self,
        block_ptr: EthereumBlockPointer,
        offset: u64,
    ) -> Result<Option<Block<Transaction>>, Error> {
        let genesis_block_hash = "d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3".parse().unwrap();
        if block_ptr.number == 1 && offset == 1 {
            let fake_genesis_block = Block {
                hash: Some(genesis_block_hash),
                parent_hash: H256::zero(),
                uncles_hash: H256::default(),
                author: H160::default(),
                state_root: H256::default(),
                transactions_root: H256::default(),
                receipts_root: H256::default(),
                number: Some(U128::from(0)),
                gas_used: U256::from(100),
                gas_limit: U256::from(1000),
                extra_data: Bytes(String::from("0x00").into_bytes()),
                logs_bloom: H2048::default(),
                timestamp: U256::from(100000),
                difficulty: U256::from(10),
                total_difficulty: U256::from(100),
                seal_fields: vec![],
                uncles: Vec::<H256>::default(),
                transactions: vec![],
                size: Some(U256::from(10000)),
            };

            return Ok(Some(fake_genesis_block));
        }
        if block_ptr.number == 1 && offset == 0 {
            return Ok(Some(Block {
                hash: Some(H256::zero()),
                parent_hash: genesis_block_hash,
                uncles_hash: H256::default(),
                author: H160::default(),
                state_root: H256::default(),
                transactions_root: H256::default(),
                receipts_root: H256::default(),
                number: Some(U128::from(1)),
                gas_used: U256::from(100),
                gas_limit: U256::from(1000),
                extra_data: Bytes(String::from("0x00").into_bytes()),
                logs_bloom: H2048::default(),
                timestamp: U256::from(100000),
                difficulty: U256::from(10),
                total_difficulty: U256::from(100),
                seal_fields: vec![],
                uncles: Vec::<H256>::default(),
                transactions: vec![],
                size: Some(U256::from(10000)),
            }));
        }

        panic!("called FakeStore")
    }
}

impl Store for FakeStore {
    fn subscribe(&self, _entities: Vec<SubgraphEntityPair>) -> EntityChangeStream {
        unimplemented!();
    }
}
