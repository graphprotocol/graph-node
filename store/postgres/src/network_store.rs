use std::sync::Arc;

use graph::prelude::{
    ethabi,
    web3::types::{Address, H256},
    BlockNumber, ChainHeadUpdateStream, ChainStore as ChainStoreTrait, CheapClone, Error,
    EthereumBlock, EthereumBlockPointer, EthereumCallCache, Future, LightEthereumBlock,
    Store as StoreTrait, Stream, SubgraphDeploymentStore,
};

use crate::chain_store::ChainStore;
use crate::store::Store;

pub struct NetworkStore {
    store: Arc<Store>,
    chain_store: ChainStore,
}

impl NetworkStore {
    pub fn new(store: Arc<Store>, chain_store: ChainStore) -> Self {
        Self { store, chain_store }
    }

    pub fn store(&self) -> Arc<Store> {
        self.store.cheap_clone()
    }

    // Only needed for tests
    #[cfg(debug_assertions)]
    pub(crate) fn clear_storage_cache(&self) {
        self.store.storage_cache.lock().unwrap().clear();
    }
}

impl StoreTrait for NetworkStore {
    fn block_ptr(
        &self,
        subgraph_id: graph::prelude::SubgraphDeploymentId,
    ) -> Result<Option<EthereumBlockPointer>, failure::Error> {
        self.store.block_ptr(subgraph_id)
    }

    fn supports_proof_of_indexing<'a>(
        &'a self,
        subgraph_id: &'a graph::prelude::SubgraphDeploymentId,
    ) -> graph::prelude::DynTryFuture<'a, bool> {
        self.store.supports_proof_of_indexing(subgraph_id)
    }

    fn get_proof_of_indexing<'a>(
        &'a self,
        subgraph_id: &'a graph::prelude::SubgraphDeploymentId,
        indexer: &'a Option<Address>,
        block_hash: H256,
    ) -> graph::prelude::DynTryFuture<'a, Option<[u8; 32]>> {
        self.store
            .get_proof_of_indexing(subgraph_id, indexer, block_hash)
    }

    fn get(
        &self,
        key: graph::prelude::EntityKey,
    ) -> Result<Option<graph::prelude::Entity>, graph::prelude::QueryExecutionError> {
        self.store.get(key)
    }

    fn get_many(
        &self,
        subgraph_id: &graph::prelude::SubgraphDeploymentId,
        ids_for_type: std::collections::BTreeMap<&str, Vec<&str>>,
    ) -> Result<
        std::collections::BTreeMap<String, Vec<graph::prelude::Entity>>,
        graph::prelude::StoreError,
    > {
        self.store.get_many(subgraph_id, ids_for_type)
    }

    fn find(
        &self,
        query: graph::prelude::EntityQuery,
    ) -> Result<Vec<graph::prelude::Entity>, graph::prelude::QueryExecutionError> {
        self.store.find(query)
    }

    fn find_one(
        &self,
        query: graph::prelude::EntityQuery,
    ) -> Result<Option<graph::prelude::Entity>, graph::prelude::QueryExecutionError> {
        self.store.find_one(query)
    }

    fn find_ens_name(
        &self,
        hash: &str,
    ) -> Result<Option<String>, graph::prelude::QueryExecutionError> {
        self.store.find_ens_name(hash)
    }

    fn transact_block_operations(
        &self,
        subgraph_id: graph::prelude::SubgraphDeploymentId,
        block_ptr_to: EthereumBlockPointer,
        mods: Vec<graph::prelude::EntityModification>,
        stopwatch: graph::prelude::StopwatchMetrics,
    ) -> Result<bool, graph::prelude::StoreError> {
        self.store
            .transact_block_operations(subgraph_id, block_ptr_to, mods, stopwatch)
    }

    fn apply_metadata_operations(
        &self,
        operations: Vec<graph::prelude::MetadataOperation>,
    ) -> Result<(), graph::prelude::StoreError> {
        self.store.apply_metadata_operations(operations)
    }

    fn build_entity_attribute_indexes(
        &self,
        subgraph: &graph::prelude::SubgraphDeploymentId,
        indexes: Vec<graph::prelude::AttributeIndexDefinition>,
    ) -> Result<(), graph::prelude::SubgraphAssignmentProviderError> {
        self.store.build_entity_attribute_indexes(subgraph, indexes)
    }

    fn revert_block_operations(
        &self,
        subgraph_id: graph::prelude::SubgraphDeploymentId,
        block_ptr_from: EthereumBlockPointer,
        block_ptr_to: EthereumBlockPointer,
    ) -> Result<(), graph::prelude::StoreError> {
        self.store
            .revert_block_operations(subgraph_id, block_ptr_from, block_ptr_to)
    }

    fn subscribe(
        &self,
        entities: Vec<graph::prelude::SubgraphEntityPair>,
    ) -> graph::prelude::StoreEventStreamBox {
        self.store.subscribe(entities)
    }

    fn deployment_state_from_name(
        &self,
        name: graph::prelude::SubgraphName,
    ) -> Result<graph::prelude::DeploymentState, graph::prelude::StoreError> {
        self.store.deployment_state_from_name(name)
    }

    fn deployment_state_from_id(
        &self,
        id: graph::prelude::SubgraphDeploymentId,
    ) -> Result<graph::prelude::DeploymentState, graph::prelude::StoreError> {
        self.store.deployment_state_from_id(id)
    }

    fn create_subgraph_deployment(
        &self,
        schema: &graph::prelude::Schema,
        ops: Vec<graph::prelude::MetadataOperation>,
    ) -> Result<(), graph::prelude::StoreError> {
        self.store.create_subgraph_deployment(schema, ops)
    }

    fn start_subgraph_deployment(
        &self,
        logger: &graph::prelude::Logger,
        subgraph_id: &graph::prelude::SubgraphDeploymentId,
        ops: Vec<graph::prelude::MetadataOperation>,
    ) -> Result<(), graph::prelude::StoreError> {
        self.store
            .start_subgraph_deployment(logger, subgraph_id, ops)
    }

    fn migrate_subgraph_deployment(
        &self,
        logger: &graph::prelude::Logger,
        subgraph_id: &graph::prelude::SubgraphDeploymentId,
        block_ptr: &EthereumBlockPointer,
    ) {
        self.store
            .migrate_subgraph_deployment(logger, subgraph_id, block_ptr)
    }

    fn block_number(
        &self,
        subgraph_id: &graph::prelude::SubgraphDeploymentId,
        block_hash: H256,
    ) -> Result<Option<BlockNumber>, graph::prelude::StoreError> {
        self.store.block_number(subgraph_id, block_hash)
    }

    fn query_store(
        self: Arc<Self>,
        for_subscription: bool,
    ) -> Arc<dyn graph::prelude::QueryStore + Send + Sync> {
        self.store.cheap_clone().query_store(for_subscription)
    }
}

impl SubgraphDeploymentStore for NetworkStore {
    fn input_schema(
        &self,
        subgraph_id: &graph::prelude::SubgraphDeploymentId,
    ) -> Result<Arc<graph::prelude::Schema>, failure::Error> {
        self.store.input_schema(subgraph_id)
    }

    fn api_schema(
        &self,
        subgraph_id: &graph::prelude::SubgraphDeploymentId,
    ) -> Result<Arc<graph::prelude::ApiSchema>, failure::Error> {
        self.store.api_schema(subgraph_id)
    }

    fn uses_relational_schema(
        &self,
        subgraph_id: &graph::prelude::SubgraphDeploymentId,
    ) -> Result<bool, failure::Error> {
        self.store.uses_relational_schema(subgraph_id)
    }

    fn network_name(
        &self,
        subgraph_id: &graph::prelude::SubgraphDeploymentId,
    ) -> Result<Option<String>, failure::Error> {
        self.store.network_name(subgraph_id)
    }
}

impl EthereumCallCache for NetworkStore {
    fn get_call(
        &self,
        contract_address: ethabi::Address,
        encoded_call: &[u8],
        block: EthereumBlockPointer,
    ) -> Result<Option<Vec<u8>>, failure::Error> {
        self.store.get_call(contract_address, encoded_call, block)
    }

    fn set_call(
        &self,
        contract_address: ethabi::Address,
        encoded_call: &[u8],
        block: EthereumBlockPointer,
        return_value: &[u8],
    ) -> Result<(), failure::Error> {
        self.store
            .set_call(contract_address, encoded_call, block, return_value)
    }
}

impl ChainStoreTrait for NetworkStore {
    fn genesis_block_ptr(&self) -> Result<EthereumBlockPointer, failure::Error> {
        self.chain_store.genesis_block_ptr()
    }

    fn upsert_blocks<B, E>(
        &self,
        blocks: B,
    ) -> Box<dyn Future<Item = (), Error = E> + Send + 'static>
    where
        B: Stream<Item = EthereumBlock, Error = E> + Send + 'static,
        E: From<Error> + Send + 'static,
        Self: Sized,
    {
        self.chain_store.upsert_blocks(blocks)
    }

    fn upsert_light_blocks(&self, blocks: Vec<LightEthereumBlock>) -> Result<(), failure::Error> {
        self.chain_store.upsert_light_blocks(blocks)
    }

    fn attempt_chain_head_update(&self, ancestor_count: u64) -> Result<Vec<H256>, failure::Error> {
        self.chain_store.attempt_chain_head_update(ancestor_count)
    }

    fn chain_head_updates(&self) -> ChainHeadUpdateStream {
        self.chain_store.chain_head_updates()
    }

    fn chain_head_ptr(&self) -> Result<Option<EthereumBlockPointer>, failure::Error> {
        self.chain_store.chain_head_ptr()
    }

    fn blocks(&self, hashes: Vec<H256>) -> Result<Vec<LightEthereumBlock>, failure::Error> {
        self.chain_store.blocks(hashes)
    }

    fn ancestor_block(
        &self,
        block_ptr: EthereumBlockPointer,
        offset: u64,
    ) -> Result<Option<EthereumBlock>, failure::Error> {
        self.chain_store.ancestor_block(block_ptr, offset)
    }

    fn cleanup_cached_blocks(
        &self,
        ancestor_count: u64,
    ) -> Result<(BlockNumber, usize), failure::Error> {
        self.chain_store.cleanup_cached_blocks(ancestor_count)
    }

    fn block_hashes_by_block_number(&self, number: u64) -> Result<Vec<H256>, failure::Error> {
        self.chain_store.block_hashes_by_block_number(number)
    }

    fn confirm_block_hash(&self, number: u64, hash: &H256) -> Result<usize, failure::Error> {
        self.chain_store.confirm_block_hash(number, hash)
    }
}
