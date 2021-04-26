use mockall::predicate::*;
use mockall::*;

use graph::{components::store::DeploymentLocator, prelude::*};
use web3::types::H256;

mock! {
    pub Store {
        fn get_mock(&self, key: EntityKey) -> Result<Option<Entity>, QueryExecutionError>;

        fn input_schema(&self, subgraph_id: &DeploymentHash) -> Result<Arc<Schema>, StoreError>;

        fn api_schema(&self, subgraph_id: &DeploymentHash) -> Result<Arc<ApiSchema>, StoreError>;

        fn network_name(&self, subgraph_id: &DeploymentHash) -> Result<Option<String>, StoreError>;
    }

    #[async_trait]
    trait ChainStore: Send + Sync + 'static {
        fn genesis_block_ptr(&self) -> Result<BlockPtr, Error>;

        async fn upsert_block(&self, block: EthereumBlock) -> Result<(), Error>;

        fn upsert_light_blocks(&self, blocks: Vec<LightEthereumBlock>) -> Result<(), Error>;

        async fn attempt_chain_head_update(self: Arc<Self>, ancestor_count: BlockNumber) -> Result<Option<H256>, Error>;

        fn chain_head_ptr(&self) -> Result<Option<BlockPtr>, Error>;

        fn blocks(&self, hashes: Vec<H256>) -> Result<Vec<LightEthereumBlock>, Error>;

        fn ancestor_block(
            &self,
            block_ptr: BlockPtr,
            offset: BlockNumber,
        ) -> Result<Option<EthereumBlock>, Error>;

        fn cleanup_cached_blocks(&self, ancestor_count: BlockNumber) -> Result<Option<(BlockNumber, usize)>, Error>;

        fn block_hashes_by_block_number(&self, number: BlockNumber) -> Result<Vec<H256>, Error>;

        fn confirm_block_hash(&self, number: BlockNumber, hash: &H256) -> Result<usize, Error>;

        fn block_number(&self, block_hash: H256) -> Result<Option<(String, BlockNumber)>, StoreError>;
    }
}

#[async_trait]
impl SubgraphStore for MockStore {
    fn find_ens_name(&self, _hash: &str) -> Result<Option<String>, QueryExecutionError> {
        unimplemented!()
    }

    fn create_subgraph_deployment(
        &self,
        _: SubgraphName,
        _: &Schema,
        _: SubgraphDeploymentEntity,
        _: NodeId,
        _: String,
        _: SubgraphVersionSwitchingMode,
    ) -> Result<DeploymentLocator, StoreError> {
        unimplemented!()
    }

    fn create_subgraph(&self, _: SubgraphName) -> Result<String, StoreError> {
        unimplemented!()
    }

    fn remove_subgraph(&self, _: SubgraphName) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn reassign_subgraph(&self, _: &DeploymentLocator, _: &NodeId) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn assigned_node(&self, _: &DeploymentLocator) -> Result<Option<NodeId>, StoreError> {
        unimplemented!()
    }

    fn assignments(&self, _: &NodeId) -> Result<Vec<DeploymentLocator>, StoreError> {
        unimplemented!()
    }

    fn subgraph_exists(&self, _: &SubgraphName) -> Result<bool, StoreError> {
        unimplemented!()
    }

    fn input_schema(&self, _: &DeploymentHash) -> Result<Arc<Schema>, StoreError> {
        unimplemented!()
    }

    fn api_schema(&self, _: &DeploymentHash) -> Result<Arc<ApiSchema>, StoreError> {
        unimplemented!()
    }

    fn writable(
        &self,
        _: &DeploymentLocator,
    ) -> Result<Arc<dyn graph::components::store::WritableStore>, StoreError> {
        todo!()
    }

    fn is_deployed(&self, _: &DeploymentHash) -> Result<bool, Error> {
        todo!()
    }

    fn least_block_ptr(&self, _: &DeploymentHash) -> Result<Option<BlockPtr>, Error> {
        unimplemented!()
    }

    fn writable_for_network_indexer(
        &self,
        _: &DeploymentHash,
    ) -> Result<Arc<dyn graph::components::store::WritableStore>, StoreError> {
        unimplemented!()
    }

    fn locators(&self, _: &str) -> Result<Vec<DeploymentLocator>, StoreError> {
        unimplemented!()
    }
}
