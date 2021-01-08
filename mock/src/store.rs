use mockall::predicate::*;
use mockall::*;
use std::collections::BTreeMap;

use graph::components::{server::index_node::VersionInfo, store::StoredDynamicDataSource};
use graph::data::subgraph::schema::SubgraphError;
use graph::prelude::*;
use graph::{components::store::EntityType, data::subgraph::status};
use web3::types::{Address, H256};

mock! {
    pub Store {
        fn get_mock(&self, key: EntityKey) -> Result<Option<Entity>, QueryExecutionError>;

        fn input_schema(&self, subgraph_id: &SubgraphDeploymentId) -> Result<Arc<Schema>, StoreError>;

        fn api_schema(&self, subgraph_id: &SubgraphDeploymentId) -> Result<Arc<ApiSchema>, StoreError>;

        fn network_name(&self, subgraph_id: &SubgraphDeploymentId) -> Result<Option<String>, StoreError>;
    }

    trait ChainStore: Send + Sync + 'static {
        fn genesis_block_ptr(&self) -> Result<EthereumBlockPointer, Error>;

        fn upsert_blocks<B, E>(&self, blocks: B) -> Box<dyn Future<Item = (), Error = E> + Send + 'static>
        where
            B: Stream<Item = EthereumBlock, Error = E> + Send + 'static,
            E: From<Error> + Send + 'static,
            Self: Sized;

        fn upsert_light_blocks(&self, blocks: Vec<LightEthereumBlock>) -> Result<(), Error>;

        fn attempt_chain_head_update(&self, ancestor_count: u64) -> Result<Vec<H256>, Error>;

        fn chain_head_updates(&self) -> ChainHeadUpdateStream;

        fn chain_head_ptr(&self) -> Result<Option<EthereumBlockPointer>, Error>;

        fn blocks(&self, hashes: Vec<H256>) -> Result<Vec<LightEthereumBlock>, Error>;

        fn ancestor_block(
            &self,
            block_ptr: EthereumBlockPointer,
            offset: u64,
        ) -> Result<Option<EthereumBlock>, Error>;

        fn cleanup_cached_blocks(&self, ancestor_count: u64) -> Result<(BlockNumber, usize), Error>;

        fn block_hashes_by_block_number(&self, number: u64) -> Result<Vec<H256>, Error>;

        fn confirm_block_hash(&self, number: u64, hash: &H256) -> Result<usize, Error>;

        fn block_number(&self, block_hash: H256) -> Result<Option<(String, BlockNumber)>, StoreError>;
    }
}

#[async_trait]
impl Store for MockStore {
    fn block_ptr(
        &self,
        _subgraph_id: &SubgraphDeploymentId,
    ) -> Result<Option<EthereumBlockPointer>, Error> {
        unimplemented!()
    }

    fn get(&self, key: EntityKey) -> Result<Option<Entity>, QueryExecutionError> {
        self.get_mock(key)
    }

    fn get_many(
        &self,
        _subgraph_id: &SubgraphDeploymentId,
        _ids_for_type: BTreeMap<&EntityType, Vec<&str>>,
    ) -> Result<BTreeMap<EntityType, Vec<Entity>>, StoreError> {
        unimplemented!()
    }

    fn supports_proof_of_indexing<'a>(
        self: Arc<Self>,
        _subgraph_id: &'a SubgraphDeploymentId,
    ) -> DynTryFuture<'a, bool> {
        unimplemented!()
    }

    fn get_proof_of_indexing<'a>(
        self: Arc<Self>,
        _subgraph_id: &'a SubgraphDeploymentId,
        _indexer: &'a Option<Address>,
        _block: EthereumBlockPointer,
    ) -> DynTryFuture<'a, Option<[u8; 32]>> {
        unimplemented!()
    }

    fn find(&self, _query: EntityQuery) -> Result<Vec<Entity>, QueryExecutionError> {
        unimplemented!()
    }

    fn find_one(&self, _query: EntityQuery) -> Result<Option<Entity>, QueryExecutionError> {
        unimplemented!()
    }

    fn find_ens_name(&self, _hash: &str) -> Result<Option<String>, QueryExecutionError> {
        unimplemented!()
    }

    fn transact_block_operations(
        &self,
        _subgraph_id: SubgraphDeploymentId,
        _block_ptr_to: EthereumBlockPointer,
        _mods: Vec<EntityModification>,
        _stopwatch: StopwatchMetrics,
        _deterministic_errors: Vec<SubgraphError>,
    ) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn revert_block_operations(
        &self,
        _subgraph_id: SubgraphDeploymentId,
        _block_ptr_to: EthereumBlockPointer,
    ) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn deployment_state_from_name(&self, _: SubgraphName) -> Result<DeploymentState, StoreError> {
        unimplemented!()
    }

    fn deployment_state_from_id(
        &self,
        id: SubgraphDeploymentId,
    ) -> Result<DeploymentState, StoreError> {
        Ok(DeploymentState {
            id,
            reorg_count: 0,
            max_reorg_depth: 0,
            latest_ethereum_block_number: 0,
        })
    }

    async fn fail_subgraph(
        &self,
        _: SubgraphDeploymentId,
        _: SubgraphError,
    ) -> Result<(), StoreError> {
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
    ) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn create_subgraph(&self, _: SubgraphName) -> Result<String, StoreError> {
        unimplemented!()
    }

    fn remove_subgraph(&self, _: SubgraphName) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn reassign_subgraph(&self, _: &SubgraphDeploymentId, _: &NodeId) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn start_subgraph_deployment(
        &self,
        _logger: &Logger,
        _subgraph_id: &SubgraphDeploymentId,
    ) -> Result<(), StoreError> {
        unimplemented!()
    }

    fn is_deployment_synced(&self, _: &SubgraphDeploymentId) -> Result<bool, Error> {
        unimplemented!()
    }

    fn deployment_synced(&self, _: &SubgraphDeploymentId) -> Result<(), Error> {
        unimplemented!()
    }

    fn status(&self, _: status::Filter) -> Result<Vec<status::Info>, StoreError> {
        unimplemented!()
    }

    async fn load_dynamic_data_sources(
        &self,
        _: SubgraphDeploymentId,
    ) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
        unimplemented!()
    }

    fn assigned_node(&self, _: &SubgraphDeploymentId) -> Result<Option<NodeId>, StoreError> {
        unimplemented!()
    }

    fn assignments(&self, _: &NodeId) -> Result<Vec<SubgraphDeploymentId>, StoreError> {
        unimplemented!()
    }

    fn subgraph_exists(&self, _: &SubgraphName) -> Result<bool, StoreError> {
        unimplemented!()
    }

    fn input_schema(&self, _: &SubgraphDeploymentId) -> Result<Arc<Schema>, StoreError> {
        unimplemented!()
    }

    fn api_schema(&self, _: &SubgraphDeploymentId) -> Result<Arc<ApiSchema>, StoreError> {
        unimplemented!()
    }

    fn network_name(&self, _: &SubgraphDeploymentId) -> Result<Option<String>, StoreError> {
        unimplemented!()
    }

    fn version_info(&self, _: &str) -> Result<VersionInfo, StoreError> {
        unimplemented!()
    }

    fn versions_for_subgraph_id(
        &self,
        _: &str,
    ) -> Result<(Option<String>, Option<String>), StoreError> {
        unimplemented!()
    }
}
