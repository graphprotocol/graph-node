use std::sync::Arc;

use graph::{
    components::{
        server::index_node::VersionInfo,
        store::{
            BlockStore as BlockStoreTrait, EntityType, QueryStoreManager, StoredDynamicDataSource,
        },
    },
    constraint_violation,
    data::subgraph::schema::SubgraphError,
    data::subgraph::status,
    prelude::{
        web3::types::Address, CheapClone, Error, EthereumBlockPointer, NodeId, QueryExecutionError,
        QueryStore as QueryStoreTrait, Schema, Store as StoreTrait, StoreError,
        SubgraphDeploymentEntity, SubgraphDeploymentId, SubgraphName, SubgraphVersionSwitchingMode,
    },
};

use crate::{block_store::BlockStore, query_store::QueryStore, SubgraphStore};

pub struct NetworkStore {
    store: Arc<SubgraphStore>,
    block_store: Arc<BlockStore>,
}

impl NetworkStore {
    pub fn new(store: Arc<SubgraphStore>, block_store: Arc<BlockStore>) -> Self {
        Self { store, block_store }
    }

    pub fn store(&self) -> Arc<SubgraphStore> {
        self.store.cheap_clone()
    }

    pub fn block_store(&self) -> Arc<BlockStore> {
        self.block_store.cheap_clone()
    }

    // Only for tests to simplify their handling of test fixtures, so that
    // tests can reset the block pointer of a subgraph by recreating it
    #[cfg(debug_assertions)]
    pub fn create_deployment_replace(
        &self,
        name: SubgraphName,
        schema: &Schema,
        deployment: SubgraphDeploymentEntity,
        node_id: NodeId,
        network_name: String,
        mode: SubgraphVersionSwitchingMode,
    ) -> Result<(), StoreError> {
        self.store
            .create_deployment_replace(name, schema, deployment, node_id, network_name, mode)
    }

    #[cfg(debug_assertions)]
    pub fn delete_all_entities_for_test_use_only(&self) -> Result<(), StoreError> {
        self.store.delete_all_entities_for_test_use_only()
    }
}

#[async_trait::async_trait]
impl StoreTrait for NetworkStore {
    fn block_ptr(
        &self,
        subgraph_id: &graph::prelude::SubgraphDeploymentId,
    ) -> Result<Option<EthereumBlockPointer>, Error> {
        self.store.block_ptr(subgraph_id)
    }

    fn supports_proof_of_indexing<'a>(
        self: Arc<Self>,
        subgraph_id: &'a graph::prelude::SubgraphDeploymentId,
    ) -> graph::prelude::DynTryFuture<'a, bool> {
        self.store.clone().supports_proof_of_indexing(subgraph_id)
    }

    fn get_proof_of_indexing<'a>(
        self: Arc<Self>,
        subgraph_id: &'a graph::prelude::SubgraphDeploymentId,
        indexer: &'a Option<Address>,
        block: EthereumBlockPointer,
    ) -> graph::prelude::DynTryFuture<'a, Option<[u8; 32]>> {
        self.store
            .clone()
            .get_proof_of_indexing(subgraph_id, indexer, block)
    }

    fn get(
        &self,
        key: graph::prelude::EntityKey,
    ) -> Result<Option<graph::prelude::Entity>, QueryExecutionError> {
        self.store.get(key)
    }

    fn get_many(
        &self,
        subgraph_id: &graph::prelude::SubgraphDeploymentId,
        ids_for_type: std::collections::BTreeMap<&EntityType, Vec<&str>>,
    ) -> Result<
        std::collections::BTreeMap<EntityType, Vec<graph::prelude::Entity>>,
        graph::prelude::StoreError,
    > {
        self.store.get_many(subgraph_id, ids_for_type)
    }

    fn find(
        &self,
        query: graph::prelude::EntityQuery,
    ) -> Result<Vec<graph::prelude::Entity>, QueryExecutionError> {
        self.store.find(query)
    }

    fn find_one(
        &self,
        query: graph::prelude::EntityQuery,
    ) -> Result<Option<graph::prelude::Entity>, QueryExecutionError> {
        self.store.find_one(query)
    }

    fn find_ens_name(&self, hash: &str) -> Result<Option<String>, QueryExecutionError> {
        self.store.find_ens_name(hash)
    }

    fn transact_block_operations(
        &self,
        subgraph_id: graph::prelude::SubgraphDeploymentId,
        block_ptr_to: EthereumBlockPointer,
        mods: Vec<graph::prelude::EntityModification>,
        stopwatch: graph::prelude::StopwatchMetrics,
        deterministic_errors: Vec<SubgraphError>,
    ) -> Result<(), graph::prelude::StoreError> {
        self.store.transact_block_operations(
            subgraph_id,
            block_ptr_to,
            mods,
            stopwatch,
            deterministic_errors,
        )
    }

    fn revert_block_operations(
        &self,
        subgraph_id: graph::prelude::SubgraphDeploymentId,
        block_ptr_to: EthereumBlockPointer,
    ) -> Result<(), graph::prelude::StoreError> {
        self.store
            .revert_block_operations(subgraph_id, block_ptr_to)
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

    async fn fail_subgraph(
        &self,
        id: SubgraphDeploymentId,
        error: SubgraphError,
    ) -> Result<(), StoreError> {
        self.store.fail_subgraph(id, error).await
    }

    fn create_subgraph_deployment(
        &self,
        name: SubgraphName,
        schema: &Schema,
        deployment: SubgraphDeploymentEntity,
        node_id: NodeId,
        network_name: String,
        mode: SubgraphVersionSwitchingMode,
    ) -> Result<(), StoreError> {
        self.store
            .create_subgraph_deployment(name, schema, deployment, node_id, network_name, mode)
    }

    fn start_subgraph_deployment(
        &self,
        logger: &graph::prelude::Logger,
        subgraph_id: &graph::prelude::SubgraphDeploymentId,
    ) -> Result<(), graph::prelude::StoreError> {
        self.store.start_subgraph_deployment(logger, subgraph_id)
    }

    fn is_deployment_synced(&self, id: &SubgraphDeploymentId) -> Result<bool, Error> {
        self.store.is_deployment_synced(id)
    }

    fn deployment_synced(&self, id: &graph::prelude::SubgraphDeploymentId) -> Result<(), Error> {
        self.store.deployment_synced(id)
    }

    fn remove_subgraph(&self, name: SubgraphName) -> Result<(), StoreError> {
        self.store.remove_subgraph(name)
    }

    fn reassign_subgraph(
        &self,
        id: &SubgraphDeploymentId,
        node: &NodeId,
    ) -> Result<(), StoreError> {
        self.store.reassign_subgraph(id, node)
    }

    fn unassign_subgraph(&self, id: &SubgraphDeploymentId) -> Result<(), StoreError> {
        self.store.unassign_subgraph(id)
    }

    fn create_subgraph(&self, name: SubgraphName) -> Result<String, StoreError> {
        self.store.create_subgraph(name)
    }

    fn status(&self, filter: status::Filter) -> Result<Vec<status::Info>, StoreError> {
        self.store.status(filter)
    }

    async fn load_dynamic_data_sources(
        &self,
        subgraph_id: SubgraphDeploymentId,
    ) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
        self.store.load_dynamic_data_sources(subgraph_id).await
    }

    fn assigned_node(&self, id: &SubgraphDeploymentId) -> Result<Option<NodeId>, StoreError> {
        self.store.assigned_node(id)
    }

    fn assignments(&self, node: &NodeId) -> Result<Vec<SubgraphDeploymentId>, StoreError> {
        self.store.assignments(node)
    }

    fn subgraph_exists(&self, name: &SubgraphName) -> Result<bool, StoreError> {
        self.store.subgraph_exists(name)
    }

    fn input_schema(
        &self,
        subgraph_id: &graph::prelude::SubgraphDeploymentId,
    ) -> Result<Arc<graph::prelude::Schema>, StoreError> {
        self.store.input_schema(subgraph_id)
    }

    fn api_schema(
        &self,
        subgraph_id: &graph::prelude::SubgraphDeploymentId,
    ) -> Result<Arc<graph::prelude::ApiSchema>, StoreError> {
        self.store.api_schema(subgraph_id)
    }

    fn network_name(
        &self,
        subgraph_id: &graph::prelude::SubgraphDeploymentId,
    ) -> Result<String, StoreError> {
        self.store.network_name(subgraph_id)
    }

    fn version_info(&self, version_id: &str) -> Result<VersionInfo, StoreError> {
        self.store.version_info(version_id)
    }

    fn versions_for_subgraph_id(
        &self,
        subgraph_id: &str,
    ) -> Result<(Option<String>, Option<String>), StoreError> {
        self.store.versions_for_subgraph_id(subgraph_id)
    }
}

impl QueryStoreManager for NetworkStore {
    fn query_store(
        &self,
        target: graph::data::query::QueryTarget,
        for_subscription: bool,
    ) -> Result<Arc<dyn QueryStoreTrait + Send + Sync>, QueryExecutionError> {
        let (store, site, replica) = self.store.replica_for_query(target, for_subscription)?;
        let chain_store = self.block_store.chain_store(&site.network).ok_or_else(|| {
            constraint_violation!(
                "Subgraphs index a known network, but {} indexes `{}` which we do not know about. This is most likely a configuration error.",
                site.deployment,
                site.network
            )
        })?;
        Ok(Arc::new(QueryStore::new(store, chain_store, site, replica)))
    }
}
