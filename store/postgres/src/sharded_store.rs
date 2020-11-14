use std::str::FromStr;
use std::{collections::BTreeMap, collections::HashMap, sync::Arc};

use diesel::Connection;

use graph::{
    components::store,
    data::subgraph::schema::MetadataType,
    data::subgraph::{schema::SubgraphError, status},
    prelude::{
        self,
        web3::types::{Address, H256},
        ApiSchema, BlockNumber, DynTryFuture, Error, EthereumBlockPointer, EthereumCallCache,
        Store as StoreTrait, SubgraphDeploymentId, SubgraphDeploymentStore, PRIMARY_SHARD,
    },
};
use prelude::{
    anyhow, DeploymentState, Entity, EntityKey, EntityModification, EntityQuery, Logger,
    MetadataOperation, QueryExecutionError, QueryStore, Schema, StopwatchMetrics, StoreError,
    StoreEventStreamBox, SubgraphEntityPair, SubgraphName,
};
use store::StoredDynamicDataSource;

use crate::store::Store;
use crate::{detail, metadata};

/// Multiplex store operations on subgraphs and deployments between a primary
/// and any number of additional storage shards. See [this document](../../docs/sharded.md)
/// for details on how storage is split up
pub struct ShardedStore {
    primary: Arc<Store>,
    stores: HashMap<String, Arc<Store>>,
}

impl ShardedStore {
    #[allow(dead_code)]
    pub fn new(stores: HashMap<String, Arc<Store>>) -> Self {
        assert_eq!(
            1,
            stores.len(),
            "The sharded store can only handle one shard for now"
        );
        let primary = stores
            .get(PRIMARY_SHARD)
            .expect("we always have a primary store")
            .clone();
        Self { primary, stores }
    }

    // Only needed for tests
    #[cfg(debug_assertions)]
    #[allow(dead_code)]
    pub(crate) fn clear_storage_cache(&self) {
        for store in self.stores.values() {
            store.storage_cache.lock().unwrap().clear();
        }
    }

    fn shard(&self, id: &SubgraphDeploymentId) -> Result<String, StoreError> {
        self.primary.shard(id)
    }

    fn store(&self, id: &SubgraphDeploymentId) -> Result<&Arc<Store>, StoreError> {
        let shard = self.shard(id)?;
        self.stores
            .get(&shard)
            .ok_or(StoreError::UnknownShard(shard))
    }

    // Only for tests to simplify their handling of test fixtures, so that
    // tests can reset the block pointer of a subgraph by recreating it
    #[cfg(debug_assertions)]
    pub fn create_deployment_replace(
        &self,
        name: SubgraphName,
        schema: &Schema,
        deployment: prelude::SubgraphDeploymentEntity,
        node_id: prelude::NodeId,
        mode: prelude::SubgraphVersionSwitchingMode,
    ) -> Result<(), StoreError> {
        // This works because we only allow one shard for now
        self.primary
            .create_deployment_replace(name, schema, deployment, node_id, mode)
    }
}

#[async_trait::async_trait]
impl StoreTrait for ShardedStore {
    fn block_ptr(
        &self,
        id: SubgraphDeploymentId,
    ) -> Result<Option<EthereumBlockPointer>, failure::Error> {
        let store = self.store(&id)?;
        store.block_ptr(id)
    }

    fn supports_proof_of_indexing<'a>(
        self: Arc<Self>,
        id: &'a SubgraphDeploymentId,
    ) -> DynTryFuture<'a, bool> {
        let store = self.store(&id).unwrap().clone();
        store.supports_proof_of_indexing(id)
    }

    fn get_proof_of_indexing<'a>(
        self: Arc<Self>,
        id: &'a SubgraphDeploymentId,
        indexer: &'a Option<Address>,
        block_hash: H256,
    ) -> DynTryFuture<'a, Option<[u8; 32]>> {
        let store = self.store(&id).unwrap().clone();
        store.get_proof_of_indexing(id, indexer, block_hash)
    }

    fn get(&self, key: EntityKey) -> Result<Option<Entity>, QueryExecutionError> {
        let store = self.store(&key.subgraph_id)?;
        store.get(key)
    }

    fn get_many(
        &self,
        id: &SubgraphDeploymentId,
        ids_for_type: BTreeMap<&str, Vec<&str>>,
    ) -> Result<BTreeMap<String, Vec<Entity>>, StoreError> {
        let store = self.store(&id)?;
        store.get_many(id, ids_for_type)
    }

    fn find(&self, query: EntityQuery) -> Result<Vec<Entity>, QueryExecutionError> {
        let store = self.store(&query.subgraph_id)?;
        store.find(query)
    }

    fn find_one(&self, query: EntityQuery) -> Result<Option<Entity>, QueryExecutionError> {
        let store = self.store(&query.subgraph_id)?;
        store.find_one(query)
    }

    fn find_ens_name(&self, hash: &str) -> Result<Option<String>, QueryExecutionError> {
        self.primary.find_ens_name(hash)
    }

    fn transact_block_operations(
        &self,
        id: SubgraphDeploymentId,
        block_ptr_to: EthereumBlockPointer,
        mods: Vec<EntityModification>,
        stopwatch: StopwatchMetrics,
        deterministic_errors: Vec<SubgraphError>,
    ) -> Result<(), StoreError> {
        assert!(
            mods.in_shard(&id),
            "can only transact operations within one shard"
        );
        let store = self.store(&id)?;
        store.transact_block_operations(id, block_ptr_to, mods, stopwatch, deterministic_errors)
    }

    fn apply_metadata_operations(
        &self,
        target_deployment: &SubgraphDeploymentId,
        operations: Vec<MetadataOperation>,
    ) -> Result<(), StoreError> {
        assert!(
            operations.in_shard(target_deployment),
            "can only apply metadata operations for SubgraphDeployment and its subobjects"
        );

        let store = self.store(&target_deployment)?;
        store.apply_metadata_operations(target_deployment, operations)
    }

    fn revert_block_operations(
        &self,
        id: SubgraphDeploymentId,
        block_ptr_from: EthereumBlockPointer,
        block_ptr_to: EthereumBlockPointer,
    ) -> Result<(), StoreError> {
        let store = self.store(&id)?;
        store.revert_block_operations(id, block_ptr_from, block_ptr_to)
    }

    fn subscribe(&self, entities: Vec<SubgraphEntityPair>) -> StoreEventStreamBox {
        // Subscriptions always go through the primary
        self.primary.subscribe(entities)
    }

    fn deployment_state_from_name(
        &self,
        name: SubgraphName,
    ) -> Result<DeploymentState, StoreError> {
        let conn = self.primary.get_conn()?;
        let id = conn.transaction(|| metadata::current_deployment_for_subgraph(&conn, name))?;
        self.deployment_state_from_id(id)
    }

    fn deployment_state_from_id(
        &self,
        id: SubgraphDeploymentId,
    ) -> Result<DeploymentState, StoreError> {
        let store = self.store(&id)?;
        store.deployment_state_from_id(id)
    }

    fn start_subgraph_deployment(
        &self,
        logger: &Logger,
        id: &SubgraphDeploymentId,
    ) -> Result<(), StoreError> {
        let store = self.store(id)?;
        store.start_subgraph_deployment(logger, id)
    }

    fn block_number(
        &self,
        id: &SubgraphDeploymentId,
        block_hash: H256,
    ) -> Result<Option<BlockNumber>, StoreError> {
        let store = self.store(&id)?;
        store.block_number(id, block_hash)
    }

    fn query_store(
        self: Arc<Self>,
        id: &SubgraphDeploymentId,
        for_subscription: bool,
    ) -> Result<Arc<dyn QueryStore + Send + Sync>, StoreError> {
        assert!(
            !id.is_meta(),
            "a query store can only be retrieved for a concrete subgraph"
        );
        let store = self.store(&id)?.clone();
        store.query_store(id, for_subscription)
    }

    fn deployment_synced(&self, id: &SubgraphDeploymentId) -> Result<(), Error> {
        let store = self.store(&id)?;
        store.deployment_synced(id)
    }

    fn create_subgraph_deployment(
        &self,
        name: SubgraphName,
        schema: &Schema,
        deployment: prelude::SubgraphDeploymentEntity,
        node_id: prelude::NodeId,
        network: String,
        mode: prelude::SubgraphVersionSwitchingMode,
    ) -> Result<(), StoreError> {
        // We only allow one shard (the primary) for now, so it is fine
        // to forward this to the primary store
        self.primary
            .create_subgraph_deployment(name, schema, deployment, node_id, network, mode)
    }

    fn create_subgraph(&self, name: SubgraphName) -> Result<String, StoreError> {
        self.primary.create_subgraph(name)
    }

    fn remove_subgraph(&self, name: SubgraphName) -> Result<(), StoreError> {
        self.primary.remove_subgraph(name)
    }

    fn reassign_subgraph(
        &self,
        id: &SubgraphDeploymentId,
        node_id: &prelude::NodeId,
    ) -> Result<(), StoreError> {
        self.primary.reassign_subgraph(id, node_id)
    }

    fn status(&self, filter: status::Filter) -> Result<Vec<status::Info>, StoreError> {
        let conn = self.primary.get_conn()?;
        let (deployments, empty_means_all) = conn.transaction(|| -> Result<_, StoreError> {
            match filter {
                status::Filter::SubgraphName(name) => {
                    let deployments = detail::deployments_for_subgraph(&conn, name)?;
                    Ok((deployments, false))
                }
                status::Filter::SubgraphVersion(name, use_current) => {
                    let deployments = detail::subgraph_version(&conn, name, use_current)?
                        .map(|d| vec![d])
                        .unwrap_or_else(|| vec![]);
                    Ok((deployments, false))
                }
                status::Filter::Deployments(deployments) => Ok((deployments, true)),
            }
        })?;

        if deployments.is_empty() && !empty_means_all {
            return Ok(Vec::new());
        }

        // Ignore invalid subgraph ids
        let deployments: Vec<SubgraphDeploymentId> = deployments
            .iter()
            .filter_map(|d| SubgraphDeploymentId::new(d).ok())
            .collect();

        // For each deployment, find the shard it lives in
        let deployments_with_shard: Vec<(SubgraphDeploymentId, String)> = deployments
            .into_iter()
            .map(|id| self.shard(&id).map(|shard| (id, shard)))
            .collect::<Result<Vec<_>, StoreError>>()?;

        // Partition the list of deployments by shard
        let deployments_by_shard: HashMap<String, Vec<SubgraphDeploymentId>> =
            deployments_with_shard
                .into_iter()
                .fold(HashMap::new(), |mut map, (id, shard)| {
                    map.entry(shard).or_default().push(id);
                    map
                });

        // Go shard-by-shard to look up deployment statuses
        let mut infos = Vec::new();
        for (shard, ids) in deployments_by_shard.into_iter() {
            let store = self
                .stores
                .get(&shard)
                .ok_or(StoreError::UnknownShard(shard))?;
            let ids = ids.into_iter().map(|id| id.to_string()).collect();
            infos.extend(store.deployment_statuses(ids)?);
        }

        Ok(infos)
    }

    fn load_dynamic_data_sources(
        &self,
        id: &SubgraphDeploymentId,
    ) -> Result<Vec<StoredDynamicDataSource>, StoreError> {
        let store = self.store(id)?;
        store.load_dynamic_data_sources(id)
    }

    async fn fail_subgraph(
        &self,
        id: SubgraphDeploymentId,
        error: graph::data::subgraph::schema::SubgraphError,
    ) -> Result<(), prelude::anyhow::Error> {
        let store = self.store(&id).map_err(|e| anyhow::anyhow!(e))?;
        store.fail_subgraph(id, error).await
    }
}

/// Methods similar to those for SubgraphDeploymentStore
impl SubgraphDeploymentStore for ShardedStore {
    fn input_schema(&self, id: &SubgraphDeploymentId) -> Result<Arc<Schema>, Error> {
        let info = self.store(&id)?.subgraph_info(id)?;
        Ok(info.input)
    }

    fn api_schema(&self, id: &SubgraphDeploymentId) -> Result<Arc<ApiSchema>, Error> {
        let info = self.store(&id)?.subgraph_info(id)?;
        Ok(info.api)
    }

    fn network_name(&self, id: &SubgraphDeploymentId) -> Result<Option<String>, Error> {
        let info = self.store(&id)?.subgraph_info(id)?;
        Ok(info.network)
    }
}

impl EthereumCallCache for ShardedStore {
    fn get_call(
        &self,
        contract_address: Address,
        encoded_call: &[u8],
        block: EthereumBlockPointer,
    ) -> Result<Option<Vec<u8>>, failure::Error> {
        self.primary.get_call(contract_address, encoded_call, block)
    }

    fn set_call(
        &self,
        contract_address: Address,
        encoded_call: &[u8],
        block: EthereumBlockPointer,
        return_value: &[u8],
    ) -> Result<(), failure::Error> {
        self.primary
            .set_call(contract_address, encoded_call, block, return_value)
    }
}

trait ShardData {
    // Return `true` if this object resides in the shard for the
    // data for the given deployment
    fn in_shard(&self, id: &SubgraphDeploymentId) -> bool;
}

impl ShardData for MetadataType {
    fn in_shard(&self, _: &SubgraphDeploymentId) -> bool {
        use MetadataType::*;

        match self {
            Subgraph | SubgraphDeploymentAssignment => false,
            SubgraphDeployment
            | SubgraphManifest
            | EthereumContractDataSource
            | DynamicEthereumContractDataSource
            | EthereumContractSource
            | EthereumContractMapping
            | EthereumContractAbi
            | EthereumBlockHandlerEntity
            | EthereumBlockHandlerFilterEntity
            | EthereumCallHandlerEntity
            | EthereumContractEventHandler
            | EthereumContractDataSourceTemplate
            | EthereumContractDataSourceTemplateSource
            | SubgraphError => true,
        }
    }
}

impl ShardData for MetadataOperation {
    fn in_shard(&self, id: &SubgraphDeploymentId) -> bool {
        use MetadataOperation::*;
        match self {
            Set { entity, .. } | Remove { entity, .. } | Update { entity, .. } => {
                entity.in_shard(id)
            }
        }
    }
}

impl<T> ShardData for Vec<T>
where
    T: ShardData,
{
    fn in_shard(&self, id: &SubgraphDeploymentId) -> bool {
        self.iter().all(|op| op.in_shard(id))
    }
}

impl ShardData for EntityModification {
    fn in_shard(&self, id: &SubgraphDeploymentId) -> bool {
        let key = self.entity_key();
        let mod_id = &key.subgraph_id;

        if mod_id.is_meta() {
            // We do not flag an unknown MetadataType as an error here since
            // there are some valid types of metadata, e.g. SubgraphVersion
            // that are not reflected in the enum. We are just careful and
            // assume they are not stored in the same shard as subgraph data
            MetadataType::from_str(&key.entity_type)
                .ok()
                .map(|typ| typ.in_shard(id))
                .unwrap_or(false)
        } else {
            mod_id == id
        }
    }
}
