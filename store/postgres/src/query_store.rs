use std::collections::HashMap;
use std::time::Instant;

use crate::deployment_store::{DeploymentStore, ReplicaId};
use graph::components::store::{DeploymentId, QueryPermit, QueryStore as QueryStoreTrait};
use graph::data::query::Trace;
use graph::data::store::QueryObject;
use graph::prelude::*;
use graph::schema::{ApiSchema, InputSchema};

use crate::primary::Site;

pub(crate) struct QueryStore {
    site: Arc<Site>,
    replica_id: ReplicaId,
    store: Arc<DeploymentStore>,
    chain_store: Arc<crate::ChainStore>,
    api_version: Arc<ApiVersion>,
}

impl QueryStore {
    pub(crate) fn new(
        store: Arc<DeploymentStore>,
        chain_store: Arc<crate::ChainStore>,
        site: Arc<Site>,
        replica_id: ReplicaId,
        api_version: Arc<ApiVersion>,
    ) -> Self {
        QueryStore {
            site,
            replica_id,
            store,
            chain_store,
            api_version,
        }
    }
}

#[async_trait]
impl QueryStoreTrait for QueryStore {
    fn find_query_values(
        &self,
        query: EntityQuery,
    ) -> Result<(Vec<QueryObject>, Trace), graph::prelude::QueryExecutionError> {
        assert_eq!(&self.site.deployment, &query.subgraph_id);
        let start = Instant::now();
        let mut conn = self
            .store
            .get_replica_conn(self.replica_id)
            .map_err(|e| QueryExecutionError::StoreError(e.into()))?;
        let wait = start.elapsed();
        self.store
            .execute_query(&mut conn, self.site.clone(), query)
            .map(|(entities, mut trace)| {
                trace.conn_wait(wait);
                (entities, trace)
            })
    }

    /// Return true if the deployment with the given id is fully synced,
    /// and return false otherwise. Errors from the store are passed back up
    async fn is_deployment_synced(&self) -> Result<bool, Error> {
        Ok(self
            .store
            .exists_and_synced(self.site.deployment.cheap_clone())
            .await?)
    }

    async fn block_ptr(&self) -> Result<Option<BlockPtr>, StoreError> {
        self.store.block_ptr(self.site.cheap_clone()).await
    }
    async fn block_number_with_timestamp_and_parent_hash(
        &self,
        block_hash: &BlockHash,
    ) -> Result<Option<(BlockNumber, Option<u64>, Option<BlockHash>)>, StoreError> {
        // We should also really check that the block with the given hash is
        // on the chain starting at the subgraph's current head. That check is
        // very expensive though with the data structures we have currently
        // available. Ideally, we'd have the last REORG_THRESHOLD blocks in
        // memory so that we can check against them, and then mark in the
        // database the blocks on the main chain that we consider final
        let subgraph_network = self.network_name();
        self.chain_store
            .block_number(block_hash)
            .await?
            .map(|(network_name, number, timestamp, parent_hash)| {
                if network_name == subgraph_network {
                    Ok((number, timestamp, parent_hash))
                } else {
                    Err(StoreError::QueryExecutionError(format!(
                        "subgraph {} belongs to network {} but block {:x} belongs to network {}",
                        &self.site.deployment, subgraph_network, block_hash, network_name
                    )))
                }
            })
            .transpose()
    }

    async fn block_number(
        &self,
        block_hash: &BlockHash,
    ) -> Result<Option<BlockNumber>, StoreError> {
        self.block_number_with_timestamp_and_parent_hash(block_hash)
            .await
            .map(|opt| opt.map(|(number, _, _)| number))
    }

    async fn block_numbers(
        &self,
        block_hashes: Vec<BlockHash>,
    ) -> Result<HashMap<BlockHash, BlockNumber>, StoreError> {
        self.chain_store.block_numbers(block_hashes).await
    }

    fn wait_stats(&self) -> Result<PoolWaitStats, StoreError> {
        self.store.wait_stats(self.replica_id)
    }

    async fn deployment_state(&self) -> Result<DeploymentState, QueryExecutionError> {
        Ok(self
            .store
            .deployment_state_from_id(self.site.deployment.clone())
            .await?)
    }

    fn api_schema(&self) -> Result<Arc<ApiSchema>, QueryExecutionError> {
        let info = self.store.subgraph_info(self.site.cheap_clone())?;
        Ok(info.api.get(&self.api_version).unwrap().clone())
    }

    fn input_schema(&self) -> Result<InputSchema, QueryExecutionError> {
        let layout = self.store.find_layout(self.site.cheap_clone())?;
        Ok(layout.input_schema.cheap_clone())
    }

    fn network_name(&self) -> &str {
        &self.site.network
    }

    async fn query_permit(&self) -> Result<QueryPermit, StoreError> {
        self.store.query_permit(self.replica_id).await
    }

    fn shard(&self) -> &str {
        self.site.shard.as_str()
    }

    fn deployment_id(&self) -> DeploymentId {
        self.site.id.into()
    }
}
