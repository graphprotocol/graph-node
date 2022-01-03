use std::collections::BTreeMap;

use web3::types::H256;

use crate::deployment_store::{DeploymentStore, ReplicaId};
use graph::components::store::QueryStore as QueryStoreTrait;
use graph::prelude::*;

use crate::primary::Site;

pub(crate) struct QueryStore {
    site: Arc<Site>,
    replica_id: ReplicaId,
    store: Arc<DeploymentStore>,
    chain_store: Arc<crate::ChainStore>,
}

impl QueryStore {
    pub(crate) fn new(
        store: Arc<DeploymentStore>,
        chain_store: Arc<crate::ChainStore>,
        site: Arc<Site>,
        replica_id: ReplicaId,
    ) -> Self {
        QueryStore {
            site,
            replica_id,
            store,
            chain_store,
        }
    }
}

#[async_trait]
impl QueryStoreTrait for QueryStore {
    fn find_query_values(
        &self,
        query: EntityQuery,
    ) -> Result<Vec<BTreeMap<String, r::Value>>, QueryExecutionError> {
        assert_eq!(&self.site.deployment, &query.subgraph_id);
        let conn = self
            .store
            .get_replica_conn(self.replica_id)
            .map_err(|e| QueryExecutionError::StoreError(e.into()))?;
        self.store.execute_query(&conn, self.site.clone(), query)
    }

    /// Return true if the deployment with the given id is fully synced,
    /// and return false otherwise. Errors from the store are passed back up
    fn is_deployment_synced(&self) -> Result<bool, Error> {
        Ok(self
            .store
            .exists_and_synced(self.site.deployment.cheap_clone())?)
    }

    fn block_ptr(&self) -> Result<Option<BlockPtr>, StoreError> {
        self.store.block_ptr(&self.site)
    }

    fn block_number(&self, block_hash: H256) -> Result<Option<BlockNumber>, StoreError> {
        // We should also really check that the block with the given hash is
        // on the chain starting at the subgraph's current head. That check is
        // very expensive though with the data structures we have currently
        // available. Ideally, we'd have the last REORG_THRESHOLD blocks in
        // memory so that we can check against them, and then mark in the
        // database the blocks on the main chain that we consider final
        let subgraph_network = self.network_name();
        self.chain_store
            .block_number(block_hash)?
            .map(|(network_name, number)| {
                if &network_name == subgraph_network {
                    BlockNumber::try_from(number)
                        .map_err(|e| StoreError::QueryExecutionError(e.to_string()))
                } else {
                    Err(StoreError::QueryExecutionError(format!(
                        "subgraph {} belongs to network {} but block {:x} belongs to network {}",
                        &self.site.deployment, subgraph_network, block_hash, network_name
                    )))
                }
            })
            .transpose()
    }

    fn wait_stats(&self) -> PoolWaitStats {
        self.store.wait_stats(self.replica_id)
    }

    async fn has_non_fatal_errors(&self, block: Option<BlockNumber>) -> Result<bool, StoreError> {
        let id = self.site.deployment.clone();
        self.store
            .with_conn(move |conn, _| {
                crate::deployment::has_non_fatal_errors(conn, &id, block).map_err(|e| e.into())
            })
            .await
    }

    async fn deployment_state(&self) -> Result<DeploymentState, QueryExecutionError> {
        Ok(self
            .store
            .deployment_state_from_id(self.site.deployment.clone())
            .await?)
    }

    fn api_schema(&self) -> Result<Arc<ApiSchema>, QueryExecutionError> {
        let info = self.store.subgraph_info(&self.site)?;
        Ok(info.api)
    }

    fn network_name(&self) -> &str {
        &self.site.network
    }

    async fn query_permit(&self) -> tokio::sync::OwnedSemaphorePermit {
        self.store.query_permit(self.replica_id).await
    }
}
