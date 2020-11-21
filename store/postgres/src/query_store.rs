use std::collections::BTreeMap;

use web3::types::H256;

use crate::store::ReplicaId;
use graph::components::store::QueryStore as QueryStoreTrait;
use graph::data::subgraph::status;
use graph::prelude::{Store as _, *};

pub(crate) struct QueryStore {
    replica_id: ReplicaId,
    store: Arc<crate::Store>,
    for_subscription: bool,
}

impl QueryStore {
    pub(crate) fn new(
        store: Arc<crate::Store>,
        for_subscription: bool,
        replica_id: ReplicaId,
    ) -> Self {
        QueryStore {
            replica_id,
            store,
            for_subscription,
        }
    }
}

impl QueryStoreTrait for QueryStore {
    fn find_query_values(
        &self,
        query: EntityQuery,
    ) -> Result<Vec<BTreeMap<String, graphql_parser::query::Value>>, QueryExecutionError> {
        let conn = self
            .store
            .get_entity_conn(&query.subgraph_id, self.replica_id)
            .map_err(|e| QueryExecutionError::StoreError(e.into()))?;
        self.store.execute_query(&conn, query)
    }

    fn subscribe(&self, entities: Vec<SubgraphEntityPair>) -> StoreEventStreamBox {
        assert!(self.for_subscription);
        assert_eq!(self.replica_id, ReplicaId::Main);
        self.store.subscribe(entities)
    }

    fn is_deployment_synced(&self, id: SubgraphDeploymentId) -> Result<bool, Error> {
        self.store.is_deployment_synced(id)
    }

    fn block_ptr(
        &self,
        subgraph_id: SubgraphDeploymentId,
    ) -> Result<Option<EthereumBlockPointer>, Error> {
        self.store.block_ptr(subgraph_id)
    }

    fn block_number(
        &self,
        subgraph_id: &SubgraphDeploymentId,
        block_hash: H256,
    ) -> Result<Option<BlockNumber>, StoreError> {
        self.store.block_number(subgraph_id, block_hash)
    }

    fn wait_stats(&self) -> &PoolWaitStats {
        self.store.wait_stats(self.replica_id)
    }

    fn status(&self, filter: status::Filter) -> Result<Vec<status::Info>, StoreError> {
        self.store.status(filter)
    }
}
