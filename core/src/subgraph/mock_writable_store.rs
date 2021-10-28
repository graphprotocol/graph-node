use async_trait::async_trait;
use graph::components::store::WritableStore;
use graph::data::subgraph::*;
use graph::prelude::StoreError;
use graph::{
    blockchain::BlockPtr,
    prelude::{Logger, StopwatchMetrics},
};
use std::result::Result;

pub struct MockWritableStore {}

#[async_trait]
impl WritableStore for MockWritableStore {
    fn block_ptr(&self) -> Result<Option<BlockPtr>, StoreError> {
        unreachable!()
    }

    fn block_cursor(&self) -> Result<Option<String>, StoreError> {
        unreachable!()
    }

    fn start_subgraph_deployment(&self, _logger: &Logger) -> Result<(), StoreError> {
        Ok(())
    }

    fn revert_block_operations(&self, _block_ptr_to: BlockPtr) -> Result<(), StoreError> {
        unreachable!()
    }

    fn unfail(
        &self,
        _current_ptr: Option<BlockPtr>,
        _parent_ptr: Option<BlockPtr>,
    ) -> Result<(), StoreError> {
        unreachable!()
    }

    async fn fail_subgraph(&self, _error: schema::SubgraphError) -> Result<(), StoreError> {
        unreachable!()
    }

    async fn supports_proof_of_indexing(&self) -> Result<bool, StoreError> {
        unreachable!()
    }

    fn get(
        &self,
        _key: &graph::prelude::EntityKey,
    ) -> Result<Option<graph::prelude::Entity>, StoreError> {
        unreachable!()
    }

    fn transact_block_operations(
        &self,
        _block_ptr_to: BlockPtr,
        _firehose_cursor: Option<String>,
        _mods: Vec<graph::prelude::EntityModification>,
        _stopwatch: StopwatchMetrics,
        _data_sources: Vec<graph::components::store::StoredDynamicDataSource>,
        _deterministic_errors: Vec<schema::SubgraphError>,
    ) -> Result<(), StoreError> {
        unreachable!()
    }

    fn get_many(
        &self,
        _ids_for_type: std::collections::BTreeMap<&graph::components::store::EntityType, Vec<&str>>,
    ) -> Result<
        std::collections::BTreeMap<
            graph::components::store::EntityType,
            Vec<graph::prelude::Entity>,
        >,
        StoreError,
    > {
        unreachable!()
    }

    fn deployment_synced(&self) -> Result<(), StoreError> {
        unreachable!()
    }

    async fn is_deployment_synced(&self) -> Result<bool, StoreError> {
        unreachable!()
    }

    fn unassign_subgraph(&self) -> Result<(), StoreError> {
        unreachable!()
    }

    async fn load_dynamic_data_sources(
        &self,
    ) -> Result<Vec<graph::components::store::StoredDynamicDataSource>, StoreError> {
        unreachable!()
    }

    fn shard(&self) -> &str {
        unreachable!()
    }
}
