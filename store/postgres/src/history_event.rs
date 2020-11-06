//! A `HistoryEvent` is used to track entity operations that belong
//! together because they came from the same block in the JSONB storage
//! scheme
use graph::prelude::{EthereumBlockPointer, SubgraphDeploymentId};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct HistoryEvent {
    pub id: Option<i32>,
    pub subgraph: SubgraphDeploymentId,
    pub block_ptr: EthereumBlockPointer,
}

impl HistoryEvent {
    pub fn create_without_event_metadata(
        subgraph: SubgraphDeploymentId,
        block_ptr: EthereumBlockPointer,
    ) -> HistoryEvent {
        HistoryEvent {
            id: None,
            subgraph,
            block_ptr,
        }
    }
}
