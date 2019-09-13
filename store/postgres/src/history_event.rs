//! A `HistoryEvent` is used to track entity operations that belong
//! together because they came from the same block in the JSONB storage
//! scheme

use graph::prelude::{EthereumBlockPointer, SubgraphDeploymentId};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct HistoryEvent {
    pub id: i32,
    pub subgraph: SubgraphDeploymentId,
    pub block_ptr: EthereumBlockPointer,
}

impl HistoryEvent {
    pub fn to_event_source_string(event: &Option<&HistoryEvent>) -> String {
        event.map_or(String::from("none"), |event| event.block_ptr.hash_hex())
    }
}
