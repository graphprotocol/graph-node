//! A `HistoryEvent` is used to track entity operations that belong
//! together because they came from the same block in the JSONB storage
//! scheme

use std::fmt;

use graph::prelude::{Deserialize, EthereumBlockPointer, Serialize, SubgraphDeploymentId};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub(crate) enum EventSource {
    EthereumBlock(EthereumBlockPointer),
}

impl fmt::Display for EventSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EventSource::EthereumBlock(block_ptr) => f.write_str(&block_ptr.hash_hex()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct HistoryEvent {
    pub id: i32,
    pub subgraph: SubgraphDeploymentId,
    pub source: EventSource,
}

impl HistoryEvent {
    pub fn to_event_source_string(event: &Option<&HistoryEvent>) -> String {
        event.map_or(String::from("none"), |event| event.source.to_string())
    }
}
