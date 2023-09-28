use graph::blockchain::{EmptyNodeCapabilities, TriggerFilter as TriggerFilterTrait};

use crate::{
    data_source::{DataSource, DataSourceTemplate},
    Chain,
};

// Currently `TriggerFilter` is useless, as it doesn't really filter anything from the block
// stream, and the complete stream is always consumed, regardless of what the subgraph is indexing.
// This is very bad for indexing performance and must be implemented for it to be considered
// production-ready.
// TODO: implement trigger filter for much better performance
#[derive(Default, Clone)]
pub struct TriggerFilter;

impl TriggerFilterTrait<Chain> for TriggerFilter {
    #[allow(unused)]
    fn extend_with_template(&mut self, data_source: impl Iterator<Item = DataSourceTemplate>) {
        todo!()
    }

    #[allow(unused)]
    fn extend<'a>(&mut self, data_sources: impl Iterator<Item = &'a DataSource> + Clone) {}

    fn node_capabilities(&self) -> EmptyNodeCapabilities<Chain> {
        todo!()
    }

    fn to_firehose_filter(self) -> Vec<prost_types::Any> {
        todo!()
    }
}
