use crate::capabilities::NodeCapabilities;
use crate::{data_source::DataSource, Chain};
use graph::blockchain as bc;
use graph::prelude::*;

#[derive(Clone, Debug, Default)]
pub struct TriggerFilter {}

impl bc::TriggerFilter<Chain> for TriggerFilter {
    fn extend<'a>(&mut self, _data_sources: impl Iterator<Item = &'a DataSource> + Clone) {}

    fn node_capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {}
    }
}
