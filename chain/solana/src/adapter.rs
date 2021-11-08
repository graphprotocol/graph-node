use crate::capabilities::NodeCapabilities;
use crate::{data_source::DataSource, Chain};
use graph::blockchain as bc;
use graph::prelude::*;

#[derive(Clone, Debug, Default)]
pub struct TriggerFilter {
    pub(crate) block: SolanaBlockFilter,
}

impl bc::TriggerFilter<Chain> for TriggerFilter {
    fn extend<'a>(&mut self, data_sources: impl Iterator<Item = &'a DataSource> + Clone) {
        self.block
            .extend(SolanaBlockFilter::from_data_sources(data_sources));
    }

    fn node_capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {}
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SolanaBlockFilter {
    pub trigger_every_block: bool,
}

impl SolanaBlockFilter {
    pub fn from_data_sources<'a>(iter: impl IntoIterator<Item = &'a DataSource>) -> Self {
        iter.into_iter()
            .filter(|data_source| data_source.source.program_id.is_some())
            .fold(Self::default(), |mut filter_opt, _data_source| {
                filter_opt.extend(Self {
                    trigger_every_block: true,
                });
                filter_opt
            })
    }

    pub fn extend(&mut self, other: SolanaBlockFilter) {
        self.trigger_every_block = self.trigger_every_block || other.trigger_every_block;
    }
}
