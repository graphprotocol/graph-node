use crate::capabilities::NodeCapabilities;
use crate::{data_source::DataSource, Chain};
use graph::blockchain as bc;
use graph::prelude::*;

#[derive(Clone, Debug, Default)]
pub struct TriggerFilter {
    pub(crate) block: NearBlockFilter,
}

impl bc::TriggerFilter<Chain> for TriggerFilter {
    fn extend<'a>(&mut self, data_sources: impl Iterator<Item = &'a DataSource> + Clone) {
        self.block
            .extend(NearBlockFilter::from_data_sources(data_sources));
    }

    fn node_capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {}
    }

    fn extend_with_template(
        &mut self,
        _data_source: impl Iterator<Item = <Chain as bc::Blockchain>::DataSourceTemplate>,
    ) {
    }

    fn to_firehose_filter(self) -> Vec<prost_types::Any> {
        vec![]
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct NearBlockFilter {
    pub trigger_every_block: bool,
}

impl NearBlockFilter {
    pub fn from_data_sources<'a>(iter: impl IntoIterator<Item = &'a DataSource>) -> Self {
        iter.into_iter()
            .filter(|data_source| data_source.source.account.is_some())
            .fold(Self::default(), |mut filter_opt, _data_source| {
                filter_opt.extend(Self {
                    trigger_every_block: true,
                });
                filter_opt
            })
    }

    pub fn extend(&mut self, other: NearBlockFilter) {
        self.trigger_every_block = self.trigger_every_block || other.trigger_every_block;
    }
}
