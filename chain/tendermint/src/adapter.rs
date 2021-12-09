use crate::capabilities::NodeCapabilities;
use crate::{data_source::DataSource, Chain};
use graph::blockchain as bc;
use graph::prelude::*;
use mockall::automock;
use mockall::predicate::*;

#[derive(Clone, Debug, Default)]
pub struct TriggerFilter {
    pub(crate) block: TendermintBlockFilter,
}

impl bc::TriggerFilter<Chain> for TriggerFilter {
    fn extend<'a>(&mut self, data_sources: impl Iterator<Item = &'a DataSource> + Clone) {
        self.block
            .extend(TendermintBlockFilter::from_data_sources(data_sources));
    }

    fn node_capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {}
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct TendermintBlockFilter {
    pub trigger_every_block: bool,
}

impl TendermintBlockFilter {
    pub fn from_data_sources<'a>(iter: impl IntoIterator<Item = &'a DataSource>) -> Self {
        iter.into_iter()
            .filter(|data_source| data_source.source.address.is_some())
            .fold(Self::default(), |mut filter_opt, _data_source| {
                filter_opt.extend(Self {
                    trigger_every_block: true,
                });
                filter_opt
            })
    }

    pub fn extend(&mut self, other: TendermintBlockFilter) {
        self.trigger_every_block = self.trigger_every_block || other.trigger_every_block;
    }
}

#[automock]
#[async_trait]
pub trait TendermintAdapter: Send + Sync + 'static {
    /// The `provider.label` from the adapter's configuration
    fn provider(&self) -> &str;
}
