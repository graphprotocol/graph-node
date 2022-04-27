use crate::capabilities::NodeCapabilities;
use crate::{data_source::DataSource, Chain};
use graph::blockchain as bc;
use graph::prelude::*;
use std::collections::HashSet;

#[derive(Clone, Debug, Default)]
pub struct TriggerFilter {
    pub(crate) block_filter: ArweaveBlockFilter,
    pub(crate) transaction_filter: ArweaveTransactionFilter,
}

impl bc::TriggerFilter<Chain> for TriggerFilter {
    fn extend<'a>(&mut self, data_sources: impl Iterator<Item = &'a DataSource> + Clone) {
        let TriggerFilter {
            block_filter,
            transaction_filter,
        } = self;

        block_filter.extend(ArweaveBlockFilter::from_data_sources(data_sources.clone()));
        transaction_filter.extend(ArweaveTransactionFilter::from_data_sources(data_sources));
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

/// ArweaveBlockFilter will match every block regardless of source being set.
/// see docs: https://thegraph.com/docs/en/supported-networks/arweave/
#[derive(Clone, Debug, Default)]
pub(crate) struct ArweaveTransactionFilter {
    owners: HashSet<Vec<u8>>,
}

impl ArweaveTransactionFilter {
    pub fn matches(&self, owner: &[u8]) -> bool {
        self.owners.contains(owner)
    }

    pub fn from_data_sources<'a>(iter: impl IntoIterator<Item = &'a DataSource>) -> Self {
        let owners: Vec<Vec<u8>> = iter
            .into_iter()
            .filter(|data_source| {
                data_source.source.owner.is_some()
                    && !data_source.mapping.transaction_handlers.is_empty()
            })
            .map(|ds| {
                base64_url::decode(&ds.source.owner.clone().unwrap_or_default()).unwrap_or_default()
            })
            .collect();

        Self {
            owners: HashSet::from_iter(owners),
        }
    }

    pub fn extend(&mut self, other: ArweaveTransactionFilter) {
        self.owners.extend(other.owners);
    }
}

/// ArweaveBlockFilter will match every block regardless of source being set.
/// see docs: https://thegraph.com/docs/en/supported-networks/arweave/
#[derive(Clone, Debug, Default)]
pub(crate) struct ArweaveBlockFilter {
    pub trigger_every_block: bool,
}

impl ArweaveBlockFilter {
    pub fn from_data_sources<'a>(iter: impl IntoIterator<Item = &'a DataSource>) -> Self {
        Self {
            trigger_every_block: iter
                .into_iter()
                .any(|data_source| !data_source.mapping.block_handlers.is_empty()),
        }
    }

    pub fn extend(&mut self, other: ArweaveBlockFilter) {
        self.trigger_every_block = self.trigger_every_block || other.trigger_every_block;
    }
}
