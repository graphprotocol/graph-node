use graph::{
    blockchain::{EmptyNodeCapabilities, TriggerFilter as TriggerFilterTrait},
    components::store::BlockNumber,
};

use crate::{
    data_source::{DataSource, DataSourceTemplate},
    Chain,
};

#[derive(Default, Clone)]
pub struct TriggerFilter {
    pub(crate) block: StarknetBlockFilter,
}

#[derive(Default, Clone)]
pub struct StarknetBlockFilter {
    pub block_ranges: Vec<BlockRange>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct BlockRange {
    pub start_block: BlockNumber,
    pub end_block: Option<BlockNumber>,
}

impl TriggerFilterTrait<Chain> for TriggerFilter {
    fn extend_with_template(&mut self, _data_source: impl Iterator<Item = DataSourceTemplate>) {}

    fn extend<'a>(&mut self, data_sources: impl Iterator<Item = &'a DataSource> + Clone) {
        self.block
            .extend(StarknetBlockFilter::from_data_sources(data_sources));
    }

    fn node_capabilities(&self) -> EmptyNodeCapabilities<Chain> {
        Default::default()
    }

    fn to_firehose_filter(self) -> Vec<prost_types::Any> {
        todo!()
    }
}

impl StarknetBlockFilter {
    pub fn from_data_sources<'a>(iter: impl IntoIterator<Item = &'a DataSource>) -> Self {
        iter.into_iter()
            .filter(|data_source| data_source.mapping.block_handler.is_some())
            .fold(Self::default(), |mut filter_opt, data_source| {
                filter_opt.extend(Self {
                    block_ranges: vec![BlockRange {
                        start_block: data_source.source.start_block,
                        end_block: data_source.source.end_block,
                    }],
                });
                filter_opt
            })
    }

    pub fn extend(&mut self, other: StarknetBlockFilter) {
        if other.is_empty() {
            return;
        }

        let StarknetBlockFilter { block_ranges } = other;

        // TODO: merge overlapping block ranges
        for new_range in block_ranges.into_iter() {
            if !self.block_ranges.contains(&new_range) {
                self.block_ranges.push(new_range);
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.block_ranges.is_empty()
    }
}
