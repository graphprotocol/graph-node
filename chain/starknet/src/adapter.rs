use std::collections::{hash_map::Entry, HashMap};

use graph::{
    blockchain::{EmptyNodeCapabilities, TriggerFilter as TriggerFilterTrait},
    components::store::BlockNumber,
};

use crate::{
    data_source::{DataSource, DataSourceTemplate},
    felt::Felt,
    Chain,
};

type TopicWithRanges = HashMap<Felt, Vec<BlockRange>>;

#[derive(Default, Clone)]
pub struct TriggerFilter {
    pub(crate) event: StarknetEventFilter,
    pub(crate) block: StarknetBlockFilter,
}

#[derive(Default, Clone)]
pub struct StarknetEventFilter {
    pub contract_addresses: HashMap<Felt, TopicWithRanges>,
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
        self.event
            .extend(StarknetEventFilter::from_data_sources(data_sources.clone()));
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

impl StarknetEventFilter {
    pub fn from_data_sources<'a>(iter: impl IntoIterator<Item = &'a DataSource>) -> Self {
        iter.into_iter()
            // Using `filter_map` instead of `filter` to avoid having to unwrap source address in
            // `fold` below.
            .filter_map(|data_source| {
                if data_source.mapping.event_handlers.is_empty() {
                    None
                } else {
                    data_source
                        .source
                        .address
                        .as_ref()
                        .map(|source_address| (data_source, source_address.to_owned()))
                }
            })
            .fold(
                Self::default(),
                |mut filter_opt, (data_source, source_address)| {
                    filter_opt.extend(Self {
                        contract_addresses: [(
                            source_address,
                            data_source
                                .mapping
                                .event_handlers
                                .iter()
                                .map(|event_handler| {
                                    (
                                        event_handler.event_selector.clone(),
                                        vec![BlockRange {
                                            start_block: data_source.source.start_block,
                                            end_block: data_source.source.end_block,
                                        }],
                                    )
                                })
                                .collect(),
                        )]
                        .into_iter()
                        .collect(),
                    });
                    filter_opt
                },
            )
    }

    pub fn extend(&mut self, other: StarknetEventFilter) {
        if other.is_empty() {
            return;
        }

        let StarknetEventFilter { contract_addresses } = other;

        for (address, topic_with_ranges) in contract_addresses.into_iter() {
            match self.contract_addresses.entry(address) {
                Entry::Occupied(entry) => {
                    let entry = entry.into_mut();
                    for (topic, mut block_ranges) in topic_with_ranges.into_iter() {
                        match entry.entry(topic) {
                            Entry::Occupied(topic_entry) => {
                                // TODO: merge overlapping block ranges
                                topic_entry.into_mut().append(&mut block_ranges);
                            }
                            Entry::Vacant(topic_entry) => {
                                topic_entry.insert(block_ranges);
                            }
                        }
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(topic_with_ranges);
                }
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.contract_addresses.is_empty()
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
