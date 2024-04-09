use std::collections::{hash_map::Entry, HashMap};

use graph::{
    anyhow::Error,
    blockchain::{EmptyNodeCapabilities, TriggerFilter as TriggerFilterTrait},
    components::store::BlockNumber,
    firehose::{
        BlockHeaderOnly as FirehoseFilterBlockHeaderOnly, BlockRange as FirehoseFilterBlockRange,
        ContractEventFilter as FirehoseFilterContractEventFilter,
        TopicWithRanges as FirehoseFilterTopicWithRanges,
        TransactionEventFilter as FirehoseFilterTransactionEventFilter,
    },
};
use prost::Message;
use prost_types::Any;

const BLOCK_HEADER_ONLY_TYPE_URL: &str =
    "type.googleapis.com/zklend.starknet.transform.v1.BlockHeaderOnly";
const TRANSACTION_EVENT_FILTER_TYPE_URL: &str =
    "type.googleapis.com/zklend.starknet.transform.v1.TransactionEventFilter";

use crate::{
    codec::Event,
    data_source::{DataSource, DataSourceTemplate},
    felt::Felt,
    Chain,
};

/// Starknet contract address, represented by the primitive [Felt] type.
type Address = Felt;

/// Starknet event signature, encoded as the first element of any event's `keys` array.
type EventSignature = Felt;

/// A hashmap for quick lookup on whether an event matches with the filter. If the event's signature
/// exists, further comparison needs to be made against the block ranges.
type EventSignatureWithRanges = HashMap<EventSignature, Vec<BlockRange>>;

/// Contains event and block filters. The two types of filters function independently: event
/// filters are only applied to event triggers; and block filters are only applied to block
/// triggers.
#[derive(Default, Clone)]
pub struct TriggerFilter {
    event: EventFilter,
    block: BlockFilter,
}

/// An event trigger is matched if and only if *ALL* conditions are met for the event:
///
/// - it's emitted from one of the `contract_addresses` keys
/// - its event signature matches with one of the keys in [EventSignatureWithRanges]
/// - it's emitted in a block within one of the block ranges for that matched signature
#[derive(Default, Clone)]
struct EventFilter {
    contract_addresses: HashMap<Address, EventSignatureWithRanges>,
}

/// A block trigger is matched if and only if its height falls into any one of the defined
/// `block_ranges`.
///
/// Note that this filter is only used to match block triggers interally inside `graph-node`, and
/// is never sent to upstream Firehose providers, as we always need the block headers for marking
/// chain head.
#[derive(Default, Clone)]
struct BlockFilter {
    block_ranges: Vec<BlockRange>,
}

/// A range of blocks defined by starting and (optional) ending height.
///
/// `start_block` is inclusive. `end_block` (if defined) is exclusive.
#[derive(Clone, PartialEq, Eq)]
struct BlockRange {
    start_block: BlockNumber,
    end_block: Option<BlockNumber>,
}

impl TriggerFilter {
    pub fn is_block_matched(&self, block_height: BlockNumber) -> bool {
        self.block.is_matched(block_height)
    }

    pub fn is_event_matched(
        &self,
        event: &Event,
        block_height: BlockNumber,
    ) -> Result<bool, Error> {
        self.event.is_matched(event, block_height)
    }
}

impl TriggerFilterTrait<Chain> for TriggerFilter {
    fn extend_with_template(&mut self, _data_source: impl Iterator<Item = DataSourceTemplate>) {}

    fn extend<'a>(&mut self, data_sources: impl Iterator<Item = &'a DataSource> + Clone) {
        self.event
            .extend(EventFilter::from_data_sources(data_sources.clone()));
        self.block
            .extend(BlockFilter::from_data_sources(data_sources));
    }

    fn node_capabilities(&self) -> EmptyNodeCapabilities<Chain> {
        Default::default()
    }

    fn to_firehose_filter(self) -> Vec<prost_types::Any> {
        // An empty event filter list means that the subgraph is not interested in events at all.
        // So we can stream just header-only blocks.
        if self.event.is_empty() {
            return vec![Any {
                type_url: BLOCK_HEADER_ONLY_TYPE_URL.into(),
                value: FirehoseFilterBlockHeaderOnly {}.encode_to_vec(),
            }];
        }

        let event_filters = self
            .event
            .contract_addresses
            .iter()
            .map(
                |(contract_address, sig_with_ranges)| FirehoseFilterContractEventFilter {
                    contract_address: contract_address.into(),
                    topics: sig_with_ranges
                        .iter()
                        .map(|(sig, ranges)| FirehoseFilterTopicWithRanges {
                            topic: sig.into(),
                            block_ranges: ranges
                                .iter()
                                .map(|range| FirehoseFilterBlockRange {
                                    start_block: range.start_block as u64,
                                    end_block: range.end_block.unwrap_or_default() as u64,
                                })
                                .collect(),
                        })
                        .collect(),
                },
            )
            .collect();

        vec![Any {
            type_url: TRANSACTION_EVENT_FILTER_TYPE_URL.into(),
            value: FirehoseFilterTransactionEventFilter { event_filters }.encode_to_vec(),
        }]
    }
}

impl EventFilter {
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

    pub fn extend(&mut self, other: EventFilter) {
        if other.is_empty() {
            return;
        }

        let EventFilter { contract_addresses } = other;

        for (address, sig_with_ranges) in contract_addresses.into_iter() {
            match self.contract_addresses.entry(address) {
                Entry::Occupied(entry) => {
                    let entry = entry.into_mut();
                    for (sig, mut block_ranges) in sig_with_ranges.into_iter() {
                        match entry.entry(sig) {
                            Entry::Occupied(sig_entry) => {
                                // TODO: merge overlapping block ranges
                                sig_entry.into_mut().append(&mut block_ranges);
                            }
                            Entry::Vacant(sig_entry) => {
                                sig_entry.insert(block_ranges);
                            }
                        }
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(sig_with_ranges);
                }
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.contract_addresses.is_empty()
    }

    pub fn is_matched(&self, event: &Event, block_height: BlockNumber) -> Result<bool, Error> {
        let from_addr: Felt = event.from_addr.as_slice().try_into()?;

        Ok(match self.contract_addresses.get(&from_addr) {
            Some(entry) => {
                let event_sig = match event.keys.first() {
                    Some(sig) => sig,
                    // Non-standard events with an empty `keys` array never match.
                    None => return Ok(false),
                };
                let event_sig: Felt = event_sig.as_slice().try_into()?;

                match entry.get(&event_sig) {
                    Some(block_ranges) => block_ranges.iter().any(|range| {
                        if block_height >= range.start_block {
                            match range.end_block {
                                // `end_block` is exclusive
                                Some(end_block) => block_height < end_block,
                                None => true,
                            }
                        } else {
                            false
                        }
                    }),
                    None => false,
                }
            }
            None => false,
        })
    }
}

impl BlockFilter {
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

    pub fn extend(&mut self, other: BlockFilter) {
        if other.is_empty() {
            return;
        }

        let BlockFilter { block_ranges } = other;

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

    pub fn is_matched(&self, block_height: BlockNumber) -> bool {
        self.block_ranges.iter().any(|range| {
            if block_height >= range.start_block {
                match range.end_block {
                    Some(end_block) => block_height < end_block,
                    None => true,
                }
            } else {
                false
            }
        })
    }
}
