use std::collections::HashSet;

use prost::Message;
use prost_types::Any;

use crate::capabilities::NodeCapabilities;
use crate::{data_source::DataSource, Chain};
use graph::blockchain as bc;
use graph::firehose::EventFilter;
use graph::prelude::*;

const EVENT_FILTER_TYPE_URL: &str = "type.googleapis.com/fig.tendermint.transform.v1.EventFilter";

#[derive(Clone, Debug, Default)]
pub struct TriggerFilter {
    pub(crate) event_filter: TendermintEventFilter,
    pub(crate) block_filter: TendermintBlockFilter,
}

impl bc::TriggerFilter<Chain> for TriggerFilter {
    fn extend<'a>(&mut self, data_sources: impl Iterator<Item = &'a DataSource> + Clone) {
        self.event_filter
            .extend_from_data_sources(data_sources.clone());
        self.block_filter.extend_from_data_sources(data_sources);
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
        if self.block_filter.trigger_every_block {
            return vec![];
        }

        if self.event_filter.event_types.is_empty() {
            return vec![];
        }

        let filter = EventFilter {
            event_types: Vec::from_iter(self.event_filter.event_types),
        };

        vec![Any {
            type_url: EVENT_FILTER_TYPE_URL.to_string(),
            value: filter.encode_to_vec(),
        }]
    }
}

pub type EventType = String;

#[derive(Clone, Debug, Default)]
pub(crate) struct TendermintEventFilter {
    pub event_types: HashSet<EventType>,
}

impl TendermintEventFilter {
    fn extend_from_data_sources<'a>(&mut self, data_sources: impl Iterator<Item = &'a DataSource>) {
        self.event_types.extend(
            data_sources.flat_map(|data_source| data_source.events().map(ToString::to_string)),
        );
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct TendermintBlockFilter {
    pub trigger_every_block: bool,
}

impl TendermintBlockFilter {
    fn extend_from_data_sources<'a>(
        &mut self,
        mut data_sources: impl Iterator<Item = &'a DataSource>,
    ) {
        if !self.trigger_every_block {
            self.trigger_every_block = data_sources.any(DataSource::has_block_handler);
        }
    }
}

#[cfg(test)]
mod test {
    use graph::blockchain::TriggerFilter as _;

    use super::*;

    #[test]
    fn test_trigger_filters() {
        let cases = [
            (TriggerFilter::new_test(false, &[]), vec![]),
            (TriggerFilter::new_test(true, &[]), vec![]),
            (
                TriggerFilter::new_test(true, &["event_1", "event_2"]),
                vec![],
            ),
            (
                TriggerFilter::new_test(false, &["event_1", "event_2", "event_3"]),
                vec!["event_1", "event_2", "event_3"],
            ),
        ];

        for (trigger_filter, expected_filter_events) in cases {
            let firehose_filter = trigger_filter.to_firehose_filter();

            if !expected_filter_events.is_empty() {
                let decoded_event_types = decode_filter(firehose_filter)
                    .expect(&format!(
                        "expected decoded filter with event types {:?}, but found none",
                        expected_filter_events
                    ))
                    .event_types;

                assert_eq!(expected_filter_events.len(), decoded_event_types.len());

                let expected_filter_events = expected_filter_events.iter().map(ToString::to_string);
                for expected_event in expected_filter_events {
                    assert!(
                        decoded_event_types.contains(&expected_event),
                        "expected encoded firehose filter with {:?} to contain {:?}, but it didn't",
                        decoded_event_types,
                        expected_event,
                    );
                }
            }
        }
    }

    impl TriggerFilter {
        fn new_test(trigger_every_block: bool, event_types: &[&str]) -> TriggerFilter {
            TriggerFilter {
                event_filter: TendermintEventFilter {
                    event_types: event_types.iter().map(ToString::to_string).collect(),
                },
                block_filter: TendermintBlockFilter {
                    trigger_every_block,
                },
            }
        }
    }

    fn decode_filter(proto_filters: Vec<Any>) -> Option<EventFilter> {
        assert!(proto_filters.len() <= 1);

        let proto_filter = proto_filters.get(0)?;

        assert_eq!(proto_filter.type_url, EVENT_FILTER_TYPE_URL);

        let firehose_filter = EventFilter::decode(&*proto_filter.value)
            .expect("Could not decode EventFilter from protobuf Any");

        Some(firehose_filter)
    }
}
