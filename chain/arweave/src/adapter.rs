use std::collections::HashSet;

use crate::capabilities::NodeCapabilities;
use crate::{data_source::DataSource, Chain};
use graph::blockchain as bc;
use graph::firehose::BasicReceiptFilter;
use graph::prelude::*;
use prost::Message;
use prost_types::Any;

const BASIC_RECEIPT_FILTER_TYPE_URL: &str =
    "type.googleapis.com/sf.near.transform.v1.BasicReceiptFilter";

#[derive(Clone, Debug, Default)]
pub struct TriggerFilter {
    pub(crate) block_filter: NearBlockFilter,
    pub(crate) receipt_filter: NearReceiptFilter,
}

impl bc::TriggerFilter<Chain> for TriggerFilter {
    fn extend<'a>(&mut self, data_sources: impl Iterator<Item = &'a DataSource> + Clone) {
        let TriggerFilter {
            block_filter,
            receipt_filter,
        } = self;

        block_filter.extend(NearBlockFilter::from_data_sources(data_sources.clone()));
        receipt_filter.extend(NearReceiptFilter::from_data_sources(data_sources));
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
        let TriggerFilter {
            block_filter: block,
            receipt_filter: receipt,
        } = self;

        if block.trigger_every_block {
            return vec![];
        }

        if receipt.is_empty() {
            return vec![];
        }

        let filter = BasicReceiptFilter {
            accounts: receipt.accounts.into_iter().collect(),
        };

        vec![Any {
            type_url: BASIC_RECEIPT_FILTER_TYPE_URL.into(),
            value: filter.encode_to_vec(),
        }]
    }
}

pub(crate) type Account = String;

/// NearReceiptFilter requires the account to be set, it will match every receipt where `source.account` is the recipient.
/// see docs: https://thegraph.com/docs/en/supported-networks/near/
#[derive(Clone, Debug, Default)]
pub(crate) struct NearReceiptFilter {
    pub accounts: HashSet<Account>,
}

impl NearReceiptFilter {
    pub fn matches(&self, account: &String) -> bool {
        self.accounts.contains(account)
    }

    pub fn is_empty(&self) -> bool {
        let NearReceiptFilter { accounts } = self;

        accounts.is_empty()
    }

    pub fn from_data_sources<'a>(iter: impl IntoIterator<Item = &'a DataSource>) -> Self {
        let accounts: Vec<String> = iter
            .into_iter()
            .filter(|data_source| {
                data_source.source.account.is_some()
                    && !data_source.mapping.receipt_handlers.is_empty()
            })
            .map(|ds| ds.source.account.as_ref().unwrap().clone())
            .collect();

        Self {
            accounts: HashSet::from_iter(accounts),
        }
    }

    pub fn extend(&mut self, other: NearReceiptFilter) {
        self.accounts.extend(other.accounts);
    }
}

/// NearBlockFilter will match every block regardless of source being set.
/// see docs: https://thegraph.com/docs/en/supported-networks/near/
#[derive(Clone, Debug, Default)]
pub(crate) struct NearBlockFilter {
    pub trigger_every_block: bool,
}

impl NearBlockFilter {
    pub fn from_data_sources<'a>(iter: impl IntoIterator<Item = &'a DataSource>) -> Self {
        Self {
            trigger_every_block: iter
                .into_iter()
                .any(|data_source| !data_source.mapping.block_handlers.is_empty()),
        }
    }

    pub fn extend(&mut self, other: NearBlockFilter) {
        self.trigger_every_block = self.trigger_every_block || other.trigger_every_block;
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use super::NearBlockFilter;
    use crate::adapter::{TriggerFilter, BASIC_RECEIPT_FILTER_TYPE_URL};
    use graph::{blockchain::TriggerFilter as _, firehose::BasicReceiptFilter};
    use prost::Message;
    use prost_types::Any;

    #[test]
    fn near_trigger_empty_filter() {
        let filter = TriggerFilter {
            block_filter: NearBlockFilter {
                trigger_every_block: false,
            },
            receipt_filter: super::NearReceiptFilter {
                accounts: HashSet::new(),
            },
        };
        assert_eq!(filter.to_firehose_filter(), vec![]);
    }

    #[test]
    fn near_trigger_filter_match_all_block() {
        let filter = TriggerFilter {
            block_filter: NearBlockFilter {
                trigger_every_block: true,
            },
            receipt_filter: super::NearReceiptFilter {
                accounts: HashSet::from_iter(vec!["acc1".into(), "acc2".into(), "acc3".into()]),
            },
        };

        let filter = filter.to_firehose_filter();
        assert_eq!(filter.len(), 0);
    }

    #[test]
    fn near_trigger_filter() {
        let filter = TriggerFilter {
            block_filter: NearBlockFilter {
                trigger_every_block: false,
            },
            receipt_filter: super::NearReceiptFilter {
                accounts: HashSet::from_iter(vec!["acc1".into(), "acc2".into(), "acc3".into()]),
            },
        };

        let filter = filter.to_firehose_filter();
        assert_eq!(filter.len(), 1);

        let firehose_filter = decode_filter(filter);

        assert_eq!(
            firehose_filter.accounts,
            vec![
                String::from("acc1"),
                String::from("acc2"),
                String::from("acc3")
            ],
        );
    }

    fn decode_filter(firehose_filter: Vec<Any>) -> BasicReceiptFilter {
        let firehose_filter = firehose_filter[0].clone();
        assert_eq!(
            firehose_filter.type_url,
            String::from(BASIC_RECEIPT_FILTER_TYPE_URL),
        );
        let mut bytes = &firehose_filter.value[..];
        let mut firehose_filter =
            BasicReceiptFilter::decode(&mut bytes).expect("unable to parse basic receipt filter");
        firehose_filter.accounts.sort();

        firehose_filter
    }
}
