use crate::capabilities::NodeCapabilities;
use crate::{data_source::DataSource, Chain};
use graph::blockchain as bc;
use graph::firehose::BasicReceiptFilter;
use graph::prelude::*;
use prost::Message;
use prost_types::Any;

const BASIC_RECEIPT_FILTER_TYPE_URL: &str =
    "type.googleapis.com/sf.arweave.transform.v1.BasicReceiptFilter";

#[derive(Clone, Debug, Default)]
pub struct TriggerFilter {
    pub(crate) block_filter: ArweaveBlockFilter,
}

impl bc::TriggerFilter<Chain> for TriggerFilter {
    fn extend<'a>(&mut self, data_sources: impl Iterator<Item = &'a DataSource> + Clone) {
        let TriggerFilter { block_filter } = self;

        block_filter.extend(ArweaveBlockFilter::from_data_sources(data_sources));
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
        } = self;

        if block.trigger_every_block {
            return vec![];
        }

        // # NOTE
        //
        // Arweave don't have receipts
        let filter = BasicReceiptFilter { accounts: vec![] };

        vec![Any {
            type_url: BASIC_RECEIPT_FILTER_TYPE_URL.into(),
            value: filter.encode_to_vec(),
        }]
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
