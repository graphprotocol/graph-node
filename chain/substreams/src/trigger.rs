use anyhow::Error;
use graph::{
    blockchain::{self, block_stream::BlockWithTriggers, BlockPtr},
    prelude::{async_trait, BlockNumber},
    slog::Logger,
};

use crate::{Block, Chain, DataSource, NodeCapabilities, NoopDataSourceTemplate};

#[derive(Eq, PartialEq, PartialOrd, Ord, Debug)]
pub struct TriggerData {}

impl blockchain::TriggerData for TriggerData {
    fn error_context(&self) -> String {
        unimplemented!()
    }
}

impl blockchain::MappingTrigger for TriggerData {
    // substreams doesn't rely on wasm on the graph-node so this is not needed.
    fn to_asc_ptr<H: graph::runtime::AscHeap>(
        self,
        _heap: &mut H,
        _gas: &graph::runtime::gas::GasCounter,
    ) -> Result<graph::runtime::AscPtr<()>, graph::runtime::DeterministicHostError> {
        unimplemented!()
    }
}

#[derive(Debug, Clone, Default)]
pub struct TriggerFilter {}

// TriggerFilter should bypass all triggers and just rely on block since all the data received
// should already have been processed.
impl blockchain::TriggerFilter<Chain> for TriggerFilter {
    fn extend_with_template(&mut self, _data_source: impl Iterator<Item = NoopDataSourceTemplate>) {
    }

    fn extend<'a>(&mut self, _data_sources: impl Iterator<Item = &'a DataSource> + Clone) {}

    fn node_capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {}
    }

    fn to_firehose_filter(self) -> Vec<prost_types::Any> {
        unimplemented!("this should never be called for this type")
    }
}

pub struct TriggersAdapter {}

#[async_trait]
impl blockchain::TriggersAdapter<Chain> for TriggersAdapter {
    async fn ancestor_block(
        &self,
        _ptr: BlockPtr,
        _offset: BlockNumber,
    ) -> Result<Option<Block>, Error> {
        unimplemented!()
    }

    async fn scan_triggers(
        &self,
        _from: BlockNumber,
        _to: BlockNumber,
        _filter: &TriggerFilter,
    ) -> Result<Vec<BlockWithTriggers<Chain>>, Error> {
        unimplemented!()
    }

    async fn triggers_in_block(
        &self,
        _logger: &Logger,
        _block: Block,
        _filter: &TriggerFilter,
    ) -> Result<BlockWithTriggers<Chain>, Error> {
        unimplemented!()
    }

    async fn is_on_main_chain(&self, _ptr: BlockPtr) -> Result<bool, Error> {
        unimplemented!()
    }

    async fn parent_ptr(&self, _block: &BlockPtr) -> Result<Option<BlockPtr>, Error> {
        unimplemented!()
    }
}
