use std::sync::Arc;

use anyhow::Error;
use graph::{
    blockchain::{
        self, block_stream::BlockWithTriggers, BlockPtr, EmptyNodeCapabilities, MappingTriggerTrait,
    },
    components::{
        store::SubgraphFork,
        subgraph::{MappingError, SharedProofOfIndexing, SubgraphType},
        trigger_processor::HostedTrigger,
    },
    prelude::{anyhow, async_trait, BlockHash, BlockNumber, BlockState, RuntimeHostBuilder},
    slog::Logger,
};
use graph_runtime_wasm::module::ToAscPtr;

use crate::{Block, Chain, NoopDataSourceTemplate};

#[derive(Eq, PartialEq, PartialOrd, Ord, Debug)]
pub struct TriggerData {}

impl MappingTriggerTrait for TriggerData {
    fn error_context(&self) -> String {
        "Failed to process substreams block".to_string()
    }
}

impl blockchain::TriggerData for TriggerData {
    // TODO(filipe): Can this be improved with some data from the block?
    fn error_context(&self) -> String {
        "Failed to process substreams block".to_string()
    }

    fn address_match(&self) -> Option<&[u8]> {
        None
    }
}

impl ToAscPtr for TriggerData {
    // substreams doesn't rely on wasm on the graph-node so this is not needed.
    fn to_asc_ptr<H: graph::runtime::AscHeap>(
        self,
        _heap: &mut H,
        _gas: &graph::runtime::gas::GasCounter,
    ) -> Result<graph::runtime::AscPtr<()>, graph::runtime::HostExportError> {
        unimplemented!()
    }
}

#[derive(Debug, Clone, Default)]
pub struct TriggerFilter {
    pub handler: Option<String>,
}

#[cfg(debug_assertions)]
impl TriggerFilter {}

// TriggerFilter should bypass all triggers and just rely on block since all the data received
// should already have been processed.
impl blockchain::TriggerFilter<Chain> for TriggerFilter {
    fn extend_with_template(&mut self, _data_source: impl Iterator<Item = NoopDataSourceTemplate>) {
    }

    /// this function is not safe to call multiple times, only one DataSource is supported for
    ///
    fn extend<'a>(
        &mut self,
        mut data_sources: impl Iterator<Item = &'a crate::DataSource> + Clone,
    ) {
        let Self { handler } = self;

        // Only one handler support, it has already been added.
        if handler.is_some() {
            return;
        }

        if let Some(ds) = data_sources.next() {
            *handler = Some(ds.mapping.handler.handler.clone());
        }
    }

    fn node_capabilities(&self) -> EmptyNodeCapabilities<Chain> {
        EmptyNodeCapabilities::default()
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
        _root: Option<BlockHash>,
    ) -> Result<Option<Block>, Error> {
        unimplemented!()
    }

    async fn scan_triggers(
        &self,
        _from: BlockNumber,
        _to: BlockNumber,
        _filter: &TriggerFilter,
    ) -> Result<(Vec<BlockWithTriggers<Chain>>, BlockNumber), Error> {
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

    async fn parent_ptr(&self, block: &BlockPtr) -> Result<Option<BlockPtr>, Error> {
        // This seems to work for a lot of the firehose chains.
        Ok(Some(BlockPtr {
            hash: BlockHash::from(vec![0xff; 32]),
            number: block.number.saturating_sub(1),
        }))
    }
}

pub struct TriggerProcessor {}

impl TriggerProcessor {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl<T> graph::prelude::TriggerProcessor<Chain, T> for TriggerProcessor
where
    T: RuntimeHostBuilder<Chain>,
{
    async fn process_trigger<'a>(
        &'a self,
        _logger: &Logger,
        _triggers: Vec<HostedTrigger<'a, Chain>>,
        _block: &Arc<Block>,
        mut _state: BlockState,
        _proof_of_indexing: &SharedProofOfIndexing,
        _causality_region: &str,
        _debug_fork: &Option<Arc<dyn SubgraphFork>>,
        _subgraph_type: &SubgraphType,
        _subgraph_metrics: &Arc<graph::prelude::SubgraphInstanceMetrics>,
        _instrument: bool,
    ) -> Result<BlockState, MappingError> {
        unreachable!("datasets dont do trigger processing")
    }
}
