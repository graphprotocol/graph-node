use std::sync::Arc;

use anyhow::Result;
use graph::blockchain::block_stream::{
    BlockStream, BlockStreamBuilder as BlockStreamBuilderTrait, FirehoseCursor,
};
use graph::blockchain::substreams_block_stream::SubstreamsBlockStream;
use graph::components::store::DeploymentLocator;
use graph::data::subgraph::UnifiedMappingApiVersion;
use graph::prelude::{async_trait, BlockNumber, BlockPtr};
use graph::slog::o;

use crate::mapper::Mapper;
use crate::{Chain, TriggerFilter};

pub struct BlockStreamBuilder {}

impl BlockStreamBuilder {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
/// Substreams doesn't actually use Firehose, the configuration for firehose and
/// the grpc substream is very similar, so we can re-use the configuration and
/// the builder for it. This is probably something to improve but for now it
/// works.
impl BlockStreamBuilderTrait<Chain> for BlockStreamBuilder {
    async fn build_firehose(
        &self,
        chain: &Chain,
        deployment: DeploymentLocator,
        block_cursor: FirehoseCursor,
        _start_blocks: Vec<BlockNumber>,
        _subgraph_current_block: Option<BlockPtr>,
        filter: Arc<TriggerFilter>,
        _unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Chain>>> {
        let firehose_endpoint = match chain.endpoints.random() {
            Some(e) => e.clone(),
            None => return Err(anyhow::format_err!("no firehose endpoint available")),
        };

        let mapper = Arc::new(Mapper {});

        let logger = chain
            .logger_factory
            .subgraph_logger(&deployment)
            .new(o!("component" => "SubstreamsBlockStream"));

        Ok(Box::new(SubstreamsBlockStream::new(
            deployment.hash,
            firehose_endpoint,
            None,
            block_cursor.as_ref().clone(),
            mapper,
            filter.modules.clone(),
            filter.module_name.clone(),
            filter.start_block.map(|x| vec![x]).unwrap_or(vec![]),
            vec![],
            logger,
            chain.metrics_registry.clone(),
        )))
    }

    async fn build_polling(
        &self,
        _chain: Arc<Chain>,
        _deployment: DeploymentLocator,
        _start_blocks: Vec<BlockNumber>,
        _subgraph_current_block: Option<BlockPtr>,
        _filter: Arc<TriggerFilter>,
        _unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Chain>>> {
        unimplemented!("polling block stream is not support for substreams")
    }
}
