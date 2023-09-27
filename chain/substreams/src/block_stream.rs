use anyhow::Result;
use std::sync::Arc;

use graph::{
    blockchain::{
        block_stream::{
            BlockStream, BlockStreamBuilder as BlockStreamBuilderTrait, FirehoseCursor,
        },
        substreams_block_stream::SubstreamsBlockStream,
        Blockchain,
    },
    components::store::DeploymentLocator,
    data::subgraph::UnifiedMappingApiVersion,
    prelude::{async_trait, BlockNumber, BlockPtr},
    schema::InputSchema,
    slog::o,
};

use crate::{mapper::Mapper, Chain, TriggerFilter};

pub struct BlockStreamBuilder {}

impl BlockStreamBuilder {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
/// Substreams doesn't actually use Firehose, the configuration for firehose and the grpc substream
/// is very similar, so we can re-use the configuration and the builder for it.
/// This is probably something to improve but for now it works.
impl BlockStreamBuilderTrait<Chain> for BlockStreamBuilder {
    async fn build_substreams(
        &self,
        chain: &Chain,
        schema: InputSchema,
        deployment: DeploymentLocator,
        block_cursor: FirehoseCursor,
        subgraph_current_block: Option<BlockPtr>,
        filter: Arc<<Chain as Blockchain>::TriggerFilter>,
    ) -> Result<Box<dyn BlockStream<Chain>>> {
        let mapper = Arc::new(Mapper {
            schema: Some(schema),
            skip_empty_blocks: true,
        });

        let logger = chain
            .logger_factory
            .subgraph_logger(&deployment)
            .new(o!("component" => "SubstreamsBlockStream"));

        Ok(Box::new(SubstreamsBlockStream::new(
            deployment.hash,
            chain.chain_client(),
            subgraph_current_block,
            block_cursor.as_ref().clone(),
            mapper,
            filter.modules.clone(),
            filter.module_name.clone(),
            filter.start_block.map(|x| vec![x]).unwrap_or_default(),
            vec![],
            logger,
            chain.metrics_registry.clone(),
        )))
    }

    async fn build_firehose(
        &self,
        _chain: &Chain,
        _deployment: DeploymentLocator,
        _block_cursor: FirehoseCursor,
        _start_blocks: Vec<BlockNumber>,
        _subgraph_current_block: Option<BlockPtr>,
        _filter: Arc<TriggerFilter>,
        _unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Chain>>> {
        unimplemented!()
    }

    async fn build_polling(
        &self,
        _chain: &Chain,
        _deployment: DeploymentLocator,
        _start_blocks: Vec<BlockNumber>,
        _subgraph_current_block: Option<BlockPtr>,
        _filter: Arc<TriggerFilter>,
        _unified_api_version: UnifiedMappingApiVersion,
    ) -> Result<Box<dyn BlockStream<Chain>>> {
        unimplemented!("polling block stream is not support for substreams")
    }
}
