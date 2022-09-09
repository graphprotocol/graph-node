use anyhow::Result;
use std::sync::Arc;

use graph::{
    blockchain::{
        block_stream::{
            BlockStream, BlockStreamBuilder as BlockStreamBuilderTrait, FirehoseCursor,
        },
        substreams_block_stream::SubstreamsBlockStream,
    },
    components::store::DeploymentLocator,
    data::subgraph::UnifiedMappingApiVersion,
    prelude::{async_trait, BlockNumber, BlockPtr},
    slog::o,
};

use crate::{mapper::Mapper, Chain, TriggerFilter};

pub(crate) struct BlockStreamBuilder {}

#[async_trait]
impl BlockStreamBuilderTrait<Chain> for BlockStreamBuilder {
    fn build_firehose(
        &self,
        chain: &Chain,
        deployment: DeploymentLocator,
        block_cursor: FirehoseCursor,
        start_blocks: Vec<BlockNumber>,
        _subgraph_current_block: Option<BlockPtr>,
        _filter: Arc<TriggerFilter>,
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
            .new(o!("component" => "FirehoseBlockStream"));

        Ok(Box::new(SubstreamsBlockStream::new(
            deployment.hash,
            firehose_endpoint,
            None,
            block_cursor.as_ref().clone(),
            mapper,
            None,
            "graph_out".into(),
            start_blocks,
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
