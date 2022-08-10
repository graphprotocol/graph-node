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
    prelude::{async_trait, serde_yaml, BlockNumber, BlockPtr, SubgraphManifest, ENV_VARS},
    slog::{o, Logger},
};

use crate::{mapper::Mapper, Chain, TriggerFilter};

pub struct BlockStreamBuilder {
    resolver: Arc<dyn graph::prelude::LinkResolver>,
}

impl BlockStreamBuilder {
    pub fn new(resolver: Arc<dyn graph::prelude::LinkResolver>) -> Self {
        Self { resolver }
    }
}

#[async_trait]
/// Substreams doesn't actually use Firehose, the configuration for firehose and the grpc substream
/// is very similar, so we can re-use the configuration and the builder for it.
/// This is probably something to improve but for now it works.
impl BlockStreamBuilderTrait<Chain> for BlockStreamBuilder {
    async fn build_firehose(
        &self,
        chain: &Chain,
        deployment: DeploymentLocator,
        block_cursor: FirehoseCursor,
        _start_blocks: Vec<BlockNumber>,
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
            .new(o!("component" => "SubstreamsBlockStream"));

        // TODO: couldn't find a good way to pass the manifest because the code that calls this in the runner doesn't have the manifest
        // either, as far as I can tell so this is the easy way but perhaps not ideal.
        let manifest: SubgraphManifest<Chain> =
            resolve_manifest(&logger, &deployment, &self.resolver).await?;

        let ds = manifest.data_sources.first().unwrap();

        let mut start_blocks: Vec<i32> = vec![];
        if let Some(ref modules) = ds.source.package.modules {
            start_blocks = modules
                .modules
                .iter()
                .map(|m| m.initial_block.try_into().map_err(anyhow::Error::from))
                .collect::<Result<Vec<i32>>>()?;
        }

        Ok(Box::new(SubstreamsBlockStream::new(
            deployment.hash,
            firehose_endpoint,
            None,
            block_cursor.as_ref().clone(),
            mapper,
            ds.source.package.modules.clone(),
            ds.source.module_name.clone(),
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

async fn resolve_manifest<C: Blockchain>(
    logger: &Logger,
    deployment: &DeploymentLocator,
    resolver: &Arc<dyn graph::prelude::LinkResolver>,
) -> Result<SubgraphManifest<C>, anyhow::Error> {
    let raw: serde_yaml::Mapping = {
        let file_bytes = resolver
            .cat(&logger, &deployment.hash.to_ipfs_link())
            .await?;

        serde_yaml::from_slice(&file_bytes)?
    };

    let manifest = SubgraphManifest::resolve_from_raw(
        deployment.hash.clone(),
        raw,
        &resolver,
        logger,
        ENV_VARS.max_spec_version.clone(),
    );

    manifest.await.map_err(anyhow::Error::from)
}
