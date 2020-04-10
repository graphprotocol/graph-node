use std::collections::HashMap;
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::Instant;

use futures03::future::{abortable, AbortHandle};
use futures03::StreamExt;
use lazy_static::lazy_static;
use uuid::Uuid;

use graph::components::ethereum::triggers_in_block;
use graph::components::store::ModificationsAndCache;
use graph::data::subgraph::schema::{
    DynamicEthereumContractDataSourceEntity, SubgraphDeploymentEntity,
};
use graph::prelude::{
    debug, error, format_err, futures03, info, o, BlockState, ChainStore, CheapClone, DataSource,
    DataSourceTemplate, DataSourceTemplateInfo, Entity, EntityCache, EntityKey, Error,
    EthereumAdapter, EthereumBlockFilter, EthereumBlockPointer, EthereumBlockWithTriggers,
    EthereumCallCache, EthereumCallFilter, EthereumLogFilter, EthereumTrigger, Future01CompatExt,
    Histogram, HistogramVec, HostMetrics, LightEthereumBlock, LogCode, Logger, LoggerFactory,
    MetricsRegistry, StopwatchMetrics, Store, Stream01CompatExt, SubgraphDeploymentId,
    SubgraphDeploymentStore, SubgraphEthRpcMetrics, SubgraphManifest, TryFutureExt,
};
use graph::util::lfu_cache::LfuCache;
use graph_runtime_wasm::{RuntimeHost, RuntimeHostBuilder};

use super::instance::SubgraphInstance;
use crate::{BlockStreamBuilder, BlockStreamEvent, BlockStreamMetrics};

lazy_static! {
    /// Size limit of the entity LFU cache, in bytes.
    // Multiplied by 1000 because the env var is in KB.
    pub static ref ENTITY_CACHE_SIZE: u64 = 1000
        * std::env::var("GRAPH_ENTITY_CACHE_SIZE")
            .unwrap_or("10000".into())
            .parse::<u64>()
            .expect("invalid GRAPH_ENTITY_CACHE_SIZE");
}

struct IndexingInputs<S, MR> {
    deployment_id: SubgraphDeploymentId,
    start_blocks: Vec<u64>,
    store: Arc<S>,
    ethereum_adapter: Arc<dyn EthereumAdapter>,
    stream_builder: BlockStreamBuilder<S, S, MR>,
    templates_use_calls: bool,
    top_level_templates: Arc<Vec<DataSourceTemplate>>,
}

struct IndexingState<S> {
    logger: Logger,
    instance: SubgraphInstance<S>,
    log_filter: EthereumLogFilter,
    call_filter: EthereumCallFilter,
    block_filter: EthereumBlockFilter,
    restarts: u64,
    entity_lfu_cache: LfuCache<EntityKey, Option<Entity>>,
}

struct IndexingContext<MR, S> {
    /// Read only inputs that are needed while indexing a subgraph.
    pub inputs: IndexingInputs<S, MR>,

    /// Mutable state that may be modified while indexing a subgraph.
    pub state: IndexingState<S>,

    /// Sensors to measure the execution of the subgraph instance
    pub subgraph_metrics: Arc<SubgraphInstanceMetrics>,

    /// Sensors to measure the execution of the subgraph's runtime hosts
    pub host_metrics: Arc<HostMetrics>,

    /// Sensors to measure the execution of eth rpc calls
    pub ethrpc_metrics: Arc<SubgraphEthRpcMetrics>,

    pub block_stream_metrics: Arc<BlockStreamMetrics>,
}

pub struct SubgraphIndexer<MR, S> {
    logger: Logger,
    logger_factory: LoggerFactory,
    store: Arc<S>,
    ethereum_adapter: Arc<dyn EthereumAdapter>,
    runtime_host_builder: RuntimeHostBuilder<S>,
    block_stream_builder: BlockStreamBuilder<S, S, MR>,
    metrics_registry: Arc<MR>,
}

enum TriggerType {
    Event,
    Call,
    Block,
}

impl TriggerType {
    fn label_value(&self) -> &str {
        match self {
            TriggerType::Event => "event",
            TriggerType::Call => "call",
            TriggerType::Block => "block",
        }
    }
}

struct SubgraphInstanceMetrics {
    pub block_trigger_count: Box<Histogram>,
    pub block_processing_duration: Box<Histogram>,
    pub block_ops_transaction_duration: Box<Histogram>,

    trigger_processing_duration: Box<HistogramVec>,
}

impl SubgraphInstanceMetrics {
    pub fn new(registry: Arc<impl MetricsRegistry>, subgraph_hash: String) -> Self {
        let block_trigger_count = registry
            .new_histogram(
                format!("subgraph_block_trigger_count_{}", subgraph_hash),
                String::from(
                    "Measures the number of triggers in each block for a subgraph deployment",
                ),
                HashMap::new(),
                vec![1.0, 5.0, 10.0, 20.0, 50.0],
            )
            .expect("failed to create `subgraph_block_trigger_count` histogram");
        let trigger_processing_duration = registry
            .new_histogram_vec(
                format!("subgraph_trigger_processing_duration_{}", subgraph_hash),
                String::from("Measures duration of trigger processing for a subgraph deployment"),
                HashMap::new(),
                vec![String::from("trigger_type")],
                vec![0.01, 0.05, 0.1, 0.5, 1.5, 5.0, 10.0, 30.0, 120.0],
            )
            .expect("failed to create `subgraph_trigger_processing_duration` histogram");
        let block_processing_duration = registry
            .new_histogram(
                format!("subgraph_block_processing_duration_{}", subgraph_hash),
                String::from("Measures duration of block processing for a subgraph deployment"),
                HashMap::new(),
                vec![0.05, 0.2, 0.7, 1.5, 4.0, 10.0, 60.0, 120.0, 240.0],
            )
            .expect("failed to create `subgraph_block_processing_duration` histogram");
        let block_ops_transaction_duration = registry
            .new_histogram(
                format!("subgraph_transact_block_operations_duration_{}", subgraph_hash),
                String::from("Measures duration of commiting all the entity operations in a block and updating the subgraph pointer"),
                HashMap::new(),
                vec![0.01, 0.05, 0.1, 0.3, 0.7, 2.0],
            )
            .expect("failed to create `subgraph_transact_block_operations_duration_{}");

        Self {
            block_trigger_count,
            block_processing_duration,
            trigger_processing_duration,
            block_ops_transaction_duration,
        }
    }

    pub fn observe_trigger_processing_duration(&self, duration: f64, trigger: TriggerType) {
        self.trigger_processing_duration
            .with_label_values(vec![trigger.label_value()].as_slice())
            .observe(duration);
    }

    pub fn unregister<M: MetricsRegistry>(&self, registry: Arc<M>) {
        registry.unregister(self.block_processing_duration.clone());
        registry.unregister(self.block_trigger_count.clone());
        registry.unregister(self.trigger_processing_duration.clone());
        registry.unregister(self.block_ops_transaction_duration.clone());
    }
}

impl<MR, S> SubgraphIndexer<MR, S>
where
    MR: MetricsRegistry,
    S: Store + ChainStore + SubgraphDeploymentStore + EthereumCallCache,
{
    /// Creates a new runtime manager.
    pub fn new(
        logger_factory: &LoggerFactory,
        store: Arc<S>,
        ethereum_adapter: Arc<dyn EthereumAdapter>,
        runtime_host_builder: RuntimeHostBuilder<S>,
        block_stream_builder: BlockStreamBuilder<S, S, MR>,
        metrics_registry: Arc<MR>,
    ) -> Self {
        let logger = logger_factory.component_logger("SubgraphIndexer", None);
        let logger_factory = logger_factory.with_parent(logger.clone());

        SubgraphIndexer {
            logger,
            logger_factory,
            store,
            ethereum_adapter,
            runtime_host_builder,
            block_stream_builder,
            metrics_registry,
        }
    }

    pub async fn start(&self, manifest: SubgraphManifest) -> Result<AbortHandle, Error> {
        let logger = self.logger_factory.subgraph_logger(&manifest.id);

        info!(
            logger,
            "Start subgraph";
            "data_sources" => manifest.data_sources.len()
        );

        async move {
            // Clear the 'failed' state of the subgraph. We were told explicitly to
            // start, which implies we assume the subgraph has not failed (yet) If
            // we can't even clear the 'failed' flag, don't try to start the
            // subgraph.
            self.store.start_subgraph_deployment(
                &manifest.id,
                SubgraphDeploymentEntity::update_failed_operations(&manifest.id, false),
            )?;

            // Collect all data source templates from the manifest
            let mut templates: Vec<DataSourceTemplate> = vec![];
            for data_source in manifest.data_sources.iter() {
                for template in data_source.templates.iter() {
                    templates.push(template.clone());
                }
            }

            // Clone the deployment ID for later
            let deployment_id = manifest.id.clone();

            // Obtain trigger filters from the manifest
            let log_filter = EthereumLogFilter::from_data_sources(&manifest.data_sources);
            let call_filter = EthereumCallFilter::from_data_sources(&manifest.data_sources);
            let block_filter = EthereumBlockFilter::from_data_sources(&manifest.data_sources);
            let start_blocks = manifest.start_blocks();

            // Identify whether there are templates with call handlers or
            // block handlers with call filters; in this case, we need to
            // include calls in all blocks so we cen reprocess the block
            // when new dynamic data sources are being created
            let templates_use_calls = templates.iter().any(|template| {
                template.has_call_handler() || template.has_block_handler_with_call_filter()
            });

            let top_level_templates = Arc::new(manifest.templates.clone());

            // Create a subgraph instance from the manifest; this moves
            // ownership of the manifest and host builder into the new instance
            let stopwatch_metrics = StopwatchMetrics::new(
                self.logger.clone(),
                deployment_id.clone(),
                self.metrics_registry.clone(),
            );
            let subgraph_metrics = Arc::new(SubgraphInstanceMetrics::new(
                self.metrics_registry.clone(),
                deployment_id.clone().to_string(),
            ));

            let subgraph_metrics_unregister = subgraph_metrics.clone();
            let host_metrics = Arc::new(HostMetrics::new(
                self.metrics_registry.clone(),
                deployment_id.clone().to_string(),
                stopwatch_metrics.clone(),
            ));
            let ethrpc_metrics = Arc::new(SubgraphEthRpcMetrics::new(
                self.metrics_registry.clone(),
                deployment_id.to_string(),
            ));
            let block_stream_metrics = Arc::new(BlockStreamMetrics::new(
                self.metrics_registry.clone(),
                ethrpc_metrics.clone(),
                deployment_id.clone(),
                stopwatch_metrics,
            ));
            let instance = SubgraphInstance::from_manifest(
                &self.logger,
                manifest,
                self.runtime_host_builder.clone(),
                host_metrics.clone(),
            )?;

            // The subgraph state tracks the state of the subgraph instance over time
            let ctx = IndexingContext {
                inputs: IndexingInputs {
                    deployment_id: deployment_id.clone(),
                    start_blocks,
                    store: self.store.clone(),
                    ethereum_adapter: self.ethereum_adapter.clone(),
                    stream_builder: self.block_stream_builder.clone(),
                    templates_use_calls,
                    top_level_templates,
                },
                state: IndexingState {
                    logger: self.logger.clone(),
                    instance,
                    log_filter,
                    call_filter,
                    block_filter,
                    restarts: 0,
                    entity_lfu_cache: LfuCache::new(),
                },
                subgraph_metrics,
                host_metrics,
                ethrpc_metrics,
                block_stream_metrics,
            };

            // Keep restarting the subgraph until it terminates. The subgraph
            // will usually only run once, but is restarted whenever a block
            // creates dynamic data sources. This allows us to recreate the
            // block stream and include events for the new data sources going
            // forward; this is easier than updating the existing block stream.
            //
            // This task has many calls to the store, so mark it as `blocking`.
            let metrics_registry = self.metrics_registry.clone();
            let (abortable, abort_handle) = abortable(run_subgraph(ctx));
            graph::spawn_blocking(async move {
                let res = abortable.await;
                subgraph_metrics_unregister.unregister(metrics_registry);
                res
            });

            Ok(abort_handle)
        }
        .map_err(|e| {
            error!(
                logger,
                "Failed to start subgraph";
                "error" => format!("{}", e),
                "code" => LogCode::SubgraphStartFailure
            );
            e
        })
        .await
    }
}

async fn run_subgraph<MR, S>(mut ctx: IndexingContext<MR, S>) -> Result<(), ()>
where
    MR: MetricsRegistry,
    S: ChainStore + Store + EthereumCallCache + SubgraphDeploymentStore,
{
    // Clone a few things for different parts of the async processing
    let subgraph_metrics = ctx.subgraph_metrics.cheap_clone();
    let store_for_err = ctx.inputs.store.cheap_clone();
    let logger = ctx.state.logger.cheap_clone();
    let id_for_err = ctx.inputs.deployment_id.clone();

    loop {
        debug!(logger, "Starting or restarting subgraph");

        // if ctx.state.cancel_handle.is_canceled() {
        //     info!(logger, "Subgraph shutting down cleanly");
        //     return Ok(());
        // }

        let mut block_stream = ctx
            .inputs
            .stream_builder
            .build(
                logger.clone(),
                ctx.inputs.deployment_id.clone(),
                ctx.inputs.start_blocks.clone(),
                ctx.state.log_filter.clone(),
                ctx.state.call_filter.clone(),
                ctx.state.block_filter.clone(),
                ctx.inputs.templates_use_calls,
                ctx.block_stream_metrics.clone(),
            )
            .compat();

        debug!(logger, "Starting block stream");

        // Process events from the stream as long as no restart is needed
        loop {
            // if ctx.state.cancel_handle.is_canceled() {
            //     info!(logger, "Subgraph shutting down cleanly");
            //     return Ok(());
            // }

            let block = match block_stream.next().await {
                Some(Ok(BlockStreamEvent::Block(block))) => block,
                Some(Ok(BlockStreamEvent::Revert)) => {
                    // On revert, clear the entity cache.
                    ctx.state.entity_lfu_cache = LfuCache::new();
                    continue;
                }
                // Log and drop the errors from the block_stream
                // The block stream will continue attempting to produce blocks
                Some(Err(e)) => {
                    debug!(
                        &logger,
                        "Block stream produced a non-fatal error";
                        "error" => format!("{}", e),
                    );
                    continue;
                }
                None => unreachable!("The block stream stopped producing blocks"),
            };

            if block.triggers.len() > 0 {
                subgraph_metrics
                    .block_trigger_count
                    .observe(block.triggers.len() as f64);
            }

            let start = Instant::now();

            let res = process_block(
                &logger,
                ctx.inputs.ethereum_adapter.cheap_clone(),
                ctx,
                block,
            )
            .await;

            let elapsed = start.elapsed().as_secs_f64();
            subgraph_metrics.block_processing_duration.observe(elapsed);

            match res {
                Ok((c, needs_restart)) => {
                    ctx = c;
                    if needs_restart {
                        // Increase the restart counter
                        ctx.state.restarts += 1;

                        // Restart the subgraph
                        break;
                    }
                }
                // Handle unexpected stream errors by marking the subgraph as failed.
                Err(e) => {
                    error!(
                        &logger,
                        "Subgraph instance failed to run: {}", e;
                        "id" => id_for_err.to_string(),
                        "code" => LogCode::SubgraphSyncingFailure
                    );

                    // Set subgraph status to Failed
                    let status_ops =
                        SubgraphDeploymentEntity::update_failed_operations(&id_for_err, true);
                    if let Err(e) = store_for_err.apply_metadata_operations(status_ops) {
                        error!(
                            &logger,
                            "Failed to set subgraph status to Failed: {}", e;
                            "id" => id_for_err.to_string(),
                            "code" => LogCode::SubgraphSyncingFailureNotRecorded
                        );
                    }
                    return Err(());
                }
            }
        }
    }
}

/// Processes a block and returns the updated context and a boolean flag indicating
/// whether new dynamic data sources have been added to the subgraph.
async fn process_block<MR, S>(
    logger: &Logger,
    ethereum_adapter: Arc<dyn EthereumAdapter>,
    mut ctx: IndexingContext<MR, S>,
    block: EthereumBlockWithTriggers,
) -> Result<(IndexingContext<MR, S>, bool), Error>
where
    MR: MetricsRegistry,
    S: ChainStore + Store + EthereumCallCache + SubgraphDeploymentStore,
{
    let triggers = block.triggers;
    let block = block.ethereum_block;

    let block_ptr = EthereumBlockPointer::from(&block);
    let logger = logger.new(o!(
        "block_number" => format!("{:?}", block_ptr.number),
        "block_hash" => format!("{:?}", block_ptr.hash)
    ));

    if triggers.len() == 1 {
        info!(&logger, "1 trigger found in this block for this subgraph");
    } else if triggers.len() > 1 {
        info!(
            &logger,
            "{} triggers found in this block for this subgraph",
            triggers.len()
        );
    }

    // Obtain current and new block pointer (after this block is processed)
    let light_block = Arc::new(block.light_block());
    let block_ptr_after = EthereumBlockPointer::from(&block);
    let block_ptr_for_new_data_sources = block_ptr_after.clone();

    let metrics = ctx.subgraph_metrics.clone();

    // Process events one after the other, passing in entity operations
    // collected previously to every new event being processed
    let (mut ctx, mut block_state) = process_triggers(
        &logger,
        BlockState::with_cache(std::mem::take(&mut ctx.state.entity_lfu_cache)),
        ctx,
        &light_block,
        triggers,
    )
    .await?;

    // If new data sources have been created, restart the subgraph after this block.
    let needs_restart = !block_state.created_data_sources.is_empty();
    let host_metrics = ctx.host_metrics.clone();

    // This loop will:
    // 1. Instantiate created data sources.
    // 2. Process those data sources for the current block.
    // Until no data sources are created or MAX_DATA_SOURCES is hit.

    // Note that this algorithm processes data sources spawned on the same block _breadth
    // first_ on the tree implied by the parent-child relationship between data sources. Only a
    // very contrived subgraph would be able to observe this.
    while !block_state.created_data_sources.is_empty() {
        // Instantiate dynamic data sources, removing them from the block state.
        let (data_sources, runtime_hosts) = create_dynamic_data_sources(
            logger.clone(),
            &mut ctx,
            host_metrics.clone(),
            block_state.created_data_sources.drain(..),
        )?;

        // Reprocess the triggers from this block that match the new data sources
        let block_with_triggers = triggers_in_block(
            ethereum_adapter.clone(),
            logger.cheap_clone(),
            ctx.inputs.store.clone(),
            ctx.ethrpc_metrics.clone(),
            EthereumLogFilter::from_data_sources(data_sources.iter()),
            EthereumCallFilter::from_data_sources(data_sources.iter()),
            EthereumBlockFilter::from_data_sources(data_sources.iter()),
            block.clone(),
        )
        .compat()
        .await?;

        let triggers = block_with_triggers.triggers;

        if triggers.len() == 1 {
            info!(
                &logger,
                "1 trigger found in this block for the new data sources"
            );
        } else if triggers.len() > 1 {
            info!(
                &logger,
                "{} triggers found in this block for the new data sources",
                triggers.len()
            );
        }

        // Add entity operations for the new data sources to the block state
        // and add runtimes for the data sources to the subgraph instance.
        persist_dynamic_data_sources(
            logger.clone(),
            &mut ctx,
            &mut block_state.entity_cache,
            data_sources,
            block_ptr_for_new_data_sources,
        );

        // Process the triggers in each host in the same order the
        // corresponding data sources have been created.
        for trigger in triggers.into_iter() {
            block_state = SubgraphInstance::<S>::process_trigger_in_runtime_hosts(
                &logger,
                &runtime_hosts,
                &light_block,
                trigger,
                block_state,
            )
            .await?;
        }
    }

    // Apply entity operations and advance the stream

    let section = ctx.host_metrics.stopwatch.start_section("as_modifications");
    let ModificationsAndCache {
        modifications: mods,
        entity_lfu_cache: mut cache,
    } = block_state
        .entity_cache
        .as_modifications(ctx.inputs.store.as_ref())
        .map_err(|e| format_err!("Error while processing block stream for a subgraph: {}", e))?;
    section.end();

    let section = ctx
        .host_metrics
        .stopwatch
        .start_section("entity_cache_evict");
    cache.evict(*ENTITY_CACHE_SIZE);
    section.end();

    // Put the cache back in the ctx, asserting that the placeholder cache was not used.
    assert!(ctx.state.entity_lfu_cache.is_empty());
    ctx.state.entity_lfu_cache = cache;

    if !mods.is_empty() {
        info!(&logger, "Applying {} entity operation(s)", mods.len());
    }

    // Transact entity operations into the store and update the
    // subgraph's block stream pointer
    let _section = ctx.host_metrics.stopwatch.start_section("transact_block");
    let subgraph_id = ctx.inputs.deployment_id.clone();
    let stopwatch = ctx.host_metrics.stopwatch.clone();
    let start = Instant::now();

    match ctx
        .inputs
        .store
        .transact_block_operations(subgraph_id, block_ptr_after, mods, stopwatch)
    {
        Ok(should_migrate) => {
            let elapsed = start.elapsed().as_secs_f64();
            metrics.block_ops_transaction_duration.observe(elapsed);
            if should_migrate {
                ctx.inputs.store.migrate_subgraph_deployment(
                    &logger,
                    &ctx.inputs.deployment_id,
                    &block_ptr_after,
                );
            }
            Ok((ctx, needs_restart))
        }
        Err(e) => {
            Err(format_err!("Error while processing block stream for a subgraph: {}", e).into())
        }
    }
}

async fn process_triggers<MR, S>(
    logger: &Logger,
    mut block_state: BlockState,
    ctx: IndexingContext<MR, S>,
    block: &Arc<LightEthereumBlock>,
    triggers: Vec<EthereumTrigger>,
) -> Result<(IndexingContext<MR, S>, BlockState), Error>
where
    MR: MetricsRegistry,
    S: Store + SubgraphDeploymentStore + EthereumCallCache,
{
    for trigger in triggers.into_iter() {
        let block_ptr = EthereumBlockPointer::from(block.as_ref());
        let subgraph_metrics = ctx.subgraph_metrics.clone();
        let trigger_type = match trigger {
            EthereumTrigger::Log(_) => TriggerType::Event,
            EthereumTrigger::Call(_) => TriggerType::Call,
            EthereumTrigger::Block(..) => TriggerType::Block,
        };
        let transaction_id = match &trigger {
            EthereumTrigger::Log(log) => log.transaction_hash,
            EthereumTrigger::Call(call) => call.transaction_hash,
            EthereumTrigger::Block(..) => None,
        };
        let start = Instant::now();
        block_state = ctx
            .state
            .instance
            .process_trigger(&logger, &block, trigger, block_state)
            .await
            .map_err(move |e| match transaction_id {
                Some(tx_hash) => format_err!(
                    "Failed to process trigger in block {}, transaction {:x}: {}",
                    block_ptr,
                    tx_hash,
                    e
                ),
                None => format_err!("Failed to process trigger: {}", e),
            })?;
        let elapsed = start.elapsed().as_secs_f64();
        subgraph_metrics.observe_trigger_processing_duration(elapsed, trigger_type);
    }
    Ok((ctx, block_state))
}

fn create_dynamic_data_sources<MR, S>(
    logger: Logger,
    ctx: &mut IndexingContext<MR, S>,
    host_metrics: Arc<HostMetrics>,
    created_data_sources: impl Iterator<Item = DataSourceTemplateInfo>,
) -> Result<(Vec<DataSource>, Vec<Arc<RuntimeHost>>), Error>
where
    MR: MetricsRegistry,
    S: ChainStore + Store + SubgraphDeploymentStore + EthereumCallCache,
{
    let mut data_sources = vec![];
    let mut runtime_hosts = vec![];

    for info in created_data_sources {
        // Try to instantiate a data source from the template
        let data_source = DataSource::try_from(info)?;

        // Try to create a runtime host for the data source
        let host = ctx.state.instance.add_dynamic_data_source(
            &logger,
            data_source.clone(),
            ctx.inputs.top_level_templates.clone(),
            host_metrics.clone(),
        )?;

        data_sources.push(data_source);
        runtime_hosts.push(host);
    }

    Ok((data_sources, runtime_hosts))
}

fn persist_dynamic_data_sources<MR, S>(
    logger: Logger,
    ctx: &mut IndexingContext<MR, S>,
    entity_cache: &mut EntityCache,
    data_sources: Vec<DataSource>,
    block_ptr: EthereumBlockPointer,
) where
    MR: MetricsRegistry,
    S: ChainStore + Store,
{
    if !data_sources.is_empty() {
        debug!(
            logger,
            "Creating {} dynamic data source(s)",
            data_sources.len()
        );
    }

    // Add entity operations to the block state in order to persist
    // the dynamic data sources
    for data_source in data_sources.iter() {
        let entity = DynamicEthereumContractDataSourceEntity::from((
            &ctx.inputs.deployment_id,
            data_source,
            &block_ptr,
        ));
        let id = format!("{}-dynamic", Uuid::new_v4().to_simple());
        let operations = entity.write_entity_operations(id.as_ref());
        entity_cache.append(operations);
    }

    // Merge log filters from data sources into the block stream builder
    ctx.state
        .log_filter
        .extend(EthereumLogFilter::from_data_sources(&data_sources));

    // Merge call filters from data sources into the block stream builder
    ctx.state
        .call_filter
        .extend(EthereumCallFilter::from_data_sources(&data_sources));

    // Merge block filters from data sources into the block stream builder
    ctx.state
        .block_filter
        .extend(EthereumBlockFilter::from_data_sources(&data_sources));
}
