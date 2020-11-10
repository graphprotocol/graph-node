use atomic_refcell::AtomicRefCell;
use futures01::sync::mpsc::{channel, Receiver, Sender};
use lazy_static::lazy_static;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Instant;

use graph::components::ethereum::{triggers_in_block, EthereumNetworks};
use graph::components::store::ModificationsAndCache;
use graph::components::subgraph::{MappingError, ProofOfIndexing, SharedProofOfIndexing};
use graph::data::store::scalar::Bytes;
use graph::data::subgraph::schema::{
    queries::LazyMetadata, DynamicEthereumContractDataSourceEntity, SubgraphError, SubgraphHealth,
    POI_OBJECT,
};
use graph::prelude::{SubgraphInstance as SubgraphInstanceTrait, *};
use graph::util::lfu_cache::LfuCache;

use super::SubgraphInstance;

lazy_static! {
    /// Size limit of the entity LFU cache, in bytes.
    // Multiplied by 1000 because the env var is in KB.
    pub static ref ENTITY_CACHE_SIZE: usize = 1000
        * std::env::var("GRAPH_ENTITY_CACHE_SIZE")
            .unwrap_or("10000".into())
            .parse::<usize>()
            .expect("invalid GRAPH_ENTITY_CACHE_SIZE");
}

type SharedInstanceKeepAliveMap = Arc<RwLock<HashMap<SubgraphDeploymentId, CancelGuard>>>;

struct IndexingInputs<B, S> {
    deployment_id: SubgraphDeploymentId,
    network_name: String,
    start_blocks: Vec<u64>,
    store: Arc<S>,
    eth_adapter: Arc<dyn EthereumAdapter>,
    stream_builder: B,
    include_calls_in_blocks: bool,
    top_level_templates: Arc<Vec<DataSourceTemplate>>,
}

struct IndexingState<T: RuntimeHostBuilder> {
    logger: Logger,
    instance: SubgraphInstance<T>,
    instances: SharedInstanceKeepAliveMap,
    log_filter: EthereumLogFilter,
    call_filter: EthereumCallFilter,
    block_filter: EthereumBlockFilter,
    entity_lfu_cache: LfuCache<EntityKey, Option<Entity>>,
}

struct IndexingContext<B, T: RuntimeHostBuilder, S> {
    /// Read only inputs that are needed while indexing a subgraph.
    pub inputs: IndexingInputs<B, S>,

    /// Mutable state that may be modified while indexing a subgraph.
    pub state: IndexingState<T>,

    /// Sensors to measure the execution of the subgraph instance
    pub subgraph_metrics: Arc<SubgraphInstanceMetrics>,

    /// Sensors to measure the execution of the subgraph's runtime hosts
    pub host_metrics: Arc<HostMetrics>,

    /// Sensors to measure the execution of eth rpc calls
    pub ethrpc_metrics: Arc<SubgraphEthRpcMetrics>,

    pub block_stream_metrics: Arc<BlockStreamMetrics>,
}

pub struct SubgraphInstanceManager {
    logger: Logger,
    input: Sender<SubgraphAssignmentProviderEvent>,
}

struct SubgraphInstanceManagerMetrics {
    pub subgraph_count: Box<Gauge>,
}

impl SubgraphInstanceManagerMetrics {
    pub fn new(registry: Arc<impl MetricsRegistry>) -> Self {
        let subgraph_count = registry
            .new_gauge(
                "deployment_count",
                "Counts the number of deployments currently being indexed by the graph-node.",
                HashMap::new(),
            )
            .expect("failed to create `deployment_count` gauge");
        Self { subgraph_count }
    }
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
    pub fn new(registry: Arc<impl MetricsRegistry>, subgraph_hash: &str) -> Self {
        let block_trigger_count = registry
            .new_deployment_histogram(
                "deployment_block_trigger_count",
                "Measures the number of triggers in each block for a subgraph deployment",
                subgraph_hash,
                vec![1.0, 5.0, 10.0, 20.0, 50.0],
            )
            .expect("failed to create `deployment_block_trigger_count` histogram");
        let trigger_processing_duration = registry
            .new_deployment_histogram_vec(
                "deployment_trigger_processing_duration",
                "Measures duration of trigger processing for a subgraph deployment",
                subgraph_hash,
                vec![String::from("trigger_type")],
                vec![0.01, 0.05, 0.1, 0.5, 1.5, 5.0, 10.0, 30.0, 120.0],
            )
            .expect("failed to create `deployment_trigger_processing_duration` histogram");
        let block_processing_duration = registry
            .new_deployment_histogram(
                "deployment_block_processing_duration",
                "Measures duration of block processing for a subgraph deployment",
                subgraph_hash,
                vec![0.05, 0.2, 0.7, 1.5, 4.0, 10.0, 60.0, 120.0, 240.0],
            )
            .expect("failed to create `deployment_block_processing_duration` histogram");
        let block_ops_transaction_duration = registry
            .new_deployment_histogram(
                "deployment_transact_block_operations_duration",
                "Measures duration of commiting all the entity operations in a block and updating the subgraph pointer",
                subgraph_hash,
                vec![0.01, 0.05, 0.1, 0.3, 0.7, 2.0],
            )
            .expect("failed to create `deployment_transact_block_operations_duration_{}");

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

impl SubgraphInstanceManager {
    /// Creates a new runtime manager.
    pub fn new<B, S, M>(
        logger_factory: &LoggerFactory,
        stores: HashMap<String, Arc<S>>,
        eth_networks: EthereumNetworks,
        host_builder: impl RuntimeHostBuilder,
        block_stream_builder: B,
        metrics_registry: Arc<M>,
        graphql_runner: Arc<impl GraphQlRunner>,
    ) -> Self
    where
        S: Store + ChainStore + SubgraphDeploymentStore + EthereumCallCache,
        B: BlockStreamBuilder,
        M: MetricsRegistry,
    {
        let logger = logger_factory.component_logger("SubgraphInstanceManager", None);
        let logger_factory = logger_factory.with_parent(logger.clone());

        // Create channel for receiving subgraph provider events.
        let (subgraph_sender, subgraph_receiver) = channel(100);

        // Handle incoming events from the subgraph provider.
        Self::handle_subgraph_events(
            logger_factory,
            subgraph_receiver,
            stores,
            eth_networks,
            host_builder,
            block_stream_builder,
            metrics_registry.clone(),
            graphql_runner,
        );

        SubgraphInstanceManager {
            logger,
            input: subgraph_sender,
        }
    }

    /// Handle incoming events from subgraph providers.
    fn handle_subgraph_events<B, S, M>(
        logger_factory: LoggerFactory,
        receiver: Receiver<SubgraphAssignmentProviderEvent>,
        stores: HashMap<String, Arc<S>>,
        eth_networks: EthereumNetworks,
        host_builder: impl RuntimeHostBuilder,
        block_stream_builder: B,
        metrics_registry: Arc<M>,
        graphql_runner: Arc<impl GraphQlRunner>,
    ) where
        S: Store + ChainStore + SubgraphDeploymentStore + EthereumCallCache,
        B: BlockStreamBuilder,
        M: MetricsRegistry,
    {
        let metrics_registry_for_manager = metrics_registry.clone();
        let metrics_registry_for_subgraph = metrics_registry.clone();
        let manager_metrics = SubgraphInstanceManagerMetrics::new(metrics_registry_for_manager);

        // Subgraph instance shutdown senders
        let instances: SharedInstanceKeepAliveMap = Default::default();

        // Blocking due to store interactions. Won't be blocking after #905.
        graph::spawn_blocking(async move {
            let mut assignment_stream = receiver.compat();

            // The channel will always be open so it never errors.
            while let Ok(event) = assignment_stream.next().await.unwrap() {
                use self::SubgraphAssignmentProviderEvent::*;

                match event {
                    SubgraphStart(manifest) => {
                        let logger = logger_factory.subgraph_logger(&manifest.id);
                        info!(
                            logger,
                            "Start subgraph";
                            "data_sources" => manifest.data_sources.len()
                        );
                        let network = manifest.network_name();
                        let required_capabilities = manifest.required_ethereum_capabilities();

                        match Self::start_subgraph(
                            logger.clone(),
                            instances.clone(),
                            host_builder.clone(),
                            block_stream_builder.clone(),
                            stores
                                .get(&network)
                                .expect(&format!(
                                    "expected store that matches subgraph network: {}",
                                    &network
                                ))
                                .clone(),
                            eth_networks
                                .adapter_with_capabilities(
                                    network.clone(),
                                    &required_capabilities)
                                .expect(&format!(
                                    "expected eth adapter that matches subgraph network {} with required capabilities: {}",
                                    &network,
                                    &required_capabilities))
                                .clone(),
                            manifest,
                            metrics_registry_for_subgraph.clone(),
                            graphql_runner.clone(),
                        )
                        .await
                        {
                            Ok(()) => manager_metrics.subgraph_count.inc(),
                            Err(err) => error!(
                                logger,
                                "Failed to start subgraph";
                                "error" => format!("{}", err),
                                "code" => LogCode::SubgraphStartFailure
                            ),
                        }
                    }
                    SubgraphStop(id) => {
                        let logger = logger_factory.subgraph_logger(&id);
                        info!(logger, "Stop subgraph");

                        Self::stop_subgraph(instances.clone(), id);
                        manager_metrics.subgraph_count.dec();
                    }
                };
            }
        });
    }

    async fn start_subgraph<B, S, M>(
        logger: Logger,
        instances: SharedInstanceKeepAliveMap,
        host_builder: impl RuntimeHostBuilder,
        stream_builder: B,
        store: Arc<S>,
        eth_adapter: Arc<dyn EthereumAdapter>,
        manifest: SubgraphManifest,
        registry: Arc<M>,
        graphql_runner: Arc<impl GraphQlRunner>,
    ) -> Result<(), Error>
    where
        B: BlockStreamBuilder,
        S: Store + ChainStore + SubgraphDeploymentStore + EthereumCallCache,
        M: MetricsRegistry,
    {
        // If a subgraph had a fatal error the last time we tried to run it, we can reset it to the
        // previous health status in hopes that it doesn't fail again.
        let status_ops = {
            let metadata = LazyMetadata {
                store: store.cheap_clone(),
                graphql_runner,
                id: manifest.id.clone(),
            };
            match metadata.health().await? {
                SubgraphHealth::Failed => {
                    let prev_health = match metadata.has_non_fatal_errors().await? {
                        false => SubgraphHealth::Healthy,
                        true => SubgraphHealth::Unhealthy,
                    };
                    SubgraphDeploymentEntity::unfail_operations(&manifest.id, prev_health)
                }
                SubgraphHealth::Healthy | SubgraphHealth::Unhealthy => vec![],
            }
        };
        store.start_subgraph_deployment(&logger, &manifest.id, status_ops)?;

        let mut templates: Vec<DataSourceTemplate> = vec![];
        for data_source in manifest.data_sources.iter() {
            for template in data_source.templates.iter() {
                templates.push(template.clone());
            }
        }

        // Clone the deployment ID for later
        let deployment_id = manifest.id.clone();
        let network_name = manifest.network_name();

        // Obtain filters from the manifest
        let log_filter = EthereumLogFilter::from_data_sources(&manifest.data_sources);
        let call_filter = EthereumCallFilter::from_data_sources(&manifest.data_sources);
        let block_filter = EthereumBlockFilter::from_data_sources(&manifest.data_sources);
        let start_blocks = manifest.start_blocks();

        // Identify whether there are mappings with call handlers or
        // block handlers with call filters; in this case, we need to
        // include calls in all blocks
        let include_calls_in_blocks = manifest.requires_traces();

        let top_level_templates = Arc::new(manifest.templates.clone());

        // Create a subgraph instance from the manifest; this moves
        // ownership of the manifest and host builder into the new instance
        let stopwatch_metrics =
            StopwatchMetrics::new(logger.clone(), deployment_id.clone(), registry.clone());
        let subgraph_metrics = Arc::new(SubgraphInstanceMetrics::new(
            registry.clone(),
            deployment_id.as_str(),
        ));
        let subgraph_metrics_unregister = subgraph_metrics.clone();
        let host_metrics = Arc::new(HostMetrics::new(
            registry.clone(),
            deployment_id.as_str(),
            stopwatch_metrics.clone(),
        ));
        let ethrpc_metrics = Arc::new(SubgraphEthRpcMetrics::new(registry.clone(), &deployment_id));
        let block_stream_metrics = Arc::new(BlockStreamMetrics::new(
            registry.clone(),
            ethrpc_metrics.clone(),
            &deployment_id,
            stopwatch_metrics,
        ));
        let instance =
            SubgraphInstance::from_manifest(&logger, manifest, host_builder, host_metrics.clone())?;

        // The subgraph state tracks the state of the subgraph instance over time
        let ctx = IndexingContext {
            inputs: IndexingInputs {
                deployment_id: deployment_id.clone(),
                network_name,
                start_blocks,
                store,
                eth_adapter,
                stream_builder,
                include_calls_in_blocks,
                top_level_templates,
            },
            state: IndexingState {
                logger,
                instance,
                instances,
                log_filter,
                call_filter,
                block_filter,
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
        graph::spawn_blocking(async move {
            let res = run_subgraph(ctx).await;
            subgraph_metrics_unregister.unregister(registry);
            res
        });

        Ok(())
    }

    fn stop_subgraph(instances: SharedInstanceKeepAliveMap, id: SubgraphDeploymentId) {
        // Drop the cancel guard to shut down the subgraph now
        let mut instances = instances.write().unwrap();
        instances.remove(&id);
    }
}

impl EventConsumer<SubgraphAssignmentProviderEvent> for SubgraphInstanceManager {
    /// Get the wrapped event sink.
    fn event_sink(
        &self,
    ) -> Box<dyn Sink<SinkItem = SubgraphAssignmentProviderEvent, SinkError = ()> + Send> {
        let logger = self.logger.clone();
        Box::new(self.input.clone().sink_map_err(move |e| {
            error!(logger, "Component was dropped: {}", e);
        }))
    }
}

async fn run_subgraph<B, T, S>(mut ctx: IndexingContext<B, T, S>) -> Result<(), ()>
where
    B: BlockStreamBuilder,
    T: RuntimeHostBuilder,
    S: ChainStore + Store + EthereumCallCache + SubgraphDeploymentStore,
{
    // Clone a few things for different parts of the async processing
    let subgraph_metrics = ctx.subgraph_metrics.cheap_clone();
    let store_for_err = ctx.inputs.store.cheap_clone();
    let logger = ctx.state.logger.cheap_clone();
    let id_for_err = ctx.inputs.deployment_id.clone();

    loop {
        debug!(logger, "Starting or restarting subgraph");

        let block_stream_canceler = CancelGuard::new();
        let block_stream_cancel_handle = block_stream_canceler.handle();
        let mut block_stream = ctx
            .inputs
            .stream_builder
            .build(
                logger.clone(),
                ctx.inputs.deployment_id.clone(),
                ctx.inputs.network_name.clone(),
                ctx.inputs.start_blocks.clone(),
                ctx.state.log_filter.clone(),
                ctx.state.call_filter.clone(),
                ctx.state.block_filter.clone(),
                ctx.inputs.include_calls_in_blocks,
                ctx.block_stream_metrics.clone(),
            )
            .map_err(CancelableError::Error)
            .cancelable(&block_stream_canceler, || CancelableError::Cancel)
            .compat();

        // Keep the stream's cancel guard around to be able to shut it down
        // when the subgraph deployment is unassigned
        ctx.state
            .instances
            .write()
            .unwrap()
            .insert(ctx.inputs.deployment_id.clone(), block_stream_canceler);

        debug!(logger, "Starting block stream");

        // Process events from the stream as long as no restart is needed
        loop {
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

            let block_ptr = EthereumBlockPointer::from(&block.ethereum_block);

            if block.triggers.len() > 0 {
                subgraph_metrics
                    .block_trigger_count
                    .observe(block.triggers.len() as f64);
            }

            let start = Instant::now();

            let res = process_block(
                &logger,
                ctx.inputs.eth_adapter.cheap_clone(),
                ctx,
                block_stream_cancel_handle.clone(),
                block,
            )
            .await;

            let elapsed = start.elapsed().as_secs_f64();
            subgraph_metrics.block_processing_duration.observe(elapsed);

            match res {
                Ok((c, needs_restart)) => {
                    ctx = c;
                    if needs_restart {
                        // Cancel the stream for real
                        ctx.state
                            .instances
                            .write()
                            .unwrap()
                            .remove(&ctx.inputs.deployment_id);

                        // And restart the subgraph
                        break;
                    }
                }
                Err(CancelableError::Cancel) => {
                    debug!(
                        &logger,
                        "Subgraph block stream shut down cleanly";
                        "id" => id_for_err.to_string(),
                    );
                    return Err(());
                }
                // Handle unexpected stream errors by marking the subgraph as failed.
                Err(CancelableError::Error(e)) => {
                    error!(
                        &logger,
                        "Subgraph instance failed to run: {}", e;
                        "id" => id_for_err.to_string(),
                        "code" => LogCode::SubgraphSyncingFailure
                    );

                    let error = SubgraphError {
                        subgraph_id: id_for_err.clone(),
                        message: e.to_string(),
                        block_ptr: Some(block_ptr),
                        handler: None,
                        deterministic: e.is_deterministic(),
                    };

                    // Set subgraph status to Failed
                    let status_ops = SubgraphDeploymentEntity::fail_operations(&id_for_err, error);
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

#[derive(thiserror::Error, Debug)]
enum BlockProcessingError {
    #[error("{0:#}")]
    Unknown(anyhow::Error),

    #[error("{0:#}")]
    Deterministic(anyhow::Error),
}

impl BlockProcessingError {
    fn is_deterministic(&self) -> bool {
        match self {
            BlockProcessingError::Unknown(_) => false,
            BlockProcessingError::Deterministic(_) => true,
        }
    }
}

impl From<failure::Error> for BlockProcessingError {
    fn from(e: failure::Error) -> Self {
        BlockProcessingError::Unknown(e.compat_err())
    }
}

/// Processes a block and returns the updated context and a boolean flag indicating
/// whether new dynamic data sources have been added to the subgraph.
async fn process_block<B: BlockStreamBuilder, T: RuntimeHostBuilder, S>(
    logger: &Logger,
    eth_adapter: Arc<dyn EthereumAdapter>,
    mut ctx: IndexingContext<B, T, S>,
    block_stream_cancel_handle: CancelHandle,
    block: EthereumBlockWithTriggers,
) -> Result<(IndexingContext<B, T, S>, bool), CancelableError<BlockProcessingError>>
where
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

    let proof_of_indexing = if ctx
        .inputs
        .store
        .supports_proof_of_indexing(&ctx.inputs.deployment_id)
        .await?
    {
        Some(Arc::new(AtomicRefCell::new(ProofOfIndexing::new(
            block_ptr.number,
        ))))
    } else {
        None
    };

    // Process events one after the other, passing in entity operations
    // collected previously to every new event being processed
    let mut block_state = match process_triggers(
        &logger,
        BlockState::new(
            ctx.inputs.store.clone(),
            std::mem::take(&mut ctx.state.entity_lfu_cache),
        ),
        proof_of_indexing.cheap_clone(),
        ctx.subgraph_metrics.clone(),
        &ctx.state.instance,
        &light_block,
        triggers,
    )
    .await
    {
        Ok(block_state) => block_state,
        Err(MappingError::Unknown(e)) => {
            return Err(CancelableError::Error(BlockProcessingError::Unknown(e)))
        }
        Err(MappingError::PossibleReorg(e)) => {
            info!(ctx.state.logger,
                    "Possible reorg detected, retrying";
                    "error" => format!("{:?}", e.to_string()),
                    "id" => ctx.inputs.deployment_id.to_string(),
            );

            // In case of a possible reorg, we want this function to do nothing and restart the
            // block stream so it has a chance to detect the reorg.
            //
            // The `ctx` is unchanged at this point, except for having cleared the entity cache.
            // Losing the cache is a bit annoying but not an issue for correctness.
            //
            // See also b21fa73b-6453-4340-99fb-1a78ec62efb1.
            return Ok((ctx, true));
        }
    };

    // If new data sources have been created, restart the subgraph after this block.
    // This is necessary to re-create the block stream.
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
            block_state.created_data_sources.split_off(0).into_iter(),
        )
        .compat_err()?;

        // Reprocess the triggers from this block that match the new data sources
        let block_with_triggers = triggers_in_block(
            eth_adapter.clone(),
            logger.cheap_clone(),
            ctx.inputs.store.clone(),
            ctx.ethrpc_metrics.clone(),
            EthereumLogFilter::from_data_sources(data_sources.iter()),
            EthereumCallFilter::from_data_sources(data_sources.iter()),
            EthereumBlockFilter::from_data_sources(data_sources.iter()),
            block.clone(),
        )
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
        )?;

        // Process the triggers in each host in the same order the
        // corresponding data sources have been created.
        for trigger in triggers.into_iter() {
            block_state = SubgraphInstance::<T>::process_trigger_in_runtime_hosts(
                &logger,
                &runtime_hosts,
                &light_block,
                trigger,
                block_state,
                proof_of_indexing.cheap_clone(),
            )
            .await
            .map_err(|e| {
                // This treats a `PossibleReorg` as an ordinary error which will fail the subgraph.
                // This can cause an unecessary subgraph failure, to fix it we need to figure out a
                // way to revert the effect of `create_dynamic_data_sources` so we may return a
                // clean context as in b21fa73b-6453-4340-99fb-1a78ec62efb1.
                match e {
                    MappingError::PossibleReorg(e) | MappingError::Unknown(e) => {
                        BlockProcessingError::Unknown(e)
                    }
                }
            })
            .map_err(CancelableError::Error)?;
        }
    }

    // Apply entity operations and advance the stream

    // Avoid writing to store if block stream has been canceled
    if block_stream_cancel_handle.is_canceled() {
        return Err(CancelableError::Cancel);
    }

    if let Some(proof_of_indexing) = proof_of_indexing {
        let proof_of_indexing = Arc::try_unwrap(proof_of_indexing).unwrap().into_inner();
        update_proof_of_indexing(
            proof_of_indexing,
            &ctx.host_metrics.stopwatch,
            &ctx.inputs.deployment_id,
            &mut block_state.entity_cache,
        )
        .await?;
    }

    let section = ctx.host_metrics.stopwatch.start_section("as_modifications");
    let ModificationsAndCache {
        modifications: mods,
        entity_lfu_cache: mut cache,
    } = block_state
        .entity_cache
        .as_modifications(ctx.inputs.store.as_ref())
        .map_err(|e| {
            CancelableError::from(format_err!(
                "Error while processing block stream for a subgraph: {}",
                e
            ))
        })?;
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

    match ctx.inputs.store.transact_block_operations(
        subgraph_id,
        block_ptr_after,
        mods,
        stopwatch,
        block_state.deterministic_errors,
    ) {
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

/// Transform the proof of indexing changes into entity updates that will be
/// inserted when as_modifications is called.
async fn update_proof_of_indexing(
    proof_of_indexing: ProofOfIndexing,
    stopwatch: &StopwatchMetrics,
    deployment_id: &SubgraphDeploymentId,
    entity_cache: &mut EntityCache,
) -> Result<(), Error> {
    let _section_guard = stopwatch.start_section("update_proof_of_indexing");

    let mut proof_of_indexing = proof_of_indexing.take();

    for (causality_region, stream) in proof_of_indexing.drain() {
        // Create the special POI entity key specific to this causality_region
        let entity_key = EntityKey {
            subgraph_id: deployment_id.clone(),
            entity_type: POI_OBJECT.to_owned(),
            entity_id: causality_region,
        };

        // Grab the current digest attribute on this entity
        let prev_poi =
            entity_cache
                .get(&entity_key)
                .map_err(Error::from)?
                .map(|entity| match entity.get("digest") {
                    Some(Value::Bytes(b)) => b.clone(),
                    _ => panic!("Expected POI entity to have a digest and for it to be bytes"),
                });

        // Finish the POI stream, getting the new POI value.
        let updated_proof_of_indexing = stream.pause(prev_poi.as_deref());
        let updated_proof_of_indexing: Bytes = (&updated_proof_of_indexing[..]).into();

        // Put this onto an entity with the same digest attribute
        // that was expected before when reading.
        let new_poi_entity = entity! {
            id: entity_key.entity_id.clone(),
            digest: updated_proof_of_indexing,
        };

        entity_cache.set(entity_key, new_poi_entity)?;
    }

    Ok(())
}

async fn process_triggers(
    logger: &Logger,
    mut block_state: BlockState,
    proof_of_indexing: SharedProofOfIndexing,
    subgraph_metrics: Arc<SubgraphInstanceMetrics>,
    instance: &SubgraphInstance<impl RuntimeHostBuilder>,
    block: &Arc<LightEthereumBlock>,
    triggers: Vec<EthereumTrigger>,
) -> Result<BlockState, MappingError> {
    for trigger in triggers.into_iter() {
        let block_ptr = EthereumBlockPointer::from(block.as_ref());
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
        block_state = instance
            .process_trigger(
                &logger,
                &block,
                trigger,
                block_state,
                proof_of_indexing.cheap_clone(),
            )
            .await
            .map_err(move |e| {
                e.context(match transaction_id {
                    Some(tx_hash) => format!(
                        "Failed to process trigger in block {}, transaction {:x}",
                        block_ptr, tx_hash
                    ),
                    None => "Failed to process trigger".to_string(),
                })
            })?;
        let elapsed = start.elapsed().as_secs_f64();
        subgraph_metrics.observe_trigger_processing_duration(elapsed, trigger_type);
    }
    Ok(block_state)
}

fn create_dynamic_data_sources<B, T: RuntimeHostBuilder, S>(
    logger: Logger,
    ctx: &mut IndexingContext<B, T, S>,
    host_metrics: Arc<HostMetrics>,
    created_data_sources: impl Iterator<Item = DataSourceTemplateInfo>,
) -> Result<(Vec<DataSource>, Vec<Arc<T::Host>>), anyhow::Error>
where
    B: BlockStreamBuilder,
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

        match host {
            Some(host) => {
                data_sources.push(data_source);
                runtime_hosts.push(host);
            }
            None => warn!(
                logger,
                "no runtime hosted created, there is already a runtime host instantiated for \
                 this data source";
                "name" => &data_source.name,
                "address" => &data_source.source.address
                    .map(|address| address.to_string())
                    .unwrap_or("none".to_string()),
                "abi" => &data_source.source.abi
            ),
        }
    }

    Ok((data_sources, runtime_hosts))
}

fn persist_dynamic_data_sources<B, T: RuntimeHostBuilder, S>(
    logger: Logger,
    ctx: &mut IndexingContext<B, T, S>,
    entity_cache: &mut EntityCache,
    data_sources: Vec<DataSource>,
    block_ptr: EthereumBlockPointer,
) -> Result<(), Error>
where
    B: BlockStreamBuilder,
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
        debug!(
            logger,
            "Persisting data_source";
            "name" => &data_source.name,
            "address" => &data_source.source.address.map(|address| address.to_string()).unwrap_or("none".to_string()),
        );
        let entity = DynamicEthereumContractDataSourceEntity::from((
            &ctx.inputs.deployment_id,
            data_source,
            &block_ptr,
        ));
        let id = DynamicEthereumContractDataSourceEntity::make_id();
        let operations = entity.write_entity_operations(id.as_ref());
        entity_cache.append(operations)?;
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

    Ok(())
}
