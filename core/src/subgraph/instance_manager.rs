use futures::future::{loop_fn, Loop};
use futures::sync::mpsc::{channel, Receiver, Sender};
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Instant;
use uuid::Uuid;

use graph::data::subgraph::schema::{
    DynamicEthereumContractDataSourceEntity, SubgraphDeploymentEntity,
};
use graph::prelude::{SubgraphInstance as SubgraphInstanceTrait, *};

use super::SubgraphInstance;

type SharedInstanceKeepAliveMap = Arc<RwLock<HashMap<SubgraphDeploymentId, CancelGuard>>>;

struct IndexingInputs<B, S> {
    deployment_id: SubgraphDeploymentId,
    network_name: String,
    start_blocks: Vec<u64>,
    store: Arc<S>,
    eth_adapter: Arc<dyn EthereumAdapter>,
    stream_builder: B,
    templates_use_calls: bool,
    top_level_templates: Vec<DataSourceTemplate>,
}

struct IndexingState<T: RuntimeHostBuilder> {
    logger: Logger,
    instance: SubgraphInstance<T>,
    instances: SharedInstanceKeepAliveMap,
    log_filter: EthereumLogFilter,
    call_filter: EthereumCallFilter,
    block_filter: EthereumBlockFilter,
    restarts: u64,
}

struct IndexingContext<B, T: RuntimeHostBuilder, S> {
    /// Read only inputs that are needed while indexing a subgraph.
    pub inputs: IndexingInputs<B, S>,

    /// Mutable state that may be modified while indexing a subgraph.
    pub state: IndexingState<T>,

    /// Sensors to measure the execution of the subgraph instance
    pub subgraph_metrics: Arc<SubgraphInstanceMetrics>,

    /// Sensors to measue the execution of the subgraphs runtime hosts
    pub host_metrics: Arc<HostMetrics>,

    /// Sensors to measue the execution of eth rpc calls
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
    pub fn new<M: MetricsRegistry>(registry: Arc<M>) -> Self {
        let subgraph_count = registry
            .new_gauge(
                String::from("subgraph_count"),
                String::from(
                    "Counts the number of subgraphs currently being indexed by the graph-node.",
                ),
                HashMap::new(),
            )
            .expect("failed to create `subgraph_count` gauge");
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
    pub fn new<M: MetricsRegistry>(registry: Arc<M>, subgraph_hash: String) -> Self {
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

impl SubgraphInstanceManager {
    /// Creates a new runtime manager.
    pub fn new<B, S, M>(
        logger_factory: &LoggerFactory,
        stores: HashMap<String, Arc<S>>,
        eth_adapters: HashMap<String, Arc<dyn EthereumAdapter>>,
        host_builder: impl RuntimeHostBuilder,
        block_stream_builder: B,
        metrics_registry: Arc<M>,
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
            eth_adapters,
            host_builder,
            block_stream_builder,
            metrics_registry.clone(),
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
        eth_adapters: HashMap<String, Arc<dyn EthereumAdapter>>,
        host_builder: impl RuntimeHostBuilder,
        block_stream_builder: B,
        metrics_registry: Arc<M>,
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

        tokio::spawn(receiver.for_each(move |event| {
            use self::SubgraphAssignmentProviderEvent::*;

            match event {
                SubgraphStart(manifest) => {
                    let logger = logger_factory.subgraph_logger(&manifest.id);
                    info!(
                        logger,
                        "Start subgraph";
                        "data_sources" => manifest.data_sources.len()
                    );

                    match manifest.network_name() {
                        Ok(n) => {
                            Self::start_subgraph(
                                logger.clone(),
                                instances.clone(),
                                host_builder.clone(),
                                block_stream_builder.clone(),
                                stores
                                    .get(&n)
                                    .expect(&format!(
                                        "expected store that matches subgraph network: {}",
                                        &n
                                    ))
                                    .clone(),
                                eth_adapters
                                    .get(&n)
                                    .expect(&format!(
                                        "expected eth adapter that matches subgraph network: {}",
                                        &n
                                    ))
                                    .clone(),
                                manifest,
                                metrics_registry_for_subgraph.clone(),
                            )
                            .map_err(|err| {
                                error!(
                                    logger,
                                    "Failed to start subgraph";
                                    "error" => format!("{}", err),
                                    "code" => LogCode::SubgraphStartFailure
                                )
                            })
                            .and_then(|_| {
                                manager_metrics.subgraph_count.inc();
                                Ok(())
                            })
                            .ok();
                        }
                        Err(err) => error!(
                            logger,
                            "Failed to start subgraph";
                             "error" => format!("{}", err),
                            "code" => LogCode::SubgraphStartFailure
                        ),
                    };
                }
                SubgraphStop(id) => {
                    let logger = logger_factory.subgraph_logger(&id);
                    info!(logger, "Stop subgraph");

                    Self::stop_subgraph(instances.clone(), id);
                    manager_metrics.subgraph_count.dec();
                }
            };

            Ok(())
        }));
    }

    fn start_subgraph<B, S, M>(
        logger: Logger,
        instances: SharedInstanceKeepAliveMap,
        host_builder: impl RuntimeHostBuilder,
        stream_builder: B,
        store: Arc<S>,
        eth_adapter: Arc<dyn EthereumAdapter>,
        manifest: SubgraphManifest,
        registry: Arc<M>,
    ) -> Result<(), Error>
    where
        B: BlockStreamBuilder,
        S: Store + ChainStore + SubgraphDeploymentStore + EthereumCallCache,
        M: MetricsRegistry,
    {
        // Clear the 'failed' state of the subgraph. We were told explicitly
        // to start, which implies we assume the subgraph has not failed (yet)
        // If we can't even clear the 'failed' flag, don't try to start
        // the subgraph.
        let status_ops = SubgraphDeploymentEntity::update_failed_operations(&manifest.id, false);
        store.start_subgraph_deployment(&manifest.id, status_ops)?;

        let mut templates: Vec<DataSourceTemplate> = vec![];
        for data_source in manifest.data_sources.iter() {
            for template in data_source.templates.iter() {
                templates.push(template.clone());
            }
        }

        // Clone the deployment ID for later
        let deployment_id = manifest.id.clone();
        let network_name = manifest.network_name()?;

        // Obtain filters from the manifest
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

        let top_level_templates = manifest.templates.clone();

        // Create a subgraph instance from the manifest; this moves
        // ownership of the manifest and host builder into the new instance
        let stopwatch_metrics =
            StopwatchMetrics::new(logger.clone(), deployment_id.clone(), registry.clone());
        let subgraph_metrics = Arc::new(SubgraphInstanceMetrics::new(
            registry.clone(),
            deployment_id.clone().to_string(),
        ));
        let subgraph_metrics_unregister = subgraph_metrics.clone();
        let host_metrics = Arc::new(HostMetrics::new(
            registry.clone(),
            deployment_id.clone().to_string(),
            stopwatch_metrics.clone(),
        ));
        let ethrpc_metrics = Arc::new(SubgraphEthRpcMetrics::new(
            registry.clone(),
            deployment_id.to_string(),
        ));
        let block_stream_metrics = Arc::new(BlockStreamMetrics::new(
            registry.clone(),
            ethrpc_metrics.clone(),
            deployment_id.clone(),
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
                templates_use_calls,
                top_level_templates,
            },
            state: IndexingState {
                logger,
                instance,
                instances,
                log_filter,
                call_filter,
                block_filter,
                restarts: 0,
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
        let subgraph_runner =
            graph::util::futures::blocking(loop_fn(ctx, move |ctx| run_subgraph(ctx))).then(
                move |res| {
                    subgraph_metrics_unregister.unregister(registry);
                    future::result(res)
                },
            );
        tokio::spawn(subgraph_runner);

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

fn run_subgraph<B, T, S>(
    ctx: IndexingContext<B, T, S>,
) -> impl Future<Item = Loop<(), IndexingContext<B, T, S>>, Error = ()>
where
    B: BlockStreamBuilder,
    T: RuntimeHostBuilder,
    S: ChainStore + Store + EthereumCallCache + SubgraphDeploymentStore,
{
    let logger = ctx.state.logger.clone();

    debug!(logger, "Starting or restarting subgraph");

    // Clone a few things for different parts of the async processing
    let id_for_err = ctx.inputs.deployment_id.clone();
    let store_for_err = ctx.inputs.store.clone();
    let logger_for_err = logger.clone();
    let logger_for_block_stream_errors = logger.clone();

    let block_stream_canceler = CancelGuard::new();
    let block_stream_cancel_handle = block_stream_canceler.handle();
    let block_stream = ctx
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
            ctx.inputs.templates_use_calls,
            ctx.block_stream_metrics.clone(),
        )
        .from_err()
        .cancelable(&block_stream_canceler, || CancelableError::Cancel);

    // Keep the stream's cancel guard around to be able to shut it down
    // when the subgraph deployment is unassigned
    ctx.state
        .instances
        .write()
        .unwrap()
        .insert(ctx.inputs.deployment_id.clone(), block_stream_canceler);

    debug!(logger, "Starting block stream");

    // The processing stream may be end due to an error or for restarting to
    // account for new data sources.
    enum StreamEnd<B: BlockStreamBuilder, T: RuntimeHostBuilder, S> {
        Error(CancelableError<Error>),
        NeedsRestart(IndexingContext<B, T, S>),
    }

    block_stream
        // Log and drop the errors from the block_stream
        // The block stream will continue attempting to produce blocks
        .then(move |result| match result {
            Ok(block) => Ok(Some(block)),
            Err(e) => {
                debug!(
                    logger_for_block_stream_errors,
                    "Block stream produced a non-fatal error";
                    "error" => format!("{}", e),
                );
                Ok(None)
            }
        })
        .filter_map(|block_opt| block_opt)
        // Process blocks from the stream as long as no restart is needed
        .fold(ctx, move |ctx, block| {
            let subgraph_metrics = ctx.subgraph_metrics.clone();
            let start = Instant::now();
            if block.triggers.len() > 0 {
                subgraph_metrics
                    .block_trigger_count
                    .observe(block.triggers.len() as f64);
            }
            process_block(
                logger.clone(),
                ctx.inputs.eth_adapter.clone(),
                ctx,
                block_stream_cancel_handle.clone(),
                block,
            )
            .map_err(|e| StreamEnd::Error(e))
            .and_then(|(ctx, needs_restart)| match needs_restart {
                false => Ok(ctx),
                true => Err(StreamEnd::NeedsRestart(ctx)),
            })
            .then(move |res| {
                let elapsed = start.elapsed().as_secs_f64();
                subgraph_metrics.block_processing_duration.observe(elapsed);
                res
            })
        })
        .then(move |res| match res {
            Ok(_) => unreachable!("block stream finished without error"),
            Err(StreamEnd::NeedsRestart(mut ctx)) => {
                // Increase the restart counter
                ctx.state.restarts += 1;

                // Cancel the stream for real
                ctx.state
                    .instances
                    .write()
                    .unwrap()
                    .remove(&ctx.inputs.deployment_id);

                // And restart the subgraph
                Ok(Loop::Continue(ctx))
            }

            Err(StreamEnd::Error(CancelableError::Cancel)) => {
                debug!(
                    logger_for_err,
                    "Subgraph block stream shut down cleanly";
                    "id" => id_for_err.to_string(),
                );
                Err(())
            }

            // Handle unexpected stream errors by marking the subgraph as failed.
            Err(StreamEnd::Error(CancelableError::Error(e))) => {
                error!(
                    logger_for_err,
                    "Subgraph instance failed to run: {}", e;
                    "id" => id_for_err.to_string(),
                    "code" => LogCode::SubgraphSyncingFailure
                );

                // Set subgraph status to Failed
                let status_ops =
                    SubgraphDeploymentEntity::update_failed_operations(&id_for_err, true);
                if let Err(e) = store_for_err.apply_metadata_operations(status_ops) {
                    error!(
                        logger_for_err,
                        "Failed to set subgraph status to Failed: {}", e;
                        "id" => id_for_err.to_string(),
                        "code" => LogCode::SubgraphSyncingFailureNotRecorded
                    );
                }
                Err(())
            }
        })
}

/// Processes a block and returns the updated context and a boolean flag indicating
/// whether new dynamic data sources have been added to the subgraph.
fn process_block<B, T: RuntimeHostBuilder, S>(
    logger: Logger,
    eth_adapter: Arc<dyn EthereumAdapter>,
    ctx: IndexingContext<B, T, S>,
    block_stream_cancel_handle: CancelHandle,
    block: EthereumBlockWithTriggers,
) -> impl Future<Item = (IndexingContext<B, T, S>, bool), Error = CancelableError<Error>>
where
    B: BlockStreamBuilder,
    S: ChainStore + Store + EthereumCallCache + SubgraphDeploymentStore,
{
    let triggers = block.triggers;
    let block = block.ethereum_block;

    let block_ptr = EthereumBlockPointer::from(&block);
    let logger = logger.new(o!(
        "block_number" => format!("{:?}", block_ptr.number),
        "block_hash" => format!("{:?}", block_ptr.hash)
    ));
    let logger1 = logger.clone();

    if triggers.len() == 1 {
        info!(logger, "1 trigger found in this block for this subgraph");
    } else if triggers.len() > 1 {
        info!(
            logger,
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
    process_triggers(
        logger.clone(),
        ctx,
        BlockState::default(),
        light_block.clone(),
        triggers,
    )
    .and_then(move |(ctx, block_state)| {
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
        loop_fn(
            (ctx, block_state),
            move |(mut ctx, mut block_state)| -> Box<dyn Future<Item = _, Error = _> + Send> {
                if block_state.created_data_sources.is_empty() {
                    // No new data sources, nothing to do.
                    return Box::new(future::ok(Loop::Break((ctx, block_state))));
                }

                // Instantiate dynamic data sources, removing them from the block state.
                let (data_sources, runtime_hosts) = match create_dynamic_data_sources(
                    logger.clone(),
                    &mut ctx,
                    host_metrics.clone(),
                    block_state.created_data_sources.drain(..),
                ) {
                    Ok(ok) => ok,
                    Err(err) => return Box::new(future::err(err.into())),
                };

                // Reprocess the triggers from this block that match the new data sources
                let logger = logger.clone();
                let logger1 = logger.clone();
                let light_block = light_block.clone();
                Box::new(
                    eth_adapter
                        .clone()
                        .triggers_in_block(
                            logger,
                            ctx.inputs.store.clone(),
                            ctx.ethrpc_metrics.clone(),
                            EthereumLogFilter::from_data_sources(data_sources.iter()),
                            EthereumCallFilter::from_data_sources(data_sources.iter()),
                            EthereumBlockFilter::from_data_sources(data_sources.iter()),
                            block.clone(),
                        )
                        .and_then(move |block_with_triggers| {
                            let triggers = block_with_triggers.triggers;

                            if triggers.len() == 1 {
                                info!(
                                    logger1,
                                    "1 trigger found in this block for the new data sources"
                                );
                            } else if triggers.len() > 1 {
                                info!(
                                    logger1,
                                    "{} triggers found in this block for the new data sources",
                                    triggers.len()
                                );
                            }

                            // Add entity operations for the new data sources to the block state
                            // and add runtimes for the data sources to the subgraph instance.
                            persist_dynamic_data_sources(
                                logger1.clone(),
                                &mut ctx,
                                &mut block_state.entity_cache,
                                data_sources,
                                block_ptr_for_new_data_sources,
                            );

                            let logger = logger1.clone();
                            Box::new(
                                stream::iter_ok(triggers)
                                    .fold(block_state, move |block_state, trigger| {
                                        // Process the triggers in each host in the same order the
                                        // corresponding data sources have been created.
                                        SubgraphInstance::<T>::process_trigger_in_runtime_hosts(
                                            &logger,
                                            runtime_hosts.iter().cloned(),
                                            light_block.clone(),
                                            trigger,
                                            block_state,
                                        )
                                    })
                                    .and_then(|block_state| {
                                        future::ok(Loop::Continue((ctx, block_state)))
                                    }),
                            )
                        }),
                )
            },
        )
        .map(move |(ctx, block_state)| (ctx, block_state, needs_restart))
        .from_err()
    })
    // Apply entity operations and advance the stream
    .and_then(move |(ctx, block_state, needs_restart)| {
        // Avoid writing to store if block stream has been canceled
        if block_stream_cancel_handle.is_canceled() {
            return Err(CancelableError::Cancel);
        }

        let section = ctx.host_metrics.stopwatch.start_section("as_modifications");
        let mods = block_state
            .entity_cache
            .as_modifications(ctx.inputs.store.as_ref())
            .map_err(|e| {
                CancelableError::from(format_err!(
                    "Error while processing block stream for a subgraph: {}",
                    e
                ))
            })?;
        section.end();

        if !mods.is_empty() {
            info!(logger1, "Applying {} entity operation(s)", mods.len());
        }

        // Transact entity operations into the store and update the
        // subgraph's block stream pointer
        let _section = ctx.host_metrics.stopwatch.start_section("transact_block");
        let subgraph_id = ctx.inputs.deployment_id.clone();
        let stopwatch = ctx.host_metrics.stopwatch.clone();
        let start = Instant::now();
        ctx.inputs
            .store
            .transact_block_operations(subgraph_id, block_ptr_after, mods, stopwatch)
            .map(|should_migrate| {
                let elapsed = start.elapsed().as_secs_f64();
                metrics.block_ops_transaction_duration.observe(elapsed);
                if should_migrate {
                    ctx.inputs.store.migrate_subgraph_deployment(
                        &logger1,
                        &ctx.inputs.deployment_id,
                        &block_ptr_after,
                    );
                }
                (ctx, needs_restart)
            })
            .map_err(|e| {
                format_err!("Error while processing block stream for a subgraph: {}", e).into()
            })
    })
}

fn process_triggers<B, T: RuntimeHostBuilder, S>(
    logger: Logger,
    ctx: IndexingContext<B, T, S>,
    block_state: BlockState,
    block: Arc<LightEthereumBlock>,
    triggers: Vec<EthereumTrigger>,
) -> impl Future<Item = (IndexingContext<B, T, S>, BlockState), Error = CancelableError<Error>>
where
    B: BlockStreamBuilder,
{
    stream::iter_ok::<_, CancelableError<Error>>(triggers)
        // Process events from the block stream
        .fold((ctx, block_state), move |(ctx, block_state), trigger| {
            let logger = logger.clone();
            let block = block.clone();
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
            ctx.state
                .instance
                .process_trigger(&logger, block, trigger, block_state)
                .map(move |block_state| {
                    let elapsed = start.elapsed().as_secs_f64();
                    subgraph_metrics.observe_trigger_processing_duration(elapsed, trigger_type);
                    (ctx, block_state)
                })
                .map_err(move |e| match transaction_id {
                    Some(tx_hash) => format_err!(
                        "Failed to process trigger in transaction {}: {}",
                        tx_hash,
                        e
                    ),
                    None => format_err!("Failed to process trigger: {}", e),
                })
        })
}

fn create_dynamic_data_sources<B, T: RuntimeHostBuilder, S>(
    logger: Logger,
    ctx: &mut IndexingContext<B, T, S>,
    host_metrics: Arc<HostMetrics>,
    created_data_sources: impl Iterator<Item = DataSourceTemplateInfo>,
) -> Result<(Vec<DataSource>, Vec<Arc<T::Host>>), Error>
where
    B: BlockStreamBuilder,
    S: ChainStore + Store + SubgraphDeploymentStore + EthereumCallCache,
{
    let mut data_sources = vec![];
    let mut runtime_hosts = vec![];

    for info in created_data_sources {
        // Try to instantiate a data source from the template
        let data_source = DataSource::try_from_template(info.template, &info.params)?;
        let host_metrics = host_metrics.clone();

        // Try to create a runtime host for the data source
        let host = ctx.state.instance.add_dynamic_data_source(
            &logger,
            data_source.clone(),
            ctx.inputs.top_level_templates.clone(),
            host_metrics,
        )?;

        data_sources.push(data_source);
        runtime_hosts.push(host);
    }

    Ok((data_sources, runtime_hosts))
}

fn persist_dynamic_data_sources<B, T: RuntimeHostBuilder, S>(
    logger: Logger,
    ctx: &mut IndexingContext<B, T, S>,
    entity_cache: &mut EntityCache,
    data_sources: Vec<DataSource>,
    block_ptr: EthereumBlockPointer,
) where
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
