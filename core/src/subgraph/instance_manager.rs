use futures::future::{loop_fn, Loop};
use futures::sync::mpsc::{channel, Receiver, Sender};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::RwLock;
use uuid::Uuid;

use graph::data::subgraph::schema::{
    DynamicEthereumContractDataSourceEntity, SubgraphDeploymentEntity,
};
use graph::prelude::{SubgraphInstance as SubgraphInstanceTrait, *};
use graph::util::extend::Extend;

use super::SubgraphInstance;

type SharedInstanceKeepAliveMap = Arc<RwLock<HashMap<SubgraphDeploymentId, CancelGuard>>>;

struct IndexingInputs<B, S, T> {
    deployment_id: SubgraphDeploymentId,
    network_name: String,
    store: Arc<S>,
    stream_builder: B,
    host_builder: T,
    include_calls_in_blocks: bool,
    top_level_templates: Vec<DataSourceTemplate>,
}

struct IndexingState<T>
where
    T: RuntimeHostBuilder,
{
    logger: Logger,
    instance: SubgraphInstance<T>,
    instances: SharedInstanceKeepAliveMap,
    log_filter: Option<EthereumLogFilter>,
    call_filter: Option<EthereumCallFilter>,
    block_filter: Option<EthereumBlockFilter>,
    restarts: u64,
}

struct IndexingContext<B, S, T>
where
    B: BlockStreamBuilder,
    S: Store + ChainStore,
    T: RuntimeHostBuilder,
{
    /// Read only inputs that are needed while indexing a subgraph.
    pub inputs: IndexingInputs<B, S, T>,

    /// Mutable state that may be modified while indexing a subgraph.
    pub state: IndexingState<T>,
}

pub struct SubgraphInstanceManager {
    logger: Logger,
    input: Sender<SubgraphAssignmentProviderEvent>,
}

impl SubgraphInstanceManager {
    /// Creates a new runtime manager.
    pub fn new<B, S, T>(
        logger_factory: &LoggerFactory,
        stores: HashMap<String, Arc<S>>,
        host_builder: T,
        block_stream_builder: B,
    ) -> Self
    where
        S: Store + ChainStore,
        T: RuntimeHostBuilder,
        B: BlockStreamBuilder + 'static,
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
            host_builder,
            block_stream_builder,
        );

        SubgraphInstanceManager {
            logger,
            input: subgraph_sender,
        }
    }

    /// Handle incoming events from subgraph providers.
    fn handle_subgraph_events<B, S, T>(
        logger_factory: LoggerFactory,
        receiver: Receiver<SubgraphAssignmentProviderEvent>,
        stores: HashMap<String, Arc<S>>,
        host_builder: T,
        block_stream_builder: B,
    ) where
        S: Store + ChainStore,
        T: RuntimeHostBuilder,
        B: BlockStreamBuilder + 'static,
    {
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
                                manifest,
                            )
                            .map_err(|err| {
                                error!(
                                    logger,
                                    "Failed to start subgraph";
                                    "error" => format!("{}", err),
                                    "code" => LogCode::SubgraphStartFailure
                                )
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
                }
            };

            Ok(())
        }));
    }

    fn start_subgraph<B, T, S>(
        logger: Logger,
        instances: SharedInstanceKeepAliveMap,
        host_builder: T,
        stream_builder: B,
        store: Arc<S>,
        manifest: SubgraphManifest,
    ) -> Result<(), Error>
    where
        T: RuntimeHostBuilder,
        B: BlockStreamBuilder + 'static,
        S: Store + ChainStore,
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
        let log_filter = EthereumLogFilter::from_data_sources_opt(manifest.data_sources.iter());
        let call_filter = EthereumCallFilter::from_data_sources_opt(manifest.data_sources.iter());
        let block_filter = EthereumBlockFilter::from_data_sources_opt(manifest.data_sources.iter());

        // Identify whether there are templates with call handlers or
        // block handlers with call filters; in this case, we need to
        // include calls in all blocks so we cen reprocess the block
        // when new dynamic data sources are being created
        let include_calls_in_blocks = templates
            .iter()
            .find(|template| {
                template.has_call_handler() || template.has_block_handler_with_call_filter()
            })
            .is_some();

        let top_level_templates = manifest.templates.clone();

        // Create a subgraph instance from the manifest; this moves
        // ownership of the manifest and host builder into the new instance
        let instance = SubgraphInstance::from_manifest(&logger, manifest, &host_builder)?;

        // The subgraph state tracks the state of the subgraph instance over time
        let ctx = IndexingContext {
            inputs: IndexingInputs {
                deployment_id,
                network_name,
                store,
                stream_builder,
                host_builder,
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
                restarts: 0,
            },
        };

        // Keep restarting the subgraph until it terminates. The subgraph
        // will usually only run once, but is restarted whenever a block
        // creates dynamic data sources. This allows us to recreate the
        // block stream and include events for the new data sources going
        // forward; this is easier than updating the existing block stream.
        //
        // This task has many calls to the store, so mark it as `blocking`.
        tokio::spawn(graph::util::futures::blocking(loop_fn(ctx, |ctx| {
            run_subgraph(ctx)
        })));

        Ok(())
    }

    fn stop_subgraph(instances: SharedInstanceKeepAliveMap, id: SubgraphDeploymentId) {
        // Drop the cancel guard to shut down the sujbgraph now
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

fn run_subgraph<B, S, T>(
    ctx: IndexingContext<B, S, T>,
) -> impl Future<Item = Loop<(), IndexingContext<B, S, T>>, Error = ()>
where
    B: BlockStreamBuilder,
    S: Store + ChainStore,
    T: RuntimeHostBuilder,
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
            ctx.state.log_filter.clone(),
            ctx.state.call_filter.clone(),
            ctx.state.block_filter.clone(),
            ctx.inputs.include_calls_in_blocks,
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
    enum StreamEnd<B: BlockStreamBuilder, S: Store + ChainStore, T: RuntimeHostBuilder> {
        Error(CancelableError<Error>),
        NeedsRestart(IndexingContext<B, S, T>),
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
            process_block(
                logger.clone(),
                ctx,
                block_stream_cancel_handle.clone(),
                block,
            )
            .map_err(|e| StreamEnd::Error(e))
            .and_then(|(ctx, needs_restart)| match needs_restart {
                false => Ok(ctx),
                true => Err(StreamEnd::NeedsRestart(ctx)),
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
                if let Err(e) = store_for_err.apply_entity_operations(status_ops, None) {
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
fn process_block<B, S, T>(
    logger: Logger,
    ctx: IndexingContext<B, S, T>,
    block_stream_cancel_handle: CancelHandle,
    block: EthereumBlockWithTriggers,
) -> impl Future<Item = (IndexingContext<B, S, T>, bool), Error = CancelableError<Error>>
where
    B: BlockStreamBuilder,
    S: ChainStore + Store,
    T: RuntimeHostBuilder,
{
    let calls = block.calls;
    let triggers = block.triggers;
    let block = block.ethereum_block;

    let logger = logger.new(o!(
        "block_number" => format!("{:?}", block.block.number.unwrap()),
        "block_hash" => format!("{:?}", block.block.hash.unwrap())
    ));

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
    let block = Arc::new(block);
    let block_ptr_now = EthereumBlockPointer::to_parent(&block);
    let block_ptr_after = EthereumBlockPointer::from(&*block);
    let block_ptr_for_new_data_sources = block_ptr_after.clone();

    // Clone a few things to pass into futures
    let logger1 = logger.clone();
    let logger2 = logger.clone();
    let logger3 = logger.clone();
    let logger4 = logger.clone();

    // Process events one after the other, passing in entity operations
    // collected previously to every new event being processed
    process_triggers(
        logger.clone(),
        ctx,
        BlockState::default(),
        block.clone(),
        triggers,
    )
    .and_then(|(ctx, block_state)| {
        // Instantiate dynamic data sources
        create_dynamic_data_sources(logger1, ctx, block_state).from_err()
    })
    .and_then(move |(ctx, block_state, data_sources, runtime_hosts)| {
        // Reprocess the triggers from this block that match the new data sources

        let created_ds_count = block_state.created_data_sources.len();
        let block_with_calls = EthereumBlockWithCalls {
            ethereum_block: block.deref().clone(),
            calls,
        };

        future::result(<B>::Stream::parse_triggers(
            EthereumLogFilter::from_data_sources_opt(&data_sources),
            EthereumCallFilter::from_data_sources_opt(&data_sources),
            EthereumBlockFilter::from_data_sources_opt(&data_sources),
            false,
            block_with_calls,
        ))
        .from_err()
        .and_then(move |block_with_triggers| {
            let triggers = block_with_triggers.triggers;

            if triggers.len() == 1 {
                info!(
                    logger,
                    "1 trigger found in this block for the new data sources"
                );
            } else if triggers.len() > 1 {
                info!(
                    logger,
                    "{} triggers found in this block for the new data sources",
                    triggers.len()
                );
            }

            process_triggers_in_runtime_hosts::<T>(
                logger2.clone(),
                runtime_hosts.clone(),
                block_state,
                block.clone(),
                triggers,
            )
            .and_then(move |block_state| {
                match block_state.created_data_sources.len() > created_ds_count {
                    false => Ok((ctx, block_state, data_sources, runtime_hosts)),
                    true => Err(err_msg(
                        "A dynamic data source, in the same block that it was created,
                        attempted to create another dynamic data source.
                        To let us know that you are affected by this bug, please comment in
                        https://github.com/graphprotocol/graph-node/issues/1105",
                    )
                    .into()),
                }
            })
        })
    })
    .and_then(move |(ctx, block_state, data_sources, runtime_hosts)| {
        // Add entity operations for the new data sources to the block state
        // and add runtimes for the data sources to the subgraph instance
        future::result(
            persist_dynamic_data_sources(
                logger3,
                ctx,
                block_state,
                data_sources,
                runtime_hosts,
                block_ptr_for_new_data_sources,
            )
            .map(move |(ctx, block_state, data_sources_created)| {
                // If new data sources have been added, indicate that the subgraph
                // needs to be restarted after this block
                (ctx, block_state, data_sources_created)
            }),
        )
        .from_err()
    })
    // Apply entity operations and advance the stream
    .and_then(move |(ctx, block_state, needs_restart)| {
        // Avoid writing to store if block stream has been canceled
        if block_stream_cancel_handle.is_canceled() {
            return Err(CancelableError::Cancel);
        }

        if !block_state.entity_operations.is_empty() {
            info!(
                logger4,
                "Applying {} entity operation(s)",
                block_state.entity_operations.len()
            );
        }

        // Transact entity operations into the store and update the
        // subgraph's block stream pointer
        ctx.inputs
            .store
            .transact_block_operations(
                ctx.inputs.deployment_id.clone(),
                block_ptr_now,
                block_ptr_after,
                block_state.entity_operations,
            )
            .map(|should_migrate| {
                if should_migrate {
                    ctx.inputs.store.migrate_subgraph_deployment(
                        &logger4,
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

fn process_triggers<B, S, T>(
    logger: Logger,
    ctx: IndexingContext<B, S, T>,
    block_state: BlockState,
    block: Arc<EthereumBlock>,
    triggers: Vec<EthereumTrigger>,
) -> impl Future<Item = (IndexingContext<B, S, T>, BlockState), Error = CancelableError<Error>>
where
    B: BlockStreamBuilder,
    S: ChainStore + Store,
    T: RuntimeHostBuilder,
{
    stream::iter_ok::<_, CancelableError<Error>>(triggers)
        // Process events from the block stream
        .fold((ctx, block_state), move |(ctx, block_state), trigger| {
            let logger = logger.clone();
            let block = block.clone();

            ctx.state
                .instance
                .process_trigger(&logger, block, trigger, block_state)
                .map(|block_state| (ctx, block_state))
                .map_err(|e| format_err!("Failed to process trigger: {}", e))
        })
}

fn process_triggers_in_runtime_hosts<T>(
    logger: Logger,
    runtime_hosts: Vec<Arc<T::Host>>,
    block_state: BlockState,
    block: Arc<EthereumBlock>,
    triggers: Vec<EthereumTrigger>,
) -> impl Future<Item = BlockState, Error = CancelableError<Error>>
where
    T: RuntimeHostBuilder,
{
    stream::iter_ok::<_, CancelableError<Error>>(triggers)
        // Process events from the block stream
        .fold(block_state, move |block_state, trigger| {
            let logger = logger.clone();
            let block = block.clone();
            let runtime_hosts = runtime_hosts.clone();

            // Process the log in each host in the same order the corresponding
            // data sources have been created
            SubgraphInstance::<T>::process_trigger_in_runtime_hosts(
                &logger,
                runtime_hosts.iter().map(|host| host.clone()),
                block.clone(),
                trigger,
                block_state,
            )
        })
}

fn create_dynamic_data_sources<B, S, T>(
    logger: Logger,
    ctx: IndexingContext<B, S, T>,
    block_state: BlockState,
) -> impl Future<
    Item = (
        IndexingContext<B, S, T>,
        BlockState,
        Vec<DataSource>,
        Vec<Arc<T::Host>>,
    ),
    Error = Error,
>
where
    B: BlockStreamBuilder,
    S: ChainStore + Store,
    T: RuntimeHostBuilder,
{
    struct State<B, S, T>
    where
        B: BlockStreamBuilder,
        S: ChainStore + Store,
        T: RuntimeHostBuilder,
    {
        ctx: IndexingContext<B, S, T>,
        block_state: BlockState,
        data_sources: Vec<DataSource>,
        runtime_hosts: Vec<Arc<T::Host>>,
    };

    let initial_state = State {
        ctx,
        block_state,
        data_sources: vec![],
        runtime_hosts: vec![],
    };

    stream::iter_ok(initial_state.block_state.created_data_sources.clone())
        .fold(initial_state, move |mut state, info| {
            // Try to instantiate a data source from the template
            let data_source = match DataSource::try_from_template(info.template, &info.params) {
                Ok(data_source) => data_source,
                Err(e) => return future::err(e),
            };

            // Try to create a runtime host for the data source
            let host = match state.ctx.inputs.host_builder.build(
                &logger,
                state.ctx.inputs.network_name.clone(),
                state.ctx.inputs.deployment_id.clone(),
                data_source.clone(),
                state.ctx.inputs.top_level_templates.clone(),
            ) {
                Ok(host) => Arc::new(host),
                Err(e) => return future::err(e),
            };

            state.data_sources.push(data_source);
            state.runtime_hosts.push(host);

            future::ok(state)
        })
        .map(|final_state| {
            (
                final_state.ctx,
                final_state.block_state,
                final_state.data_sources,
                final_state.runtime_hosts,
            )
        })
}

fn persist_dynamic_data_sources<B, S, T>(
    logger: Logger,
    mut ctx: IndexingContext<B, S, T>,
    mut block_state: BlockState,
    data_sources: Vec<DataSource>,
    runtime_hosts: Vec<Arc<T::Host>>,
    block_ptr: EthereumBlockPointer,
) -> Result<(IndexingContext<B, S, T>, BlockState, bool), Error>
where
    B: BlockStreamBuilder,
    S: ChainStore + Store,
    T: RuntimeHostBuilder,
{
    if !data_sources.is_empty() {
        debug!(
            logger,
            "Creating {} dynamic data source(s)",
            data_sources.len()
        );
    }

    // If there are any new dynamic data sources, we'll have to restart
    // the subgraph after this block
    let needs_restart = !data_sources.is_empty();

    // Add entity operations to the block state in order to persist
    // the dynamic data sources
    for data_source in data_sources.iter() {
        let entity = DynamicEthereumContractDataSourceEntity::from((
            &ctx.inputs.deployment_id,
            data_source,
            &block_ptr,
        ));
        let id = format!("{}-dynamic", Uuid::new_v4().to_simple());
        let operations = entity.write_operations(id.as_ref());
        block_state.entity_operations.extend(operations);
    }

    // Merge log filters from data sources into the block stream builder
    if let Some(filter) = EthereumLogFilter::from_data_sources_opt(data_sources.iter()) {
        ctx.state.log_filter = ctx.state.log_filter.extend(filter);
    }

    // Merge call filters from data sources into the block stream builder
    if let Some(filter) = EthereumCallFilter::from_data_sources_opt(data_sources.iter()) {
        ctx.state.call_filter = ctx.state.call_filter.extend(filter);
    }

    // Merge block filters from data sources into the block stream builder
    if let Some(filter) = EthereumBlockFilter::from_data_sources_opt(data_sources.iter()) {
        ctx.state.block_filter = ctx.state.block_filter.extend(filter);
    }

    // Add the new data sources to the subgraph instance
    ctx.state
        .instance
        .add_dynamic_data_sources(runtime_hosts)
        .map(move |_| (ctx, block_state, needs_restart))
}
