use futures::future::{loop_fn, Loop};
use futures::sync::mpsc::{channel, Receiver, Sender};
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;
use std::time::Duration;
use uuid::Uuid;

use graph::data::subgraph::schema::{
    DynamicEthereumContractDataSourceEntity, SubgraphDeploymentEntity,
};
use graph::prelude::{SubgraphInstance as SubgraphInstanceTrait, *};
use graph::util::extend::Extend;

use super::SubgraphInstance;
use crate::elastic_logger;
use crate::split_logger;
use crate::ElasticDrainConfig;
use crate::ElasticLoggingConfig;

type SharedInstanceKeepAliveMap = Arc<RwLock<HashMap<SubgraphDeploymentId, CancelGuard>>>;

struct IndexingInputs<B, S, T> {
    pub deployment_id: SubgraphDeploymentId,
    pub store: Arc<S>,
    pub stream_builder: B,
    pub host_builder: T,
    pub templates: Vec<(String, DataSourceTemplate)>,
    pub include_calls_in_blocks: bool,
}

struct IndexingState<T>
where
    T: RuntimeHostBuilder,
{
    pub logger: Logger,
    pub instance: SubgraphInstance<T>,
    pub instances: SharedInstanceKeepAliveMap,
    pub log_filter: Option<EthereumLogFilter>,
    pub call_filter: Option<EthereumCallFilter>,
    pub block_filter: Option<EthereumBlockFilter>,
    pub restarts: u64,
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
        logger: &Logger,
        store: Arc<S>,
        host_builder: T,
        block_stream_builder: B,
        elastic_config: Option<ElasticLoggingConfig>,
    ) -> Self
    where
        S: Store + ChainStore,
        T: RuntimeHostBuilder,
        B: BlockStreamBuilder + 'static,
    {
        let logger = logger.new(o!("component" => "SubgraphInstanceManager"));

        // Create channel for receiving subgraph provider events.
        let (subgraph_sender, subgraph_receiver) = channel(100);

        // Handle incoming events from the subgraph provider.
        Self::handle_subgraph_events(
            logger.clone(),
            subgraph_receiver,
            store,
            host_builder,
            block_stream_builder,
            elastic_config,
        );

        SubgraphInstanceManager {
            logger,
            input: subgraph_sender,
        }
    }

    /// Handle incoming events from subgraph providers.
    fn handle_subgraph_events<B, S, T>(
        logger: Logger,
        receiver: Receiver<SubgraphAssignmentProviderEvent>,
        store: Arc<S>,
        host_builder: T,
        block_stream_builder: B,
        elastic_config: Option<ElasticLoggingConfig>,
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
                    // Write subgraph logs to the terminal and, if enabled, Elasticsearch
                    let term_logger = logger.new(o!("subgraph_id" => manifest.id.to_string()));
                    let logger = elastic_config
                        .clone()
                        .map(|elastic_config| {
                            split_logger(
                                term_logger.clone(),
                                elastic_logger(
                                    ElasticDrainConfig {
                                        general: elastic_config,
                                        index: String::from("subgraph-logs"),
                                        document_type: String::from("log"),
                                        subgraph_id: manifest.id.clone(),
                                        flush_interval: Duration::from_secs(5),
                                    },
                                    term_logger.clone(),
                                ),
                            )
                        })
                        .unwrap_or(term_logger);

                    info!(
                        logger,
                        "Start subgraph";
                        "data_sources" => manifest.data_sources.len()
                    );

                    Self::start_subgraph(
                        logger.clone(),
                        instances.clone(),
                        host_builder.clone(),
                        block_stream_builder.clone(),
                        store.clone(),
                        manifest,
                    )
                    .map_err(|err| error!(logger, "Failed to start subgraph: {}", err))
                    .ok();
                }
                SubgraphStop(id) => {
                    info!(logger, "Stopping subgraph"; "subgraph_id" => id.to_string());
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
        store.apply_entity_operations(status_ops, EventSource::None)?;

        // Create copies of the data source templates; this creates a vector of
        // the form
        // ```
        // vec![
        //   ("DataSource1", <template struct>),
        //   ("DataSource2", <template struct>),
        //   ("DataSource2", <template struct>),
        // ]
        // ```
        // for easy filtering later
        let mut templates: Vec<(String, DataSourceTemplate)> = vec![];
        for data_source in manifest.data_sources.iter() {
            for template in data_source.templates.iter().flatten() {
                templates.push((data_source.name.clone(), template.clone()));
            }
        }

        // Clone the deployment ID for later
        let deployment_id = manifest.id.clone();

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
            .find(|(_, template)| {
                template.has_call_handler() || template.has_block_handler_with_call_filter()
            })
            .is_some();

        // Create a subgraph instance from the manifest; this moves
        // ownership of the manifest and host builder into the new instance
        let instance = SubgraphInstance::from_manifest(&logger, manifest, &host_builder)?;

        // The subgraph state tracks the state of the subgraph instance over time
        let ctx = IndexingContext {
            inputs: IndexingInputs {
                deployment_id,
                templates,
                store,
                stream_builder,
                host_builder,
                include_calls_in_blocks,
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
        tokio::spawn(loop_fn(ctx, move |ctx| run_subgraph(ctx)));

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
    ) -> Box<Sink<SinkItem = SubgraphAssignmentProviderEvent, SinkError = ()> + Send> {
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
    // Log restarts as "updates" here to not freak people out thinking that
    // those restarts could be crashes or similar; they are deliberate
    let logger = ctx.state.logger.new(o!("updates" => ctx.state.restarts));

    debug!(logger, "Starting or restarting subgraph");

    // Clone a few things for different parts of the async processing
    let id_for_err = ctx.inputs.deployment_id.clone();
    let store_for_err = ctx.inputs.store.clone();
    let logger_for_err = logger.clone();
    let logger_for_restart_check = logger.clone();

    let block_stream_canceler = CancelGuard::new();
    let block_stream_cancel_handle = block_stream_canceler.handle();
    let block_stream = ctx
        .inputs
        .stream_builder
        .build(
            logger.clone(),
            ctx.inputs.deployment_id.clone(),
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

    // Flag that indicates when the subgraph needs to be
    // restarted due to new dynamic data sources being added
    let needs_restart_set = Arc::new(AtomicBool::new(false));

    // Handle to the flag for checking it before every block
    let needs_restart_check = needs_restart_set.clone();

    block_stream
        // Take blocks from the stream as long as no dynamic data sources
        // have been created. Once that has happened, we need to restart
        // the stream to include blocks for these data sources as well.
        .take_while(move |_| {
            if needs_restart_check.load(Ordering::SeqCst) {
                debug!(
                    logger_for_restart_check,
                    "New data sources added, restart the subgraph",
                );
                Ok(false)
            } else {
                Ok(true)
            }
        })
        // Process blocks from the stream as long as no restart is needed
        .fold(ctx, move |ctx, block| {
            let needs_restart_set = needs_restart_set.clone();

            process_block(
                logger.clone(),
                ctx,
                block_stream_cancel_handle.clone(),
                block,
            )
            .map(move |(ctx, needs_restart)| {
                // If new data sources were detected in this block, set the
                // needs restart flag. This will cause `take_while` to stop
                // emitting blocks and finish this `fold` loop as well
                if needs_restart {
                    needs_restart_set.store(true, Ordering::SeqCst);
                }
                ctx
            })
        })
        // A restart is needed
        .and_then(move |mut ctx| {
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
        })
        // Handle unexpected stream errors by marking the subgraph as failed
        .map_err(move |e| match e {
            CancelableError::Cancel => {
                debug!(
                    logger_for_err,
                    "Subgraph block stream shut down cleanly";
                    "id" => id_for_err.to_string()
                );
            }
            CancelableError::Error(e) => {
                error!(
                    logger_for_err,
                    "Subgraph instance failed to run: {}", e;
                    "id" => id_for_err.to_string()
                );

                // Set subgraph status to Failed
                let status_ops =
                    SubgraphDeploymentEntity::update_failed_operations(&id_for_err, true);
                if let Err(e) = store_for_err.apply_entity_operations(status_ops, EventSource::None)
                {
                    error!(
                        logger_for_err,
                        "Failed to set subgraph status to Failed: {}", e;
                        "id" => id_for_err.to_string()
                    );
                }
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
            .map(move |block_state| (ctx, block_state, data_sources, runtime_hosts))
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
            .map(|_| (ctx, needs_restart))
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
            // Find the template for this data source
            let template = match state
                .ctx
                .inputs
                .templates
                .iter()
                .find(|(data_source_name, template)| {
                    data_source_name == &info.data_source && template.name == info.template
                })
                .map(|(_, template)| template)
                .ok_or_else(|| {
                    format_err!(
                        "Failed to create data source with name `{}`. \
                         No template with this name in parent data \
                         source `{}`.",
                        info.template,
                        info.data_source
                    )
                }) {
                Ok(template) => template,
                Err(e) => return future::err(e),
            };

            // Try to instantiate a data source from the template
            let data_source = match DataSource::try_from_template(&template, &info.params) {
                Ok(data_source) => data_source,
                Err(e) => return future::err(e),
            };

            // Try to create a runtime host for the data source
            let host = match state.ctx.inputs.host_builder.build(
                &logger,
                state.ctx.inputs.deployment_id.clone(),
                data_source.clone(),
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
