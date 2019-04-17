use futures::future::{loop_fn, Loop};
use futures::sync::mpsc::{channel, Receiver, Sender};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;
use std::time::Duration;
use uuid::Uuid;

use graph::data::subgraph::schema::{
    DynamicEthereumContractDataSourceEntity, SubgraphDeploymentEntity,
};
use graph::prelude::{SubgraphInstance as SubgraphInstanceTrait, *};
use graph::web3::types::Log;

use super::SubgraphInstance;
use crate::elastic_logger;
use crate::split_logger;
use crate::ElasticDrainConfig;
use crate::ElasticLoggingConfig;

struct SubgraphState<B, S, T>
where
    B: BlockStreamBuilder,
    S: Store + ChainStore,
    T: RuntimeHostBuilder,
{
    pub deployment_id: SubgraphDeploymentId,
    pub instance: Arc<RwLock<SubgraphInstance<T>>>,
    pub logger: Logger,
    pub templates: Vec<(String, DataSourceTemplate)>,
    pub store: Arc<S>,
    pub stream_builder: B,
    pub host_builder: T,
    pub instances: SharedInstanceKeepAliveMap,
    pub log_filter: EthereumLogFilter,
    pub restarts: u64,
}

type SharedInstanceKeepAliveMap = Arc<RwLock<HashMap<SubgraphDeploymentId, CancelGuard>>>;

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
        //   ("DataSource1", "Template1", <template struct>),
        //   ("DataSource2", "Template1", <template struct>),
        //   ("DataSource2", "Template2", <template struct>),
        // ]
        // ```
        // for easy filtering later
        let mut templates: Vec<(String, DataSourceTemplate)> = vec![];
        for data_source in manifest.data_sources.iter() {
            for template in data_source.templates.iter().flatten() {
                templates.push((data_source.name.clone(), template.expensive_clone()));
            }
        }

        // Clone the deployment ID for later
        let deployment_id = manifest.id.clone();

        // Obtain a log filter from the manifest
        let log_filter = EthereumLogFilter::from_iter(manifest.data_sources.iter());

        // Create a subgraph instance from the manifest; this moves
        // ownership of the manifest and host builder into the new instance
        let instance = Arc::new(RwLock::new(SubgraphInstance::from_manifest(
            &logger,
            manifest,
            &host_builder,
        )?));

        // The subgraph state tracks the state of the subgraph instance over time
        let subgraph_state = SubgraphState {
            logger,
            deployment_id,
            instance,
            templates,
            store,
            stream_builder,
            host_builder,
            instances,
            log_filter,
            restarts: 0,
        };

        // Keep restarting the subgraph until it terminates. The subgraph
        // will usually only run once, but is restarted whenever a block
        // creates dynamic data sources. This allows us to recreate the
        // block stream and include events for the new data sources going
        // forward; this is easier than updating the existing block stream.
        tokio::spawn(loop_fn(subgraph_state, move |subgraph_state| {
            run_subgraph(subgraph_state)
        }));

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
    subgraph_state: SubgraphState<B, S, T>,
) -> impl Future<Item = Loop<(), SubgraphState<B, S, T>>, Error = ()>
where
    B: BlockStreamBuilder,
    S: Store + ChainStore,
    T: RuntimeHostBuilder,
{
    let logger = subgraph_state
        .logger
        .new(o!("restarts" => subgraph_state.restarts));

    debug!(logger, "Starting or restarting subgraph");

    // Clone a few things for different parts of the async processing
    let id_for_err = subgraph_state.deployment_id.clone();
    let store_for_err = subgraph_state.store.clone();
    let logger_for_err = logger.clone();
    let logger_for_restart_check = logger.clone();

    let block_stream_canceler = CancelGuard::new();
    let block_stream_cancel_handle = block_stream_canceler.handle();
    let block_stream = subgraph_state
        .stream_builder
        .build(
            logger.clone(),
            subgraph_state.deployment_id.clone(),
            subgraph_state.log_filter.clone(),
        )
        .from_err()
        .cancelable(&block_stream_canceler, || CancelableError::Cancel);

    // Keep the stream's cancel guard around to be able to shut it down
    // when the subgraph deployment is unassigned
    subgraph_state
        .instances
        .write()
        .unwrap()
        .insert(subgraph_state.deployment_id.clone(), block_stream_canceler);

    // Flag that indicates when the subgraph needs to be
    // restarted due to new dynamic data sources being added
    let needs_restart = Arc::new(AtomicBool::new(false));
    let needs_restart_read = needs_restart.clone();

    debug!(logger, "Starting block stream");

    block_stream
        // Take blocks from the stream as long as no dynamic data sources
        // have been created. Once that has happened, we need to restart
        // the stream to include blocks for these data sources as well.
        .take_while(move |_| {
            if needs_restart_read.load(Ordering::SeqCst) {
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
        .fold(subgraph_state, move |subgraph_state, block| {
            process_block(
                logger.clone(),
                subgraph_state,
                block_stream_cancel_handle.clone(),
                needs_restart.clone(),
                block,
            )
        })
        // A restart is needed
        .and_then(move |mut subgraph_state| {
            subgraph_state.restarts += 1;

            // Cancel the stream for real
            subgraph_state
                .instances
                .write()
                .unwrap()
                .remove(&subgraph_state.deployment_id);

            // And restart the subgraph
            Ok(Loop::Continue(subgraph_state))
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

fn process_block<B, S, T>(
    logger: Logger,
    subgraph_state: SubgraphState<B, S, T>,
    block_stream_cancel_handle: CancelHandle,
    needs_restart: Arc<AtomicBool>,
    block: EthereumBlock,
) -> impl Future<Item = SubgraphState<B, S, T>, Error = CancelableError<Error>>
where
    B: BlockStreamBuilder,
    S: ChainStore + Store,
    T: RuntimeHostBuilder,
{
    let logger = logger.new(o!(
        "block_number" => format!("{:?}", block.block.number.unwrap()),
        "block_hash" => format!("{:?}", block.block.hash.unwrap())
    ));

    // Extract logs relevant to the subgraph
    let instance = subgraph_state.instance.clone();
    let logs: Vec<_> = block
        .transaction_receipts
        .iter()
        .flat_map(|receipt| {
            receipt
                .logs
                .iter()
                .filter(|log| instance.read().unwrap().matches_log(&log))
        })
        .cloned()
        .collect();

    if logs.len() == 0 {
        debug!(logger, "No events found in this block for this subgraph");
    } else if logs.len() == 1 {
        info!(logger, "1 event found in this block for this subgraph");
    } else {
        info!(
            logger,
            "{} events found in this block for this subgraph",
            logs.len()
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
    process_logs(
        logger.clone(),
        subgraph_state.instance.clone(),
        BlockState::default(),
        block.clone(),
        logs,
    )
    .and_then(|block_state| {
        // Instantiate dynamic data sources
        create_dynamic_data_sources(logger1, subgraph_state, block_state).from_err()
    })
    .and_then(
        move |(subgraph_state, block_state, data_sources, runtime_hosts)| {
            // Reprocess the logs that only match the new data sources
            //
            // Note: Once we support other triggers as well, we may have to
            // re-request the current block from the block stream's adapter
            // and process that instead of the block we already have here.

            // Extract logs relevant to the new runtime hosts
            let logs: Vec<_> = if runtime_hosts.is_empty() {
                vec![]
            } else {
                block
                    .transaction_receipts
                    .iter()
                    .flat_map(|receipt| {
                        receipt
                            .logs
                            .iter()
                            .filter(|log| runtime_hosts.iter().any(|host| host.matches_log(&log)))
                    })
                    .cloned()
                    .collect()
            };

            if logs.len() == 0 {
                debug!(
                    logger,
                    "No events found in this block for the new data sources"
                );
            } else if logs.len() == 1 {
                info!(
                    logger,
                    "1 event found in this block for the new data sources"
                );
            } else {
                info!(
                    logger,
                    "{} events found in this block for the new data sources",
                    logs.len()
                );
            }

            process_logs_in_runtime_hosts(
                logger2.clone(),
                runtime_hosts.clone(),
                block_state,
                block.clone(),
                logs,
            )
            .map(move |block_state| (subgraph_state, block_state, data_sources, runtime_hosts))
        },
    )
    .and_then(
        move |(subgraph_state, block_state, data_sources, runtime_hosts)| {
            // Add entity operations for the new data sources to the block state
            // and add runtimes for the data sources to the subgraph instance
            persist_dynamic_data_sources(
                logger3,
                subgraph_state,
                block_state,
                data_sources,
                runtime_hosts,
                block_ptr_for_new_data_sources,
            )
            .map(move |(subgraph_state, block_state, data_sources_created)| {
                // If new data sources have been added, indicate that the subgraph
                // needs to be restarted after this block
                if data_sources_created {
                    needs_restart.swap(true, Ordering::SeqCst);
                }

                (subgraph_state, block_state)
            })
            .from_err()
        },
    )
    // Apply entity operations and advance the stream
    .and_then(move |(subgraph_state, block_state)| {
        // Avoid writing to store if block stream has been canceled
        if block_stream_cancel_handle.is_canceled() {
            return Err(CancelableError::Cancel);
        }

        info!(
            logger4,
            "Applying {} entity operation(s)",
            block_state.entity_operations.len()
        );

        // Transact entity operations into the store and update the
        // subgraph's block stream pointer
        subgraph_state
            .store
            .transact_block_operations(
                subgraph_state.deployment_id.clone(),
                block_ptr_now,
                block_ptr_after,
                block_state.entity_operations,
            )
            .map(|_| subgraph_state)
            .map_err(|e| {
                format_err!("Error while processing block stream for a subgraph: {}", e).into()
            })
    })
}

fn process_logs<T>(
    logger: Logger,
    instance: Arc<RwLock<SubgraphInstance<T>>>,
    block_state: BlockState,
    block: Arc<EthereumBlock>,
    logs: Vec<Log>,
) -> impl Future<Item = BlockState, Error = CancelableError<Error>>
where
    T: RuntimeHostBuilder,
{
    stream::iter_ok::<_, CancelableError<Error>>(logs)
        // Process events from the block stream
        .fold(block_state, move |block_state, log| {
            let logger = logger.clone();
            let instance = instance.clone();
            let block = block.clone();

            let transaction = block
                .transaction_for_log(&log)
                .map(Arc::new)
                .ok_or_else(|| format_err!("Found no transaction for event"));

            future::result(transaction).and_then(move |transaction| {
                instance
                    .read()
                    .unwrap()
                    .process_log(&logger, block, transaction, log, block_state)
                    .map(|block_state| block_state)
                    .map_err(|e| format_err!("Failed to process event: {}", e))
            })
        })
}

fn process_logs_in_runtime_hosts<R>(
    logger: Logger,
    runtime_hosts: Vec<Arc<R>>,
    block_state: BlockState,
    block: Arc<EthereumBlock>,
    logs: Vec<Log>,
) -> impl Future<Item = BlockState, Error = CancelableError<Error>>
where
    R: RuntimeHost + 'static,
{
    stream::iter_ok::<_, CancelableError<Error>>(logs)
        // Process events from the block stream
        .fold(block_state, move |block_state, log| {
            let logger = logger.clone();
            let block = block.clone();
            let runtime_hosts = runtime_hosts.clone();

            let transaction = block
                .transaction_for_log(&log)
                .map(Arc::new)
                .ok_or_else(|| format_err!("Found no transaction for event"));

            future::result(transaction).and_then(move |transaction| {
                // Identify runtime hosts that will handle this event
                let matching_hosts: Vec<_> = runtime_hosts
                    .iter()
                    .filter(|host| host.matches_log(&log))
                    .cloned()
                    .collect();

                let log = Arc::new(log);

                // Process the log in each host in the same order the corresponding
                // data sources have been created
                Box::new(stream::iter_ok(matching_hosts).fold(
                    block_state,
                    move |block_state, host| {
                        host.process_log(
                            logger.clone(),
                            block.clone(),
                            transaction.clone(),
                            log.clone(),
                            block_state,
                        )
                    },
                ))
            })
        })
}

fn create_dynamic_data_sources<B, S, T>(
    logger: Logger,
    subgraph_state: SubgraphState<B, S, T>,
    block_state: BlockState,
) -> impl Future<
    Item = (
        SubgraphState<B, S, T>,
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
        subgraph_state: SubgraphState<B, S, T>,
        block_state: BlockState,
        data_sources: Vec<DataSource>,
        runtime_hosts: Vec<Arc<T::Host>>,
    };

    let initial_state = State {
        subgraph_state,
        block_state,
        data_sources: vec![],
        runtime_hosts: vec![],
    };

    stream::iter_ok(initial_state.block_state.created_data_sources.clone())
        .fold(initial_state, move |mut state, info| {
            // Find the template for this data source
            let template = match state
                .subgraph_state
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
            let host = match state.subgraph_state.host_builder.build(
                &logger,
                state.subgraph_state.deployment_id.clone(),
                data_source.expensive_clone(),
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
                final_state.subgraph_state,
                final_state.block_state,
                final_state.data_sources,
                final_state.runtime_hosts,
            )
        })
}

fn persist_dynamic_data_sources<B, S, T>(
    logger: Logger,
    mut subgraph_state: SubgraphState<B, S, T>,
    mut block_state: BlockState,
    data_sources: Vec<DataSource>,
    runtime_hosts: Vec<Arc<T::Host>>,
    block_ptr: EthereumBlockPointer,
) -> impl Future<Item = (SubgraphState<B, S, T>, BlockState, bool), Error = Error>
where
    B: BlockStreamBuilder,
    S: ChainStore + Store,
    T: RuntimeHostBuilder,
{
    debug!(
        logger,
        "Creating {} dynamic data source(s)",
        data_sources.len()
    );

    // If there are any new dynamic data sources, we'll have to restart
    // the subgraph after this block
    let needs_restart = !data_sources.is_empty();

    // Add entity operations to the block state in order to persist
    // the dynamic data sources
    for data_source in data_sources.iter() {
        let entity = DynamicEthereumContractDataSourceEntity::from((
            &subgraph_state.deployment_id,
            data_source,
            &block_ptr,
        ));
        let id = format!("{}-dynamic", Uuid::new_v4().to_simple());
        let operations = entity.write_operations(id.as_ref());
        block_state.entity_operations.extend(operations);
    }

    // Merge log filters from data sources into the block stream builder
    subgraph_state
        .log_filter
        .extend(EthereumLogFilter::from_iter(data_sources.iter()));

    // Add the new data sources to the subgraph instance
    match subgraph_state
        .instance
        .clone()
        .write()
        .unwrap()
        .add_dynamic_data_sources(runtime_hosts)
    {
        Ok(_) => future::ok((subgraph_state, block_state, needs_restart)),
        Err(e) => future::err(e),
    }
}
