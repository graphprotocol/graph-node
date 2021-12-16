use super::loader::load_dynamic_data_sources;
use super::SubgraphInstance;
use atomic_refcell::AtomicRefCell;
use fail::fail_point;
use graph::blockchain::{BlockchainKind, DataSource};
use graph::data::store::scalar::Bytes;
use graph::data::subgraph::{UnifiedMappingApiVersion, MAX_SPEC_VERSION};
use graph::prelude::TryStreamExt;
use graph::prelude::{SubgraphInstanceManager as SubgraphInstanceManagerTrait, *};
use graph::util::{backoff::ExponentialBackoff, lfu_cache::LfuCache};
use graph::{blockchain::block_stream::BlockStreamMetrics, components::store::WritableStore};
use graph::{blockchain::block_stream::BlockWithTriggers, data::subgraph::SubgraphFeature};
use graph::{
    blockchain::NodeCapabilities,
    blockchain::TriggersAdapter,
    data::subgraph::schema::{SubgraphError, SubgraphHealth, POI_OBJECT},
};
use graph::{
    blockchain::{block_stream::BlockStreamEvent, Blockchain, TriggerFilter as _},
    components::subgraph::{CausalityRegion, MappingError, ProofOfIndexing, SharedProofOfIndexing},
};
use graph::{
    blockchain::{Block, BlockchainMap},
    components::store::{DeploymentId, DeploymentLocator, ModificationsAndCache},
};
use lazy_static::lazy_static;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::task;

const MINUTE: Duration = Duration::from_secs(60);

lazy_static! {
    /// Size limit of the entity LFU cache, in bytes.
    // Multiplied by 1000 because the env var is in KB.
    pub static ref ENTITY_CACHE_SIZE: usize = 1000
        * std::env::var("GRAPH_ENTITY_CACHE_SIZE")
            .unwrap_or("10000".into())
            .parse::<usize>()
            .expect("invalid GRAPH_ENTITY_CACHE_SIZE");

    // Keep deterministic errors non-fatal even if the subgraph is pending.
    // Used for testing Graph Node itself.
    pub static ref DISABLE_FAIL_FAST: bool =
        std::env::var("GRAPH_DISABLE_FAIL_FAST").is_ok();

    /// Ceiling for the backoff retry of non-deterministic errors, in seconds.
    pub static ref SUBGRAPH_ERROR_RETRY_CEIL_SECS: Duration =
        std::env::var("GRAPH_SUBGRAPH_ERROR_RETRY_CEIL_SECS")
            .unwrap_or((MINUTE * 30).as_secs().to_string())
            .parse::<u64>()
            .map(Duration::from_secs)
            .expect("invalid GRAPH_SUBGRAPH_ERROR_RETRY_CEIL_SECS");
}

type SharedInstanceKeepAliveMap = Arc<RwLock<HashMap<DeploymentId, CancelGuard>>>;

struct IndexingInputs<C: Blockchain> {
    deployment: DeploymentLocator,
    features: BTreeSet<SubgraphFeature>,
    start_blocks: Vec<BlockNumber>,
    store: Arc<dyn WritableStore>,
    triggers_adapter: Arc<C::TriggersAdapter>,
    chain: Arc<C>,
    templates: Arc<Vec<C::DataSourceTemplate>>,
    unified_api_version: UnifiedMappingApiVersion,
}

struct IndexingState<T: RuntimeHostBuilder<C>, C: Blockchain> {
    logger: Logger,
    instance: SubgraphInstance<C, T>,
    instances: SharedInstanceKeepAliveMap,
    filter: C::TriggerFilter,
    entity_lfu_cache: LfuCache<EntityKey, Option<Entity>>,
}

struct IndexingContext<T: RuntimeHostBuilder<C>, C: Blockchain> {
    /// Read only inputs that are needed while indexing a subgraph.
    pub inputs: IndexingInputs<C>,

    /// Mutable state that may be modified while indexing a subgraph.
    pub state: IndexingState<T, C>,

    /// Sensors to measure the execution of the subgraph instance
    pub subgraph_metrics: Arc<SubgraphInstanceMetrics>,

    /// Sensors to measure the execution of the subgraph's runtime hosts
    pub host_metrics: Arc<HostMetrics>,

    pub block_stream_metrics: Arc<BlockStreamMetrics>,
}

pub struct SubgraphInstanceManager<S, M, L> {
    logger_factory: LoggerFactory,
    subgraph_store: Arc<S>,
    chains: Arc<BlockchainMap>,
    metrics_registry: Arc<M>,
    manager_metrics: SubgraphInstanceManagerMetrics,
    instances: SharedInstanceKeepAliveMap,
    link_resolver: Arc<L>,
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

struct SubgraphInstanceMetrics {
    pub block_trigger_count: Box<Histogram>,
    pub block_processing_duration: Box<Histogram>,
    pub block_ops_transaction_duration: Box<Histogram>,

    trigger_processing_duration: Box<Histogram>,
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
            .new_deployment_histogram(
                "deployment_trigger_processing_duration",
                "Measures duration of trigger processing for a subgraph deployment",
                subgraph_hash,
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

    pub fn observe_trigger_processing_duration(&self, duration: f64) {
        self.trigger_processing_duration.observe(duration);
    }

    pub fn unregister<M: MetricsRegistry>(&self, registry: Arc<M>) {
        registry.unregister(self.block_processing_duration.clone());
        registry.unregister(self.block_trigger_count.clone());
        registry.unregister(self.trigger_processing_duration.clone());
        registry.unregister(self.block_ops_transaction_duration.clone());
    }
}

#[async_trait]
impl<S, M, L> SubgraphInstanceManagerTrait for SubgraphInstanceManager<S, M, L>
where
    S: SubgraphStore,
    M: MetricsRegistry,
    L: LinkResolver + Clone,
{
    async fn start_subgraph(
        self: Arc<Self>,
        loc: DeploymentLocator,
        manifest: serde_yaml::Mapping,
    ) {
        let logger = self.logger_factory.subgraph_logger(&loc);
        let err_logger = logger.clone();
        let instance_manager = self.cheap_clone();

        let subgraph_start_future = async move {
            match BlockchainKind::from_manifest(&manifest)? {
                BlockchainKind::Ethereum => {
                    instance_manager
                        .start_subgraph_inner::<graph_chain_ethereum::Chain>(logger, loc, manifest)
                        .await
                }

                BlockchainKind::Near => {
                    instance_manager
                        .start_subgraph_inner::<graph_chain_near::Chain>(logger, loc, manifest)
                        .await
                }
            }
        };
        // Perform the actual work of starting the subgraph in a separate
        // task. If the subgraph is a graft or a copy, starting it will
        // perform the actual work of grafting/copying, which can take
        // hours. Running it in the background makes sure the instance
        // manager does not hang because of that work.
        graph::spawn(async move {
            match subgraph_start_future.await {
                Ok(()) => self.manager_metrics.subgraph_count.inc(),
                Err(err) => error!(
                    err_logger,
                    "Failed to start subgraph";
                    "error" => format!("{}", err),
                    "code" => LogCode::SubgraphStartFailure
                ),
            }
        });
    }

    fn stop_subgraph(&self, loc: DeploymentLocator) {
        let logger = self.logger_factory.subgraph_logger(&loc);
        info!(logger, "Stop subgraph");

        // Drop the cancel guard to shut down the subgraph now
        let mut instances = self.instances.write().unwrap();
        instances.remove(&loc.id);

        self.manager_metrics.subgraph_count.dec();
    }
}

impl<S, M, L> SubgraphInstanceManager<S, M, L>
where
    S: SubgraphStore,
    M: MetricsRegistry,
    L: LinkResolver + Clone,
{
    pub fn new(
        logger_factory: &LoggerFactory,
        subgraph_store: Arc<S>,
        chains: Arc<BlockchainMap>,
        metrics_registry: Arc<M>,
        link_resolver: Arc<L>,
    ) -> Self {
        let logger = logger_factory.component_logger("SubgraphInstanceManager", None);
        let logger_factory = logger_factory.with_parent(logger.clone());

        SubgraphInstanceManager {
            logger_factory,
            subgraph_store,
            chains,
            manager_metrics: SubgraphInstanceManagerMetrics::new(metrics_registry.cheap_clone()),
            metrics_registry,
            instances: SharedInstanceKeepAliveMap::default(),
            link_resolver,
        }
    }

    async fn start_subgraph_inner<C: Blockchain>(
        self: Arc<Self>,
        logger: Logger,
        deployment: DeploymentLocator,
        manifest: serde_yaml::Mapping,
    ) -> Result<(), Error> {
        let subgraph_store = self.subgraph_store.cheap_clone();
        let registry = self.metrics_registry.cheap_clone();
        let store = self
            .subgraph_store
            .cheap_clone()
            .writable(logger.clone(), deployment.id)
            .await?;

        // Start the subgraph deployment before reading dynamic data
        // sources; if the subgraph is a graft or a copy, starting it will
        // do the copying and dynamic data sources won't show up until after
        // that is done
        {
            let store = store.clone();
            let logger = logger.clone();

            // `start_subgraph_deployment` is blocking.
            task::spawn_blocking(move || {
                store
                    .start_subgraph_deployment(&logger)
                    .map_err(Error::from)
            })
            .await
            .map_err(Error::from)
            .and_then(|x| x)?;
        }

        let manifest: SubgraphManifest<C> = {
            info!(logger, "Resolve subgraph files using IPFS");

            let mut manifest = SubgraphManifest::resolve_from_raw(
                deployment.hash.cheap_clone(),
                manifest,
                // Allow for infinite retries for subgraph definition files.
                &self.link_resolver.as_ref().clone().with_retries(),
                &logger,
                MAX_SPEC_VERSION.clone(),
            )
            .await
            .context("Failed to resolve subgraph from IPFS")?;

            let data_sources = load_dynamic_data_sources::<C>(
                store.clone(),
                logger.clone(),
                manifest.templates.clone(),
            )
            .await
            .context("Failed to load dynamic data sources")?;

            info!(logger, "Successfully resolved subgraph files using IPFS");

            // Add dynamic data sources to the subgraph
            manifest.data_sources.extend(data_sources);

            info!(
                logger,
                "Data source count at start: {}",
                manifest.data_sources.len()
            );

            manifest
        };

        let required_capabilities = C::NodeCapabilities::from_data_sources(&manifest.data_sources);
        let network = manifest.network_name();

        let chain = self
            .chains
            .get::<C>(network.clone())
            .with_context(|| format!("no chain configured for network {}", network))?
            .clone();

        // Obtain filters from the manifest
        let filter = C::TriggerFilter::from_data_sources(manifest.data_sources.iter());
        let start_blocks = manifest.start_blocks();

        let templates = Arc::new(manifest.templates.clone());

        // Create a subgraph instance from the manifest; this moves
        // ownership of the manifest and host builder into the new instance
        let stopwatch_metrics = StopwatchMetrics::new(
            logger.clone(),
            deployment.hash.clone(),
            self.metrics_registry.clone(),
        );

        let unified_mapping_api_version = manifest.unified_mapping_api_version()?;
        let triggers_adapter = chain.triggers_adapter(&deployment, &required_capabilities, unified_mapping_api_version ,stopwatch_metrics.clone()).map_err(|e|
                anyhow!(
                "expected triggers adapter that matches deployment {} with required capabilities: {}: {}",
                &deployment,
                &required_capabilities, e))?.clone();

        let subgraph_metrics = Arc::new(SubgraphInstanceMetrics::new(
            registry.cheap_clone(),
            deployment.hash.as_str(),
        ));
        let subgraph_metrics_unregister = subgraph_metrics.clone();
        let host_metrics = Arc::new(HostMetrics::new(
            registry.cheap_clone(),
            deployment.hash.as_str(),
            stopwatch_metrics.clone(),
        ));
        let block_stream_metrics = Arc::new(BlockStreamMetrics::new(
            registry.cheap_clone(),
            &deployment.hash,
            manifest.network_name(),
            store.shard().to_string(),
            stopwatch_metrics,
        ));

        // Initialize deployment_head with current deployment head. Any sort of trouble in
        // getting the deployment head ptr leads to initializing with 0
        let deployment_head = store
            .block_ptr()
            .ok()
            .and_then(|ptr| ptr.map(|ptr| ptr.number))
            .unwrap_or(0) as f64;
        block_stream_metrics.deployment_head.set(deployment_head);

        let host_builder = graph_runtime_wasm::RuntimeHostBuilder::new(
            chain.runtime_adapter(),
            self.link_resolver.cheap_clone(),
            subgraph_store,
        );

        let features = manifest.features.clone();
        let unified_api_version = manifest.unified_mapping_api_version()?;
        let instance =
            SubgraphInstance::from_manifest(&logger, manifest, host_builder, host_metrics.clone())?;

        // The subgraph state tracks the state of the subgraph instance over time
        let ctx = IndexingContext {
            inputs: IndexingInputs {
                deployment: deployment.clone(),
                features,
                start_blocks,
                store,
                triggers_adapter,
                chain,
                templates,
                unified_api_version,
            },
            state: IndexingState {
                logger: logger.cheap_clone(),
                instance,
                instances: self.instances.cheap_clone(),
                filter,
                entity_lfu_cache: LfuCache::new(),
            },
            subgraph_metrics,
            host_metrics,
            block_stream_metrics,
        };

        // Keep restarting the subgraph until it terminates. The subgraph
        // will usually only run once, but is restarted whenever a block
        // creates dynamic data sources. This allows us to recreate the
        // block stream and include events for the new data sources going
        // forward; this is easier than updating the existing block stream.
        //
        // This is a long-running and unfortunately a blocking future (see #905), so it is run in
        // its own thread. It is also run with `task::unconstrained` because we have seen deadlocks
        // occur without it, possibly caused by our use of legacy futures and tokio versions in the
        // codebase and dependencies, which may not play well with the tokio 1.0 cooperative
        // scheduling. It is also logical in terms of performance to run this with `unconstrained`,
        // it has a dedicated OS thread so the OS will handle the preemption. See
        // https://github.com/tokio-rs/tokio/issues/3493.
        graph::spawn_thread(deployment.to_string(), move || {
            if let Err(e) = graph::block_on(task::unconstrained(run_subgraph(ctx))) {
                error!(
                    &logger,
                    "Subgraph instance failed to run: {}",
                    format!("{:#}", e)
                );
            }
            subgraph_metrics_unregister.unregister(registry);
        });

        Ok(())
    }
}

async fn run_subgraph<T, C>(mut ctx: IndexingContext<T, C>) -> Result<(), Error>
where
    T: RuntimeHostBuilder<C>,
    C: Blockchain,
{
    // Clone a few things for different parts of the async processing
    let subgraph_metrics = ctx.subgraph_metrics.cheap_clone();
    let store_for_err = ctx.inputs.store.cheap_clone();
    let logger = ctx.state.logger.cheap_clone();
    let id_for_err = ctx.inputs.deployment.hash.clone();
    let mut should_try_unfail_deterministic = true;
    let mut should_try_unfail_non_deterministic = true;

    // Exponential backoff that starts with two minutes and keeps
    // increasing its timeout exponentially until it reaches the ceiling.
    let mut backoff = ExponentialBackoff::new(MINUTE * 2, *SUBGRAPH_ERROR_RETRY_CEIL_SECS);

    loop {
        debug!(logger, "Starting or restarting subgraph");

        let block_stream_canceler = CancelGuard::new();
        let block_stream_cancel_handle = block_stream_canceler.handle();
        let mut block_stream = ctx
            .inputs
            .chain
            .new_block_stream(
                ctx.inputs.deployment.clone(),
                ctx.inputs.start_blocks.clone(),
                Arc::new(ctx.state.filter.clone()),
                ctx.block_stream_metrics.clone(),
                ctx.inputs.unified_api_version.clone(),
            )
            .await?
            .map_err(CancelableError::Error)
            .cancelable(&block_stream_canceler, || Err(CancelableError::Cancel));

        // Keep the stream's cancel guard around to be able to shut it down
        // when the subgraph deployment is unassigned
        ctx.state
            .instances
            .write()
            .unwrap()
            .insert(ctx.inputs.deployment.id, block_stream_canceler);

        debug!(logger, "Starting block stream");

        // Process events from the stream as long as no restart is needed
        loop {
            let (block, cursor) = match block_stream.next().await {
                Some(Ok(BlockStreamEvent::ProcessBlock(block, cursor))) => (block, cursor),
                Some(Ok(BlockStreamEvent::Revert(subgraph_ptr, _, optional_parent_ptr))) => {
                    info!(
                        logger,
                        "Reverting block to get back to main chain";
                        "block_number" => format!("{}", subgraph_ptr.number),
                        "block_hash" => format!("{}", subgraph_ptr.hash)
                    );

                    // We would like to revert the DB state to the parent of the current block.
                    match optional_parent_ptr {
                        Some(parent_ptr) => {
                            if let Err(e) = ctx.inputs.store.revert_block_operations(parent_ptr) {
                                error!(
                                    &logger,
                                    "Could not revert block. Retrying";
                                    "block_number" => format!("{}", subgraph_ptr.number),
                                    "block_hash" => format!("{}", subgraph_ptr.hash),
                                    "error" => e.to_string(),
                                );
                                continue;
                            }
                        }
                        None => {
                            // First, load the block in order to get the parent hash.
                            if let Err(e) = ctx
                                .inputs
                                .triggers_adapter
                                .parent_ptr(&subgraph_ptr)
                                .await
                                .map(|parent_ptr| {
                                    parent_ptr.expect("genesis block cannot be reverted")
                                })
                                .and_then(|parent_ptr| {
                                    // Revert entity changes from this block, and update subgraph ptr.
                                    ctx.inputs
                                        .store
                                        .revert_block_operations(parent_ptr)
                                        .map_err(Into::into)
                                })
                            {
                                error!(
                                    &logger,
                                    "Could not revert block. \
                                    The likely cause is the block not being found due to a deep reorg. \
                                    Retrying";
                                    "block_number" => format!("{}", subgraph_ptr.number),
                                    "block_hash" => format!("{}", subgraph_ptr.hash),
                                    "error" => e.to_string(),
                                );
                                continue;
                            }
                        }
                    }

                    ctx.block_stream_metrics
                        .reverted_blocks
                        .set(subgraph_ptr.number as f64);

                    // Revert the in-memory state:
                    // - Remove hosts for reverted dynamic data sources.
                    // - Clear the entity cache.
                    //
                    // Note that we do not currently revert the filters, which means the filters
                    // will be broader than necessary. This is not ideal for performance, but is not
                    // incorrect since we will discard triggers that match the filters but do not
                    // match any data sources.
                    ctx.state.instance.revert_data_sources(subgraph_ptr.number);
                    ctx.state.entity_lfu_cache = LfuCache::new();
                    continue;
                }
                // Log and drop the errors from the block_stream
                // The block stream will continue attempting to produce blocks
                Some(Err(e)) => {
                    if block_stream_cancel_handle.is_canceled() {
                        debug!(&logger, "Subgraph block stream shut down cleanly");
                        return Ok(());
                    }

                    debug!(
                        &logger,
                        "Block stream produced a non-fatal error";
                        "error" => format!("{}", e),
                    );
                    continue;
                }
                None => unreachable!("The block stream stopped producing blocks"),
            };

            let block_ptr = block.ptr();

            if block.trigger_count() > 0 {
                subgraph_metrics
                    .block_trigger_count
                    .observe(block.trigger_count() as f64);
            }

            let start = Instant::now();
            let deployment_failed = ctx.block_stream_metrics.deployment_failed.clone();

            // If a subgraph failed for deterministic reasons, before processing a new block, we
            // revert the deployment head. It should lead to the same result since the error was
            // deterministic.
            //
            // As an optimization we check this only on the first run.
            if should_try_unfail_deterministic {
                should_try_unfail_deterministic = false;

                if let Some(current_ptr) = ctx.inputs.store.block_ptr()? {
                    if let Some(parent_ptr) =
                        ctx.inputs.triggers_adapter.parent_ptr(&current_ptr).await?
                    {
                        // This reverts the deployment head to the parent_ptr if
                        // deterministic errors happened.
                        //
                        // There's no point in calling it if we have no current or parent block
                        // pointers, because there would be: no block to revert to or to search
                        // errors from (first execution).
                        ctx.inputs
                            .store
                            .unfail_deterministic_error(&current_ptr, &parent_ptr)?;
                    }
                }
            }

            let res = process_block(
                &logger,
                ctx.inputs.triggers_adapter.cheap_clone(),
                &mut ctx,
                block_stream_cancel_handle.clone(),
                block,
                cursor.into(),
            )
            .await;

            let elapsed = start.elapsed().as_secs_f64();
            subgraph_metrics.block_processing_duration.observe(elapsed);

            match res {
                Ok(needs_restart) => {
                    // Keep trying to unfail subgraph for everytime it advances block(s) until it's
                    // health is not Failed anymore.
                    if should_try_unfail_non_deterministic {
                        // If the deployment head advanced, we can unfail
                        // the non-deterministic error (if there's any).
                        ctx.inputs
                            .store
                            .unfail_non_deterministic_error(&block_ptr)?;

                        match ctx.inputs.store.health(&ctx.inputs.deployment.hash).await? {
                            SubgraphHealth::Failed => {
                                // If the unfail call didn't change the subgraph health, we keep
                                // `should_try_unfail_non_deterministic` as `true` until it's
                                // actually unfailed.
                            }
                            SubgraphHealth::Healthy | SubgraphHealth::Unhealthy => {
                                // Stop trying to unfail.
                                should_try_unfail_non_deterministic = false;
                                deployment_failed.set(0.0);
                                backoff.reset();
                            }
                        };
                    }

                    if needs_restart {
                        // Cancel the stream for real
                        ctx.state
                            .instances
                            .write()
                            .unwrap()
                            .remove(&ctx.inputs.deployment.id);

                        // And restart the subgraph
                        break;
                    }
                }
                Err(BlockProcessingError::Canceled) => {
                    debug!(&logger, "Subgraph block stream shut down cleanly");
                    return Ok(());
                }

                // Handle unexpected stream errors by marking the subgraph as failed.
                Err(e) => {
                    deployment_failed.set(1.0);

                    let message = format!("{:#}", e).replace("\n", "\t");
                    let err = anyhow!("{}, code: {}", message, LogCode::SubgraphSyncingFailure);
                    let deterministic = e.is_deterministic();

                    let error = SubgraphError {
                        subgraph_id: id_for_err.clone(),
                        message,
                        block_ptr: Some(block_ptr),
                        handler: None,
                        deterministic,
                    };

                    match deterministic {
                        true => {
                            // Fail subgraph:
                            // - Change status/health.
                            // - Save the error to the database.
                            store_for_err
                                .fail_subgraph(error)
                                .await
                                .context("Failed to set subgraph status to `failed`")?;

                            return Err(err);
                        }
                        false => {
                            // Shouldn't fail subgraph if it's already failed for non-deterministic
                            // reasons.
                            //
                            // If we don't do this check we would keep adding the same error to the
                            // database.
                            let should_fail_subgraph =
                                ctx.inputs.store.health(&ctx.inputs.deployment.hash).await?
                                    != SubgraphHealth::Failed;

                            if should_fail_subgraph {
                                // Fail subgraph:
                                // - Change status/health.
                                // - Save the error to the database.
                                store_for_err
                                    .fail_subgraph(error)
                                    .await
                                    .context("Failed to set subgraph status to `failed`")?;
                            }

                            // Retry logic below:

                            // Cancel the stream for real.
                            ctx.state
                                .instances
                                .write()
                                .unwrap()
                                .remove(&ctx.inputs.deployment.id);

                            error!(logger, "Subgraph failed for non-deterministic error: {}", e;
                                "attempt" => backoff.attempt,
                                "retry_delay_s" => backoff.delay().as_secs());

                            // Sleep before restarting.
                            backoff.sleep_async().await;

                            should_try_unfail_non_deterministic = true;

                            // And restart the subgraph.
                            break;
                        }
                    }
                }
            }
        }
    }
}

#[derive(thiserror::Error, Debug)]
enum BlockProcessingError {
    #[error("{0:#}")]
    Unknown(Error),

    // The error had a deterministic cause but, for a possibly non-deterministic reason, we chose to
    // halt processing due to the error.
    #[error("{0}")]
    Deterministic(SubgraphError),

    #[error("subgraph stopped while processing triggers")]
    Canceled,
}

impl BlockProcessingError {
    fn is_deterministic(&self) -> bool {
        matches!(self, BlockProcessingError::Deterministic(_))
    }
}

impl From<Error> for BlockProcessingError {
    fn from(e: Error) -> Self {
        BlockProcessingError::Unknown(e)
    }
}

impl From<StoreError> for BlockProcessingError {
    fn from(e: StoreError) -> Self {
        BlockProcessingError::Unknown(e.into())
    }
}

/// Processes a block and returns the updated context and a boolean flag indicating
/// whether new dynamic data sources have been added to the subgraph.
async fn process_block<T: RuntimeHostBuilder<C>, C: Blockchain>(
    logger: &Logger,
    triggers_adapter: Arc<C::TriggersAdapter>,
    ctx: &mut IndexingContext<T, C>,
    block_stream_cancel_handle: CancelHandle,
    block: BlockWithTriggers<C>,
    firehose_cursor: Option<String>,
) -> Result<bool, BlockProcessingError> {
    let triggers = block.trigger_data;
    let block = Arc::new(block.block);
    let block_ptr = block.ptr();

    let logger = logger.new(o!(
        "block_number" => format!("{:?}", block_ptr.number),
        "block_hash" => format!("{}", block_ptr.hash)
    ));

    if triggers.len() == 1 {
        debug!(&logger, "1 candidate trigger in this block");
    } else if triggers.len() > 1 {
        debug!(
            &logger,
            "{} candidate triggers in this block",
            triggers.len()
        );
    }

    let metrics = ctx.subgraph_metrics.clone();

    let proof_of_indexing = if ctx
        .inputs
        .store
        .clone()
        .supports_proof_of_indexing()
        .await?
    {
        Some(Arc::new(AtomicRefCell::new(ProofOfIndexing::new(
            block_ptr.number,
        ))))
    } else {
        None
    };

    // There are currently no other causality regions since offchain data is not supported.
    let causality_region = CausalityRegion::from_network(ctx.state.instance.network());

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
        &block,
        triggers,
        &causality_region,
    )
    .await
    {
        // Triggers processed with no errors or with only determinstic errors.
        Ok(block_state) => block_state,

        // Some form of unknown or non-deterministic error ocurred.
        Err(MappingError::Unknown(e)) => return Err(BlockProcessingError::Unknown(e)),
        Err(MappingError::PossibleReorg(e)) => {
            info!(logger,
                    "Possible reorg detected, retrying";
                    "error" => format!("{:#}", e),
            );

            // In case of a possible reorg, we want this function to do nothing and restart the
            // block stream so it has a chance to detect the reorg.
            //
            // The `ctx` is unchanged at this point, except for having cleared the entity cache.
            // Losing the cache is a bit annoying but not an issue for correctness.
            //
            // See also b21fa73b-6453-4340-99fb-1a78ec62efb1.
            return Ok(true);
        }
    };

    // If new data sources have been created, restart the subgraph after this block.
    // This is necessary to re-create the block stream.
    let needs_restart = block_state.has_created_data_sources();
    let host_metrics = ctx.host_metrics.clone();

    // This loop will:
    // 1. Instantiate created data sources.
    // 2. Process those data sources for the current block.
    // Until no data sources are created or MAX_DATA_SOURCES is hit.

    // Note that this algorithm processes data sources spawned on the same block _breadth
    // first_ on the tree implied by the parent-child relationship between data sources. Only a
    // very contrived subgraph would be able to observe this.
    while block_state.has_created_data_sources() {
        // Instantiate dynamic data sources, removing them from the block state.
        let (data_sources, runtime_hosts) = create_dynamic_data_sources(
            logger.clone(),
            ctx,
            host_metrics.clone(),
            block_state.drain_created_data_sources(),
        )?;

        let filter = C::TriggerFilter::from_data_sources(data_sources.iter());

        // Reprocess the triggers from this block that match the new data sources
        let block_with_triggers = triggers_adapter
            .triggers_in_block(&logger, block.as_ref().clone(), &filter)
            .await?;

        let triggers = block_with_triggers.trigger_data;

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
            ctx,
            &mut block_state.entity_cache,
            data_sources,
        );

        // Process the triggers in each host in the same order the
        // corresponding data sources have been created.
        for trigger in triggers {
            block_state = SubgraphInstance::<C, T>::process_trigger_in_runtime_hosts(
                &logger,
                &runtime_hosts,
                &block,
                &trigger,
                block_state,
                proof_of_indexing.cheap_clone(),
                &causality_region,
            )
            .await
            .map_err(|e| {
                // This treats a `PossibleReorg` as an ordinary error which will fail the subgraph.
                // This can cause an unnecessary subgraph failure, to fix it we need to figure out a
                // way to revert the effect of `create_dynamic_data_sources` so we may return a
                // clean context as in b21fa73b-6453-4340-99fb-1a78ec62efb1.
                match e {
                    MappingError::PossibleReorg(e) | MappingError::Unknown(e) => {
                        BlockProcessingError::Unknown(e)
                    }
                }
            })?;
        }
    }

    let has_errors = block_state.has_errors();
    let is_non_fatal_errors_active = ctx
        .inputs
        .features
        .contains(&SubgraphFeature::NonFatalErrors);

    // Apply entity operations and advance the stream

    // Avoid writing to store if block stream has been canceled
    if block_stream_cancel_handle.is_canceled() {
        return Err(BlockProcessingError::Canceled);
    }

    if let Some(proof_of_indexing) = proof_of_indexing {
        let proof_of_indexing = Arc::try_unwrap(proof_of_indexing).unwrap().into_inner();
        update_proof_of_indexing(
            proof_of_indexing,
            &ctx.host_metrics.stopwatch,
            &ctx.inputs.deployment.hash,
            &mut block_state.entity_cache,
        )
        .await?;
    }

    let section = ctx.host_metrics.stopwatch.start_section("as_modifications");
    let ModificationsAndCache {
        modifications: mut mods,
        data_sources,
        entity_lfu_cache: mut cache,
    } = block_state
        .entity_cache
        .as_modifications()
        .map_err(|e| BlockProcessingError::Unknown(e.into()))?;
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

    let err_count = block_state.deterministic_errors.len();
    for (i, e) in block_state.deterministic_errors.iter().enumerate() {
        let message = format!("{:#}", e).replace("\n", "\t");
        error!(&logger, "Subgraph error {}/{}", i + 1, err_count;
            "error" => message,
            "code" => LogCode::SubgraphSyncingFailure
        );
    }

    // Transact entity operations into the store and update the
    // subgraph's block stream pointer
    let _section = ctx.host_metrics.stopwatch.start_section("transact_block");
    let stopwatch = ctx.host_metrics.stopwatch.clone();
    let start = Instant::now();

    let store = &ctx.inputs.store;

    // If a deterministic error has happened, make the PoI to be the only entity that'll be stored.
    if has_errors && !is_non_fatal_errors_active {
        let is_poi_entity = |entity_mod: &EntityModification| {
            entity_mod.entity_key().entity_type.as_str() == "Poi$"
        };
        mods.retain(is_poi_entity);
        // Confidence check
        assert!(
            mods.len() == 1,
            "There should be only one PoI EntityModification"
        );
    }

    let BlockState {
        deterministic_errors,
        ..
    } = block_state;

    let first_error = deterministic_errors.first().cloned();

    match store.transact_block_operations(
        block_ptr,
        firehose_cursor,
        mods,
        stopwatch,
        data_sources,
        deterministic_errors,
    ) {
        Ok(_) => {
            // For subgraphs with `nonFatalErrors` feature disabled, we consider
            // any error as fatal.
            //
            // So we do an early return to make the subgraph stop processing blocks.
            //
            // In this scenario the only entity that is stored/transacted is the PoI,
            // all of the others are discarded.
            if has_errors && !is_non_fatal_errors_active {
                // Only the first error is reported.
                return Err(BlockProcessingError::Deterministic(first_error.unwrap()));
            }

            let elapsed = start.elapsed().as_secs_f64();
            metrics.block_ops_transaction_duration.observe(elapsed);

            // To prevent a buggy pending version from replacing a current version, if errors are
            // present the subgraph will be unassigned.
            if has_errors && !*DISABLE_FAIL_FAST && !store.is_deployment_synced().await? {
                store
                    .unassign_subgraph()
                    .map_err(|e| BlockProcessingError::Unknown(e.into()))?;

                // Use `Canceled` to avoiding setting the subgraph health to failed, an error was
                // just transacted so it will be already be set to unhealthy.
                return Err(BlockProcessingError::Canceled);
            }

            Ok(needs_restart)
        }

        Err(e) => Err(anyhow!("Error while processing block stream for a subgraph: {}", e).into()),
    }
}

/// Transform the proof of indexing changes into entity updates that will be
/// inserted when as_modifications is called.
async fn update_proof_of_indexing(
    proof_of_indexing: ProofOfIndexing,
    stopwatch: &StopwatchMetrics,
    deployment_id: &DeploymentHash,
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

        entity_cache.set(entity_key, new_poi_entity);
    }

    Ok(())
}

async fn process_triggers<C: Blockchain>(
    logger: &Logger,
    mut block_state: BlockState<C>,
    proof_of_indexing: SharedProofOfIndexing,
    subgraph_metrics: Arc<SubgraphInstanceMetrics>,
    instance: &SubgraphInstance<C, impl RuntimeHostBuilder<C>>,
    block: &Arc<C::Block>,
    triggers: Vec<C::TriggerData>,
    causality_region: &str,
) -> Result<BlockState<C>, MappingError> {
    use graph::blockchain::TriggerData;

    for trigger in triggers.into_iter() {
        let start = Instant::now();
        block_state = instance
            .process_trigger(
                &logger,
                block,
                &trigger,
                block_state,
                proof_of_indexing.cheap_clone(),
                causality_region,
            )
            .await
            .map_err(move |mut e| {
                let error_context = trigger.error_context();
                if !error_context.is_empty() {
                    e = e.context(error_context);
                }
                e.context("failed to process trigger".to_string())
            })?;
        let elapsed = start.elapsed().as_secs_f64();
        subgraph_metrics.observe_trigger_processing_duration(elapsed);
    }
    Ok(block_state)
}

fn create_dynamic_data_sources<T: RuntimeHostBuilder<C>, C: Blockchain>(
    logger: Logger,
    ctx: &mut IndexingContext<T, C>,
    host_metrics: Arc<HostMetrics>,
    created_data_sources: Vec<DataSourceTemplateInfo<C>>,
) -> Result<(Vec<C::DataSource>, Vec<Arc<T::Host>>), Error> {
    let mut data_sources = vec![];
    let mut runtime_hosts = vec![];

    for info in created_data_sources {
        // Try to instantiate a data source from the template
        let data_source = C::DataSource::try_from(info)?;

        // Try to create a runtime host for the data source
        let host = ctx.state.instance.add_dynamic_data_source(
            &logger,
            data_source.clone(),
            ctx.inputs.templates.clone(),
            host_metrics.clone(),
        )?;

        match host {
            Some(host) => {
                data_sources.push(data_source);
                runtime_hosts.push(host);
            }
            None => {
                fail_point!("error_on_duplicate_ds", |_| Err(anyhow!("duplicate ds")));
                warn!(
                    logger,
                    "no runtime hosted created, there is already a runtime host instantiated for \
                     this data source";
                    "name" => &data_source.name(),
                    "address" => &data_source.address()
                        .map(|address| hex::encode(address))
                        .unwrap_or("none".to_string()),
                )
            }
        }
    }

    Ok((data_sources, runtime_hosts))
}

fn persist_dynamic_data_sources<T: RuntimeHostBuilder<C>, C: Blockchain>(
    logger: Logger,
    ctx: &mut IndexingContext<T, C>,
    entity_cache: &mut EntityCache,
    data_sources: Vec<C::DataSource>,
) {
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
            "name" => &data_source.name(),
            "address" => &data_source.address().map(|address| hex::encode(address)).unwrap_or("none".to_string()),
        );
        entity_cache.add_data_source(data_source);
    }

    // Merge filters from data sources into the block stream builder
    ctx.state.filter.extend(data_sources.iter());
}
