use crate::subgraph::context::IndexingContext;
use crate::subgraph::error::{
    ClassifyErrorHelper as _, DetailHelper as _, NonDeterministicErrorHelper as _, ProcessingError,
};
use crate::subgraph::inputs::IndexingInputs;
use crate::subgraph::state::IndexingState;
use crate::subgraph::stream::new_block_stream;
use anyhow::Context as _;
use graph::blockchain::block_stream::{
    BlockStream, BlockStreamError, BlockStreamEvent, BlockWithTriggers, FirehoseCursor,
};
use graph::blockchain::{
    Block, BlockTime, Blockchain, DataSource as _, SubgraphFilter, Trigger, TriggerFilter as _,
    TriggerFilterWrapper,
};
use graph::components::store::{EmptyStore, GetScope, ReadStore, StoredDynamicDataSource};
use graph::components::subgraph::InstanceDSTemplate;
use graph::components::trigger_processor::RunnableTriggers;
use graph::components::{
    store::ModificationsAndCache,
    subgraph::{MappingError, PoICausalityRegion, ProofOfIndexing, SharedProofOfIndexing},
};
use graph::data::store::scalar::Bytes;
use graph::data::subgraph::schema::{SubgraphError, SubgraphHealth};
use graph::data_source::{
    offchain, CausalityRegion, DataSource, DataSourceCreationError, TriggerData,
};
use graph::env::EnvVars;
use graph::ext::futures::Cancelable;
use graph::futures03::stream::StreamExt;
use graph::prelude::{
    anyhow, hex, retry, thiserror, BlockNumber, BlockPtr, BlockState, CancelGuard, CancelHandle,
    CancelToken as _, CancelableError, CheapClone as _, EntityCache, EntityModification, Error,
    InstanceDSTemplateInfo, LogCode, RunnerMetrics, RuntimeHostBuilder, StopwatchMetrics,
    StoreError, StreamExtension, UnfailOutcome, Value, ENV_VARS,
};
use graph::schema::EntityKey;
use graph::slog::{debug, error, info, o, trace, warn, Logger};
use graph::util::lfu_cache::EvictStats;
use graph::util::{backoff::ExponentialBackoff, lfu_cache::LfuCache};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::vec;

const MINUTE: Duration = Duration::from_secs(60);

const SKIP_PTR_UPDATES_THRESHOLD: Duration = Duration::from_secs(60 * 5);
const HANDLE_REVERT_SECTION_NAME: &str = "handle_revert";
const PROCESS_BLOCK_SECTION_NAME: &str = "process_block";
const PROCESS_WASM_BLOCK_SECTION_NAME: &str = "process_wasm_block";
const PROCESS_TRIGGERS_SECTION_NAME: &str = "process_triggers";
const HANDLE_CREATED_DS_SECTION_NAME: &str = "handle_new_data_sources";

pub struct SubgraphRunner<C, T>
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    ctx: IndexingContext<C, T>,
    state: IndexingState,
    inputs: Arc<IndexingInputs<C>>,
    logger: Logger,
    pub metrics: RunnerMetrics,
    cancel_handle: Option<CancelHandle>,
}

#[derive(Debug, thiserror::Error)]
pub enum SubgraphRunnerError {
    #[error("subgraph runner terminated because a newer one was active")]
    Duplicate,

    #[error(transparent)]
    Unknown(#[from] Error),
}

impl<C, T> SubgraphRunner<C, T>
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    pub fn new(
        inputs: IndexingInputs<C>,
        ctx: IndexingContext<C, T>,
        logger: Logger,
        metrics: RunnerMetrics,
        env_vars: Arc<EnvVars>,
    ) -> Self {
        Self {
            inputs: Arc::new(inputs),
            ctx,
            state: IndexingState {
                should_try_unfail_non_deterministic: true,
                skip_ptr_updates_timer: Instant::now(),
                backoff: ExponentialBackoff::with_jitter(
                    (MINUTE * 2).min(env_vars.subgraph_error_retry_ceil),
                    env_vars.subgraph_error_retry_ceil,
                    env_vars.subgraph_error_retry_jitter,
                ),
                entity_lfu_cache: LfuCache::new(),
                cached_head_ptr: None,
            },
            logger,
            metrics,
            cancel_handle: None,
        }
    }

    /// Revert the state to a previous block. When handling revert operations
    /// or failed block processing, it is necessary to remove part of the existing
    /// in-memory state to keep it constent with DB changes.
    /// During block processing new dynamic data sources are added directly to the
    /// IndexingContext of the runner. This means that if, for whatever reason,
    /// the changes don;t complete then the remnants of that block processing must
    /// be removed. The same thing also applies to the block cache.
    /// This function must be called before continuing to process in order to avoid
    /// duplicated host insertion and POI issues with dirty entity changes.
    fn revert_state_to(&mut self, block_number: BlockNumber) -> Result<(), Error> {
        self.state.entity_lfu_cache = LfuCache::new();

        // 1. Revert all hosts(created by DDS) at a block higher than `block_number`.
        // 2. Unmark any offchain data sources that were marked done on the blocks being removed.
        // When no offchain datasources are present, 2. should be a noop.
        self.ctx.revert_data_sources(block_number + 1)?;
        Ok(())
    }

    #[cfg(debug_assertions)]
    pub fn context(&self) -> &IndexingContext<C, T> {
        &self.ctx
    }

    #[cfg(debug_assertions)]
    pub async fn run_for_test(self, break_on_restart: bool) -> Result<Self, Error> {
        self.run_inner(break_on_restart).await.map_err(Into::into)
    }

    fn is_static_filters_enabled(&self) -> bool {
        self.inputs.static_filters || self.ctx.hosts_len() > ENV_VARS.static_filters_threshold
    }

    fn build_filter(&self) -> TriggerFilterWrapper<C> {
        let current_ptr = self.inputs.store.block_ptr();
        let static_filters = self.is_static_filters_enabled();

        // Filter out data sources that have reached their end block
        let end_block_filter = |ds: &&C::DataSource| match current_ptr.as_ref() {
            // We filter out datasources for which the current block is at or past their end block.
            Some(block) => ds.end_block().map_or(true, |end| block.number < end),
            // If there is no current block, we keep all datasources.
            None => true,
        };

        let data_sources = self.ctx.static_data_sources();

        let subgraph_filter = data_sources
            .iter()
            .filter_map(|ds| ds.as_subgraph())
            .map(|ds| SubgraphFilter {
                subgraph: ds.source.address(),
                start_block: ds.source.start_block,
                entities: ds
                    .mapping
                    .handlers
                    .iter()
                    .map(|handler| handler.entity.clone())
                    .collect(),
                manifest_idx: ds.manifest_idx,
            })
            .collect::<Vec<_>>();

        // if static_filters is not enabled we just stick to the filter based on all the data sources.
        if !static_filters {
            return TriggerFilterWrapper::new(
                C::TriggerFilter::from_data_sources(
                    self.ctx.onchain_data_sources().filter(end_block_filter),
                ),
                subgraph_filter,
            );
        }

        // if static_filters is enabled, build a minimal filter with the static data sources and
        // add the necessary filters based on templates.
        // This specifically removes dynamic data sources based filters because these can be derived
        // from templates AND this reduces the cost of egress traffic by making the payloads smaller.

        if !self.inputs.static_filters {
            info!(self.logger, "forcing subgraph to use static filters.")
        }

        let data_sources = self.ctx.static_data_sources();

        let mut filter = C::TriggerFilter::from_data_sources(
            data_sources
                .iter()
                .filter_map(|ds| ds.as_onchain())
                // Filter out data sources that have reached their end block if the block is final.
                .filter(end_block_filter),
        );

        let templates = self.ctx.templates();

        filter.extend_with_template(templates.iter().filter_map(|ds| ds.as_onchain()).cloned());

        TriggerFilterWrapper::new(filter, subgraph_filter)
    }

    #[cfg(debug_assertions)]
    pub fn build_filter_for_test(&self) -> TriggerFilterWrapper<C> {
        self.build_filter()
    }

    async fn start_block_stream(&mut self) -> Result<Cancelable<Box<dyn BlockStream<C>>>, Error> {
        let block_stream_canceler = CancelGuard::new();
        let block_stream_cancel_handle = block_stream_canceler.handle();
        // TriggerFilter needs to be rebuilt eveytime the blockstream is restarted
        self.ctx.filter = Some(self.build_filter());

        let block_stream = new_block_stream(
            &self.inputs,
            self.ctx.filter.clone().unwrap(), // Safe to unwrap as we just called `build_filter` in the previous line
            &self.metrics.subgraph,
        )
        .await?
        .cancelable(&block_stream_canceler);

        self.cancel_handle = Some(block_stream_cancel_handle);

        // Keep the stream's cancel guard around to be able to shut it down when the subgraph
        // deployment is unassigned
        self.ctx
            .instances
            .insert(self.inputs.deployment.id, block_stream_canceler);

        Ok(block_stream)
    }

    fn is_canceled(&self) -> bool {
        if let Some(ref cancel_handle) = self.cancel_handle {
            cancel_handle.is_canceled()
        } else {
            false
        }
    }

    pub async fn run(self) -> Result<(), SubgraphRunnerError> {
        self.run_inner(false).await.map(|_| ())
    }

    async fn run_inner(mut self, break_on_restart: bool) -> Result<Self, SubgraphRunnerError> {
        self.update_deployment_synced_metric();

        // If a subgraph failed for deterministic reasons, before start indexing, we first
        // revert the deployment head. It should lead to the same result since the error was
        // deterministic.
        if let Some(current_ptr) = self.inputs.store.block_ptr() {
            if let Some(parent_ptr) = self
                .inputs
                .triggers_adapter
                .parent_ptr(&current_ptr)
                .await?
            {
                // This reverts the deployment head to the parent_ptr if
                // deterministic errors happened.
                //
                // There's no point in calling it if we have no current or parent block
                // pointers, because there would be: no block to revert to or to search
                // errors from (first execution).
                //
                // We attempt to unfail deterministic errors to mitigate deterministic
                // errors caused by wrong data being consumed from the providers. It has
                // been a frequent case in the past so this helps recover on a larger scale.
                let _outcome = self
                    .inputs
                    .store
                    .unfail_deterministic_error(&current_ptr, &parent_ptr)
                    .await?;
            }

            // Stop subgraph when we reach maximum endblock.
            if let Some(max_end_block) = self.inputs.max_end_block {
                if max_end_block <= current_ptr.block_number() {
                    info!(self.logger, "Stopping subgraph as we reached maximum endBlock";
                                "max_end_block" => max_end_block,
                                "current_block" => current_ptr.block_number());
                    self.inputs.store.flush().await?;
                    return Ok(self);
                }
            }
        }

        loop {
            debug!(self.logger, "Starting or restarting subgraph");

            let mut block_stream = self.start_block_stream().await?;

            debug!(self.logger, "Started block stream");

            self.metrics.subgraph.deployment_status.running();

            // Process events from the stream as long as no restart is needed
            loop {
                let event = {
                    let _section = self.metrics.stream.stopwatch.start_section("scan_blocks");

                    block_stream.next().await
                };

                // TODO: move cancel handle to the Context
                // This will require some code refactor in how the BlockStream is created
                let block_start = Instant::now();

                let action = self.handle_stream_event(event).await.map(|res| {
                    self.metrics
                        .subgraph
                        .observe_block_processed(block_start.elapsed(), res.block_finished());
                    res
                })?;

                self.update_deployment_synced_metric();

                // It is possible that the subgraph was unassigned, but the runner was in
                // a retry delay state and did not observe the cancel signal.
                if self.is_canceled() {
                    // It is also possible that the runner was in a retry delay state while
                    // the subgraph was reassigned and a new runner was started.
                    if self.ctx.instances.contains(&self.inputs.deployment.id) {
                        warn!(
                            self.logger,
                            "Terminating the subgraph runner because a newer one is active. \
                             Possible reassignment detected while the runner was in a non-cancellable pending state",
                        );
                        return Err(SubgraphRunnerError::Duplicate);
                    }

                    warn!(
                        self.logger,
                        "Terminating the subgraph runner because subgraph was unassigned",
                    );
                    return Ok(self);
                }

                match action {
                    Action::Continue => continue,
                    Action::Stop => {
                        info!(self.logger, "Stopping subgraph");
                        self.inputs.store.flush().await?;
                        return Ok(self);
                    }
                    Action::Restart if break_on_restart => {
                        info!(self.logger, "Stopping subgraph on break");
                        self.inputs.store.flush().await?;
                        return Ok(self);
                    }
                    Action::Restart => {
                        // Restart the store to clear any errors that it
                        // might have encountered and use that from now on
                        let store = self.inputs.store.cheap_clone();
                        if let Some(store) = store.restart().await? {
                            let last_good_block =
                                store.block_ptr().map(|ptr| ptr.number).unwrap_or(0);
                            self.revert_state_to(last_good_block)?;
                            self.inputs = Arc::new(self.inputs.with_store(store));
                        }
                        break;
                    }
                };
            }
        }
    }

    async fn transact_block_state(
        &mut self,
        logger: &Logger,
        block_ptr: BlockPtr,
        firehose_cursor: FirehoseCursor,
        block_time: BlockTime,
        block_state: BlockState,
        proof_of_indexing: SharedProofOfIndexing,
        offchain_mods: Vec<EntityModification>,
        processed_offchain_data_sources: Vec<StoredDynamicDataSource>,
    ) -> Result<(), ProcessingError> {
        fn log_evict_stats(logger: &Logger, evict_stats: &EvictStats) {
            trace!(logger, "Entity cache statistics";
                "weight" => evict_stats.new_weight,
                "evicted_weight" => evict_stats.evicted_weight,
                "count" => evict_stats.new_count,
                "evicted_count" => evict_stats.evicted_count,
                "stale_update" => evict_stats.stale_update,
                "hit_rate" => format!("{:.0}%", evict_stats.hit_rate_pct()),
                "accesses" => evict_stats.accesses,
                "evict_time_ms" => evict_stats.evict_time.as_millis());
        }

        let BlockState {
            deterministic_errors,
            persisted_data_sources,
            metrics: block_state_metrics,
            mut entity_cache,
            ..
        } = block_state;
        let first_error = deterministic_errors.first().cloned();
        let has_errors = first_error.is_some();

        // Avoid writing to store if block stream has been canceled
        if self.is_canceled() {
            return Err(ProcessingError::Canceled);
        }

        if let Some(proof_of_indexing) = proof_of_indexing.into_inner() {
            update_proof_of_indexing(
                proof_of_indexing,
                block_time,
                &self.metrics.host.stopwatch,
                &mut entity_cache,
            )
            .await
            .non_deterministic()?;
        }

        let section = self
            .metrics
            .host
            .stopwatch
            .start_section("as_modifications");
        let ModificationsAndCache {
            modifications: mut mods,
            entity_lfu_cache: cache,
            evict_stats,
        } = entity_cache.as_modifications(block_ptr.number).classify()?;
        section.end();

        log_evict_stats(&self.logger, &evict_stats);

        mods.extend(offchain_mods);

        // Put the cache back in the state, asserting that the placeholder cache was not used.
        assert!(self.state.entity_lfu_cache.is_empty());
        self.state.entity_lfu_cache = cache;

        if !mods.is_empty() {
            info!(&logger, "Applying {} entity operation(s)", mods.len());
        }

        let err_count = deterministic_errors.len();
        for (i, e) in deterministic_errors.iter().enumerate() {
            let message = format!("{:#}", e).replace('\n', "\t");
            error!(&logger, "Subgraph error {}/{}", i + 1, err_count;
                "error" => message,
                "code" => LogCode::SubgraphSyncingFailure
            );
        }

        // Transact entity operations into the store and update the
        // subgraph's block stream pointer
        let _section = self.metrics.host.stopwatch.start_section("transact_block");
        let start = Instant::now();

        // If a deterministic error has happened, make the PoI to be the only entity that'll be stored.
        if has_errors && self.inputs.errors_are_fatal() {
            let is_poi_entity =
                |entity_mod: &EntityModification| entity_mod.key().entity_type.is_poi();
            mods.retain(is_poi_entity);
            // Confidence check
            assert!(
                mods.len() == 1,
                "There should be only one PoI EntityModification"
            );
        }

        let is_caught_up = self.is_caught_up(&block_ptr).await.non_deterministic()?;

        self.inputs
            .store
            .transact_block_operations(
                block_ptr.clone(),
                block_time,
                firehose_cursor,
                mods,
                &self.metrics.host.stopwatch,
                persisted_data_sources,
                deterministic_errors,
                processed_offchain_data_sources,
                self.inputs.errors_are_non_fatal(),
                is_caught_up,
            )
            .await
            .classify()
            .detail("Failed to transact block operations")?;

        // For subgraphs with `nonFatalErrors` feature disabled, we consider
        // any error as fatal.
        //
        // So we do an early return to make the subgraph stop processing blocks.
        //
        // In this scenario the only entity that is stored/transacted is the PoI,
        // all of the others are discarded.
        if has_errors && self.inputs.errors_are_fatal() {
            // Only the first error is reported.
            return Err(ProcessingError::Deterministic(Box::new(
                first_error.unwrap(),
            )));
        }

        let elapsed = start.elapsed().as_secs_f64();
        self.metrics
            .subgraph
            .block_ops_transaction_duration
            .observe(elapsed);

        block_state_metrics
            .flush_metrics_to_store(&logger, block_ptr, self.inputs.deployment.id)
            .non_deterministic()?;

        if has_errors {
            self.maybe_cancel()?;
        }

        Ok(())
    }

    /// Cancel the subgraph if `disable_fail_fast` is not set and it is not
    /// synced
    fn maybe_cancel(&self) -> Result<(), ProcessingError> {
        // To prevent a buggy pending version from replacing a current version, if errors are
        // present the subgraph will be unassigned.
        let store = &self.inputs.store;
        if !ENV_VARS.disable_fail_fast && !store.is_deployment_synced() {
            store
                .pause_subgraph()
                .map_err(|e| ProcessingError::Unknown(e.into()))?;

            // Use `Canceled` to avoiding setting the subgraph health to failed, an error was
            // just transacted so it will be already be set to unhealthy.
            Err(ProcessingError::Canceled.into())
        } else {
            Ok(())
        }
    }

    async fn match_and_decode_many<'a, F>(
        &'a self,
        logger: &Logger,
        block: &Arc<C::Block>,
        triggers: Vec<Trigger<C>>,
        hosts_filter: F,
    ) -> Result<Vec<RunnableTriggers<'a, C>>, MappingError>
    where
        F: Fn(&TriggerData<C>) -> Box<dyn Iterator<Item = &'a T::Host> + Send + 'a>,
    {
        let triggers = triggers.into_iter().map(|t| match t {
            Trigger::Chain(t) => TriggerData::Onchain(t),
            Trigger::Subgraph(t) => TriggerData::Subgraph(t),
        });

        self.ctx
            .decoder
            .match_and_decode_many(
                &logger,
                &block,
                triggers,
                hosts_filter,
                &self.metrics.subgraph,
            )
            .await
    }

    /// Processes a block and returns the updated context and a boolean flag indicating
    /// whether new dynamic data sources have been added to the subgraph.
    async fn process_block(
        &mut self,
        block: BlockWithTriggers<C>,
        firehose_cursor: FirehoseCursor,
    ) -> Result<Action, ProcessingError> {
        fn log_triggers_found<C: Blockchain>(logger: &Logger, triggers: &[Trigger<C>]) {
            if triggers.len() == 1 {
                info!(logger, "1 trigger found in this block");
            } else if triggers.len() > 1 {
                info!(logger, "{} triggers found in this block", triggers.len());
            }
        }

        let triggers = block.trigger_data;
        let block = Arc::new(block.block);
        let block_ptr = block.ptr();

        let logger = self.logger.new(o!(
                "block_number" => format!("{:?}", block_ptr.number),
                "block_hash" => format!("{}", block_ptr.hash)
        ));

        debug!(logger, "Start processing block";
               "triggers" => triggers.len());

        let proof_of_indexing =
            SharedProofOfIndexing::new(block_ptr.number, self.inputs.poi_version);

        // Causality region for onchain triggers.
        let causality_region = PoICausalityRegion::from_network(&self.inputs.network);

        let mut block_state = BlockState::new(
            self.inputs.store.clone(),
            std::mem::take(&mut self.state.entity_lfu_cache),
        );

        let _section = self
            .metrics
            .stream
            .stopwatch
            .start_section(PROCESS_TRIGGERS_SECTION_NAME);

        // Match and decode all triggers in the block
        let hosts_filter = |trigger: &TriggerData<C>| self.ctx.instance.hosts_for_trigger(trigger);
        let match_res = self
            .match_and_decode_many(&logger, &block, triggers, hosts_filter)
            .await;

        // Process events one after the other, passing in entity operations
        // collected previously to every new event being processed
        let mut res = Ok(block_state);
        match match_res {
            Ok(runnables) => {
                for runnable in runnables {
                    let process_res = self
                        .ctx
                        .trigger_processor
                        .process_trigger(
                            &self.logger,
                            runnable.hosted_triggers,
                            &block,
                            res.unwrap(),
                            &proof_of_indexing,
                            &causality_region,
                            &self.inputs.debug_fork,
                            &self.metrics.subgraph,
                            self.inputs.instrument,
                        )
                        .await
                        .map_err(|e| e.add_trigger_context(&runnable.trigger));
                    match process_res {
                        Ok(state) => res = Ok(state),
                        Err(e) => {
                            res = Err(e);
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                res = Err(e);
            }
        };

        match res {
            // Triggers processed with no errors or with only deterministic errors.
            Ok(state) => block_state = state,

            // Some form of unknown or non-deterministic error ocurred.
            Err(MappingError::Unknown(e)) => return Err(ProcessingError::Unknown(e)),
            Err(MappingError::PossibleReorg(e)) => {
                info!(logger,
                    "Possible reorg detected, retrying";
                    "error" => format!("{:#}", e),
                );

                // In case of a possible reorg, we want this function to do nothing and restart the
                // block stream so it has a chance to detect the reorg.
                //
                // The state is unchanged at this point, except for having cleared the entity cache.
                // Losing the cache is a bit annoying but not an issue for correctness.
                //
                // See also b21fa73b-6453-4340-99fb-1a78ec62efb1.
                return Ok(Action::Restart);
            }
        }

        // Check if there are any datasources that have expired in this block. ie: the end_block
        // of that data source is equal to the block number of the current block.
        let has_expired_data_sources = self.inputs.end_blocks.contains(&block_ptr.number);

        // If new onchain data sources have been created, and static filters are not in use, it is necessary
        // to restart the block stream with the new filters.
        let created_data_sources_needs_restart =
            !self.is_static_filters_enabled() && block_state.has_created_on_chain_data_sources();

        // Determine if the block stream needs to be restarted due to newly created on-chain data sources
        // or data sources that have reached their end block.
        let needs_restart = created_data_sources_needs_restart || has_expired_data_sources;

        {
            let _section = self
                .metrics
                .stream
                .stopwatch
                .start_section(HANDLE_CREATED_DS_SECTION_NAME);

            // This loop will:
            // 1. Instantiate created data sources.
            // 2. Process those data sources for the current block.
            // Until no data sources are created or MAX_DATA_SOURCES is hit.

            // Note that this algorithm processes data sources spawned on the same block _breadth
            // first_ on the tree implied by the parent-child relationship between data sources. Only a
            // very contrived subgraph would be able to observe this.
            while block_state.has_created_data_sources() {
                // Instantiate dynamic data sources, removing them from the block state.
                let (data_sources, runtime_hosts) =
                    self.create_dynamic_data_sources(block_state.drain_created_data_sources())?;

                let filter = &Arc::new(TriggerFilterWrapper::new(
                    C::TriggerFilter::from_data_sources(
                        data_sources.iter().filter_map(DataSource::as_onchain),
                    ),
                    vec![],
                ));

                // TODO: We have to pass a reference to `block` to
                // `refetch_block`, otherwise the call to
                // handle_offchain_triggers below gets an error that `block`
                // has moved. That is extremely fishy since it means that
                // `handle_offchain_triggers` uses the non-refetched block
                //
                // It's also not clear why refetching needs to happen inside
                // the loop; will firehose really return something diffrent
                // each time even though the cursor doesn't change?
                let block = self
                    .refetch_block(&logger, &block, &firehose_cursor)
                    .await?;

                // Reprocess the triggers from this block that match the new data sources
                let block_with_triggers = self
                    .inputs
                    .triggers_adapter
                    .triggers_in_block(&logger, block.as_ref().clone(), filter)
                    .await
                    .non_deterministic()?;

                let triggers = block_with_triggers.trigger_data;
                log_triggers_found(&logger, &triggers);

                // Add entity operations for the new data sources to the block state
                // and add runtimes for the data sources to the subgraph instance.
                self.persist_dynamic_data_sources(&mut block_state, data_sources);

                // Process the triggers in each host in the same order the
                // corresponding data sources have been created.
                let hosts_filter = |_: &'_ TriggerData<C>| -> Box<dyn Iterator<Item = _> + Send> {
                    Box::new(runtime_hosts.iter().map(Arc::as_ref))
                };
                let match_res: Result<Vec<_>, _> = self
                    .match_and_decode_many(&logger, &block, triggers, hosts_filter)
                    .await;

                let mut res = Ok(block_state);
                match match_res {
                    Ok(runnables) => {
                        for runnable in runnables {
                            let process_res = self
                                .ctx
                                .trigger_processor
                                .process_trigger(
                                    &self.logger,
                                    runnable.hosted_triggers,
                                    &block,
                                    res.unwrap(),
                                    &proof_of_indexing,
                                    &causality_region,
                                    &self.inputs.debug_fork,
                                    &self.metrics.subgraph,
                                    self.inputs.instrument,
                                )
                                .await
                                .map_err(|e| e.add_trigger_context(&runnable.trigger));
                            match process_res {
                                Ok(state) => res = Ok(state),
                                Err(e) => {
                                    res = Err(e);
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        res = Err(e);
                    }
                }

                block_state = res.map_err(|e| {
                    // This treats a `PossibleReorg` as an ordinary error which will fail the subgraph.
                    // This can cause an unnecessary subgraph failure, to fix it we need to figure out a
                    // way to revert the effect of `create_dynamic_data_sources` so we may return a
                    // clean context as in b21fa73b-6453-4340-99fb-1a78ec62efb1.
                    match e {
                        MappingError::PossibleReorg(e) | MappingError::Unknown(e) => {
                            ProcessingError::Unknown(e)
                        }
                    }
                })?;
            }
        }

        // Check for offchain events and process them, including their entity modifications in the
        // set to be transacted.
        let offchain_events = self
            .ctx
            .offchain_monitor
            .ready_offchain_events()
            .non_deterministic()?;
        let (offchain_mods, processed_offchain_data_sources, persisted_off_chain_data_sources) =
            self.handle_offchain_triggers(offchain_events, &block)
                .await
                .non_deterministic()?;
        block_state
            .persisted_data_sources
            .extend(persisted_off_chain_data_sources);

        self.transact_block_state(
            &logger,
            block_ptr.clone(),
            firehose_cursor.clone(),
            block.timestamp(),
            block_state,
            proof_of_indexing,
            offchain_mods,
            processed_offchain_data_sources,
        )
        .await?;

        match needs_restart {
            true => Ok(Action::Restart),
            false => Ok(Action::Continue),
        }
    }

    /// Refetch the block if it that is needed. Otherwise return the block as is.
    async fn refetch_block(
        &mut self,
        logger: &Logger,
        block: &Arc<C::Block>,
        firehose_cursor: &FirehoseCursor,
    ) -> Result<Arc<C::Block>, ProcessingError> {
        if !self.inputs.chain.is_refetch_block_required() {
            return Ok(block.cheap_clone());
        }

        let cur = firehose_cursor.clone();
        let log = logger.cheap_clone();
        let chain = self.inputs.chain.cheap_clone();
        let block = retry(
            "refetch firehose block after dynamic datasource was added",
            logger,
        )
        .limit(5)
        .no_timeout()
        .run(move || {
            let cur = cur.clone();
            let log = log.cheap_clone();
            let chain = chain.cheap_clone();
            async move { chain.refetch_firehose_block(&log, cur).await }
        })
        .await
        .non_deterministic()?;
        Ok(Arc::new(block))
    }

    async fn process_wasm_block(
        &mut self,
        proof_of_indexing: &SharedProofOfIndexing,
        block_ptr: BlockPtr,
        block_time: BlockTime,
        block_data: Box<[u8]>,
        handler: String,
        causality_region: &str,
    ) -> Result<BlockState, MappingError> {
        let block_state = BlockState::new(
            self.inputs.store.clone(),
            std::mem::take(&mut self.state.entity_lfu_cache),
        );

        self.ctx
            .process_block(
                &self.logger,
                block_ptr,
                block_time,
                block_data,
                handler,
                block_state,
                proof_of_indexing,
                causality_region,
                &self.inputs.debug_fork,
                &self.metrics.subgraph,
                self.inputs.instrument,
            )
            .await
    }

    fn create_dynamic_data_sources(
        &mut self,
        created_data_sources: Vec<InstanceDSTemplateInfo>,
    ) -> Result<(Vec<DataSource<C>>, Vec<Arc<T::Host>>), ProcessingError> {
        let mut data_sources = vec![];
        let mut runtime_hosts = vec![];

        for info in created_data_sources {
            let manifest_idx = info
                .template
                .manifest_idx()
                .ok_or_else(|| anyhow!("Expected template to have an idx"))
                .non_deterministic()?;
            let created_ds_template = self
                .inputs
                .templates
                .iter()
                .find(|t| t.manifest_idx() == manifest_idx)
                .ok_or_else(|| anyhow!("Expected to find a template for this dynamic data source"))
                .non_deterministic()?;

            // Try to instantiate a data source from the template
            let data_source = {
                let res = match info.template {
                    InstanceDSTemplate::Onchain(_) => {
                        C::DataSource::from_template_info(info, created_ds_template)
                            .map(DataSource::Onchain)
                            .map_err(DataSourceCreationError::from)
                    }
                    InstanceDSTemplate::Offchain(_) => offchain::DataSource::from_template_info(
                        info,
                        self.ctx.causality_region_next_value(),
                    )
                    .map(DataSource::Offchain),
                };
                match res {
                    Ok(ds) => ds,
                    Err(e @ DataSourceCreationError::Ignore(..)) => {
                        warn!(self.logger, "{}", e.to_string());
                        continue;
                    }
                    Err(DataSourceCreationError::Unknown(e)) => return Err(e).non_deterministic(),
                }
            };

            // Try to create a runtime host for the data source
            let host = self
                .ctx
                .add_dynamic_data_source(&self.logger, data_source.clone())
                .non_deterministic()?;

            match host {
                Some(host) => {
                    data_sources.push(data_source);
                    runtime_hosts.push(host);
                }
                None => {
                    warn!(
                        self.logger,
                        "no runtime host created, there is already a runtime host instantiated for \
                        this data source";
                        "name" => &data_source.name(),
                        "address" => &data_source.address()
                        .map(hex::encode)
                        .unwrap_or("none".to_string()),
                    )
                }
            }
        }

        Ok((data_sources, runtime_hosts))
    }

    async fn handle_action(
        &mut self,
        start: Instant,
        block_ptr: BlockPtr,
        action: Result<Action, ProcessingError>,
    ) -> Result<Action, Error> {
        self.state.skip_ptr_updates_timer = Instant::now();

        let elapsed = start.elapsed().as_secs_f64();
        self.metrics
            .subgraph
            .block_processing_duration
            .observe(elapsed);

        match action {
            Ok(action) => {
                // Keep trying to unfail subgraph for everytime it advances block(s) until it's
                // health is not Failed anymore.
                if self.state.should_try_unfail_non_deterministic {
                    // If the deployment head advanced, we can unfail
                    // the non-deterministic error (if there's any).
                    let outcome = self
                        .inputs
                        .store
                        .unfail_non_deterministic_error(&block_ptr)?;

                    // Stop trying to unfail.
                    self.state.should_try_unfail_non_deterministic = false;

                    if let UnfailOutcome::Unfailed = outcome {
                        self.metrics.subgraph.deployment_status.running();
                        self.state.backoff.reset();
                    }
                }

                if let Some(stop_block) = self.inputs.stop_block {
                    if block_ptr.number >= stop_block {
                        info!(self.logger, "Stop block reached for subgraph");
                        return Ok(Action::Stop);
                    }
                }

                if let Some(max_end_block) = self.inputs.max_end_block {
                    if block_ptr.number >= max_end_block {
                        info!(
                            self.logger,
                            "Stopping subgraph as maximum endBlock reached";
                            "max_end_block" => max_end_block,
                            "current_block" => block_ptr.number
                        );
                        return Ok(Action::Stop);
                    }
                }

                return Ok(action);
            }
            Err(ProcessingError::Canceled) => {
                debug!(self.logger, "Subgraph block stream shut down cleanly");
                return Ok(Action::Stop);
            }

            // Handle unexpected stream errors by marking the subgraph as failed.
            Err(e) => {
                self.metrics.subgraph.deployment_status.failed();
                let last_good_block = self
                    .inputs
                    .store
                    .block_ptr()
                    .map(|ptr| ptr.number)
                    .unwrap_or(0);
                self.revert_state_to(last_good_block)?;

                let message = format!("{:#}", e).replace('\n', "\t");
                let err = anyhow!("{}, code: {}", message, LogCode::SubgraphSyncingFailure);
                let deterministic = e.is_deterministic();

                let error = SubgraphError {
                    subgraph_id: self.inputs.deployment.hash.clone(),
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
                        self.inputs
                            .store
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
                            self.inputs.store.health().await? != SubgraphHealth::Failed;

                        if should_fail_subgraph {
                            // Fail subgraph:
                            // - Change status/health.
                            // - Save the error to the database.
                            self.inputs
                                .store
                                .fail_subgraph(error)
                                .await
                                .context("Failed to set subgraph status to `failed`")?;
                        }

                        // Retry logic below:

                        let message = format!("{:#}", e).replace('\n', "\t");
                        error!(self.logger, "Subgraph failed with non-deterministic error: {}", message;
                            "attempt" => self.state.backoff.attempt,
                            "retry_delay_s" => self.state.backoff.delay().as_secs());

                        // Sleep before restarting.
                        self.state.backoff.sleep_async().await;

                        self.state.should_try_unfail_non_deterministic = true;

                        // And restart the subgraph.
                        return Ok(Action::Restart);
                    }
                }
            }
        }
    }

    fn persist_dynamic_data_sources(
        &mut self,
        block_state: &mut BlockState,
        data_sources: Vec<DataSource<C>>,
    ) {
        if !data_sources.is_empty() {
            debug!(
                self.logger,
                "Creating {} dynamic data source(s)",
                data_sources.len()
            );
        }

        // Add entity operations to the block state in order to persist
        // the dynamic data sources
        for data_source in data_sources.iter() {
            debug!(
                self.logger,
                "Persisting data_source";
                "name" => &data_source.name(),
                "address" => &data_source.address().map(hex::encode).unwrap_or("none".to_string()),
            );
            block_state.persist_data_source(data_source.as_stored_dynamic_data_source());
        }
    }

    /// We consider a subgraph caught up when it's at most 10 blocks behind the chain head.
    async fn is_caught_up(&mut self, block_ptr: &BlockPtr) -> Result<bool, Error> {
        const CAUGHT_UP_DISTANCE: BlockNumber = 10;

        // Ensure that `state.cached_head_ptr` has a value since it could be `None` on the first
        // iteration of loop. If the deployment head has caught up to the `cached_head_ptr`, update
        // it so that we are up to date when checking if synced.
        let cached_head_ptr = self.state.cached_head_ptr.cheap_clone();
        if cached_head_ptr.is_none()
            || close_to_chain_head(&block_ptr, &cached_head_ptr, CAUGHT_UP_DISTANCE)
        {
            self.state.cached_head_ptr = self.inputs.chain.chain_head_ptr().await?;
        }
        let is_caught_up =
            close_to_chain_head(&block_ptr, &self.state.cached_head_ptr, CAUGHT_UP_DISTANCE);
        if is_caught_up {
            // Stop recording time-to-sync metrics.
            self.metrics.stream.stopwatch.disable();
        }
        Ok(is_caught_up)
    }
}

impl<C, T> SubgraphRunner<C, T>
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    async fn handle_stream_event(
        &mut self,
        event: Option<Result<BlockStreamEvent<C>, CancelableError<BlockStreamError>>>,
    ) -> Result<Action, Error> {
        let stopwatch = &self.metrics.stream.stopwatch;
        let action = match event {
            Some(Ok(BlockStreamEvent::ProcessWasmBlock(
                block_ptr,
                block_time,
                data,
                handler,
                cursor,
            ))) => {
                let _section = stopwatch.start_section(PROCESS_WASM_BLOCK_SECTION_NAME);
                let res = self
                    .handle_process_wasm_block(block_ptr.clone(), block_time, data, handler, cursor)
                    .await;
                let start = Instant::now();
                self.handle_action(start, block_ptr, res).await?
            }
            Some(Ok(BlockStreamEvent::ProcessBlock(block, cursor))) => {
                let _section = stopwatch.start_section(PROCESS_BLOCK_SECTION_NAME);
                self.handle_process_block(block, cursor).await?
            }
            Some(Ok(BlockStreamEvent::Revert(revert_to_ptr, cursor))) => {
                let _section = stopwatch.start_section(HANDLE_REVERT_SECTION_NAME);
                self.handle_revert(revert_to_ptr, cursor).await?
            }
            // Log and drop the errors from the block_stream
            // The block stream will continue attempting to produce blocks
            Some(Err(e)) => self.handle_err(e).await?,
            // If the block stream ends, that means that there is no more indexing to do.
            // Typically block streams produce indefinitely, but tests are an example of finite block streams.
            None => Action::Stop,
        };

        Ok(action)
    }

    async fn handle_offchain_triggers(
        &mut self,
        triggers: Vec<offchain::TriggerData>,
        block: &Arc<C::Block>,
    ) -> Result<
        (
            Vec<EntityModification>,
            Vec<StoredDynamicDataSource>,
            Vec<StoredDynamicDataSource>,
        ),
        Error,
    > {
        let mut mods = vec![];
        let mut processed_data_sources = vec![];
        let mut persisted_data_sources = vec![];

        for trigger in triggers {
            // Using an `EmptyStore` and clearing the cache for each trigger is a makeshift way to
            // get causality region isolation.
            let schema = ReadStore::input_schema(&self.inputs.store);
            let mut block_state = BlockState::new(EmptyStore::new(schema), LfuCache::new());

            // PoI ignores offchain events.
            // See also: poi-ignores-offchain
            let proof_of_indexing = SharedProofOfIndexing::ignored();
            let causality_region = "";

            let trigger = TriggerData::Offchain(trigger);
            let process_res = {
                let hosts = self.ctx.instance.hosts_for_trigger(&trigger);
                let triggers_res = self.ctx.decoder.match_and_decode(
                    &self.logger,
                    block,
                    trigger,
                    hosts,
                    &self.metrics.subgraph,
                );
                match triggers_res {
                    Ok(runnable) => {
                        self.ctx
                            .trigger_processor
                            .process_trigger(
                                &self.logger,
                                runnable.hosted_triggers,
                                block,
                                block_state,
                                &proof_of_indexing,
                                causality_region,
                                &self.inputs.debug_fork,
                                &self.metrics.subgraph,
                                self.inputs.instrument,
                            )
                            .await
                    }
                    Err(e) => Err(e),
                }
            };
            match process_res {
                Ok(state) => block_state = state,
                Err(err) => {
                    let err = match err {
                        // Ignoring `PossibleReorg` isn't so bad since the subgraph will retry
                        // non-deterministic errors.
                        MappingError::PossibleReorg(e) | MappingError::Unknown(e) => e,
                    };
                    return Err(err.context("failed to process trigger".to_string()));
                }
            }

            anyhow::ensure!(
                !block_state.has_created_on_chain_data_sources(),
                "Attempted to create on-chain data source in offchain data source handler. This is not yet supported.",
            );

            let (data_sources, _) =
                self.create_dynamic_data_sources(block_state.drain_created_data_sources())?;

            // Add entity operations for the new data sources to the block state
            // and add runtimes for the data sources to the subgraph instance.
            self.persist_dynamic_data_sources(&mut block_state, data_sources);

            // This propagates any deterministic error as a non-deterministic one. Which might make
            // sense considering offchain data sources are non-deterministic.
            if let Some(err) = block_state.deterministic_errors.into_iter().next() {
                return Err(anyhow!("{}", err.to_string()));
            }

            mods.extend(
                block_state
                    .entity_cache
                    .as_modifications(block.number())?
                    .modifications,
            );
            processed_data_sources.extend(block_state.processed_data_sources);
            persisted_data_sources.extend(block_state.persisted_data_sources)
        }

        Ok((mods, processed_data_sources, persisted_data_sources))
    }

    fn update_deployment_synced_metric(&self) {
        self.metrics
            .subgraph
            .deployment_synced
            .record(self.inputs.store.is_deployment_synced());
    }
}

#[derive(Debug)]
enum Action {
    Continue,
    Stop,
    Restart,
}

impl Action {
    /// Return `true` if the action indicates that we are done with a block
    fn block_finished(&self) -> bool {
        match self {
            Action::Restart => false,
            Action::Continue | Action::Stop => true,
        }
    }
}

impl<C, T> SubgraphRunner<C, T>
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    async fn handle_process_wasm_block(
        &mut self,
        block_ptr: BlockPtr,
        block_time: BlockTime,
        block_data: Box<[u8]>,
        handler: String,
        cursor: FirehoseCursor,
    ) -> Result<Action, ProcessingError> {
        let logger = self.logger.new(o!(
                "block_number" => format!("{:?}", block_ptr.number),
                "block_hash" => format!("{}", block_ptr.hash)
        ));

        debug!(logger, "Start processing wasm block";);

        self.metrics
            .stream
            .deployment_head
            .set(block_ptr.number as f64);

        let proof_of_indexing =
            SharedProofOfIndexing::new(block_ptr.number, self.inputs.poi_version);

        // Causality region for onchain triggers.
        let causality_region = PoICausalityRegion::from_network(&self.inputs.network);

        let block_state = {
            match self
                .process_wasm_block(
                    &proof_of_indexing,
                    block_ptr.clone(),
                    block_time,
                    block_data,
                    handler,
                    &causality_region,
                )
                .await
            {
                // Triggers processed with no errors or with only deterministic errors.
                Ok(block_state) => block_state,

                // Some form of unknown or non-deterministic error ocurred.
                Err(MappingError::Unknown(e)) => return Err(ProcessingError::Unknown(e).into()),
                Err(MappingError::PossibleReorg(e)) => {
                    info!(logger,
                        "Possible reorg detected, retrying";
                        "error" => format!("{:#}", e),
                    );

                    // In case of a possible reorg, we want this function to do nothing and restart the
                    // block stream so it has a chance to detect the reorg.
                    //
                    // The state is unchanged at this point, except for having cleared the entity cache.
                    // Losing the cache is a bit annoying but not an issue for correctness.
                    //
                    // See also b21fa73b-6453-4340-99fb-1a78ec62efb1.
                    return Ok(Action::Restart);
                }
            }
        };

        self.transact_block_state(
            &logger,
            block_ptr.clone(),
            cursor.clone(),
            block_time,
            block_state,
            proof_of_indexing,
            vec![],
            vec![],
        )
        .await?;

        Ok(Action::Continue)
    }

    async fn handle_process_block(
        &mut self,
        block: BlockWithTriggers<C>,
        cursor: FirehoseCursor,
    ) -> Result<Action, Error> {
        let block_ptr = block.ptr();
        self.metrics
            .stream
            .deployment_head
            .set(block_ptr.number as f64);

        if block.trigger_count() > 0 {
            self.metrics
                .subgraph
                .block_trigger_count
                .observe(block.trigger_count() as f64);
        }

        if block.trigger_count() == 0
            && self.state.skip_ptr_updates_timer.elapsed() <= SKIP_PTR_UPDATES_THRESHOLD
            && !self.inputs.store.is_deployment_synced()
            && !close_to_chain_head(
                &block_ptr,
                &self.inputs.chain.chain_head_ptr().await?,
                // The "skip ptr updates timer" is ignored when a subgraph is at most 1000 blocks
                // behind the chain head.
                1000,
            )
        {
            return Ok(Action::Continue);
        } else {
            self.state.skip_ptr_updates_timer = Instant::now();
        }

        let start = Instant::now();

        let res = self.process_block(block, cursor).await;

        self.handle_action(start, block_ptr, res).await
    }

    async fn handle_revert(
        &mut self,
        revert_to_ptr: BlockPtr,
        cursor: FirehoseCursor,
    ) -> Result<Action, Error> {
        // Current deployment head in the database / WritableAgent Mutex cache.
        //
        // Safe unwrap because in a Revert event we're sure the subgraph has
        // advanced at least once.
        let subgraph_ptr = self.inputs.store.block_ptr().unwrap();
        if revert_to_ptr.number >= subgraph_ptr.number {
            info!(&self.logger, "Block to revert is higher than subgraph pointer, nothing to do"; "subgraph_ptr" => &subgraph_ptr, "revert_to_ptr" => &revert_to_ptr);
            return Ok(Action::Continue);
        }

        info!(&self.logger, "Reverting block to get back to main chain"; "subgraph_ptr" => &subgraph_ptr, "revert_to_ptr" => &revert_to_ptr);

        if let Err(e) = self
            .inputs
            .store
            .revert_block_operations(revert_to_ptr.clone(), cursor)
            .await
        {
            error!(&self.logger, "Could not revert block. Retrying"; "error" => %e);

            // Exit inner block stream consumption loop and go up to loop that restarts subgraph
            return Ok(Action::Restart);
        }

        self.metrics
            .stream
            .reverted_blocks
            .set(subgraph_ptr.number as f64);
        self.metrics
            .stream
            .deployment_head
            .set(subgraph_ptr.number as f64);

        self.revert_state_to(revert_to_ptr.number)?;

        let needs_restart: bool = self.needs_restart(revert_to_ptr, subgraph_ptr);

        let action = if needs_restart {
            Action::Restart
        } else {
            Action::Continue
        };

        Ok(action)
    }

    async fn handle_err(
        &mut self,
        err: CancelableError<BlockStreamError>,
    ) -> Result<Action, Error> {
        if self.is_canceled() {
            debug!(&self.logger, "Subgraph block stream shut down cleanly");
            return Ok(Action::Stop);
        }

        let err = match err {
            CancelableError::Error(BlockStreamError::Fatal(msg)) => {
                error!(
                    &self.logger,
                    "The block stream encountered a substreams fatal error and will not retry: {}",
                    msg
                );

                // If substreams returns a deterministic error we may not necessarily have a specific block
                // but we should not retry since it will keep failing.
                self.inputs
                    .store
                    .fail_subgraph(SubgraphError {
                        subgraph_id: self.inputs.deployment.hash.clone(),
                        message: msg,
                        block_ptr: None,
                        handler: None,
                        deterministic: true,
                    })
                    .await
                    .context("Failed to set subgraph status to `failed`")?;

                return Ok(Action::Stop);
            }
            e => e,
        };

        debug!(
            &self.logger,
            "Block stream produced a non-fatal error";
            "error" => format!("{}", err),
        );

        Ok(Action::Continue)
    }

    /// Determines if the subgraph needs to be restarted.
    /// Currently returns true when there are data sources that have reached their end block
    /// in the range between `revert_to_ptr` and `subgraph_ptr`.
    fn needs_restart(&self, revert_to_ptr: BlockPtr, subgraph_ptr: BlockPtr) -> bool {
        self.inputs
            .end_blocks
            .range(revert_to_ptr.number..=subgraph_ptr.number)
            .next()
            .is_some()
    }
}

impl From<StoreError> for SubgraphRunnerError {
    fn from(err: StoreError) -> Self {
        Self::Unknown(err.into())
    }
}

/// Transform the proof of indexing changes into entity updates that will be
/// inserted when as_modifications is called.
async fn update_proof_of_indexing(
    proof_of_indexing: ProofOfIndexing,
    block_time: BlockTime,
    stopwatch: &StopwatchMetrics,
    entity_cache: &mut EntityCache,
) -> Result<(), Error> {
    // Helper to store the digest as a PoI entity in the cache
    fn store_poi_entity(
        entity_cache: &mut EntityCache,
        key: EntityKey,
        digest: Bytes,
        block_time: BlockTime,
        block: BlockNumber,
    ) -> Result<(), Error> {
        let digest_name = entity_cache.schema.poi_digest();
        let mut data = vec![
            (
                graph::data::store::ID.clone(),
                Value::from(key.entity_id.to_string()),
            ),
            (digest_name, Value::from(digest)),
        ];
        if entity_cache.schema.has_aggregations() {
            let block_time = Value::Int8(block_time.as_secs_since_epoch() as i64);
            data.push((entity_cache.schema.poi_block_time(), block_time));
        }
        let poi = entity_cache.make_entity(data)?;
        entity_cache.set(key, poi, block, None)
    }

    let _section_guard = stopwatch.start_section("update_proof_of_indexing");

    let block_number = proof_of_indexing.get_block();
    let mut proof_of_indexing = proof_of_indexing.take();

    for (causality_region, stream) in proof_of_indexing.drain() {
        // Create the special POI entity key specific to this causality_region
        // There are two things called causality regions here, one is the causality region for
        // the poi which is a string and the PoI entity id. The other is the data source
        // causality region to which the PoI belongs as an entity. Currently offchain events do
        // not affect PoI so it is assumed to be `ONCHAIN`.
        // See also: poi-ignores-offchain
        let entity_key = entity_cache
            .schema
            .poi_type()
            .key_in(causality_region, CausalityRegion::ONCHAIN);

        // Grab the current digest attribute on this entity
        let poi_digest = entity_cache.schema.poi_digest().clone();
        let prev_poi = entity_cache
            .get(&entity_key, GetScope::Store)
            .map_err(Error::from)?
            .map(|entity| match entity.get(poi_digest.as_str()) {
                Some(Value::Bytes(b)) => b.clone(),
                _ => panic!("Expected POI entity to have a digest and for it to be bytes"),
            });

        // Finish the POI stream, getting the new POI value.
        let updated_proof_of_indexing = stream.pause(prev_poi.as_deref());
        let updated_proof_of_indexing: Bytes = (&updated_proof_of_indexing[..]).into();

        // Put this onto an entity with the same digest attribute
        // that was expected before when reading.
        store_poi_entity(
            entity_cache,
            entity_key,
            updated_proof_of_indexing,
            block_time,
            block_number,
        )?;
    }

    Ok(())
}

/// Checks if the Deployment BlockPtr is within N blocks of the chain head or ahead.
fn close_to_chain_head(
    deployment_head_ptr: &BlockPtr,
    chain_head_ptr: &Option<BlockPtr>,
    n: BlockNumber,
) -> bool {
    matches!((deployment_head_ptr, &chain_head_ptr), (b1, Some(b2)) if b1.number >= (b2.number - n))
}

#[test]
fn test_close_to_chain_head() {
    let offset = 1;

    let block_0 = BlockPtr::try_from((
        "bd34884280958002c51d3f7b5f853e6febeba33de0f40d15b0363006533c924f",
        0,
    ))
    .unwrap();
    let block_1 = BlockPtr::try_from((
        "8511fa04b64657581e3f00e14543c1d522d5d7e771b54aa3060b662ade47da13",
        1,
    ))
    .unwrap();
    let block_2 = BlockPtr::try_from((
        "b98fb783b49de5652097a989414c767824dff7e7fd765a63b493772511db81c1",
        2,
    ))
    .unwrap();

    assert!(!close_to_chain_head(&block_0, &None, offset));
    assert!(!close_to_chain_head(&block_2, &None, offset));

    assert!(!close_to_chain_head(
        &block_0,
        &Some(block_2.clone()),
        offset
    ));

    assert!(close_to_chain_head(
        &block_1,
        &Some(block_2.clone()),
        offset
    ));
    assert!(close_to_chain_head(
        &block_2,
        &Some(block_2.clone()),
        offset
    ));
}
