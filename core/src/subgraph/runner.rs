use crate::subgraph::context::IndexingContext;
use crate::subgraph::error::BlockProcessingError;
use crate::subgraph::inputs::IndexingInputs;
use crate::subgraph::metrics::SubgraphInstanceMetrics;
use crate::subgraph::SubgraphInstance;
use atomic_refcell::AtomicRefCell;
use fail::fail_point;
use graph::blockchain::block_stream::{
    BlockStream, BlockStreamEvent, BlockWithTriggers, BufferedBlockStream,
};
use graph::blockchain::{Block, Blockchain, DataSource, TriggerFilter as _, TriggersAdapter};
use graph::components::{
    store::{ModificationsAndCache, SubgraphFork},
    subgraph::{CausalityRegion, MappingError, ProofOfIndexing, SharedProofOfIndexing},
};
use graph::data::store::scalar::Bytes;
use graph::data::subgraph::{
    schema::{SubgraphError, SubgraphHealth, POI_OBJECT},
    SubgraphFeature,
};
use graph::prelude::*;
use graph::util::{backoff::ExponentialBackoff, lfu_cache::LfuCache};
use lazy_static::lazy_static;
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::{Duration, Instant};

const MINUTE: Duration = Duration::from_secs(60);
const SKIP_PTR_UPDATES_THRESHOLD: Duration = Duration::from_secs(60 * 5);

const BUFFERED_BLOCK_STREAM_SIZE: usize = 100;
const BUFFERED_FIREHOSE_STREAM_SIZE: usize = 1;

lazy_static! {
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

async fn new_block_stream<C: Blockchain>(
    inputs: Arc<IndexingInputs<C>>,
    filter: C::TriggerFilter,
) -> Result<Box<dyn BlockStream<C>>, Error> {
    let chain = inputs.chain.cheap_clone();
    let is_firehose = chain.is_firehose_supported();

    let buffer_size = match is_firehose {
        true => BUFFERED_FIREHOSE_STREAM_SIZE,
        false => BUFFERED_BLOCK_STREAM_SIZE,
    };

    let current_ptr = inputs.store.block_ptr();

    let block_stream = match is_firehose {
        true => chain.new_firehose_block_stream(
            inputs.deployment.clone(),
            inputs.store.block_cursor(),
            inputs.start_blocks.clone(),
            current_ptr,
            Arc::new(filter.clone()),
            inputs.unified_api_version.clone(),
            inputs.firehose_grpc_filters,
        ),
        false => chain.new_polling_block_stream(
            inputs.deployment.clone(),
            inputs.start_blocks.clone(),
            current_ptr,
            Arc::new(filter.clone()),
            inputs.unified_api_version.clone(),
        ),
    }
    .await?;

    Ok(BufferedBlockStream::spawn_from_stream(
        block_stream,
        buffer_size,
    ))
}

pub struct SubgraphRunner<C: Blockchain, T: RuntimeHostBuilder<C>> {
    ctx: IndexingContext<T, C>,
    inputs: Arc<IndexingInputs<C>>,
}

impl<C, T> SubgraphRunner<C, T>
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    pub fn new(inputs: IndexingInputs<C>, ctx: IndexingContext<T, C>) -> Self {
        Self {
            inputs: Arc::new(inputs),
            ctx,
        }
    }

    pub async fn run(mut self) -> Result<(), Error> {
        // Clone a few things for different parts of the async processing
        let subgraph_metrics = self.ctx.subgraph_metrics.cheap_clone();
        let store_for_err = self.inputs.store.cheap_clone();
        let logger = self.ctx.state.logger.cheap_clone();
        let id_for_err = self.inputs.deployment.hash.clone();
        let mut should_try_unfail_non_deterministic = true;
        let mut synced = false;
        let mut skip_ptr_updates_timer = Instant::now();

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
                let _outcome = self
                    .inputs
                    .store
                    .unfail_deterministic_error(&current_ptr, &parent_ptr)?;
            }
        }

        // Exponential backoff that starts with two minutes and keeps
        // increasing its timeout exponentially until it reaches the ceiling.
        let mut backoff = ExponentialBackoff::new(MINUTE * 2, *SUBGRAPH_ERROR_RETRY_CEIL_SECS);

        loop {
            debug!(logger, "Starting or restarting subgraph");

            let block_stream_canceler = CancelGuard::new();
            let block_stream_cancel_handle = block_stream_canceler.handle();

            let metrics = self.ctx.block_stream_metrics.clone();
            let filter = self.ctx.state.filter.clone();
            let stream_inputs = self.inputs.clone();
            let mut block_stream = new_block_stream(stream_inputs, filter)
                .await?
                .map_err(CancelableError::Error)
                .cancelable(&block_stream_canceler, || Err(CancelableError::Cancel));
            let chain = self.inputs.chain.clone();
            let chain_store = chain.chain_store();

            // Keep the stream's cancel guard around to be able to shut it down
            // when the subgraph deployment is unassigned
            self.ctx
                .state
                .instances
                .write()
                .unwrap()
                .insert(self.inputs.deployment.id, block_stream_canceler);

            debug!(logger, "Starting block stream");

            // Process events from the stream as long as no restart is needed
            loop {
                let event = {
                    let _section = metrics.stopwatch.start_section("scan_blocks");

                    block_stream.next().await
                };

                let (block, cursor) = match event {
                    Some(Ok(BlockStreamEvent::ProcessBlock(block, cursor))) => (block, cursor),
                    Some(Ok(BlockStreamEvent::Revert(revert_to_ptr, cursor))) => {
                        // Current deployment head in the database / WritableAgent Mutex cache.
                        //
                        // Safe unwrap because in a Revert event we're sure the subgraph has
                        // advanced at least once.
                        let subgraph_ptr = self.inputs.store.block_ptr().unwrap();
                        if revert_to_ptr.number >= subgraph_ptr.number {
                            info!(&logger, "Block to revert is higher than subgraph pointer, nothing to do"; "subgraph_ptr" => &subgraph_ptr, "revert_to_ptr" => &revert_to_ptr);
                            continue;
                        }

                        info!(&logger, "Reverting block to get back to main chain"; "subgraph_ptr" => &subgraph_ptr, "revert_to_ptr" => &revert_to_ptr);

                        if let Err(e) = self
                            .inputs
                            .store
                            .revert_block_operations(revert_to_ptr, cursor.as_deref())
                        {
                            error!(&logger, "Could not revert block. Retrying"; "error" => %e);

                            // Exit inner block stream consumption loop and go up to loop that restarts subgraph
                            break;
                        }

                        self.ctx
                            .block_stream_metrics
                            .reverted_blocks
                            .set(subgraph_ptr.number as f64);
                        metrics.deployment_head.set(subgraph_ptr.number as f64);

                        // Revert the in-memory state:
                        // - Remove hosts for reverted dynamic data sources.
                        // - Clear the entity cache.
                        //
                        // Note that we do not currently revert the filters, which means the filters
                        // will be broader than necessary. This is not ideal for performance, but is not
                        // incorrect since we will discard triggers that match the filters but do not
                        // match any data sources.
                        self.ctx
                            .state
                            .instance
                            .revert_data_sources(subgraph_ptr.number);
                        self.ctx.state.entity_lfu_cache = LfuCache::new();
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
                    // Scenario where this can happen: 1504c9d8-36e4-45bb-b4f2-71cf58789ed9
                    None => unreachable!("The block stream stopped producing blocks"),
                };

                let block_ptr = block.ptr();
                metrics.deployment_head.set(block_ptr.number as f64);

                if block.trigger_count() > 0 {
                    subgraph_metrics
                        .block_trigger_count
                        .observe(block.trigger_count() as f64);
                }

                if block.trigger_count() == 0
                    && skip_ptr_updates_timer.elapsed() <= SKIP_PTR_UPDATES_THRESHOLD
                    && !synced
                {
                    continue;
                } else {
                    skip_ptr_updates_timer = Instant::now();
                }

                let start = Instant::now();
                let deployment_failed = self.ctx.block_stream_metrics.deployment_failed.clone();

                let res = self
                    .process_block(
                        &logger,
                        self.inputs.triggers_adapter.cheap_clone(),
                        block_stream_cancel_handle.clone(),
                        block,
                        cursor.into(),
                    )
                    .await;

                let elapsed = start.elapsed().as_secs_f64();
                subgraph_metrics.block_processing_duration.observe(elapsed);

                match res {
                    Ok(needs_restart) => {
                        // Once synced, no need to try to update the status again.
                        if !synced
                            && is_deployment_synced(&block_ptr, chain_store.cached_head_ptr()?)
                        {
                            // Updating the sync status is an one way operation.
                            // This state change exists: not synced -> synced
                            // This state change does NOT: synced -> not synced
                            self.inputs.store.deployment_synced()?;

                            // Stop trying to update the sync status.
                            synced = true;

                            // Stop recording time-to-sync metrics.
                            self.ctx.block_stream_metrics.stopwatch.disable();
                        }

                        // Keep trying to unfail subgraph for everytime it advances block(s) until it's
                        // health is not Failed anymore.
                        if should_try_unfail_non_deterministic {
                            // If the deployment head advanced, we can unfail
                            // the non-deterministic error (if there's any).
                            let outcome = self
                                .inputs
                                .store
                                .unfail_non_deterministic_error(&block_ptr)?;

                            if let UnfailOutcome::Unfailed = outcome {
                                // Stop trying to unfail.
                                should_try_unfail_non_deterministic = false;
                                deployment_failed.set(0.0);
                                backoff.reset();
                            }
                        }

                        if needs_restart && !self.inputs.static_filters {
                            // Cancel the stream for real
                            self.ctx
                                .state
                                .instances
                                .write()
                                .unwrap()
                                .remove(&self.inputs.deployment.id);

                            // And restart the subgraph
                            break;
                        }

                        if let Some(stop_block) = &self.inputs.stop_block {
                            if block_ptr.number >= *stop_block {
                                info!(&logger, "stop block reached for subgraph");
                                return Ok(());
                            }
                        }
                    }
                    Err(BlockProcessingError::Canceled) => {
                        debug!(&logger, "Subgraph block stream shut down cleanly");
                        return Ok(());
                    }

                    // Handle unexpected stream errors by marking the subgraph as failed.
                    Err(e) => {
                        // Clear entity cache when a subgraph fails.
                        //
                        // This is done to be safe and sure that there's no state that's
                        // out of sync from the database.
                        //
                        // Without it, POI changes on failure would be kept in the entity cache
                        // and be transacted incorrectly in the next run.
                        self.ctx.state.entity_lfu_cache = LfuCache::new();

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
                                let should_fail_subgraph = self
                                    .inputs
                                    .store
                                    .health(&self.inputs.deployment.hash)
                                    .await?
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
                                self.ctx
                                    .state
                                    .instances
                                    .write()
                                    .unwrap()
                                    .remove(&self.inputs.deployment.id);

                                let message = format!("{:#}", e).replace("\n", "\t");
                                error!(logger, "Subgraph failed with non-deterministic error: {}", message;
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

    /// Processes a block and returns the updated context and a boolean flag indicating
    /// whether new dynamic data sources have been added to the subgraph.
    async fn process_block(
        &mut self,
        logger: &Logger,
        triggers_adapter: Arc<C::TriggersAdapter>,
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
        } else {
            debug!(
                &logger,
                "{} candidate triggers in this block",
                triggers.len()
            );
        }

        let metrics = self.ctx.subgraph_metrics.clone();

        let proof_of_indexing = if self
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
        let causality_region = CausalityRegion::from_network(self.ctx.state.instance.network());

        // Process events one after the other, passing in entity operations
        // collected previously to every new event being processed
        let mut block_state = match Self::process_triggers(
            &logger,
            BlockState::new(
                self.inputs.store.clone(),
                std::mem::take(&mut self.ctx.state.entity_lfu_cache),
            ),
            &proof_of_indexing,
            &self.ctx.subgraph_metrics,
            &self.ctx.state.instance,
            &block,
            triggers,
            &causality_region,
            &self.inputs.debug_fork,
        )
        .await
        {
            // Triggers processed with no errors or with only deterministic errors.
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
        let host_metrics = self.ctx.host_metrics.clone();

        // This loop will:
        // 1. Instantiate created data sources.
        // 2. Process those data sources for the current block.
        // Until no data sources are created or MAX_DATA_SOURCES is hit.

        // Note that this algorithm processes data sources spawned on the same block _breadth
        // first_ on the tree implied by the parent-child relationship between data sources. Only a
        // very contrived subgraph would be able to observe this.
        while block_state.has_created_data_sources() {
            // Instantiate dynamic data sources, removing them from the block state.
            let (data_sources, runtime_hosts) = self.create_dynamic_data_sources(
                logger.clone(),
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
            self.persist_dynamic_data_sources(
                logger.clone(),
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
                    &proof_of_indexing,
                    &causality_region,
                    &self.inputs.debug_fork,
                    &self.ctx.subgraph_metrics,
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
        let is_non_fatal_errors_active = self
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
                &self.ctx.host_metrics.stopwatch,
                &self.inputs.deployment.hash,
                &mut block_state.entity_cache,
            )
            .await?;
        }

        let section = self
            .ctx
            .host_metrics
            .stopwatch
            .start_section("as_modifications");
        let ModificationsAndCache {
            modifications: mut mods,
            data_sources,
            entity_lfu_cache: cache,
        } = block_state
            .entity_cache
            .as_modifications()
            .map_err(|e| BlockProcessingError::Unknown(e.into()))?;
        section.end();

        // Put the cache back in the ctx, asserting that the placeholder cache was not used.
        assert!(self.ctx.state.entity_lfu_cache.is_empty());
        self.ctx.state.entity_lfu_cache = cache;

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
        let _section = self
            .ctx
            .host_metrics
            .stopwatch
            .start_section("transact_block");
        let stopwatch = self.ctx.host_metrics.stopwatch.clone();
        let start = Instant::now();

        let store = &self.inputs.store;

        // If a deterministic error has happened, make the PoI to be the only entity that'll be stored.
        if has_errors && !is_non_fatal_errors_active {
            let is_poi_entity =
                |entity_mod: &EntityModification| entity_mod.entity_key().entity_type.is_poi();
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

            Err(e) => {
                Err(anyhow!("Error while processing block stream for a subgraph: {}", e).into())
            }
        }
    }

    async fn process_triggers(
        logger: &Logger,
        mut block_state: BlockState<C>,
        proof_of_indexing: &SharedProofOfIndexing,
        subgraph_metrics: &Arc<SubgraphInstanceMetrics>,
        instance: &SubgraphInstance<C, impl RuntimeHostBuilder<C>>,
        block: &Arc<C::Block>,
        triggers: Vec<C::TriggerData>,
        causality_region: &str,
        debug_fork: &Option<Arc<dyn SubgraphFork>>,
    ) -> Result<BlockState<C>, MappingError> {
        use graph::blockchain::TriggerData;

        for trigger in triggers {
            block_state = instance
                .process_trigger(
                    &logger,
                    block,
                    &trigger,
                    block_state,
                    proof_of_indexing,
                    causality_region,
                    debug_fork,
                    subgraph_metrics,
                )
                .await
                .map_err(move |mut e| {
                    let error_context = trigger.error_context();
                    if !error_context.is_empty() {
                        e = e.context(error_context);
                    }
                    e.context("failed to process trigger".to_string())
                })?;
        }
        Ok(block_state)
    }

    fn create_dynamic_data_sources(
        &mut self,
        logger: Logger,
        host_metrics: Arc<HostMetrics>,
        created_data_sources: Vec<DataSourceTemplateInfo<C>>,
    ) -> Result<(Vec<C::DataSource>, Vec<Arc<T::Host>>), Error> {
        let mut data_sources = vec![];
        let mut runtime_hosts = vec![];

        for info in created_data_sources {
            // Try to instantiate a data source from the template
            let data_source = C::DataSource::try_from(info)?;

            // Try to create a runtime host for the data source
            let host = self.ctx.state.instance.add_dynamic_data_source(
                &logger,
                data_source.clone(),
                self.inputs.templates.clone(),
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

    fn persist_dynamic_data_sources(
        &mut self,
        logger: Logger,
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
        self.ctx.state.filter.extend(data_sources.iter());
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

        entity_cache.set(entity_key, new_poi_entity)?;
    }

    Ok(())
}

/// Checks if the Deployment BlockPtr is at least one block behind to the chain head.
fn is_deployment_synced(deployment_head_ptr: &BlockPtr, chain_head_ptr: Option<BlockPtr>) -> bool {
    matches!((deployment_head_ptr, &chain_head_ptr), (b1, Some(b2)) if b1.number >= (b2.number - 1))
}

#[test]
fn test_is_deployment_synced() {
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

    assert!(!is_deployment_synced(&block_0, None));
    assert!(!is_deployment_synced(&block_2, None));

    assert!(!is_deployment_synced(&block_0, Some(block_2.clone())));

    assert!(is_deployment_synced(&block_1, Some(block_2.clone())));
    assert!(is_deployment_synced(&block_2, Some(block_2.clone())));
}
