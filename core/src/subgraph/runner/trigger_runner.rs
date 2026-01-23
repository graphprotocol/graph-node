//! Trigger execution component for the subgraph runner.
//!
//! This module provides the `TriggerRunner` struct which handles executing
//! pre-matched triggers against their hosts, accumulating state changes.

use graph::blockchain::Blockchain;
use graph::components::store::SubgraphFork;
use graph::components::subgraph::{MappingError, SharedProofOfIndexing};
use graph::components::trigger_processor::RunnableTriggers;
use graph::prelude::{BlockState, RuntimeHostBuilder, SubgraphInstanceMetrics, TriggerProcessor};
use graph::slog::Logger;
use std::sync::Arc;

/// Handles executing triggers against hosts, accumulating state.
///
/// `TriggerRunner` encapsulates the common trigger execution loop that was
/// previously duplicated between initial trigger processing and dynamic
/// data source trigger processing in `process_block`.
pub struct TriggerRunner<'a, C: Blockchain, T: RuntimeHostBuilder<C>> {
    processor: &'a dyn TriggerProcessor<C, T>,
    logger: &'a Logger,
    debug_fork: &'a Option<Arc<dyn SubgraphFork>>,
    subgraph_metrics: &'a Arc<SubgraphInstanceMetrics>,
    instrument: bool,
}

impl<'a, C, T> TriggerRunner<'a, C, T>
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    /// Create a new TriggerRunner with the given context.
    pub fn new(
        processor: &'a dyn TriggerProcessor<C, T>,
        logger: &'a Logger,
        debug_fork: &'a Option<Arc<dyn SubgraphFork>>,
        subgraph_metrics: &'a Arc<SubgraphInstanceMetrics>,
        instrument: bool,
    ) -> Self {
        Self {
            processor,
            logger,
            debug_fork,
            subgraph_metrics,
            instrument,
        }
    }

    /// Execute a batch of pre-matched triggers, accumulating state.
    ///
    /// This method processes each runnable trigger in order, calling the
    /// trigger processor for each one and accumulating the resulting state.
    /// If any trigger fails with a non-deterministic error, execution stops
    /// immediately and the error is returned.
    ///
    /// # Arguments
    ///
    /// * `block` - The block being processed
    /// * `runnables` - Pre-matched triggers ready for execution
    /// * `block_state` - Initial state to accumulate into
    /// * `proof_of_indexing` - Proof of indexing for this block
    /// * `causality_region` - The causality region for PoI
    ///
    /// # Returns
    ///
    /// The accumulated block state on success, or a `MappingError` on failure.
    pub async fn execute(
        &self,
        block: &Arc<C::Block>,
        runnables: Vec<RunnableTriggers<'a, C>>,
        mut block_state: BlockState,
        proof_of_indexing: &SharedProofOfIndexing,
        causality_region: &str,
    ) -> Result<BlockState, MappingError> {
        for runnable in runnables {
            block_state = self
                .processor
                .process_trigger(
                    self.logger,
                    runnable.hosted_triggers,
                    block,
                    block_state,
                    proof_of_indexing,
                    causality_region,
                    self.debug_fork,
                    self.subgraph_metrics,
                    self.instrument,
                )
                .await
                .map_err(|e| e.add_trigger_context(&runnable.trigger))?;
        }
        Ok(block_state)
    }
}
