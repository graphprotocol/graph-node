use std::sync::Arc;

use graph::blockchain::Blockchain;
use graph::components::store::SubgraphFork;
use graph::components::subgraph::{MappingError, SharedProofOfIndexing};
use graph::components::trigger_processor::RunnableTriggers;
use graph::prelude::{BlockState, RuntimeHostBuilder, SubgraphInstanceMetrics, TriggerProcessor};
use graph::slog::Logger;

/// Handles the execution of triggers against runtime hosts, accumulating state.
///
/// This component unifies the trigger processing loop that was previously duplicated
/// for initial triggers and dynamically created data source triggers.
pub struct TriggerRunner<'a, C: Blockchain, T: RuntimeHostBuilder<C>> {
    processor: &'a dyn TriggerProcessor<C, T>,
    logger: &'a Logger,
    metrics: &'a Arc<SubgraphInstanceMetrics>,
    debug_fork: &'a Option<Arc<dyn SubgraphFork>>,
    instrument: bool,
}

impl<'a, C, T> TriggerRunner<'a, C, T>
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    /// Create a new TriggerRunner with the given dependencies.
    pub fn new(
        processor: &'a dyn TriggerProcessor<C, T>,
        logger: &'a Logger,
        metrics: &'a Arc<SubgraphInstanceMetrics>,
        debug_fork: &'a Option<Arc<dyn SubgraphFork>>,
        instrument: bool,
    ) -> Self {
        Self {
            processor,
            logger,
            metrics,
            debug_fork,
            instrument,
        }
    }

    /// Execute a sequence of runnable triggers, accumulating state changes.
    ///
    /// Processes each trigger in order. If any trigger fails with a non-deterministic
    /// error, processing stops and the error is returned. Deterministic errors are
    /// accumulated in the block state.
    pub async fn execute(
        &self,
        block: &Arc<C::Block>,
        runnables: Vec<RunnableTriggers<C>>,
        block_state: BlockState,
        proof_of_indexing: &SharedProofOfIndexing,
        causality_region: &str,
    ) -> Result<BlockState, MappingError> {
        let mut state = block_state;

        for runnable in runnables {
            state = self
                .processor
                .process_trigger(
                    self.logger,
                    runnable.hosted_triggers,
                    block,
                    state,
                    proof_of_indexing,
                    causality_region,
                    self.debug_fork,
                    self.metrics,
                    self.instrument,
                )
                .await
                .map_err(|e| e.add_trigger_context(&runnable.trigger))?;
        }

        Ok(state)
    }
}
