use std::sync::Arc;

use async_trait::async_trait;
use graph::blockchain::{Block, Blockchain};
use graph::cheap_clone::CheapClone;
use graph::components::store::SubgraphFork;
use graph::components::subgraph::{MappingError, SharedProofOfIndexing};
use graph::prelude::tokio::time::Instant;
use graph::prelude::{BlockState, RuntimeHost, RuntimeHostBuilder};
use graph::slog::Logger;

use super::metrics::SubgraphInstanceMetrics;

#[async_trait]
pub trait TriggerProcessor<C, T>: Sized + Sync + Send
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    async fn process_trigger(
        &self,
        logger: &Logger,
        hosts: &[Arc<T::Host>],
        block: &Arc<C::Block>,
        trigger: &C::TriggerData,
        mut state: BlockState<C>,
        proof_of_indexing: &SharedProofOfIndexing,
        causality_region: &str,
        debug_fork: &Option<Arc<dyn SubgraphFork>>,
        subgraph_metrics: &Arc<SubgraphInstanceMetrics>,
    ) -> Result<BlockState<C>, MappingError>;
}

pub struct SubgraphTriggerProcessor {}

#[async_trait]
impl<C, T> TriggerProcessor<C, T> for SubgraphTriggerProcessor
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    async fn process_trigger(
        &self,
        logger: &Logger,
        hosts: &[Arc<T::Host>],
        block: &Arc<C::Block>,
        trigger: &C::TriggerData,
        mut state: BlockState<C>,
        proof_of_indexing: &SharedProofOfIndexing,
        causality_region: &str,
        debug_fork: &Option<Arc<dyn SubgraphFork>>,
        subgraph_metrics: &Arc<SubgraphInstanceMetrics>,
    ) -> Result<BlockState<C>, MappingError> {
        let error_count = state.deterministic_errors.len();

        if let Some(proof_of_indexing) = proof_of_indexing {
            proof_of_indexing
                .borrow_mut()
                .start_handler(causality_region);
        }

        for host in hosts {
            let mapping_trigger = match host.match_and_decode(trigger, block, logger)? {
                // Trigger matches and was decoded as a mapping trigger.
                Some(mapping_trigger) => mapping_trigger,

                // Trigger does not match, do not process it.
                None => continue,
            };

            let start = Instant::now();
            state = host
                .process_mapping_trigger(
                    logger,
                    block.ptr(),
                    mapping_trigger,
                    state,
                    proof_of_indexing.cheap_clone(),
                    debug_fork,
                )
                .await?;
            let elapsed = start.elapsed().as_secs_f64();
            subgraph_metrics.observe_trigger_processing_duration(elapsed);
        }

        if let Some(proof_of_indexing) = proof_of_indexing {
            if state.deterministic_errors.len() != error_count {
                assert!(state.deterministic_errors.len() == error_count + 1);

                // If a deterministic error has happened, write a new
                // ProofOfIndexingEvent::DeterministicError to the SharedProofOfIndexing.
                proof_of_indexing
                    .borrow_mut()
                    .write_deterministic_error(&logger, causality_region);
            }
        }

        Ok(state)
    }
}
