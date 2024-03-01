use async_trait::async_trait;
use graph::blockchain::{Block, Blockchain};
use graph::cheap_clone::CheapClone;
use graph::components::store::SubgraphFork;
use graph::components::subgraph::{MappingError, SharedProofOfIndexing};
use graph::components::trigger_processor::HostedTrigger;
use graph::data_source::TriggerData;
use graph::prelude::tokio::time::Instant;
use graph::prelude::{
    BlockState, RuntimeHost, RuntimeHostBuilder, SubgraphInstanceMetrics, TriggerProcessor,
};
use graph::slog::Logger;
use std::sync::Arc;

pub struct SubgraphTriggerProcessor {}

#[async_trait]
impl<C, T> TriggerProcessor<C, T> for SubgraphTriggerProcessor
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    async fn process_trigger<'a>(
        &'a self,
        logger: &Logger,
        hosts: Box<dyn Iterator<Item = &'a T::Host> + Send + 'a>,
        block: &Arc<C::Block>,
        trigger: &TriggerData<C>,
        mut state: BlockState,
        proof_of_indexing: &SharedProofOfIndexing,
        causality_region: &str,
        debug_fork: &Option<Arc<dyn SubgraphFork>>,
        subgraph_metrics: &Arc<SubgraphInstanceMetrics>,
        instrument: bool,
    ) -> Result<BlockState, MappingError> {
        let error_count = state.deterministic_errors.len();

        let host_mapping: Vec<HostedTrigger<'a, C>> =
            <Self as graph::prelude::TriggerProcessor<C, T>>::match_and_decode(
                self,
                logger,
                block,
                trigger,
                hosts,
                subgraph_metrics,
            )?;

        if host_mapping.is_empty() {
            return Ok(state);
        }

        if let Some(proof_of_indexing) = proof_of_indexing {
            proof_of_indexing
                .borrow_mut()
                .start_handler(causality_region);
        }

        for HostedTrigger {
            host,
            mapping_trigger,
        } in host_mapping
        {
            let start = Instant::now();
            state = host
                .process_mapping_trigger(
                    logger,
                    mapping_trigger,
                    state,
                    proof_of_indexing.cheap_clone(),
                    debug_fork,
                    instrument,
                )
                .await?;
            let elapsed = start.elapsed().as_secs_f64();
            subgraph_metrics.observe_trigger_processing_duration(elapsed);

            if let Some(ds) = host.data_source().as_offchain() {
                ds.mark_processed_at(block.number());
                // Remove this offchain data source since it has just been processed.
                state
                    .processed_data_sources
                    .push(ds.as_stored_dynamic_data_source());
            }
        }

        if let Some(proof_of_indexing) = proof_of_indexing {
            if state.deterministic_errors.len() != error_count {
                assert!(state.deterministic_errors.len() == error_count + 1);

                // If a deterministic error has happened, write a new
                // ProofOfIndexingEvent::DeterministicError to the SharedProofOfIndexing.
                proof_of_indexing
                    .borrow_mut()
                    .write_deterministic_error(logger, causality_region);
            }
        }

        Ok(state)
    }

    fn match_and_decode<'a>(
        &'a self,
        logger: &Logger,
        block: &Arc<C::Block>,
        trigger: &TriggerData<C>,
        hosts: Box<dyn Iterator<Item = &'a T::Host> + Send + 'a>,
        subgraph_metrics: &Arc<SubgraphInstanceMetrics>,
    ) -> Result<Vec<HostedTrigger<'a, C>>, MappingError> {
        let mut host_mapping = vec![];

        {
            let _section = subgraph_metrics.stopwatch.start_section("match_and_decode");

            for host in hosts {
                let mapping_trigger = match host.match_and_decode(trigger, block, logger)? {
                    // Trigger matches and was decoded as a mapping trigger.
                    Some(mapping_trigger) => mapping_trigger,

                    // Trigger does not match, do not process it.
                    None => continue,
                };

                host_mapping.push(HostedTrigger {
                    host,
                    mapping_trigger,
                });
            }
        }
        Ok(host_mapping)
    }
}
