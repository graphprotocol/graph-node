use std::sync::Arc;

use async_trait::async_trait;
use slog::Logger;

use super::store::SubgraphFork;
use super::subgraph::{BlockState, MappingError, RuntimeHostBuilder, SharedProofOfIndexing};
use crate::blockchain::Blockchain;
use crate::data_source::TriggerData;
use crate::prelude::SubgraphInstanceMetrics;

#[async_trait]
pub trait TriggerProcessor<C, T>: Sync + Send
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    async fn process_trigger(
        &self,
        logger: &Logger,
        hosts: &[Arc<T::Host>],
        block: &Arc<C::Block>,
        trigger: &TriggerData<C>,
        mut state: BlockState<C>,
        proof_of_indexing: &SharedProofOfIndexing,
        causality_region: &str,
        debug_fork: &Option<Arc<dyn SubgraphFork>>,
        subgraph_metrics: &Arc<SubgraphInstanceMetrics>,
    ) -> Result<BlockState<C>, MappingError>;
}
