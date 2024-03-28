use std::sync::Arc;

use async_trait::async_trait;
use slog::Logger;

use crate::{
    blockchain::Blockchain,
    data_source::{MappingTrigger, TriggerData, TriggerWithHandler},
    prelude::SubgraphInstanceMetrics,
};

use super::{
    store::SubgraphFork,
    subgraph::{BlockState, MappingError, RuntimeHost, RuntimeHostBuilder, SharedProofOfIndexing},
};

/// A trigger that is almost ready to run: we have a host to run it on, and
/// transformed the `TriggerData` into a `MappingTrigger`.
pub struct HostedTrigger<'a, C>
where
    C: Blockchain,
{
    pub host: &'a dyn RuntimeHost<C>,
    pub mapping_trigger: TriggerWithHandler<MappingTrigger<C>>,
}

/// The `TriggerData` and the `HostedTriggers` that were derived from it. We
/// need to hang on to the `TriggerData` solely for error reporting.
pub struct RunnableTriggers<'a, C>
where
    C: Blockchain,
{
    pub trigger: TriggerData<C>,
    pub hosted_triggers: Vec<HostedTrigger<'a, C>>,
}

#[async_trait]
pub trait TriggerProcessor<C, T>: Sync + Send
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    async fn process_trigger<'a>(
        &'a self,
        logger: &Logger,
        triggers: Vec<HostedTrigger<'a, C>>,
        block: &Arc<C::Block>,
        mut state: BlockState,
        proof_of_indexing: &SharedProofOfIndexing,
        causality_region: &str,
        debug_fork: &Option<Arc<dyn SubgraphFork>>,
        subgraph_metrics: &Arc<SubgraphInstanceMetrics>,
        instrument: bool,
    ) -> Result<BlockState, MappingError>;
}

/// A trait for taking triggers as `TriggerData` (usually from the block
/// stream) and turning them into `HostedTrigger`s that are ready to run.
///
/// The output triggers will be run in the order in which they are returned.
pub trait Decoder<C, T>: Sync + Send
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    fn match_and_decode<'a>(
        &'a self,
        logger: &Logger,
        block: &Arc<C::Block>,
        trigger: TriggerData<C>,
        hosts: Box<dyn Iterator<Item = &'a T::Host> + Send + 'a>,
        subgraph_metrics: &Arc<SubgraphInstanceMetrics>,
    ) -> Result<RunnableTriggers<'a, C>, MappingError>;

    fn match_and_decode_many<'a, F>(
        &'a self,
        logger: &Logger,
        block: &Arc<C::Block>,
        triggers: Box<dyn Iterator<Item = TriggerData<C>>>,
        hosts_filter: F,
        subgraph_metrics: &Arc<SubgraphInstanceMetrics>,
    ) -> Result<Vec<RunnableTriggers<'a, C>>, MappingError>
    where
        F: Fn(&TriggerData<C>) -> Box<dyn Iterator<Item = &'a T::Host> + Send + 'a>,
    {
        let mut runnables = vec![];
        for trigger in triggers {
            let hosts = hosts_filter(&trigger);
            match self.match_and_decode(logger, block, trigger, hosts, subgraph_metrics) {
                Ok(runnable_triggers) => runnables.push(runnable_triggers),
                Err(e) => return Err(e),
            }
        }
        Ok(runnables)
    }
}
