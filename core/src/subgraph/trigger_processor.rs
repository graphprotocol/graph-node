use async_trait::async_trait;
use graph::blockchain::{Block, Blockchain, DecoderHook as _};
use graph::cheap_clone::CheapClone;
use graph::components::store::SubgraphFork;
use graph::components::subgraph::{MappingError, SharedProofOfIndexing};
use graph::components::trigger_processor::{HostedTrigger, RunnableTriggers};
use graph::data_source::TriggerData;
use graph::prelude::tokio::sync::Semaphore;
use graph::prelude::tokio::time::{Duration, Instant};
use graph::prelude::{
    BlockState, RuntimeHost, RuntimeHostBuilder, SubgraphInstanceMetrics,
    TriggerProcessor,
};
use graph::slog::{debug, Logger};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

/// Configuration for the trigger processor
#[derive(Clone, Debug)]
pub struct TriggerProcessorConfig {
    /// Number of shards (pools) to create
    pub num_shards: usize,
    /// Number of worker threads per shard
    pub workers_per_shard: usize,
    /// Maximum queue size per subgraph before applying backpressure
    pub max_queue_per_subgraph: usize,
    /// Time window for fair scheduling (ms)
    pub fairness_window_ms: u64,
}

impl Default for TriggerProcessorConfig {
    fn default() -> Self {
        Self {
            // For 2500 subgraphs on 32 vCPUs:
            // 32 shards = ~78 subgraphs per shard
            num_shards: 32,
            // 32 workers per shard = 1024 total concurrent executions
            workers_per_shard: 32,
            // Prevent any single subgraph from queuing too much work
            max_queue_per_subgraph: 100,
            // Ensure each subgraph gets processing time within 100ms
            fairness_window_ms: 100,
        }
    }
}


/// Scalable trigger processor that shards subgraphs across multiple pools
#[derive(Clone)]
pub struct SubgraphTriggerProcessor {
    // Use multiple semaphores for sharding instead of complex worker pools
    semaphores: Vec<Arc<Semaphore>>,
    config: TriggerProcessorConfig,
}

impl SubgraphTriggerProcessor {
    pub fn new(config: TriggerProcessorConfig) -> Self {
        let mut semaphores = Vec::with_capacity(config.num_shards);

        // Create a semaphore per shard
        for _ in 0..config.num_shards {
            semaphores.push(Arc::new(Semaphore::new(config.workers_per_shard)));
        }

        Self {
            semaphores,
            config,
        }
    }

}

#[async_trait]
impl<C, T> TriggerProcessor<C, T> for SubgraphTriggerProcessor
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    async fn process_trigger<'a>(
        &'a self,
        logger: &Logger,
        triggers: Vec<HostedTrigger<'a, C>>,
        _block: &Arc<C::Block>,
        mut state: BlockState,
        proof_of_indexing: &SharedProofOfIndexing,
        causality_region: &str,
        debug_fork: &Option<Arc<dyn SubgraphFork>>,
        subgraph_metrics: &Arc<SubgraphInstanceMetrics>,
        instrument: bool,
    ) -> Result<BlockState, MappingError> {
        // Use the data source name as a hash to determine shard
        // This ensures consistent sharding for the same data source/subgraph
        let shard_id = if let Some(first_trigger) = triggers.first() {
            let data_source_name = first_trigger.host.data_source().name();
            let hash = data_source_name
                .bytes()
                .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
            (hash as usize) % self.config.num_shards
        } else {
            return Ok(state);
        };
        let semaphore = &self.semaphores[shard_id];

        debug!(logger, "Processing triggers in shard";
            "shard" => shard_id,
            "trigger_count" => triggers.len()
        );

        proof_of_indexing.start_handler(causality_region);

        for HostedTrigger {
            host,
            mapping_trigger,
        } in triggers
        {
            // Acquire permit from the specific shard
            let _permit = semaphore.acquire().await.unwrap();

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

            let elapsed = start.elapsed();
            subgraph_metrics.observe_trigger_processing_duration(elapsed.as_secs_f64());

            if elapsed > Duration::from_secs(30) {
                debug!(logger, "Trigger processing took a long time";
                    "duration_ms" => elapsed.as_millis(),
                    "shard" => shard_id
                );
            }
        }

        Ok(state)
    }
}

impl SubgraphTriggerProcessor {
    /// Get metrics for monitoring
    pub async fn get_metrics(&self) -> HashMap<String, usize> {
        let mut metrics = HashMap::new();

        for (i, semaphore) in self.semaphores.iter().enumerate() {
            let available_permits = semaphore.available_permits();
            let total_permits = self.config.workers_per_shard;
            let in_use = total_permits - available_permits;

            metrics.insert(format!("shard_{}_permits_in_use", i), in_use);
            metrics.insert(format!("shard_{}_permits_available", i), available_permits);
        }

        metrics.insert("total_shards".to_string(), self.config.num_shards);
        metrics.insert("workers_per_shard".to_string(), self.config.workers_per_shard);

        metrics
    }
}

pub struct Decoder<C, T>
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    hook: C::DecoderHook,
    _builder: PhantomData<T>,
}

impl<C, T> Decoder<C, T>
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    pub fn new(hook: C::DecoderHook) -> Self {
        Decoder {
            hook,
            _builder: PhantomData,
        }
    }
}

impl<C: Blockchain, T: RuntimeHostBuilder<C>> Decoder<C, T> {
    fn match_and_decode_inner<'a>(
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

    pub(crate) fn match_and_decode<'a>(
        &'a self,
        logger: &Logger,
        block: &Arc<C::Block>,
        trigger: TriggerData<C>,
        hosts: Box<dyn Iterator<Item = &'a T::Host> + Send + 'a>,
        subgraph_metrics: &Arc<SubgraphInstanceMetrics>,
    ) -> Result<RunnableTriggers<'a, C>, MappingError> {
        self.match_and_decode_inner(logger, block, &trigger, hosts, subgraph_metrics)
            .map_err(|e| e.add_trigger_context(&trigger))
            .map(|hosted_triggers| RunnableTriggers {
                trigger,
                hosted_triggers,
            })
    }

    pub(crate) async fn match_and_decode_many<'a, F>(
        &'a self,
        logger: &Logger,
        block: &Arc<C::Block>,
        triggers: impl Iterator<Item = TriggerData<C>>,
        hosts_filter: F,
        metrics: &Arc<SubgraphInstanceMetrics>,
    ) -> Result<Vec<RunnableTriggers<'a, C>>, MappingError>
    where
        F: Fn(&TriggerData<C>) -> Box<dyn Iterator<Item = &'a T::Host> + Send + 'a>,
    {
        let mut runnables = vec![];
        for trigger in triggers {
            let hosts = hosts_filter(&trigger);
            match self.match_and_decode(logger, block, trigger, hosts, metrics) {
                Ok(runnable_triggers) => runnables.push(runnable_triggers),
                Err(e) => return Err(e),
            }
        }
        self.hook
            .after_decode(logger, &block.ptr(), runnables, metrics)
            .await
    }
}
