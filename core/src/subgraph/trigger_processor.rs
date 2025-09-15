use async_trait::async_trait;
use graph::blockchain::{Block, Blockchain, DecoderHook as _};
use graph::cheap_clone::CheapClone;
use graph::components::store::SubgraphFork;
use graph::components::subgraph::{MappingError, SharedProofOfIndexing};
use graph::components::trigger_processor::{HostedTrigger, RunnableTriggers};
use graph::data_source::TriggerData;
use graph::prelude::tokio::sync::Semaphore;
use graph::prelude::tokio::time::{sleep, Duration, Instant};
use graph::prelude::{
    BlockState, DeploymentHash, RuntimeHost, RuntimeHostBuilder, SubgraphInstanceMetrics,
    TriggerProcessor,
};
use graph::slog::{debug, warn, Logger};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

// Use the standard library's hasher for now
use std::collections::hash_map::DefaultHasher;

/// Configuration for the trigger processor
#[derive(Clone, Debug)]
pub struct TriggerProcessorConfig {
    /// Enable sharded processing (false = legacy single semaphore mode)
    pub enable_sharding: bool,
    /// Number of shards (pools) to create when sharding is enabled
    pub num_shards: usize,
    /// Number of worker threads per shard (or total when not sharding)
    pub workers_per_shard: usize,
    /// Maximum queue size per subgraph before applying backpressure
    pub max_queue_per_subgraph: usize,
    /// Time window for fair scheduling (ms)
    pub fairness_window_ms: u64,
}

impl Default for TriggerProcessorConfig {
    fn default() -> Self {
        Self {
            // Default to legacy mode to not surprise existing users
            enable_sharding: false,
            // When sharding is disabled, this is ignored
            num_shards: 1,
            // Default to 32 workers (same as before)
            workers_per_shard: 32,
            // Prevent any single subgraph from queuing too much work
            max_queue_per_subgraph: 100,
            // Ensure each subgraph gets processing time within 100ms
            fairness_window_ms: 100,
        }
    }
}

/// Tracks per-shard load and metrics
#[derive(Debug)]
struct ShardMetrics {
    /// Current number of active permits
    active_permits: AtomicUsize,
    /// Total triggers processed
    total_processed: AtomicUsize,
    /// Number of subgraphs assigned to this shard
    assigned_subgraphs: AtomicUsize,
}

impl ShardMetrics {
    fn new() -> Self {
        Self {
            active_permits: AtomicUsize::new(0),
            total_processed: AtomicUsize::new(0),
            assigned_subgraphs: AtomicUsize::new(0),
        }
    }
}

/// Tracks per-subgraph state for backpressure
#[derive(Debug, Clone)]
struct SubgraphState {
    /// Current queue depth for this subgraph
    queue_depth: Arc<AtomicUsize>,
    /// Shard assignment for this subgraph
    shard_id: usize,
}

/// Scalable trigger processor that optionally shards subgraphs across multiple pools
#[derive(Clone)]
pub struct SubgraphTriggerProcessor {
    /// Semaphores for concurrency control
    /// In legacy mode: single semaphore
    /// In sharded mode: one semaphore per shard
    semaphores: Vec<Arc<Semaphore>>,
    /// Track subgraph to shard assignments for consistent routing
    /// Using RwLock instead of DashMap for simplicity
    subgraph_shards: Arc<RwLock<HashMap<DeploymentHash, SubgraphState>>>,
    /// Metrics per shard
    shard_metrics: Arc<Vec<ShardMetrics>>,
    /// Configuration
    config: TriggerProcessorConfig,
}

impl SubgraphTriggerProcessor {
    pub fn new(config: TriggerProcessorConfig) -> Self {
        let effective_shards = if config.enable_sharding {
            config.num_shards.max(1)
        } else {
            1 // Legacy mode: single semaphore
        };

        let mut semaphores = Vec::with_capacity(effective_shards);
        let mut shard_metrics = Vec::with_capacity(effective_shards);

        // Create semaphores and metrics
        for _ in 0..effective_shards {
            semaphores.push(Arc::new(Semaphore::new(config.workers_per_shard)));
            shard_metrics.push(ShardMetrics::new());
        }

        Self {
            semaphores,
            subgraph_shards: Arc::new(RwLock::new(HashMap::new())),
            shard_metrics: Arc::new(shard_metrics),
            config,
        }
    }

    /// Get or assign a shard for a deployment using consistent hashing
    fn get_shard_for_deployment(&self, deployment: &DeploymentHash) -> usize {
        // Check if already assigned
        {
            let shards = self.subgraph_shards.read().unwrap();
            if let Some(state) = shards.get(deployment) {
                return state.shard_id;
            }
        }

        // Assign new shard using DefaultHasher
        let mut hasher = DefaultHasher::new();
        deployment.hash(&mut hasher);
        let shard_id = (hasher.finish() as usize) % self.semaphores.len();

        // Track the assignment
        let state = SubgraphState {
            queue_depth: Arc::new(AtomicUsize::new(0)),
            shard_id,
        };

        {
            let mut shards = self.subgraph_shards.write().unwrap();
            shards.insert(deployment.clone(), state);
        }

        self.shard_metrics[shard_id]
            .assigned_subgraphs
            .fetch_add(1, Ordering::Relaxed);

        shard_id
    }

    /// Get or create subgraph state
    fn get_or_create_subgraph_state(&self, deployment: &DeploymentHash) -> SubgraphState {
        // Atomically check, insert, and return the subgraph state under a write lock
        let mut shards = self.subgraph_shards.write().unwrap();
        if let Some(state) = shards.get(deployment) {
            return state.clone();
        }

        // Assign new shard using DefaultHasher
        let mut hasher = DefaultHasher::new();
        deployment.hash(&mut hasher);
        let shard_id = (hasher.finish() as usize) % self.semaphores.len();

        // Track the assignment
        let state = SubgraphState {
            queue_depth: Arc::new(AtomicUsize::new(0)),
            shard_id,
        };
        shards.insert(deployment.clone(), state.clone());

        self.shard_metrics[shard_id]
            .assigned_subgraphs
            .fetch_add(1, Ordering::Relaxed);

        state
    }

    /// Apply backpressure if queue is too deep
    async fn apply_backpressure(
        &self,
        logger: &Logger,
        deployment: &DeploymentHash,
        queue_depth: usize,
    ) {
        if queue_depth > self.config.max_queue_per_subgraph {
            warn!(logger, "Applying backpressure for overloaded subgraph";
                "deployment" => deployment.to_string(),
                "queue_depth" => queue_depth,
                "max_allowed" => self.config.max_queue_per_subgraph
            );

            // Exponential backoff based on queue depth
            let delay_ms =
                ((queue_depth - self.config.max_queue_per_subgraph) * 10).min(1000) as u64; // Cap at 1 second
            sleep(Duration::from_millis(delay_ms)).await;
        }
    }


    /// Get comprehensive metrics for monitoring
    pub async fn get_metrics(&self) -> HashMap<String, usize> {
        let mut metrics = HashMap::new();

        // Basic configuration metrics
        metrics.insert(
            "sharding_enabled".to_string(),
            self.config.enable_sharding as usize,
        );
        metrics.insert("total_shards".to_string(), self.semaphores.len());
        metrics.insert(
            "workers_per_shard".to_string(),
            self.config.workers_per_shard,
        );
        metrics.insert(
            "max_queue_per_subgraph".to_string(),
            self.config.max_queue_per_subgraph,
        );

        // Per-shard metrics
        for (i, (semaphore, shard_metric)) in self
            .semaphores
            .iter()
            .zip(self.shard_metrics.iter())
            .enumerate()
        {
            let available_permits = semaphore.available_permits();
            let active_permits = shard_metric.active_permits.load(Ordering::Relaxed);
            let total_processed = shard_metric.total_processed.load(Ordering::Relaxed);
            let assigned_subgraphs = shard_metric.assigned_subgraphs.load(Ordering::Relaxed);

            metrics.insert(format!("shard_{}_permits_available", i), available_permits);
            metrics.insert(format!("shard_{}_permits_active", i), active_permits);
            metrics.insert(format!("shard_{}_total_processed", i), total_processed);
            metrics.insert(
                format!("shard_{}_assigned_subgraphs", i),
                assigned_subgraphs,
            );
        }

        // Overall statistics
        let shards = self.subgraph_shards.read().unwrap();
        metrics.insert("total_assigned_subgraphs".to_string(), shards.len());

        // Calculate load imbalance
        if self.config.enable_sharding && self.semaphores.len() > 1 {
            let loads: Vec<usize> = self
                .shard_metrics
                .iter()
                .map(|m| m.assigned_subgraphs.load(Ordering::Relaxed))
                .collect();

            let max_load = *loads.iter().max().unwrap_or(&0);
            let min_load = *loads.iter().min().unwrap_or(&0);
            let imbalance = if min_load > 0 {
                ((max_load - min_load) * 100) / min_load
            } else {
                0
            };

            metrics.insert("shard_imbalance_percent".to_string(), imbalance);
        }

        metrics
    }

    /// Get detailed status for a specific deployment
    pub fn get_deployment_status(
        &self,
        deployment: &DeploymentHash,
    ) -> Option<HashMap<String, usize>> {
        let shards = self.subgraph_shards.read().unwrap();
        shards.get(deployment).map(|state| {
            let mut status = HashMap::new();
            status.insert("shard_id".to_string(), state.shard_id);
            status.insert(
                "queue_depth".to_string(),
                state.queue_depth.load(Ordering::Relaxed),
            );
            status
        })
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
        if triggers.is_empty() {
            return Ok(state);
        }

        // Create a synthetic deployment hash from data source name for consistent sharding.
        // This ensures triggers from the same data source/subgraph are always routed to 
        // the same shard, maintaining cache locality.
        let data_source_name = triggers[0].host.data_source().name();
        let deployment_hash = DeploymentHash::new(data_source_name)
            .unwrap_or_else(|_| DeploymentHash::new("unknown").unwrap());

        // Determine shard assignment
        let shard_id = if self.config.enable_sharding {
            self.get_shard_for_deployment(&deployment_hash)
        } else {
            0 // Legacy mode: always use first (and only) semaphore
        };

        let semaphore = &self.semaphores[shard_id];

        // Get subgraph state for backpressure
        let subgraph_state = self.get_or_create_subgraph_state(&deployment_hash);

        // Check current queue depth before adding new triggers (avoid increment-then-check)
        let current_queue_depth = subgraph_state.queue_depth.load(Ordering::Relaxed);
        let projected_queue_depth = current_queue_depth + triggers.len();

        // Apply backpressure if needed BEFORE incrementing queue depth
        self.apply_backpressure(logger, &deployment_hash, projected_queue_depth)
            .await;

        // Only increment queue depth after backpressure check passes
        subgraph_state
            .queue_depth
            .fetch_add(triggers.len(), Ordering::Relaxed);

        debug!(logger, "Processing triggers";
            "deployment" => deployment_hash.to_string(),
            "shard" => shard_id,
            "trigger_count" => triggers.len(),
            "sharding_enabled" => self.config.enable_sharding
        );

        proof_of_indexing.start_handler(causality_region);

        // Track processed triggers to ensure proper queue depth cleanup
        let mut processed_count = 0;

        // Use a closure to ensure queue depth is properly decremented on any exit path
        let process_result = async {
            for HostedTrigger {
                host,
                mapping_trigger,
            } in triggers
            {
                // Acquire permit and hold it during processing
                let permit = semaphore.acquire().await.unwrap();

                // Track active permits
                self.shard_metrics[shard_id]
                    .active_permits
                    .fetch_add(1, Ordering::Relaxed);

                let start = Instant::now();

                // Process with permit held
                let result = host
                    .process_mapping_trigger(
                        logger,
                        mapping_trigger,
                        state,
                        proof_of_indexing.cheap_clone(),
                        debug_fork,
                        instrument,
                    )
                    .await;

                // Permit is automatically dropped here, releasing it
                drop(permit);

                // Update metrics
                self.shard_metrics[shard_id]
                    .active_permits
                    .fetch_sub(1, Ordering::Relaxed);
                self.shard_metrics[shard_id]
                    .total_processed
                    .fetch_add(1, Ordering::Relaxed);

                // Increment processed count for queue cleanup
                processed_count += 1;

                // Handle result
                state = result?;

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
        };

        // Execute processing and ensure queue depth cleanup regardless of outcome
        let result = process_result.await;
        
        // Always decrement queue depth by the number of processed triggers
        // This ensures cleanup even if processing failed partway through
        if !triggers.is_empty() {
            subgraph_state.queue_depth.fetch_sub(triggers.len(), Ordering::Relaxed);
        }

        result
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
