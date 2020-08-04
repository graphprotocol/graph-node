use std::sync::Arc;

use graph::prelude::{
    Aggregate, Counter, Gauge, MetricsRegistry, StopwatchMetrics, SubgraphDeploymentId,
};

pub struct NetworkIndexerMetrics {
    // Overall indexing time, broken down into standard sections
    pub stopwatch: StopwatchMetrics,

    // High-level syncing status
    pub chain_head: Box<Gauge>,
    pub local_head: Box<Gauge>,

    // Reorg stats
    pub reorg_count: Box<Counter>,
    pub reorg_cancel_count: Box<Counter>,
    pub reorg_depth: Aggregate,

    // Different stages of the algorithm
    pub poll_chain_head: Aggregate,
    pub fetch_block_by_number: Aggregate,
    pub fetch_block_by_hash: Aggregate,
    pub fetch_full_block: Aggregate,
    pub fetch_ommers: Aggregate,
    pub load_local_head: Aggregate,
    pub revert_local_head: Aggregate,
    pub write_block: Aggregate,

    // Problems
    pub poll_chain_head_problems: Box<Gauge>,
    pub fetch_block_by_number_problems: Box<Gauge>,
    pub fetch_block_by_hash_problems: Box<Gauge>,
    pub fetch_full_block_problems: Box<Gauge>,
    pub fetch_ommers_problems: Box<Gauge>,
    pub load_local_head_problems: Box<Gauge>,
    pub revert_local_head_problems: Box<Gauge>,
    pub write_block_problems: Box<Gauge>,

    // Timestamp for the last received chain update, i.e.
    // a chain head that was different from before
    pub last_new_chain_head_time: Box<Gauge>,

    // Timestamp for the last written block; if this is old,
    // it indicates that the network indexer is not healthy
    pub last_written_block_time: Box<Gauge>,
}

impl NetworkIndexerMetrics {
    pub fn new(
        subgraph_id: &SubgraphDeploymentId,
        stopwatch: StopwatchMetrics,
        registry: Arc<dyn MetricsRegistry>,
    ) -> Self {
        let make_gauge = |name: &str, help: &str| {
            registry
                .new_deployment_gauge(name, help, subgraph_id.as_str())
                .expect(
                    format!("failed to register metric `{}` for {}", name, subgraph_id).as_str(),
                )
        };

        let make_counter = |name: &str, help: &str| {
            registry
                .new_deployment_counter(name, help, subgraph_id.as_str())
                .expect(
                    format!("failed to register metric `{}` for {}", name, subgraph_id).as_str(),
                )
        };

        Self {
            stopwatch,

            chain_head: make_gauge("deployment_chain_head", "The current chain head block"),

            local_head: make_gauge("deployment_local_head", "The current local head block"),

            reorg_count: make_counter("deployment_reorg_count", "The number of reorgs handled"),

            reorg_cancel_count: make_counter(
                "deployment_reorg_cancel_count",
                "The number of reorgs that had to be canceled / restarted",
            ),

            reorg_depth: Aggregate::new(
                "deployment_reorg_depth",
                subgraph_id.as_str(),
                "The depth of reorgs over time",
                registry.clone(),
            ),

            poll_chain_head: Aggregate::new(
                "deployment_poll_chain_head",
                subgraph_id.as_str(),
                "Polling the network's chain head",
                registry.clone(),
            ),

            fetch_block_by_number: Aggregate::new(
                "deployment_fetch_block_by_number",
                subgraph_id.as_str(),
                "Fetching a block using a block number",
                registry.clone(),
            ),

            fetch_block_by_hash: Aggregate::new(
                "deployment_fetch_block_by_hash",
                subgraph_id.as_str(),
                "Fetching a block using a block hash",
                registry.clone(),
            ),

            fetch_full_block: Aggregate::new(
                "deployment_fetch_full_block",
                subgraph_id.as_str(),
                "Fetching a full block",
                registry.clone(),
            ),

            fetch_ommers: Aggregate::new(
                "deployment_fetch_ommers",
                subgraph_id.as_str(),
                "Fetching the ommers of a block",
                registry.clone(),
            ),

            load_local_head: Aggregate::new(
                "deployment_load_local_head",
                subgraph_id.as_str(),
                "Load the local head block from the store",
                registry.clone(),
            ),

            revert_local_head: Aggregate::new(
                "deployment_revert_local_head",
                subgraph_id.as_str(),
                "Revert the local head block in the store",
                registry.clone(),
            ),

            write_block: Aggregate::new(
                "deployment_write_block",
                subgraph_id.as_str(),
                "Write a block to the store",
                registry.clone(),
            ),

            poll_chain_head_problems: make_gauge(
                "deployment_poll_chain_head_problems",
                "Problems polling the chain head",
            ),

            fetch_block_by_number_problems: make_gauge(
                "deployment_fetch_block_by_number_problems",
                "Problems fetching a block by number",
            ),

            fetch_block_by_hash_problems: make_gauge(
                "deployment_fetch_block_by_hash_problems",
                "Problems fetching a block by hash",
            ),

            fetch_full_block_problems: make_gauge(
                "deployment_fetch_full_block_problems",
                "Problems fetching a full block",
            ),

            fetch_ommers_problems: make_gauge(
                "deployment_fetch_ommers_problems",
                "Problems fetching ommers of a block",
            ),

            load_local_head_problems: make_gauge(
                "deployment_load_local_head_problems",
                "Problems loading the local head block",
            ),

            revert_local_head_problems: make_gauge(
                "deployment_revert_local_head_problems",
                "Problems reverting the local head block during a reorg",
            ),

            write_block_problems: make_gauge(
                "deployment_write_block_problems",
                "Problems writing a block to the store",
            ),

            last_new_chain_head_time: make_gauge(
                "deployment_last_new_chain_head_time",
                "The last time a chain head was received that was different from before",
            ),

            last_written_block_time: make_gauge(
                "deployment_last_written_block_time",
                "The last time a block was written to the store",
            ),
        }
    }
}
