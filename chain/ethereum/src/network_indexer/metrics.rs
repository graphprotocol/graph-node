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
        subgraph_id: SubgraphDeploymentId,
        stopwatch: StopwatchMetrics,
        registry: Arc<dyn MetricsRegistry>,
    ) -> Self {
        let make_gauge = |name: &str, help: &str| {
            registry
                .new_subgraph_gauge(name, help, subgraph_id.as_str())
                .expect(
                    format!("failed to register metric `{}` for {}", name, subgraph_id).as_str(),
                )
        };

        let make_counter = |name: &str, help: &str| {
            registry
                .new_subgraph_counter(name, help, subgraph_id.as_str())
                .expect(
                    format!("failed to register metric `{}` for {}", name, subgraph_id).as_str(),
                )
        };

        Self {
            stopwatch,

            chain_head: make_gauge("subgraph_chain_head", "The current chain head block"),

            local_head: make_gauge("subgraph_local_head", "The current local head block"),

            reorg_count: make_counter("subgraph_reorg_count", "The number of reorgs handled"),

            reorg_cancel_count: make_counter(
                "subgraph_reorg_cancel_count",
                "The number of reorgs that had to be canceled / restarted",
            ),

            reorg_depth: Aggregate::new(
                "subgraph_reorg_depth",
                subgraph_id.to_string(),
                "The depth of reorgs over time",
                registry.clone(),
            ),

            poll_chain_head: Aggregate::new(
                "subgraph_poll_chain_head",
                subgraph_id.to_string(),
                "Polling the network's chain head",
                registry.clone(),
            ),

            fetch_block_by_number: Aggregate::new(
                "subgraph_fetch_block_by_number",
                subgraph_id.to_string(),
                "Fetching a block using a block number",
                registry.clone(),
            ),

            fetch_block_by_hash: Aggregate::new(
                "subgraph_fetch_block_by_hash",
                subgraph_id.to_string(),
                "Fetching a block using a block hash",
                registry.clone(),
            ),

            fetch_full_block: Aggregate::new(
                "subgraph_fetch_full_block",
                subgraph_id.to_string(),
                "Fetching a full block",
                registry.clone(),
            ),

            fetch_ommers: Aggregate::new(
                "subgraph_fetch_ommers",
                subgraph_id.to_string(),
                "Fetching the ommers of a block",
                registry.clone(),
            ),

            load_local_head: Aggregate::new(
                "subgraph_load_local_head",
                subgraph_id.to_string(),
                "Load the local head block from the store",
                registry.clone(),
            ),

            revert_local_head: Aggregate::new(
                "subgraph_id_revert_local_head",
                subgraph_id.to_string(),
                "Revert the local head block in the store",
                registry.clone(),
            ),

            write_block: Aggregate::new(
                "subgraph_write_block",
                subgraph_id.to_string(),
                "Write a block to the store",
                registry.clone(),
            ),

            poll_chain_head_problems: make_gauge(
                "subgraph_poll_chain_head_problems",
                "Problems polling the chain head",
            ),

            fetch_block_by_number_problems: make_gauge(
                "subgraph_fetch_block_by_number_problems",
                "Problems fetching a block by number",
            ),

            fetch_block_by_hash_problems: make_gauge(
                "subgraph_fetch_block_by_hash_problems",
                "Problems fetching a block by hash",
            ),

            fetch_full_block_problems: make_gauge(
                "subgraph_fetch_full_block_problems",
                "Problems fetching a full block",
            ),

            fetch_ommers_problems: make_gauge(
                "subgraph_fetch_ommers_problems",
                "Problems fetching ommers of a block",
            ),

            load_local_head_problems: make_gauge(
                "subgraph_load_local_head_problems",
                "Problems loading the local head block",
            ),

            revert_local_head_problems: make_gauge(
                "subgraph_revert_local_head_problems",
                "Problems reverting the local head block during a reorg",
            ),

            write_block_problems: make_gauge(
                "subgraph_write_block_problems",
                "Problems writing a block to the store",
            ),

            last_new_chain_head_time: make_gauge(
                "subgraph_last_new_chain_head_time",
                "The last time a chain head was received that was different from before",
            ),

            last_written_block_time: make_gauge(
                "subgraph_last_written_block_time",
                "The last time a block was written to the store",
            ),
        }
    }
}
