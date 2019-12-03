use std::collections::HashMap;
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
    pub collect_reorg_data: Aggregate,
    pub fetch_new_blocks: Aggregate,
    pub load_parent_block: Aggregate,
    pub find_common_ancestor: Aggregate,
    pub revert_blocks: Aggregate,
    pub write_block: Aggregate,

    // Problems
    pub poll_chain_head_problems: Box<Gauge>,
    pub fetch_block_by_number_problems: Box<Gauge>,
    pub fetch_block_by_hash_problems: Box<Gauge>,
    pub fetch_full_block_problems: Box<Gauge>,
    pub fetch_ommers_problems: Box<Gauge>,
    pub load_local_head_problems: Box<Gauge>,
    pub collect_reorg_data_problems: Box<Gauge>,
    pub fetch_new_blocks_problems: Box<Gauge>,
    pub load_parent_block_problems: Box<Gauge>,
    pub find_common_ancestor_problems: Box<Gauge>,
    pub revert_blocks_problems: Box<Gauge>,
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
        Self {
            stopwatch,

            chain_head: registry
                .new_gauge(
                    format!("{}_chain_head", subgraph_id),
                    "The current chain head block".into(),
                    HashMap::new(),
                )
                .expect(format!("failed to register metric `{}_chain_head`", subgraph_id).as_str()),

            local_head: registry
                .new_gauge(
                    format!("{}_local_head", subgraph_id),
                    "The current local head block".into(),
                    HashMap::new(),
                )
                .expect(format!("failed to register metric `{}_local_head`", subgraph_id).as_str()),

            reorg_count: registry
                .new_counter(
                    format!("{}_reorg_count", subgraph_id),
                    "The number of reorgs handled".into(),
                    HashMap::new(),
                )
                .expect(
                    format!("failed to register metric `{}_reorg_count`", subgraph_id).as_str(),
                ),

            reorg_cancel_count: registry
                .new_counter(
                    format!("{}_reorg_cancel_count", subgraph_id),
                    "The number of reorgs that had to be canceled / restarted".into(),
                    HashMap::new(),
                )
                .expect(
                    format!(
                        "failed to register metric `{}_reorg_cancel_count`",
                        subgraph_id
                    )
                    .as_str(),
                ),

            reorg_depth: Aggregate::new(
                format!("{}_reorg_depth", subgraph_id),
                "The depth of reorgs over time",
                registry.clone(),
            ),

            poll_chain_head: Aggregate::new(
                format!("{}_poll_chain_head", subgraph_id),
                "Polling the network's chain head",
                registry.clone(),
            ),

            fetch_block_by_number: Aggregate::new(
                format!("{}_fetch_block_by_number", subgraph_id),
                "Fetching a block using a block number",
                registry.clone(),
            ),

            fetch_block_by_hash: Aggregate::new(
                format!("{}_fetch_block_by_hash", subgraph_id),
                "Fetching a block using a block hash",
                registry.clone(),
            ),

            fetch_full_block: Aggregate::new(
                format!("{}_fetch_full_block", subgraph_id),
                "Fetching a full block",
                registry.clone(),
            ),

            fetch_ommers: Aggregate::new(
                format!("{}_fetch_ommers", subgraph_id),
                "Fetching the ommers of a block",
                registry.clone(),
            ),

            load_local_head: Aggregate::new(
                format!("{}_load_local_head", subgraph_id),
                "Load the local head block from the store",
                registry.clone(),
            ),

            collect_reorg_data: Aggregate::new(
                format!("{}_collect_reorg_data", subgraph_id),
                "Collect data to handle a reorg",
                registry.clone(),
            ),

            fetch_new_blocks: Aggregate::new(
                format!("{}_fetch_new_blocks", subgraph_id),
                "Fetch new blocks for a reorg",
                registry.clone(),
            ),

            load_parent_block: Aggregate::new(
                format!("{}_load_parent_block", subgraph_id),
                "Load the parent of a block from the store",
                registry.clone(),
            ),

            find_common_ancestor: Aggregate::new(
                format!("{}_find_common_ancestor", subgraph_id),
                "Identify the common ancestor for a reorg",
                registry.clone(),
            ),

            revert_blocks: Aggregate::new(
                format!("{}_revert_blocks", subgraph_id),
                "Revert blocks in the store",
                registry.clone(),
            ),

            write_block: Aggregate::new(
                format!("{}_write_block", subgraph_id),
                "Write a block to the store",
                registry.clone(),
            ),

            poll_chain_head_problems: registry
                .new_gauge(
                    format!("{}_poll_chain_head_problems", subgraph_id),
                    "Problems polling the chain head".into(),
                    HashMap::new(),
                )
                .expect(
                    format!(
                        "failed to create metric `{}_poll_chain_head_problems",
                        subgraph_id
                    )
                    .as_str(),
                ),

            fetch_block_by_number_problems: registry
                .new_gauge(
                    format!("{}_fetch_block_by_number_problems", subgraph_id),
                    "Problems fetching a block by number".into(),
                    HashMap::new(),
                )
                .expect(
                    format!(
                        "failed to create metric `{}_fetch_block_by_number_problems",
                        subgraph_id
                    )
                    .as_str(),
                ),

            fetch_block_by_hash_problems: registry
                .new_gauge(
                    format!("{}_fetch_block_by_hash_problems", subgraph_id),
                    "Problems fetching a block by hash".into(),
                    HashMap::new(),
                )
                .expect(
                    format!(
                        "failed to create metric `{}_fetch_block_by_hash_problems",
                        subgraph_id
                    )
                    .as_str(),
                ),

            fetch_full_block_problems: registry
                .new_gauge(
                    format!("{}_fetch_full_block_problems", subgraph_id),
                    "Problems fetching a full block".into(),
                    HashMap::new(),
                )
                .expect(
                    format!(
                        "failed to create metric `{}_fetch_full_block_problems",
                        subgraph_id
                    )
                    .as_str(),
                ),

            fetch_ommers_problems: registry
                .new_gauge(
                    format!("{}_fetch_ommers_problems", subgraph_id),
                    "Problems fetching ommers of a block".into(),
                    HashMap::new(),
                )
                .expect(
                    format!(
                        "failed to create metric `{}_fetch_ommers_problems",
                        subgraph_id
                    )
                    .as_str(),
                ),

            load_local_head_problems: registry
                .new_gauge(
                    format!("{}_load_local_head_problems", subgraph_id),
                    "Problems loading the local head block".into(),
                    HashMap::new(),
                )
                .expect(
                    format!(
                        "failed to create metric `{}_load_local_head_problems",
                        subgraph_id
                    )
                    .as_str(),
                ),

            collect_reorg_data_problems: registry
                .new_gauge(
                    format!("{}_collect_reorg_data_problems", subgraph_id),
                    "Problems collecting data for handling a reorg".into(),
                    HashMap::new(),
                )
                .expect(
                    format!(
                        "failed to create metric `{}_collect_reorg_data_problems",
                        subgraph_id
                    )
                    .as_str(),
                ),

            fetch_new_blocks_problems: registry
                .new_gauge(
                    format!("{}_fetch_new_blocks_problems", subgraph_id),
                    "Problems fetching new blocks for a reorg".into(),
                    HashMap::new(),
                )
                .expect(
                    format!(
                        "failed to create metric `{}_fetch_new_blocks_problems",
                        subgraph_id
                    )
                    .as_str(),
                ),

            load_parent_block_problems: registry
                .new_gauge(
                    format!("{}_load_parent_block_problems", subgraph_id),
                    "Problems loading a parent block from the store".into(),
                    HashMap::new(),
                )
                .expect(
                    format!(
                        "failed to create metric `{}_load_parent_block_problems",
                        subgraph_id
                    )
                    .as_str(),
                ),

            find_common_ancestor_problems: registry
                .new_gauge(
                    format!("{}_find_common_ancestor_problems", subgraph_id),
                    "Problems identifying the common ancestor for a reorg".into(),
                    HashMap::new(),
                )
                .expect(
                    format!(
                        "failed to create metric `{}_find_common_ancestor_problems",
                        subgraph_id
                    )
                    .as_str(),
                ),

            revert_blocks_problems: registry
                .new_gauge(
                    format!("{}_revert_blocks_problems", subgraph_id),
                    "Problems reverting old blocks during a reorg".into(),
                    HashMap::new(),
                )
                .expect(
                    format!(
                        "failed to create metric `{}_revert_blocks_problems",
                        subgraph_id
                    )
                    .as_str(),
                ),

            write_block_problems: registry
                .new_gauge(
                    format!("{}_write_block_problems", subgraph_id),
                    "Problems writing a block to the store".into(),
                    HashMap::new(),
                )
                .expect(
                    format!(
                        "failed to create metric `{}_write_block_problems",
                        subgraph_id
                    )
                    .as_str(),
                ),

            last_new_chain_head_time: registry
                .new_gauge(
                    format!("{}_last_new_chain_head_time", subgraph_id),
                    "The last time a chain head was received that was different from before".into(),
                    HashMap::new(),
                )
                .expect(
                    format!(
                        "failed to create metric `{}_last_written_block_time",
                        subgraph_id
                    )
                    .as_str(),
                ),

            last_written_block_time: registry
                .new_gauge(
                    format!("{}_last_written_block_time", subgraph_id),
                    "The last time a block was written to the store".into(),
                    HashMap::new(),
                )
                .expect(
                    format!(
                        "failed to create metric `{}_last_written_block_time",
                        subgraph_id
                    )
                    .as_str(),
                ),
        }
    }
}
