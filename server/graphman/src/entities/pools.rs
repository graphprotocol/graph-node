use std::collections::BTreeMap;

use async_graphql::SimpleObject;
use graphman::commands::config::pools::Pools;

#[derive(Clone, Debug, SimpleObject)]
pub struct PoolsResponse {
    /// Size of database pools for each node.
    pub pools: Vec<Pools>,
    /// Connections by shard rather than by node
    pub shards: BTreeMap<String, u32>,
}
