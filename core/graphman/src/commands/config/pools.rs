use std::collections::BTreeMap;

use anyhow::Error as AnyError;
use async_graphql::SimpleObject;
use graph::prelude::NodeId;
use thiserror::Error;

use crate::config::Config;

#[derive(Debug, Error)]
pub enum PoolsError {
    #[error("illegal node name `{0}`")]
    IllegalNodeName(String),
    #[error(transparent)]
    Common(#[from] AnyError),
}

#[derive(Clone, Debug, SimpleObject)]
pub struct Pools {
    pub shards: BTreeMap<String, u32>,
    pub node: String,
}

#[derive(Debug)]
pub struct PoolsResult {
    pub pools: Vec<Pools>,
    pub shards: BTreeMap<String, u32>,
}

pub fn pools(config: &Config, nodes: &Vec<String>) -> Result<PoolsResult, PoolsError> {
    // Quietly replace `-` with `_` in node names to make passing in pod names
    // from k8s less annoying
    let nodes: Vec<_> = nodes
        .into_iter()
        .map(|name| {
            NodeId::new(name.replace('-', "_"))
                .map_err(|()| PoolsError::IllegalNodeName(name.to_string()))
        })
        .collect::<Result<_, _>>()?;
    // node -> shard_name -> size
    let mut sizes = BTreeMap::new();
    for node in &nodes {
        let mut shard_sizes = BTreeMap::new();
        for (name, shard) in &config.stores {
            let size = shard
                .pool_size
                .size_for(node, name)
                .map_err(PoolsError::Common)?;
            shard_sizes.insert(name.to_string(), size);
            for (replica_name, replica) in &shard.replicas {
                let qname = format!("{}.{}", name, replica_name);
                let size = replica
                    .pool_size
                    .size_for(node, &qname)
                    .map_err(PoolsError::Common)?;
                shard_sizes.insert(qname, size);
            }
        }
        sizes.insert(node.to_string(), shard_sizes);
    }

    let mut by_shard: BTreeMap<String, u32> = BTreeMap::new();
    for shard_sizes in sizes.values() {
        for (shard_name, size) in shard_sizes {
            *by_shard.entry(shard_name.to_string()).or_default() += size;
        }
    }
    let mut pools: Vec<Pools> = Vec::new();
    for node in &nodes {
        let empty = BTreeMap::new();
        let node_sizes = sizes.get(node.as_str()).unwrap_or(&empty);
        pools.push(Pools {
            shards: node_sizes.clone(),
            node: node.to_string(),
        })
    }
    Ok(PoolsResult {
        pools,
        shards: by_shard,
    })
}
