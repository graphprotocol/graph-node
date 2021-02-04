use std::collections::BTreeMap;

use graph::prelude::{
    anyhow::{anyhow, Error},
    NodeId,
};
use graph_store_postgres::DeploymentPlacer;

use crate::config::Config;

pub fn place(placer: &dyn DeploymentPlacer, name: &str, network: &str) -> Result<(), Error> {
    match placer.place(name, network).map_err(|s| anyhow!(s))? {
        None => {
            println!(
                "no matching placement rule; default placement from JSON RPC call would be used"
            );
        }
        Some((shard, nodes)) => {
            let nodes: Vec<_> = nodes.into_iter().map(|n| n.to_string()).collect();
            println!("subgraph: {}", name);
            println!("network:  {}", network);
            println!("shard:    {}", shard);
            println!("nodes:    {}", nodes.join(", "));
        }
    }
    Ok(())
}

pub fn check(config: &Config, print: bool) -> Result<(), Error> {
    match config.to_json() {
        Ok(txt) => {
            if print {
                println!("{}", txt);
            } else {
                println!("Successfully validated configuration");
            }
            Ok(())
        }
        Err(e) => Err(anyhow!("error serializing config: {}", e)),
    }
}

pub fn pools(config: &Config, nodes: Vec<String>, shard: bool) -> Result<(), Error> {
    // Quietly replace `-` with `_` in node names to make passing in pod names
    // from k8s less annoying
    let nodes: Vec<_> = nodes
        .into_iter()
        .map(|name| {
            NodeId::new(name.replace("-", "_"))
                .map_err(|()| anyhow!("illegal node name `{}`", name))
        })
        .collect::<Result<_, _>>()?;
    // node -> shard_name -> size
    let mut sizes = BTreeMap::new();
    for node in &nodes {
        let mut shard_sizes = BTreeMap::new();
        for (name, shard) in &config.stores {
            let size = shard.pool_size.size_for(node, name)?;
            shard_sizes.insert(name.to_string(), size);
            for (replica_name, replica) in &shard.replicas {
                let qname = format!("{}.{}", name, replica_name);
                let size = replica.pool_size.size_for(node, &qname)?;
                shard_sizes.insert(qname, size);
            }
        }
        sizes.insert(node.to_string(), shard_sizes);
    }

    if shard {
        let mut by_shard: BTreeMap<&str, u32> = BTreeMap::new();
        for shard_sizes in sizes.values() {
            for (shard_name, size) in shard_sizes {
                *by_shard.entry(shard_name).or_default() += size;
            }
        }
        for (shard_name, size) in by_shard {
            println!("{}: {}", shard_name, size);
        }
    } else {
        for node in &nodes {
            let empty = BTreeMap::new();
            println!("{}:", node);
            let node_sizes = sizes.get(node.as_str()).unwrap_or(&empty);
            for (shard, size) in node_sizes {
                println!("    {}: {}", shard, size);
            }
        }
    }
    Ok(())
}
