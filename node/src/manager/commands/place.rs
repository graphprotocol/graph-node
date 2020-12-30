use graph::prelude::anyhow::{anyhow, Error};
use graph_store_postgres::DeploymentPlacer;

pub fn run(placer: &dyn DeploymentPlacer, name: &str, network: &str) -> Result<(), Error> {
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
