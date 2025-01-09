use anyhow::Error;
use graph_store_postgres::DeploymentPlacer;
use graphman::commands::config::place::{place, PlaceError};

pub fn run(placer: &dyn DeploymentPlacer, name: &String, network: &String) -> Result<(), Error> {
    let res = place(placer, name, network);
    match res {
        Ok(result) => {
            println!("subgraph: {}", name);
            println!("network:  {}", network);
            println!("shard:    {}", result.shards.join(", "));
            println!("nodes:    {}", result.nodes.join(", "));
        }
        Err(PlaceError::NoPlacementRule) => {
            println!("{}", PlaceError::NoPlacementRule);
        }
        Err(PlaceError::Common(e)) => {
            println!("Error: {}", e.to_string());
        }
    };
    Ok(())
}
