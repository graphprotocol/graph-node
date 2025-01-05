use anyhow::{anyhow, Error as AnyError};
use graph_store_postgres::DeploymentPlacer;
use thiserror::Error;

pub struct PlaceResult {
    pub shards: Vec<String>,
    pub nodes: Vec<String>,
}

#[derive(Debug, Error)]
pub enum PlaceError {
    #[error("No matching placement rule; default placement from JSON RPC call would be used")]
    NoPlacementRule,

    #[error(transparent)]
    Common(#[from] AnyError),
}

pub fn place(
    placer: &dyn DeploymentPlacer,
    name: &str,
    network: &str,
) -> Result<PlaceResult, PlaceError> {
    match placer.place(name, network).map_err(|s| anyhow!(s))? {
        None => Err(PlaceError::NoPlacementRule),
        Some((shards, nodes)) => {
            let nodes: Vec<_> = nodes.into_iter().map(|n| n.to_string()).collect();
            let shards: Vec<_> = shards.into_iter().map(|s| s.to_string()).collect();
            Ok(PlaceResult { shards, nodes })
        }
    }
}
