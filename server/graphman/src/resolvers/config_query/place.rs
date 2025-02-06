use async_graphql::Result;
use graphman::commands::config::place::place;

use crate::entities::PlaceResponse;

use super::check::fetch_config;

pub async fn run(subgraph: String, network: String) -> Result<PlaceResponse> {
    let config = fetch_config()?;
    let res = place(&config.deployment, &subgraph, &network)?;

    Ok(PlaceResponse::from(
        subgraph, network, res.shards, res.nodes,
    ))
}
