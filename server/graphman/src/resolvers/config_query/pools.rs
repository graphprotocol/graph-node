use async_graphql::Result;
use graphman::commands::config::pools::pools;

use crate::entities::PoolsResponse;

use super::check::fetch_config;

pub async fn run(nodes: Vec<String>) -> Result<PoolsResponse> {
    let config = fetch_config()?;
    let res = pools(&config, &nodes);

    match res {
        Ok(res) => Ok(PoolsResponse {
            pools: res.pools,
            shards: res.shards,
        }),
        Err(e) => Err(e.into()),
    }
}
