use async_graphql::Object;
use async_graphql::Result;

use crate::entities::ConfigCheckResponse;
use crate::entities::PlaceResponse;
use crate::entities::PoolsResponse;

mod check;
mod place;
mod pools;
pub struct ConfigQuery;

#[Object]
impl ConfigQuery {
    /// Check and validate the configuration file
    pub async fn check(&self) -> Result<ConfigCheckResponse> {
        check::run()
    }

    /// Returns how a specific subgraph would be placed
    pub async fn place(
        &self,
        #[graphql(desc = "Subgraph name")] subgraph: String,
        #[graphql(desc = "Network name")] network: String,
    ) -> Result<PlaceResponse> {
        place::run(subgraph, network).await
    }

    // Information about the size of database pools
    pub async fn pools(
        &self,
        #[graphql(desc = "The names of the nodes that are going to run")] nodes: Vec<String>,
    ) -> Result<PoolsResponse> {
        pools::run(nodes).await
    }
}
