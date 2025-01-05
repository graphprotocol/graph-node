use async_graphql::Object;
use async_graphql::Result;

use crate::entities::ConfigCheckResponse;
use crate::entities::PlaceResponse;

mod check;
mod place;
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
}
