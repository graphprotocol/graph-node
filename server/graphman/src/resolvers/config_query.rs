use async_graphql::Object;
use async_graphql::Result;

use crate::entities::ConfigCheckResponse;

mod check;
pub struct ConfigQuery;

#[Object]
impl ConfigQuery {
    /// Check and validate the configuration file
    pub async fn check(&self) -> Result<ConfigCheckResponse> {
        check::run()
    }
}
