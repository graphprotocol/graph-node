use crate::prelude::Error;
use async_trait::async_trait;

#[async_trait]
pub trait ThreeBoxAdapter: Send + Sync {
    async fn profile(
        &self,
        address: &str,
    ) -> Result<serde_json::Map<String, serde_json::Value>, Error>;
}
