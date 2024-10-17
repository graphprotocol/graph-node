use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;

use crate::blockchain::ChainIdentifier;
use crate::components::network_provider::ProviderName;

#[async_trait]
pub trait NetworkDetails: Send + Sync + 'static {
    fn provider_name(&self) -> ProviderName;

    async fn chain_identifier(&self) -> Result<ChainIdentifier>;

    async fn chain_identifier_with_timeout(&self, timeout: Duration) -> Result<ChainIdentifier> {
        tokio::time::timeout(timeout, self.chain_identifier()).await?
    }

    async fn provides_extended_blocks(&self) -> Result<bool>;

    async fn provides_extended_blocks_with_timeout(&self, timeout: Duration) -> Result<bool> {
        tokio::time::timeout(timeout, self.provides_extended_blocks()).await?
    }
}
