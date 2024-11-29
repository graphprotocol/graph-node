use anyhow::Result;
use async_trait::async_trait;

use crate::blockchain::ChainIdentifier;
use crate::components::network_provider::ProviderName;

/// Additional requirements for network providers that are necessary for provider checks.
#[async_trait]
pub trait NetworkDetails: Send + Sync + 'static {
    fn provider_name(&self) -> ProviderName;

    /// Returns the data that helps to uniquely identify a chain.
    async fn chain_identifier(&self) -> Result<ChainIdentifier>;

    /// Returns true if the provider supports extended block details.
    async fn provides_extended_blocks(&self) -> Result<bool>;
}
