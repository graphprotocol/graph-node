use std::collections::HashSet;
use std::time::Instant;

use async_trait::async_trait;
use slog::error;
use slog::Logger;

use crate::components::network_provider::ChainName;
use crate::components::network_provider::NetworkDetails;
use crate::components::network_provider::ProviderCheck;
use crate::components::network_provider::ProviderCheckStatus;
use crate::components::network_provider::ProviderName;

/// Requires providers to support extended block details.
pub struct ExtendedBlocksCheck {
    disabled_for_chains: HashSet<ChainName>,
}

impl ExtendedBlocksCheck {
    pub fn new(disabled_for_chains: impl IntoIterator<Item = ChainName>) -> Self {
        Self {
            disabled_for_chains: disabled_for_chains.into_iter().collect(),
        }
    }
}

#[async_trait]
impl ProviderCheck for ExtendedBlocksCheck {
    async fn check(
        &self,
        logger: &Logger,
        chain_name: &ChainName,
        provider_name: &ProviderName,
        adapter: &dyn NetworkDetails,
    ) -> ProviderCheckStatus {
        if self.disabled_for_chains.contains(chain_name) {
            return ProviderCheckStatus::Valid;
        }

        match adapter.provides_extended_blocks().await {
            Ok(true) => ProviderCheckStatus::Valid,
            Ok(false) => {
                error!(
                    logger,
                    "Provider '{}' does not support extended blocks on chain '{}'",
                    provider_name,
                    chain_name,
                );

                ProviderCheckStatus::Failed
            }
            Err(err) => {
                error!(
                    logger,
                    "Failed to check if provider '{}' supports extended blocks on chain '{}': {:#}",
                    provider_name,
                    chain_name,
                    err,
                );

                ProviderCheckStatus::TemporaryFailure {
                    checked_at: Instant::now(),
                }
            }
        }
    }
}
