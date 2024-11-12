use std::time::Instant;

use async_trait::async_trait;
use slog::Logger;

use crate::components::network_provider::ChainName;
use crate::components::network_provider::NetworkDetails;
use crate::components::network_provider::ProviderName;

#[async_trait]
pub trait ProviderCheck: Send + Sync + 'static {
    fn name(&self) -> &'static str;

    async fn check(
        &self,
        logger: &Logger,
        chain_name: &ChainName,
        provider_name: &ProviderName,
        adapter: &dyn NetworkDetails,
    ) -> ProviderCheckStatus;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProviderCheckStatus {
    NotChecked,
    TemporaryFailure {
        checked_at: Instant,
        message: String,
    },
    Valid,
    Failed {
        message: String,
    },
}

impl ProviderCheckStatus {
    pub fn is_valid(&self) -> bool {
        matches!(self, ProviderCheckStatus::Valid)
    }

    pub fn is_failed(&self) -> bool {
        matches!(self, ProviderCheckStatus::Failed { .. })
    }
}
