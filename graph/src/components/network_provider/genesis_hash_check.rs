use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use slog::error;
use slog::warn;
use slog::Logger;

use crate::components::network_provider::ChainIdentifierStore;
use crate::components::network_provider::ChainIdentifierStoreError;
use crate::components::network_provider::ChainName;
use crate::components::network_provider::NetworkDetails;
use crate::components::network_provider::ProviderCheck;
use crate::components::network_provider::ProviderCheckStatus;
use crate::components::network_provider::ProviderName;

/// Requires providers to have the same network version and genesis hash as one
/// previously stored in the database.
pub struct GenesisHashCheck {
    chain_identifier_store: Arc<dyn ChainIdentifierStore>,
}

impl GenesisHashCheck {
    pub fn new(chain_identifier_store: Arc<dyn ChainIdentifierStore>) -> Self {
        Self {
            chain_identifier_store,
        }
    }
}

#[async_trait]
impl ProviderCheck for GenesisHashCheck {
    async fn check(
        &self,
        logger: &Logger,
        chain_name: &ChainName,
        provider_name: &ProviderName,
        adapter: &dyn NetworkDetails,
    ) -> ProviderCheckStatus {
        let chain_identifier = match adapter.chain_identifier().await {
            Ok(chain_identifier) => chain_identifier,
            Err(err) => {
                error!(
                    logger,
                    "Failed to get chain identifier from the provider '{}' on chain '{}': {:#}",
                    provider_name,
                    chain_name,
                    err,
                );

                return ProviderCheckStatus::TemporaryFailure {
                    checked_at: Instant::now(),
                };
            }
        };

        let check_result = self
            .chain_identifier_store
            .validate_identifier(chain_name, &chain_identifier);

        use ChainIdentifierStoreError::*;

        match check_result {
            Ok(()) => ProviderCheckStatus::Valid,
            Err(IdentifierNotSet(_)) => {
                let update_result = self
                    .chain_identifier_store
                    .update_identifier(chain_name, &chain_identifier);

                if let Err(err) = update_result {
                    error!(
                        logger,
                        "Failed to store chain identifier for chain '{}' using provider '{}': {:#}",
                        chain_name,
                        provider_name,
                        err,
                    );

                    return ProviderCheckStatus::TemporaryFailure {
                        checked_at: Instant::now(),
                    };
                }

                ProviderCheckStatus::Valid
            }
            Err(NetVersionMismatch {
                store_net_version,
                chain_net_version,
                ..
            }) if store_net_version == "0" => {
                warn!(
                    logger,
                    "The net version for chain '{}' has changed from '0' to '{}' while using provider '{}'; \
                     The difference is probably caused by Firehose, since it does not provide the net version, and the default value was stored",
                    chain_name,
                    chain_net_version,
                    provider_name,
                );

                ProviderCheckStatus::Valid
            }
            Err(err @ NetVersionMismatch { .. }) => {
                error!(
                    logger,
                    "Genesis hash validation failed on provider '{}': {:#}", provider_name, err
                );

                ProviderCheckStatus::Failed
            }
            Err(err @ GenesisBlockHashMismatch { .. }) => {
                error!(
                    logger,
                    "Genesis hash validation failed on provider '{}': {:#}", provider_name, err
                );

                ProviderCheckStatus::Failed
            }
            Err(err @ Store(_)) => {
                error!(
                    logger,
                    "Genesis hash validation failed on provider '{}': {:#}", provider_name, err
                );

                ProviderCheckStatus::TemporaryFailure {
                    checked_at: Instant::now(),
                }
            }
        }
    }
}
