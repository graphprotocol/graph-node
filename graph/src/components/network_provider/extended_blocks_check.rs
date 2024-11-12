use std::collections::HashSet;
use std::time::Instant;

use async_trait::async_trait;
use slog::error;
use slog::warn;
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
    fn name(&self) -> &'static str {
        "ExtendedBlocksCheck"
    }

    async fn check(
        &self,
        logger: &Logger,
        chain_name: &ChainName,
        provider_name: &ProviderName,
        adapter: &dyn NetworkDetails,
    ) -> ProviderCheckStatus {
        if self.disabled_for_chains.contains(chain_name) {
            warn!(
                logger,
                "Extended blocks check for provider '{}' was disabled on chain '{}'",
                provider_name,
                chain_name,
            );

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

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use anyhow::anyhow;
    use anyhow::Result;

    use super::*;
    use crate::blockchain::ChainIdentifier;
    use crate::log::discard;

    macro_rules! lock {
        ($obj:ident.$field:ident. $( $call:tt )+) => {{
            $obj.$field.lock().unwrap(). $( $call )+
        }};
    }

    #[derive(Default)]
    struct TestAdapter {
        provides_extended_blocks_calls: Mutex<Vec<Result<bool>>>,
    }

    #[async_trait]
    impl NetworkDetails for TestAdapter {
        fn provider_name(&self) -> ProviderName {
            unimplemented!();
        }

        async fn chain_identifier(&self) -> Result<ChainIdentifier> {
            unimplemented!();
        }

        async fn provides_extended_blocks(&self) -> Result<bool> {
            lock!(self.provides_extended_blocks_calls.remove(0))
        }
    }

    impl Drop for TestAdapter {
        fn drop(&mut self) {
            assert!(lock!(self.provides_extended_blocks_calls.is_empty()));
        }
    }

    #[tokio::test]
    async fn check_valid_when_disabled_for_chain() {
        let check = ExtendedBlocksCheck::new(["chain-1".into()]);
        let adapter = TestAdapter::default();

        let status = check
            .check(
                &discard(),
                &("chain-1".into()),
                &("provider-1".into()),
                &adapter,
            )
            .await;

        assert_eq!(status, ProviderCheckStatus::Valid);
    }

    #[tokio::test]
    async fn check_valid_when_disabled_for_multiple_chains() {
        let check = ExtendedBlocksCheck::new(["chain-1".into(), "chain-2".into()]);
        let adapter = TestAdapter::default();

        let status = check
            .check(
                &discard(),
                &("chain-1".into()),
                &("provider-1".into()),
                &adapter,
            )
            .await;

        assert_eq!(status, ProviderCheckStatus::Valid);

        let status = check
            .check(
                &discard(),
                &("chain-2".into()),
                &("provider-2".into()),
                &adapter,
            )
            .await;

        assert_eq!(status, ProviderCheckStatus::Valid);
    }

    #[tokio::test]
    async fn check_valid_when_extended_blocks_are_supported() {
        let check = ExtendedBlocksCheck::new([]);

        let adapter = TestAdapter::default();
        lock! { adapter.provides_extended_blocks_calls.push(Ok(true)) };

        let status = check
            .check(
                &discard(),
                &("chain-1".into()),
                &("provider-1".into()),
                &adapter,
            )
            .await;

        assert_eq!(status, ProviderCheckStatus::Valid);
    }

    #[tokio::test]
    async fn check_fails_when_extended_blocks_are_not_supported() {
        let check = ExtendedBlocksCheck::new([]);

        let adapter = TestAdapter::default();
        lock! { adapter.provides_extended_blocks_calls.push(Ok(false)) };

        let status = check
            .check(
                &discard(),
                &("chain-1".into()),
                &("provider-1".into()),
                &adapter,
            )
            .await;

        assert_eq!(status, ProviderCheckStatus::Failed);
    }

    #[tokio::test]
    async fn check_temporary_failure_when_provider_request_fails() {
        let check = ExtendedBlocksCheck::new([]);

        let adapter = TestAdapter::default();
        lock! { adapter.provides_extended_blocks_calls.push(Err(anyhow!("error"))) };

        let status = check
            .check(
                &discard(),
                &("chain-1".into()),
                &("provider-1".into()),
                &adapter,
            )
            .await;

        assert!(matches!(
            status,
            ProviderCheckStatus::TemporaryFailure { .. }
        ))
    }
}
