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
    fn name(&self) -> &'static str {
        "GenesisHashCheck"
    }

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
                let message = format!(
                    "Failed to get chain identifier from the provider '{}' on chain '{}': {:#}",
                    provider_name, chain_name, err,
                );

                error!(logger, "{}", message);

                return ProviderCheckStatus::TemporaryFailure {
                    checked_at: Instant::now(),
                    message,
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
                    let message = format!(
                        "Failed to store chain identifier for chain '{}' using provider '{}': {:#}",
                        chain_name, provider_name, err,
                    );

                    error!(logger, "{}", message);

                    return ProviderCheckStatus::TemporaryFailure {
                        checked_at: Instant::now(),
                        message,
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
                let message = format!(
                    "Genesis hash validation failed on provider '{}': {:#}",
                    provider_name, err,
                );

                error!(logger, "{}", message);

                ProviderCheckStatus::Failed { message }
            }
            Err(err @ GenesisBlockHashMismatch { .. }) => {
                let message = format!(
                    "Genesis hash validation failed on provider '{}': {:#}",
                    provider_name, err,
                );

                error!(logger, "{}", message);

                ProviderCheckStatus::Failed { message }
            }
            Err(err @ Store(_)) => {
                let message = format!(
                    "Genesis hash validation failed on provider '{}': {:#}",
                    provider_name, err,
                );

                error!(logger, "{}", message);

                ProviderCheckStatus::TemporaryFailure {
                    checked_at: Instant::now(),
                    message,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use anyhow::anyhow;
    use anyhow::Result;

    use super::*;
    use crate::blockchain::ChainIdentifier;
    use crate::log::discard;

    #[derive(Default)]
    struct TestChainIdentifierStore {
        validate_identifier_calls: Mutex<Vec<Result<(), ChainIdentifierStoreError>>>,
        update_identifier_calls: Mutex<Vec<Result<(), ChainIdentifierStoreError>>>,
    }

    impl TestChainIdentifierStore {
        fn validate_identifier_call(&self, x: Result<(), ChainIdentifierStoreError>) {
            self.validate_identifier_calls.lock().unwrap().push(x)
        }

        fn update_identifier_call(&self, x: Result<(), ChainIdentifierStoreError>) {
            self.update_identifier_calls.lock().unwrap().push(x)
        }
    }

    impl Drop for TestChainIdentifierStore {
        fn drop(&mut self) {
            let Self {
                validate_identifier_calls,
                update_identifier_calls,
            } = self;

            assert!(validate_identifier_calls.lock().unwrap().is_empty());
            assert!(update_identifier_calls.lock().unwrap().is_empty());
        }
    }

    #[async_trait]
    impl ChainIdentifierStore for TestChainIdentifierStore {
        fn validate_identifier(
            &self,
            _chain_name: &ChainName,
            _chain_identifier: &ChainIdentifier,
        ) -> Result<(), ChainIdentifierStoreError> {
            self.validate_identifier_calls.lock().unwrap().remove(0)
        }

        fn update_identifier(
            &self,
            _chain_name: &ChainName,
            _chain_identifier: &ChainIdentifier,
        ) -> Result<(), ChainIdentifierStoreError> {
            self.update_identifier_calls.lock().unwrap().remove(0)
        }
    }

    #[derive(Default)]
    struct TestAdapter {
        chain_identifier_calls: Mutex<Vec<Result<ChainIdentifier>>>,
    }

    impl TestAdapter {
        fn chain_identifier_call(&self, x: Result<ChainIdentifier>) {
            self.chain_identifier_calls.lock().unwrap().push(x)
        }
    }

    impl Drop for TestAdapter {
        fn drop(&mut self) {
            let Self {
                chain_identifier_calls,
            } = self;

            assert!(chain_identifier_calls.lock().unwrap().is_empty());
        }
    }

    #[async_trait]
    impl NetworkDetails for TestAdapter {
        fn provider_name(&self) -> ProviderName {
            unimplemented!();
        }

        async fn chain_identifier(&self) -> Result<ChainIdentifier> {
            self.chain_identifier_calls.lock().unwrap().remove(0)
        }

        async fn provides_extended_blocks(&self) -> Result<bool> {
            unimplemented!();
        }
    }

    #[tokio::test]
    async fn check_temporary_failure_when_network_provider_request_fails() {
        let store = Arc::new(TestChainIdentifierStore::default());
        let check = GenesisHashCheck::new(store);

        let adapter = TestAdapter::default();
        adapter.chain_identifier_call(Err(anyhow!("error")));

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
        ));
    }

    #[tokio::test]
    async fn check_valid_when_store_successfully_validates_chain_identifier() {
        let store = Arc::new(TestChainIdentifierStore::default());
        store.validate_identifier_call(Ok(()));

        let check = GenesisHashCheck::new(store);

        let chain_identifier = ChainIdentifier {
            net_version: "1".to_owned(),
            genesis_block_hash: vec![1].into(),
        };

        let adapter = TestAdapter::default();
        adapter.chain_identifier_call(Ok(chain_identifier));

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
    async fn check_temporary_failure_on_initial_chain_identifier_update_error() {
        let store = Arc::new(TestChainIdentifierStore::default());
        store.validate_identifier_call(Err(ChainIdentifierStoreError::IdentifierNotSet(
            "chain-1".into(),
        )));
        store.update_identifier_call(Err(ChainIdentifierStoreError::Store(anyhow!("error"))));

        let check = GenesisHashCheck::new(store);

        let chain_identifier = ChainIdentifier {
            net_version: "1".to_owned(),
            genesis_block_hash: vec![1].into(),
        };

        let adapter = TestAdapter::default();
        adapter.chain_identifier_call(Ok(chain_identifier));

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
        ));
    }

    #[tokio::test]
    async fn check_valid_on_initial_chain_identifier_update() {
        let store = Arc::new(TestChainIdentifierStore::default());
        store.validate_identifier_call(Err(ChainIdentifierStoreError::IdentifierNotSet(
            "chain-1".into(),
        )));
        store.update_identifier_call(Ok(()));

        let check = GenesisHashCheck::new(store);

        let chain_identifier = ChainIdentifier {
            net_version: "1".to_owned(),
            genesis_block_hash: vec![1].into(),
        };

        let adapter = TestAdapter::default();
        adapter.chain_identifier_call(Ok(chain_identifier));

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
    async fn check_valid_when_stored_identifier_network_version_is_zero() {
        let store = Arc::new(TestChainIdentifierStore::default());
        store.validate_identifier_call(Err(ChainIdentifierStoreError::NetVersionMismatch {
            chain_name: "chain-1".into(),
            store_net_version: "0".to_owned(),
            chain_net_version: "1".to_owned(),
        }));

        let check = GenesisHashCheck::new(store);

        let chain_identifier = ChainIdentifier {
            net_version: "1".to_owned(),
            genesis_block_hash: vec![1].into(),
        };

        let adapter = TestAdapter::default();
        adapter.chain_identifier_call(Ok(chain_identifier));

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
    async fn check_fails_on_identifier_network_version_mismatch() {
        let store = Arc::new(TestChainIdentifierStore::default());
        store.validate_identifier_call(Err(ChainIdentifierStoreError::NetVersionMismatch {
            chain_name: "chain-1".into(),
            store_net_version: "2".to_owned(),
            chain_net_version: "1".to_owned(),
        }));

        let check = GenesisHashCheck::new(store);

        let chain_identifier = ChainIdentifier {
            net_version: "1".to_owned(),
            genesis_block_hash: vec![1].into(),
        };

        let adapter = TestAdapter::default();
        adapter.chain_identifier_call(Ok(chain_identifier));

        let status = check
            .check(
                &discard(),
                &("chain-1".into()),
                &("provider-1".into()),
                &adapter,
            )
            .await;

        assert!(matches!(status, ProviderCheckStatus::Failed { .. }));
    }

    #[tokio::test]
    async fn check_fails_on_identifier_genesis_hash_mismatch() {
        let store = Arc::new(TestChainIdentifierStore::default());
        store.validate_identifier_call(Err(ChainIdentifierStoreError::GenesisBlockHashMismatch {
            chain_name: "chain-1".into(),
            store_genesis_block_hash: vec![2].into(),
            chain_genesis_block_hash: vec![1].into(),
        }));

        let check = GenesisHashCheck::new(store);

        let chain_identifier = ChainIdentifier {
            net_version: "1".to_owned(),
            genesis_block_hash: vec![1].into(),
        };

        let adapter = TestAdapter::default();
        adapter.chain_identifier_call(Ok(chain_identifier));

        let status = check
            .check(
                &discard(),
                &("chain-1".into()),
                &("provider-1".into()),
                &adapter,
            )
            .await;

        assert!(matches!(status, ProviderCheckStatus::Failed { .. }));
    }

    #[tokio::test]
    async fn check_temporary_failure_on_store_errors() {
        let store = Arc::new(TestChainIdentifierStore::default());
        store.validate_identifier_call(Err(ChainIdentifierStoreError::Store(anyhow!("error"))));

        let check = GenesisHashCheck::new(store);

        let chain_identifier = ChainIdentifier {
            net_version: "1".to_owned(),
            genesis_block_hash: vec![1].into(),
        };

        let adapter = TestAdapter::default();
        adapter.chain_identifier_call(Ok(chain_identifier));

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
        ));
    }
}
