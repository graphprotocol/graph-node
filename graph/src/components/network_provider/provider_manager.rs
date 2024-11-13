use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use derivative::Derivative;
use itertools::Itertools;
use slog::error;
use slog::info;
use slog::warn;
use slog::Logger;
use thiserror::Error;
use tokio::sync::RwLock;

use crate::components::network_provider::ChainName;
use crate::components::network_provider::NetworkDetails;
use crate::components::network_provider::ProviderCheck;
use crate::components::network_provider::ProviderCheckStatus;
use crate::components::network_provider::ProviderName;

/// The total time all providers have to perform all checks.
const VALIDATION_MAX_DURATION: Duration = Duration::from_secs(30);

/// Providers that failed validation with a temporary failure are re-validated at this interval.
const VALIDATION_RETRY_INTERVAL: Duration = Duration::from_secs(300);

/// ProviderManager is responsible for validating providers before they are returned to consumers.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct ProviderManager<T: NetworkDetails> {
    #[derivative(Debug = "ignore")]
    inner: Arc<Inner<T>>,

    validation_max_duration: Duration,
    validation_retry_interval: Duration,
}

/// The strategy used by the [ProviderManager] when checking providers.
#[derive(Clone)]
pub enum ProviderCheckStrategy<'a> {
    /// Marks a provider as valid without performing any checks on it.
    MarkAsValid,

    /// Requires a provider to pass all specified checks to be considered valid.
    RequireAll(&'a [Arc<dyn ProviderCheck>]),
}

#[derive(Debug, Error)]
pub enum ProviderManagerError {
    #[error("provider validation timed out on chain '{0}'")]
    ProviderValidationTimeout(ChainName),

    #[error("no providers available for chain '{0}'")]
    NoProvidersAvailable(ChainName),

    #[error("all providers failed for chain '{0}'")]
    AllProvidersFailed(ChainName),
}

struct Inner<T: NetworkDetails> {
    logger: Logger,
    adapters: HashMap<ChainName, Box<[Adapter<T>]>>,
    validations: Box<[Validation]>,
    enabled_checks: Box<[Arc<dyn ProviderCheck>]>,
}

struct Adapter<T: NetworkDetails> {
    /// An index from the validations vector that is used to directly access the validation state
    /// of the provider without additional checks or pointer dereferences.
    ///
    /// This is useful because the same provider can have multiple adapters to increase the number
    /// of concurrent requests, but it does not make sense to perform multiple validations on
    /// the same provider.
    ///
    /// It is guaranteed to be a valid index from the validations vector.
    validation_index: usize,

    inner: T,
}

/// Contains all the information needed to determine whether a provider is valid or not.
struct Validation {
    chain_name: ChainName,
    provider_name: ProviderName,

    /// Used to avoid acquiring the lock if possible.
    ///
    /// If it is not set, it means that validation is required.
    /// If it is 'true', it means that the provider has passed all the checks.
    /// If it is 'false', it means that the provider has failed at least one check.
    is_valid: OnceLock<bool>,

    /// Contains the statuses resulting from performing provider checks on the provider.
    /// It is guaranteed to have the same number of elements as the number of checks enabled.
    check_results: RwLock<Box<[ProviderCheckStatus]>>,
}

impl<T: NetworkDetails> ProviderManager<T> {
    /// Creates a new provider manager for the specified providers.
    ///
    /// Performs enabled provider checks on each provider when it is accessed.
    pub fn new(
        logger: Logger,
        adapters: impl IntoIterator<Item = (ChainName, Vec<T>)>,
        strategy: ProviderCheckStrategy<'_>,
    ) -> Self {
        let enabled_checks = match strategy {
            ProviderCheckStrategy::MarkAsValid => {
                warn!(
                    &logger,
                    "No network provider checks enabled. \
                     This can cause data inconsistency and many other issues."
                );

                &[]
            }
            ProviderCheckStrategy::RequireAll(checks) => {
                info!(
                    &logger,
                    "All network providers have checks enabled. \
                     To be considered valid they will have to pass the following checks: [{}]",
                    checks.iter().map(|x| x.name()).join(",")
                );

                checks
            }
        };

        let mut validations: Vec<Validation> = Vec::new();
        let adapters = Self::adapters_by_chain_names(adapters, &mut validations, &enabled_checks);

        let inner = Inner {
            logger,
            adapters,
            validations: validations.into(),
            enabled_checks: enabled_checks.to_vec().into(),
        };

        Self {
            inner: Arc::new(inner),
            validation_max_duration: VALIDATION_MAX_DURATION,
            validation_retry_interval: VALIDATION_RETRY_INTERVAL,
        }
    }

    /// Returns the total number of providers available for the chain.
    ///
    /// Does not take provider validation status into account.
    pub fn len(&self, chain_name: &ChainName) -> usize {
        self.inner
            .adapters
            .get(chain_name)
            .map(|adapter| adapter.len())
            .unwrap_or_default()
    }

    /// Returns all available providers for the chain.
    ///
    /// Does not perform any provider validation and does not guarantee that providers will be
    /// accessible or return the expected data.
    pub fn providers_unchecked(&self, chain_name: &ChainName) -> impl Iterator<Item = &T> {
        self.inner.adapters_unchecked(chain_name)
    }

    /// Returns all valid providers for the chain.
    ///
    /// Performs all enabled provider checks for each available provider for the chain.
    /// A provider is considered valid if it successfully passes all checks.
    ///
    /// Note: Provider checks may take some time to complete.
    pub async fn providers(
        &self,
        chain_name: &ChainName,
    ) -> Result<impl Iterator<Item = &T>, ProviderManagerError> {
        tokio::time::timeout(
            self.validation_max_duration,
            self.inner
                .adapters(chain_name, self.validation_retry_interval),
        )
        .await
        .map_err(|_| ProviderManagerError::ProviderValidationTimeout(chain_name.clone()))?
    }

    fn adapters_by_chain_names(
        adapters: impl IntoIterator<Item = (ChainName, Vec<T>)>,
        validations: &mut Vec<Validation>,
        enabled_checks: &[Arc<dyn ProviderCheck>],
    ) -> HashMap<ChainName, Box<[Adapter<T>]>> {
        adapters
            .into_iter()
            .map(|(chain_name, adapters)| {
                let adapters = adapters
                    .into_iter()
                    .map(|adapter| {
                        let provider_name = adapter.provider_name();

                        let validation_index = Self::get_or_init_validation_index(
                            validations,
                            enabled_checks,
                            &chain_name,
                            &provider_name,
                        );

                        Adapter {
                            validation_index,
                            inner: adapter,
                        }
                    })
                    .collect_vec();

                (chain_name, adapters.into())
            })
            .collect()
    }

    fn get_or_init_validation_index(
        validations: &mut Vec<Validation>,
        enabled_checks: &[Arc<dyn ProviderCheck>],
        chain_name: &ChainName,
        provider_name: &ProviderName,
    ) -> usize {
        validations
            .iter()
            .position(|validation| {
                validation.chain_name == *chain_name && validation.provider_name == *provider_name
            })
            .unwrap_or_else(|| {
                validations.push(Validation {
                    chain_name: chain_name.clone(),
                    provider_name: provider_name.clone(),
                    is_valid: if enabled_checks.is_empty() {
                        OnceLock::from(true)
                    } else {
                        OnceLock::new()
                    },
                    check_results: RwLock::new(
                        vec![ProviderCheckStatus::NotChecked; enabled_checks.len()].into(),
                    ),
                });

                validations.len() - 1
            })
    }
}

// Used to simplify some tests.
impl<T: NetworkDetails> Default for ProviderManager<T> {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner {
                logger: crate::log::discard(),
                adapters: HashMap::new(),
                validations: vec![].into(),
                enabled_checks: vec![].into(),
            }),
            validation_max_duration: VALIDATION_MAX_DURATION,
            validation_retry_interval: VALIDATION_RETRY_INTERVAL,
        }
    }
}

impl<T: NetworkDetails> Inner<T> {
    fn adapters_unchecked(&self, chain_name: &ChainName) -> impl Iterator<Item = &T> {
        match self.adapters.get(chain_name) {
            Some(adapters) => adapters.iter(),
            None => [].iter(),
        }
        .map(|adapter| &adapter.inner)
    }

    async fn adapters(
        &self,
        chain_name: &ChainName,
        validation_retry_interval: Duration,
    ) -> Result<impl Iterator<Item = &T>, ProviderManagerError> {
        use std::iter::once;

        let (initial_size, adapters) = match self.adapters.get(chain_name) {
            Some(adapters) => {
                if !self.enabled_checks.is_empty() {
                    self.validate_adapters(adapters, validation_retry_interval)
                        .await;
                }

                (adapters.len(), adapters.iter())
            }
            None => (0, [].iter()),
        };

        let mut valid_adapters = adapters
            .clone()
            .filter(|adapter| {
                self.validations[adapter.validation_index].is_valid.get() == Some(&true)
            })
            .map(|adapter| &adapter.inner);

        // A thread-safe and fast way to check if an iterator has elements.
        // Note: Using `.peekable()` is not thread safe.
        if let first_valid_adapter @ Some(_) = valid_adapters.next() {
            return Ok(once(first_valid_adapter).flatten().chain(valid_adapters));
        }

        // This is done to maintain backward compatibility with the previous implementation,
        // and to avoid breaking modules that may rely on empty results in some cases.
        if initial_size == 0 {
            // Even though we know there are no adapters at this point,
            // we still need to return the same type.
            return Ok(once(None).flatten().chain(valid_adapters));
        }

        let failed_count = adapters
            .filter(|adapter| {
                self.validations[adapter.validation_index].is_valid.get() == Some(&false)
            })
            .count();

        if failed_count == initial_size {
            return Err(ProviderManagerError::AllProvidersFailed(chain_name.clone()));
        }

        Err(ProviderManagerError::NoProvidersAvailable(
            chain_name.clone(),
        ))
    }

    async fn validate_adapters(
        &self,
        adapters: &[Adapter<T>],
        validation_retry_interval: Duration,
    ) {
        let validation_futs = adapters
            .iter()
            .filter(|adapter| {
                self.validations[adapter.validation_index]
                    .is_valid
                    .get()
                    .is_none()
            })
            .map(|adapter| self.validate_adapter(adapter, validation_retry_interval));

        let _outputs: Vec<()> = crate::futures03::future::join_all(validation_futs).await;
    }

    async fn validate_adapter(&self, adapter: &Adapter<T>, validation_retry_interval: Duration) {
        let validation = &self.validations[adapter.validation_index];

        let chain_name = &validation.chain_name;
        let provider_name = &validation.provider_name;
        let mut check_results = validation.check_results.write().await;

        // Make sure that when we get the lock, the adapter is still not validated.
        if validation.is_valid.get().is_some() {
            return;
        }

        for (i, check_result) in check_results.iter_mut().enumerate() {
            use ProviderCheckStatus::*;

            match check_result {
                NotChecked => {
                    // Check is required;
                }
                TemporaryFailure {
                    checked_at,
                    message: _,
                } => {
                    if checked_at.elapsed() < validation_retry_interval {
                        continue;
                    }

                    // A new check is required;
                }
                Valid => continue,
                Failed { message: _ } => continue,
            }

            *check_result = self.enabled_checks[i]
                .check(&self.logger, chain_name, provider_name, &adapter.inner)
                .await;

            // One failure is enough to not even try to perform any further checks,
            // because that adapter will never be considered valid.
            if check_result.is_failed() {
                validation.is_valid.get_or_init(|| false);
                return;
            }
        }

        if check_results.iter().all(|x| x.is_valid()) {
            validation.is_valid.get_or_init(|| true);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Instant;

    use anyhow::Result;
    use async_trait::async_trait;

    use super::*;
    use crate::blockchain::ChainIdentifier;
    use crate::log::discard;

    struct TestAdapter {
        id: usize,
        provider_name_calls: Mutex<Vec<ProviderName>>,
    }

    impl TestAdapter {
        fn new(id: usize) -> Self {
            Self {
                id,
                provider_name_calls: Default::default(),
            }
        }

        fn provider_name_call(&self, x: ProviderName) {
            self.provider_name_calls.lock().unwrap().push(x)
        }
    }

    impl Drop for TestAdapter {
        fn drop(&mut self) {
            let Self {
                id: _,
                provider_name_calls,
            } = self;

            assert!(provider_name_calls.lock().unwrap().is_empty());
        }
    }

    #[async_trait]
    impl NetworkDetails for Arc<TestAdapter> {
        fn provider_name(&self) -> ProviderName {
            self.provider_name_calls.lock().unwrap().remove(0)
        }

        async fn chain_identifier(&self) -> Result<ChainIdentifier> {
            unimplemented!();
        }

        async fn provides_extended_blocks(&self) -> Result<bool> {
            unimplemented!();
        }
    }

    #[derive(Default)]
    struct TestProviderCheck {
        check_calls: Mutex<Vec<Box<dyn FnOnce() -> ProviderCheckStatus + Send>>>,
    }

    impl TestProviderCheck {
        fn check_call(&self, x: Box<dyn FnOnce() -> ProviderCheckStatus + Send>) {
            self.check_calls.lock().unwrap().push(x)
        }
    }

    impl Drop for TestProviderCheck {
        fn drop(&mut self) {
            assert!(self.check_calls.lock().unwrap().is_empty());
        }
    }

    #[async_trait]
    impl ProviderCheck for TestProviderCheck {
        fn name(&self) -> &'static str {
            "TestProviderCheck"
        }

        async fn check(
            &self,
            _logger: &Logger,
            _chain_name: &ChainName,
            _provider_name: &ProviderName,
            _adapter: &dyn NetworkDetails,
        ) -> ProviderCheckStatus {
            self.check_calls.lock().unwrap().remove(0)()
        }
    }

    fn chain_name() -> ChainName {
        "test_chain".into()
    }

    fn other_chain_name() -> ChainName {
        "other_chain".into()
    }

    fn ids<'a>(adapters: impl Iterator<Item = &'a Arc<TestAdapter>>) -> Vec<usize> {
        adapters.map(|adapter| adapter.id).collect()
    }

    #[tokio::test]
    async fn no_providers() {
        let manager: ProviderManager<Arc<TestAdapter>> =
            ProviderManager::new(discard(), [], ProviderCheckStrategy::MarkAsValid);

        assert_eq!(manager.len(&chain_name()), 0);
        assert_eq!(manager.providers_unchecked(&chain_name()).count(), 0);
        assert_eq!(manager.providers(&chain_name()).await.unwrap().count(), 0);
    }

    #[tokio::test]
    async fn no_providers_for_chain() {
        let adapter_1 = Arc::new(TestAdapter::new(1));
        adapter_1.provider_name_call("provider_1".into());

        let manager: ProviderManager<Arc<TestAdapter>> = ProviderManager::new(
            discard(),
            [(other_chain_name(), vec![adapter_1.clone()])],
            ProviderCheckStrategy::MarkAsValid,
        );

        assert_eq!(manager.len(&chain_name()), 0);
        assert_eq!(manager.len(&other_chain_name()), 1);

        assert_eq!(manager.providers_unchecked(&chain_name()).count(), 0);

        assert_eq!(
            ids(manager.providers_unchecked(&other_chain_name())),
            vec![1],
        );

        assert_eq!(manager.providers(&chain_name()).await.unwrap().count(), 0);

        assert_eq!(
            ids(manager.providers(&other_chain_name()).await.unwrap()),
            vec![1],
        );
    }

    #[tokio::test]
    async fn multiple_providers() {
        let adapter_1 = Arc::new(TestAdapter::new(1));
        adapter_1.provider_name_call("provider_1".into());

        let adapter_2 = Arc::new(TestAdapter::new(2));
        adapter_2.provider_name_call("provider_2".into());

        let manager: ProviderManager<Arc<TestAdapter>> = ProviderManager::new(
            discard(),
            [(chain_name(), vec![adapter_1.clone(), adapter_2.clone()])],
            ProviderCheckStrategy::MarkAsValid,
        );

        assert_eq!(manager.len(&chain_name()), 2);

        assert_eq!(ids(manager.providers_unchecked(&chain_name())), vec![1, 2]);

        assert_eq!(
            ids(manager.providers(&chain_name()).await.unwrap()),
            vec![1, 2],
        );
    }

    #[tokio::test]
    async fn providers_unchecked_skips_provider_checks() {
        let adapter_1 = Arc::new(TestAdapter::new(1));
        adapter_1.provider_name_call("provider_1".into());

        let check_1 = Arc::new(TestProviderCheck::default());

        let manager: ProviderManager<Arc<TestAdapter>> = ProviderManager::new(
            discard(),
            [(chain_name(), vec![adapter_1.clone()])],
            ProviderCheckStrategy::RequireAll(&[check_1.clone()]),
        );

        assert_eq!(ids(manager.providers_unchecked(&chain_name())), vec![1]);
    }

    #[tokio::test]
    async fn successful_provider_check() {
        let adapter_1 = Arc::new(TestAdapter::new(1));
        adapter_1.provider_name_call("provider_1".into());

        let check_1 = Arc::new(TestProviderCheck::default());
        check_1.check_call(Box::new(|| ProviderCheckStatus::Valid));

        let manager: ProviderManager<Arc<TestAdapter>> = ProviderManager::new(
            discard(),
            [(chain_name(), vec![adapter_1.clone()])],
            ProviderCheckStrategy::RequireAll(&[check_1.clone()]),
        );

        assert_eq!(
            ids(manager.providers(&chain_name()).await.unwrap()),
            vec![1]
        );

        // Another call will not trigger a new validation.
        assert_eq!(
            ids(manager.providers(&chain_name()).await.unwrap()),
            vec![1]
        );
    }

    #[tokio::test]
    async fn multiple_successful_provider_checks() {
        let adapter_1 = Arc::new(TestAdapter::new(1));
        adapter_1.provider_name_call("provider_1".into());

        let check_1 = Arc::new(TestProviderCheck::default());
        check_1.check_call(Box::new(|| ProviderCheckStatus::Valid));

        let check_2 = Arc::new(TestProviderCheck::default());
        check_2.check_call(Box::new(|| ProviderCheckStatus::Valid));

        let manager: ProviderManager<Arc<TestAdapter>> = ProviderManager::new(
            discard(),
            [(chain_name(), vec![adapter_1.clone()])],
            ProviderCheckStrategy::RequireAll(&[check_1.clone(), check_2.clone()]),
        );

        assert_eq!(
            ids(manager.providers(&chain_name()).await.unwrap()),
            vec![1]
        );

        // Another call will not trigger a new validation.
        assert_eq!(
            ids(manager.providers(&chain_name()).await.unwrap()),
            vec![1]
        );
    }

    #[tokio::test]
    async fn multiple_successful_provider_checks_on_multiple_adapters() {
        let adapter_1 = Arc::new(TestAdapter::new(1));
        adapter_1.provider_name_call("provider_1".into());

        let adapter_2 = Arc::new(TestAdapter::new(2));
        adapter_2.provider_name_call("provider_2".into());

        let check_1 = Arc::new(TestProviderCheck::default());
        check_1.check_call(Box::new(|| ProviderCheckStatus::Valid));
        check_1.check_call(Box::new(|| ProviderCheckStatus::Valid));

        let check_2 = Arc::new(TestProviderCheck::default());
        check_2.check_call(Box::new(|| ProviderCheckStatus::Valid));
        check_2.check_call(Box::new(|| ProviderCheckStatus::Valid));

        let manager: ProviderManager<Arc<TestAdapter>> = ProviderManager::new(
            discard(),
            [(chain_name(), vec![adapter_1.clone(), adapter_2.clone()])],
            ProviderCheckStrategy::RequireAll(&[check_1.clone(), check_2.clone()]),
        );

        assert_eq!(
            ids(manager.providers(&chain_name()).await.unwrap()),
            vec![1, 2],
        );

        // Another call will not trigger a new validation.
        assert_eq!(
            ids(manager.providers(&chain_name()).await.unwrap()),
            vec![1, 2],
        );
    }

    #[tokio::test]
    async fn successful_provider_check_for_a_pool_of_adapters_for_a_provider() {
        let adapter_1 = Arc::new(TestAdapter::new(1));
        adapter_1.provider_name_call("provider_1".into());

        let adapter_2 = Arc::new(TestAdapter::new(2));
        adapter_2.provider_name_call("provider_1".into());

        let check_1 = Arc::new(TestProviderCheck::default());
        check_1.check_call(Box::new(|| ProviderCheckStatus::Valid));

        let manager: ProviderManager<Arc<TestAdapter>> = ProviderManager::new(
            discard(),
            [(chain_name(), vec![adapter_1.clone(), adapter_2.clone()])],
            ProviderCheckStrategy::RequireAll(&[check_1.clone()]),
        );

        assert_eq!(
            ids(manager.providers(&chain_name()).await.unwrap()),
            vec![1, 2],
        );

        // Another call will not trigger a new validation.
        assert_eq!(
            ids(manager.providers(&chain_name()).await.unwrap()),
            vec![1, 2],
        );
    }

    #[tokio::test]
    async fn multiple_successful_provider_checks_for_a_pool_of_adapters_for_a_provider() {
        let adapter_1 = Arc::new(TestAdapter::new(1));
        adapter_1.provider_name_call("provider_1".into());

        let adapter_2 = Arc::new(TestAdapter::new(2));
        adapter_2.provider_name_call("provider_1".into());

        let check_1 = Arc::new(TestProviderCheck::default());
        check_1.check_call(Box::new(|| ProviderCheckStatus::Valid));

        let check_2 = Arc::new(TestProviderCheck::default());
        check_2.check_call(Box::new(|| ProviderCheckStatus::Valid));

        let manager: ProviderManager<Arc<TestAdapter>> = ProviderManager::new(
            discard(),
            [(chain_name(), vec![adapter_1.clone(), adapter_2.clone()])],
            ProviderCheckStrategy::RequireAll(&[check_1.clone(), check_2.clone()]),
        );

        assert_eq!(
            ids(manager.providers(&chain_name()).await.unwrap()),
            vec![1, 2],
        );

        // Another call will not trigger a new validation.
        assert_eq!(
            ids(manager.providers(&chain_name()).await.unwrap()),
            vec![1, 2],
        );
    }

    #[tokio::test]
    async fn provider_validation_timeout() {
        let adapter_1 = Arc::new(TestAdapter::new(1));
        adapter_1.provider_name_call("provider_1".into());

        let check_1 = Arc::new(TestProviderCheck::default());
        check_1.check_call(Box::new(|| {
            std::thread::sleep(Duration::from_millis(200));
            ProviderCheckStatus::Valid
        }));

        let mut manager: ProviderManager<Arc<TestAdapter>> = ProviderManager::new(
            discard(),
            [(chain_name(), vec![adapter_1.clone()])],
            ProviderCheckStrategy::RequireAll(&[check_1.clone()]),
        );

        manager.validation_max_duration = Duration::from_millis(100);

        match manager.providers(&chain_name()).await {
            Ok(_) => {}
            Err(err) => {
                assert_eq!(
                    err.to_string(),
                    ProviderManagerError::ProviderValidationTimeout(chain_name()).to_string(),
                );
            }
        };
    }

    #[tokio::test]
    async fn no_providers_available() {
        let adapter_1 = Arc::new(TestAdapter::new(1));
        adapter_1.provider_name_call("provider_1".into());

        let check_1 = Arc::new(TestProviderCheck::default());
        check_1.check_call(Box::new(|| ProviderCheckStatus::TemporaryFailure {
            checked_at: Instant::now(),
            message: "error".to_owned(),
        }));

        let manager: ProviderManager<Arc<TestAdapter>> = ProviderManager::new(
            discard(),
            [(chain_name(), vec![adapter_1.clone()])],
            ProviderCheckStrategy::RequireAll(&[check_1.clone()]),
        );

        match manager.providers(&chain_name()).await {
            Ok(_) => {}
            Err(err) => {
                assert_eq!(
                    err.to_string(),
                    ProviderManagerError::NoProvidersAvailable(chain_name()).to_string(),
                );
            }
        };
    }

    #[tokio::test]
    async fn all_providers_failed() {
        let adapter_1 = Arc::new(TestAdapter::new(1));
        adapter_1.provider_name_call("provider_1".into());

        let check_1 = Arc::new(TestProviderCheck::default());
        check_1.check_call(Box::new(|| ProviderCheckStatus::Failed {
            message: "error".to_owned(),
        }));

        let manager: ProviderManager<Arc<TestAdapter>> = ProviderManager::new(
            discard(),
            [(chain_name(), vec![adapter_1.clone()])],
            ProviderCheckStrategy::RequireAll(&[check_1.clone()]),
        );

        match manager.providers(&chain_name()).await {
            Ok(_) => {}
            Err(err) => {
                assert_eq!(
                    err.to_string(),
                    ProviderManagerError::AllProvidersFailed(chain_name()).to_string(),
                );
            }
        };
    }

    #[tokio::test]
    async fn temporary_provider_check_failures_are_retried() {
        let adapter_1 = Arc::new(TestAdapter::new(1));
        adapter_1.provider_name_call("provider_1".into());

        let check_1 = Arc::new(TestProviderCheck::default());
        check_1.check_call(Box::new(|| ProviderCheckStatus::TemporaryFailure {
            checked_at: Instant::now(),
            message: "error".to_owned(),
        }));
        check_1.check_call(Box::new(|| ProviderCheckStatus::Valid));

        let mut manager: ProviderManager<Arc<TestAdapter>> = ProviderManager::new(
            discard(),
            [(chain_name(), vec![adapter_1.clone()])],
            ProviderCheckStrategy::RequireAll(&[check_1.clone()]),
        );

        manager.validation_retry_interval = Duration::from_millis(100);

        assert!(manager.providers(&chain_name()).await.is_err());

        tokio::time::sleep(Duration::from_millis(200)).await;

        assert_eq!(
            ids(manager.providers(&chain_name()).await.unwrap()),
            vec![1]
        );
    }

    #[tokio::test]
    async fn final_provider_check_failures_are_not_retried() {
        let adapter_1 = Arc::new(TestAdapter::new(1));
        adapter_1.provider_name_call("provider_1".into());

        let check_1 = Arc::new(TestProviderCheck::default());
        check_1.check_call(Box::new(|| ProviderCheckStatus::Failed {
            message: "error".to_owned(),
        }));

        let mut manager: ProviderManager<Arc<TestAdapter>> = ProviderManager::new(
            discard(),
            [(chain_name(), vec![adapter_1.clone()])],
            ProviderCheckStrategy::RequireAll(&[check_1.clone()]),
        );

        manager.validation_retry_interval = Duration::from_millis(100);

        assert!(manager.providers(&chain_name()).await.is_err());

        tokio::time::sleep(Duration::from_millis(200)).await;

        assert!(manager.providers(&chain_name()).await.is_err());
    }

    #[tokio::test]
    async fn mix_valid_and_invalid_providers() {
        let adapter_1 = Arc::new(TestAdapter::new(1));
        adapter_1.provider_name_call("provider_1".into());

        let adapter_2 = Arc::new(TestAdapter::new(2));
        adapter_2.provider_name_call("provider_2".into());

        let adapter_3 = Arc::new(TestAdapter::new(3));
        adapter_3.provider_name_call("provider_3".into());

        let check_1 = Arc::new(TestProviderCheck::default());
        check_1.check_call(Box::new(|| ProviderCheckStatus::Valid));
        check_1.check_call(Box::new(|| ProviderCheckStatus::Failed {
            message: "error".to_owned(),
        }));
        check_1.check_call(Box::new(|| ProviderCheckStatus::TemporaryFailure {
            checked_at: Instant::now(),
            message: "error".to_owned(),
        }));

        let manager: ProviderManager<Arc<TestAdapter>> = ProviderManager::new(
            discard(),
            [(
                chain_name(),
                vec![adapter_1.clone(), adapter_2.clone(), adapter_3.clone()],
            )],
            ProviderCheckStrategy::RequireAll(&[check_1.clone()]),
        );

        assert_eq!(
            ids(manager.providers(&chain_name()).await.unwrap()),
            vec![1]
        );
    }

    #[tokio::test]
    async fn one_provider_check_failure_is_enough_to_mark_an_provider_as_invalid() {
        let adapter_1 = Arc::new(TestAdapter::new(1));
        adapter_1.provider_name_call("provider_1".into());

        let check_1 = Arc::new(TestProviderCheck::default());
        check_1.check_call(Box::new(|| ProviderCheckStatus::Valid));

        let check_2 = Arc::new(TestProviderCheck::default());
        check_2.check_call(Box::new(|| ProviderCheckStatus::Failed {
            message: "error".to_owned(),
        }));

        let check_3 = Arc::new(TestProviderCheck::default());

        let manager: ProviderManager<Arc<TestAdapter>> = ProviderManager::new(
            discard(),
            [(chain_name(), vec![adapter_1.clone()])],
            ProviderCheckStrategy::RequireAll(&[check_1.clone(), check_2.clone(), check_3.clone()]),
        );

        assert!(manager.providers(&chain_name()).await.is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_providers_access_does_not_trigger_multiple_validations() {
        let adapter_1 = Arc::new(TestAdapter::new(1));
        adapter_1.provider_name_call("provider_1".into());

        let check_1 = Arc::new(TestProviderCheck::default());
        check_1.check_call(Box::new(|| ProviderCheckStatus::Valid));

        let manager: ProviderManager<Arc<TestAdapter>> = ProviderManager::new(
            discard(),
            [(chain_name(), vec![adapter_1.clone()])],
            ProviderCheckStrategy::RequireAll(&[check_1.clone()]),
        );

        let fut = || {
            let manager = manager.clone();

            async move {
                let chain_name = chain_name();

                ids(manager.providers(&chain_name).await.unwrap())
            }
        };

        let results = crate::futures03::future::join_all([fut(), fut(), fut(), fut()]).await;

        assert_eq!(
            results.into_iter().flatten().collect_vec(),
            vec![1, 1, 1, 1],
        );
    }
}
