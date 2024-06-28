use std::{
    collections::HashMap,
    ops::{Add, Deref},
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};

use itertools::Itertools;
use slog::{o, warn, Discard, Logger};
use thiserror::Error;

use crate::{
    blockchain::{BlockHash, ChainIdentifier},
    cheap_clone::CheapClone,
    data::value::Word,
    prelude::error,
    tokio::sync::RwLock,
};

use crate::components::store::{BlockStore as BlockStoreTrait, ChainStore as ChainStoreTrait};

const VALIDATION_ATTEMPT_TTL: Duration = Duration::minutes(5);

#[derive(Debug, Error)]
pub enum ProviderManagerError {
    #[error("unknown error {0}")]
    Unknown(#[from] anyhow::Error),
    #[error("provider {provider} on chain {chain_id} failed verification, expected ident {expected}, got {actual}")]
    ProviderFailedValidation {
        chain_id: ChainId,
        provider: ProviderName,
        expected: ChainIdentifier,
        actual: ChainIdentifier,
    },
    #[error("no providers available for chain {0}")]
    NoProvidersAvailable(ChainId),
    #[error("all providers for chain_id {0} have failed")]
    AllProvidersFailed(ChainId),
}

#[async_trait]
pub trait NetIdentifiable: Sync + Send {
    async fn net_identifiers(&self) -> Result<ChainIdentifier, anyhow::Error>;
    fn provider_name(&self) -> ProviderName;
}

#[async_trait]
impl<T: NetIdentifiable> NetIdentifiable for Arc<T> {
    async fn net_identifiers(&self) -> Result<ChainIdentifier, anyhow::Error> {
        self.as_ref().net_identifiers().await
    }
    fn provider_name(&self) -> ProviderName {
        self.as_ref().provider_name()
    }
}

pub type ProviderName = Word;
pub type ChainId = Word;

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
struct Ident {
    provider: ProviderName,
    chain_id: ChainId,
}

#[derive(Error, Debug, Clone, PartialEq)]
pub enum IdentValidatorError {
    #[error("database error: {0}")]
    UnknownError(String),
    #[error("Store ident wasn't set")]
    UnsetIdent,
    #[error("the net version for chain {chain_id} has changed from {store_net_version} to {chain_net_version} since the last time we ran")]
    ChangedNetVersion {
        chain_id: ChainId,
        store_net_version: String,
        chain_net_version: String,
    },
    #[error("the genesis block hash for chain {chain_id} has changed from {store_hash} to {chain_hash} since the last time we ran")]
    ChangedHash {
        chain_id: ChainId,
        store_hash: BlockHash,
        chain_hash: BlockHash,
    },
    #[error("unable to get store for chain {0}")]
    UnavailableStore(ChainId),
}

impl From<anyhow::Error> for IdentValidatorError {
    fn from(value: anyhow::Error) -> Self {
        Self::from(&value)
    }
}

impl From<&anyhow::Error> for IdentValidatorError {
    fn from(value: &anyhow::Error) -> Self {
        IdentValidatorError::UnknownError(value.to_string())
    }
}

#[async_trait]
/// IdentValidator validates that the provided chain ident matches the expected value for a certain
/// chain_id. This is probably only going to matter for the ChainStore but this allows us to decouple
/// the all the trait bounds and database integration from the ProviderManager and tests.
pub trait IdentValidator: Sync + Send {
    fn check_ident(
        &self,
        chain_id: &ChainId,
        ident: &ChainIdentifier,
    ) -> Result<(), IdentValidatorError>;

    fn update_ident(
        &self,
        chain_id: &ChainId,
        ident: &ChainIdentifier,
    ) -> Result<(), anyhow::Error>;
}

impl<T: ChainStoreTrait, B: BlockStoreTrait<ChainStore = T>> IdentValidator for B {
    fn check_ident(
        &self,
        chain_id: &ChainId,
        ident: &ChainIdentifier,
    ) -> Result<(), IdentValidatorError> {
        let network_chain = self
            .chain_store(&chain_id)
            .ok_or_else(|| IdentValidatorError::UnavailableStore(chain_id.clone()))?;
        let store_ident = network_chain
            .chain_identifier()
            .map_err(IdentValidatorError::from)?;

        if store_ident == ChainIdentifier::default() {
            return Err(IdentValidatorError::UnsetIdent);
        }

        if store_ident.net_version != ident.net_version {
            // This behavior is preserved from the previous implementation, firehose does not provide
            // a net_version so switching to and from firehose will cause this value to be different.
            // we prioritise rpc when creating the chain but it's possible that it is created by firehose
            // firehose always return 0 on net_version so we need to allow switching between the two.
            if store_ident.net_version != "0" && ident.net_version != "0" {
                return Err(IdentValidatorError::ChangedNetVersion {
                    chain_id: chain_id.clone(),
                    store_net_version: store_ident.net_version.clone(),
                    chain_net_version: ident.net_version.clone(),
                });
            }
        }

        let store_hash = &store_ident.genesis_block_hash;
        let chain_hash = &ident.genesis_block_hash;
        if store_hash != chain_hash {
            return Err(IdentValidatorError::ChangedHash {
                chain_id: chain_id.clone(),
                store_hash: store_hash.clone(),
                chain_hash: chain_hash.clone(),
            });
        }

        return Ok(());
    }

    fn update_ident(
        &self,
        chain_id: &ChainId,
        ident: &ChainIdentifier,
    ) -> Result<(), anyhow::Error> {
        let network_chain = self
            .chain_store(&chain_id)
            .ok_or_else(|| IdentValidatorError::UnavailableStore(chain_id.clone()))?;

        network_chain.set_chain_identifier(ident)?;

        Ok(())
    }
}

pub struct MockIdentValidator;

impl IdentValidator for MockIdentValidator {
    fn check_ident(
        &self,
        _chain_id: &ChainId,
        _ident: &ChainIdentifier,
    ) -> Result<(), IdentValidatorError> {
        Ok(())
    }

    fn update_ident(
        &self,
        _chain_id: &ChainId,
        _ident: &ChainIdentifier,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

/// ProviderCorrectness will maintain a list of providers which have had their
/// ChainIdentifiers checked. The first identifier is considered correct, if a later
/// provider for the same chain offers a different ChainIdentifier, this will be considered a
/// failed validation and it will be disabled.
#[derive(Clone, Debug)]
pub struct ProviderManager<T: NetIdentifiable + Clone + 'static> {
    inner: Arc<Inner<T>>,
}

impl<T: NetIdentifiable + Clone> CheapClone for ProviderManager<T> {
    fn cheap_clone(&self) -> Self {
        Self {
            inner: self.inner.cheap_clone(),
        }
    }
}

impl<T: NetIdentifiable + Clone> Default for ProviderManager<T> {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner {
                logger: Logger::root(Discard, o!()),
                adapters: HashMap::default(),
                status: vec![],
                validator: Arc::new(MockIdentValidator {}),
            }),
        }
    }
}

impl<T: NetIdentifiable + Clone + 'static> ProviderManager<T> {
    pub fn new(
        logger: Logger,
        adapters: impl Iterator<Item = (ChainId, Vec<T>)>,
        validator: Arc<dyn IdentValidator>,
    ) -> Self {
        let mut status: Vec<(Ident, RwLock<GenesisCheckStatus>)> = Vec::new();

        let adapters = HashMap::from_iter(adapters.map(|(chain_id, adapters)| {
            let adapters = adapters
                .into_iter()
                .map(|adapter| {
                    let name = adapter.provider_name();

                    // Get status index or add new status.
                    let index = match status
                        .iter()
                        .find_position(|(ident, _)| ident.provider.eq(&name))
                    {
                        Some((index, _)) => index,
                        None => {
                            status.push((
                                Ident {
                                    provider: name,
                                    chain_id: chain_id.clone(),
                                },
                                RwLock::new(GenesisCheckStatus::NotChecked),
                            ));
                            status.len() - 1
                        }
                    };
                    (index, adapter)
                })
                .collect_vec();

            (chain_id, adapters)
        }));

        Self {
            inner: Arc::new(Inner {
                logger,
                adapters,
                status,
                validator,
            }),
        }
    }

    pub fn len(&self, chain_id: &ChainId) -> usize {
        self.inner
            .adapters
            .get(chain_id)
            .map(|a| a.len())
            .unwrap_or_default()
    }

    #[cfg(debug_assertions)]
    pub async fn mark_all_valid(&self) {
        for (_, status) in self.inner.status.iter() {
            let mut s = status.write().await;
            *s = GenesisCheckStatus::Valid;
        }
    }

    async fn verify(&self, adapters: &Vec<(usize, T)>) -> Result<(), ProviderManagerError> {
        let mut tasks = vec![];

        for (index, adapter) in adapters.into_iter() {
            let inner = self.inner.cheap_clone();
            let adapter = adapter.clone();
            let index = *index;
            tasks.push(inner.verify_provider(index, adapter));
        }

        crate::futures03::future::join_all(tasks)
            .await
            .into_iter()
            .collect::<Result<Vec<()>, ProviderManagerError>>()?;

        Ok(())
    }

    /// get_all_unverified it's an escape hatch for places where checking the adapter status is
    /// undesirable or just can't be done because async can't be used. This function just returns
    /// the stored adapters and doesn't try to perform any verification. It will also return
    /// adapters that failed verification. For the most part this should be fine since ideally
    /// get_all would have been used before. Nevertheless, it is possible that a misconfigured
    /// adapter is returned from this list even after validation.
    pub fn get_all_unverified(&self, chain_id: &ChainId) -> Vec<&T> {
        self.inner
            .adapters
            .get(chain_id)
            .map(|v| v.iter().map(|v| &v.1).collect())
            .unwrap_or_default()
    }

    /// get_all will trigger the verification of the endpoints for the provided chain_id, hence the
    /// async. If this is undesirable, check `get_all_unverified` as an alternatives that does not
    /// cause the validation but also doesn't not guaratee any adapters have been validated.
    pub async fn get_all(&self, chain_id: &ChainId) -> Result<Vec<&T>, ProviderManagerError> {
        tokio::time::timeout(std::time::Duration::from_secs(5), async move {
            let adapters = match self.inner.adapters.get(chain_id) {
                Some(adapters) if !adapters.is_empty() => adapters,
                _ => return Ok(vec![]),
            };

            // Optimistic check
            if self.inner.is_all_verified(&adapters).await {
                return Ok(adapters.iter().map(|v| &v.1).collect());
            }

            match self.verify(adapters).await {
                Ok(_) => {}
                Err(error) => error!(
                    self.inner.logger,
                    "unable to verify genesis for adapter: {}",
                    error.to_string()
                ),
            }

            self.inner.get_verified_for_chain(&chain_id).await
        })
        .await
        .map_err(|_| crate::anyhow::anyhow!("timed out, validation took too long"))?
    }
}

struct Inner<T: NetIdentifiable> {
    logger: Logger,
    // Most operations start by getting the value so we keep track of the index to minimize the
    // locked surface.
    adapters: HashMap<ChainId, Vec<(usize, T)>>,
    // Status per (ChainId, ProviderName) pair. The RwLock here helps prevent multiple concurrent
    // checks for the same provider, when one provider is being checked, all other uses will wait,
    // this is correct because no provider should be used until they have been validated.
    // There shouldn't be many values here so Vec is fine even if less ergonomic, because we track
    // the index alongside the adapter it should be O(1) after initialization.
    status: Vec<(Ident, RwLock<GenesisCheckStatus>)>,
    // Validator used to compare the existing identifier to the one returned by an adapter.
    validator: Arc<dyn IdentValidator + 'static>,
}

impl<T: NetIdentifiable + 'static> std::fmt::Debug for Inner<T> {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl<T: NetIdentifiable + 'static> Inner<T> {
    async fn is_all_verified(&self, adapters: &Vec<(usize, T)>) -> bool {
        for (index, _) in adapters.iter() {
            let status = self.status.get(*index).unwrap().1.read().await;
            if *status != GenesisCheckStatus::Valid {
                return false;
            }
        }

        true
    }

    /// Returns any adapters that have been validated, empty if none are defined or an error if
    /// all adapters have failed or are unavailable, returns different errors for these use cases
    /// so that that caller can handle the different situations, as one is permanent and the other
    /// is retryable.
    async fn get_verified_for_chain(
        &self,
        chain_id: &ChainId,
    ) -> Result<Vec<&T>, ProviderManagerError> {
        let mut out = vec![];
        let adapters = match self.adapters.get(chain_id) {
            Some(adapters) if !adapters.is_empty() => adapters,
            _ => return Ok(vec![]),
        };

        let mut failed = 0;
        for (index, adapter) in adapters.iter() {
            let status = self.status.get(*index).unwrap().1.read().await;
            match status.deref() {
                GenesisCheckStatus::Valid => {}
                GenesisCheckStatus::Failed => {
                    failed += 1;
                    continue;
                }
                GenesisCheckStatus::NotChecked | GenesisCheckStatus::TemporaryFailure { .. } => {
                    continue
                }
            }
            out.push(adapter);
        }

        if out.is_empty() {
            if failed == adapters.len() {
                return Err(ProviderManagerError::AllProvidersFailed(chain_id.clone()));
            }

            return Err(ProviderManagerError::NoProvidersAvailable(chain_id.clone()));
        }

        Ok(out)
    }

    async fn get_ident_status(&self, index: usize) -> (Ident, GenesisCheckStatus) {
        match self.status.get(index) {
            Some(status) => (status.0.clone(), status.1.read().await.clone()),
            None => (Ident::default(), GenesisCheckStatus::Failed),
        }
    }

    fn ttl_has_elapsed(checked_at: &DateTime<Utc>) -> bool {
        checked_at.add(VALIDATION_ATTEMPT_TTL) < Utc::now()
    }

    fn should_verify(status: &GenesisCheckStatus) -> bool {
        match status {
            GenesisCheckStatus::TemporaryFailure { checked_at }
                if Self::ttl_has_elapsed(checked_at) =>
            {
                true
            }
            // Let check the provider
            GenesisCheckStatus::NotChecked => true,
            _ => false,
        }
    }

    async fn verify_provider(
        self: Arc<Inner<T>>,
        index: usize,
        adapter: T,
    ) -> Result<(), ProviderManagerError> {
        let (ident, status) = self.get_ident_status(index).await;
        if !Self::should_verify(&status) {
            return Ok(());
        }

        let mut status = self.status.get(index).unwrap().1.write().await;
        // double check nothing has changed.
        if !Self::should_verify(&status) {
            return Ok(());
        }

        let chain_ident = match adapter.net_identifiers().await {
            Ok(ident) => ident,
            Err(err) => {
                error!(
                    &self.logger,
                    "failed to get net identifiers: {}",
                    err.to_string()
                );
                *status = GenesisCheckStatus::TemporaryFailure {
                    checked_at: Utc::now(),
                };

                return Err(err.into());
            }
        };

        match self.validator.check_ident(&ident.chain_id, &chain_ident) {
            Ok(_) => {
                *status = GenesisCheckStatus::Valid;
            }
            Err(err) => match err {
                IdentValidatorError::UnsetIdent => {
                    self.validator
                        .update_ident(&ident.chain_id, &chain_ident)
                        .map_err(ProviderManagerError::from)?;
                    *status = GenesisCheckStatus::Valid;
                }
                IdentValidatorError::ChangedNetVersion {
                    chain_id,
                    store_net_version,
                    chain_net_version,
                } if store_net_version == "0" => {
                    warn!(self.logger,
                            "the net version for chain {} has changed from 0 to {} since the last time we ran, ignoring difference because 0 means UNSET and firehose does not provide it",
                            chain_id,
                            chain_net_version,
                            );
                    *status = GenesisCheckStatus::Valid;
                }
                IdentValidatorError::ChangedNetVersion {
                    store_net_version,
                    chain_net_version,
                    ..
                } => {
                    *status = GenesisCheckStatus::Failed;
                    return Err(ProviderManagerError::ProviderFailedValidation {
                        provider: ident.provider,
                        expected: ChainIdentifier {
                            net_version: store_net_version,
                            genesis_block_hash: chain_ident.genesis_block_hash.clone(),
                        },
                        actual: ChainIdentifier {
                            net_version: chain_net_version,
                            genesis_block_hash: chain_ident.genesis_block_hash,
                        },
                        chain_id: ident.chain_id.clone(),
                    });
                }
                IdentValidatorError::ChangedHash {
                    store_hash,
                    chain_hash,
                    ..
                } => {
                    *status = GenesisCheckStatus::Failed;
                    return Err(ProviderManagerError::ProviderFailedValidation {
                        provider: ident.provider,
                        expected: ChainIdentifier {
                            net_version: chain_ident.net_version.clone(),
                            genesis_block_hash: store_hash,
                        },
                        actual: ChainIdentifier {
                            net_version: chain_ident.net_version,
                            genesis_block_hash: chain_hash,
                        },
                        chain_id: ident.chain_id.clone(),
                    });
                }
                e @ IdentValidatorError::UnavailableStore(_)
                | e @ IdentValidatorError::UnknownError(_) => {
                    *status = GenesisCheckStatus::TemporaryFailure {
                        checked_at: Utc::now(),
                    };

                    return Err(ProviderManagerError::Unknown(crate::anyhow::anyhow!(
                        e.to_string()
                    )));
                }
            },
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum GenesisCheckStatus {
    NotChecked,
    TemporaryFailure { checked_at: DateTime<Utc> },
    Valid,
    Failed,
}

#[cfg(test)]
mod test {
    use std::{
        ops::Sub,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
    };

    use crate::{
        bail,
        blockchain::BlockHash,
        components::adapter::{ChainId, GenesisCheckStatus, MockIdentValidator},
        data::value::Word,
        prelude::lazy_static,
    };
    use async_trait::async_trait;
    use chrono::{Duration, Utc};
    use ethabi::ethereum_types::H256;
    use slog::{o, Discard, Logger};

    use crate::{blockchain::ChainIdentifier, components::adapter::ProviderManagerError};

    use super::{
        IdentValidator, IdentValidatorError, NetIdentifiable, ProviderManager, ProviderName,
        VALIDATION_ATTEMPT_TTL,
    };

    const TEST_CHAIN_ID: &str = "valid";

    lazy_static! {
        static ref UNTESTABLE_ADAPTER: MockAdapter =
                    MockAdapter{
                provider: "untestable".into(),
                status: GenesisCheckStatus::TemporaryFailure { checked_at: Utc::now()},
            };

                        // way past TTL, ready to check again
        static ref TESTABLE_ADAPTER: MockAdapter =
            MockAdapter{
                provider: "testable".into(),
                status: GenesisCheckStatus::TemporaryFailure { checked_at: Utc::now().sub(Duration::seconds(10000000)) },
            };
        static ref VALID_ADAPTER: MockAdapter = MockAdapter {provider: "valid".into(), status: GenesisCheckStatus::Valid,};
        static ref FAILED_ADAPTER: MockAdapter = MockAdapter {provider: "FAILED".into(), status: GenesisCheckStatus::Failed,};
        static ref NEW_CHAIN_IDENT: ChainIdentifier =ChainIdentifier { net_version: "123".to_string(), genesis_block_hash:  BlockHash::from( H256::repeat_byte(1))};
    }

    struct TestValidator {
        check_result: Result<(), IdentValidatorError>,
        expected_new_ident: Option<ChainIdentifier>,
    }

    impl IdentValidator for TestValidator {
        fn check_ident(
            &self,
            _chain_id: &ChainId,
            _ident: &ChainIdentifier,
        ) -> Result<(), IdentValidatorError> {
            self.check_result.clone()
        }

        fn update_ident(
            &self,
            _chain_id: &ChainId,
            ident: &ChainIdentifier,
        ) -> Result<(), anyhow::Error> {
            match self.expected_new_ident.as_ref() {
                None => unreachable!("unexpected call to update_ident"),
                Some(ident_expected) if ident_expected.eq(ident) => Ok(()),
                Some(_) => bail!("update_ident called with unexpected value"),
            }
        }
    }

    #[derive(Clone, PartialEq, Eq, Debug)]
    struct MockAdapter {
        provider: Word,
        status: GenesisCheckStatus,
    }

    #[async_trait]
    impl NetIdentifiable for MockAdapter {
        async fn net_identifiers(&self) -> Result<ChainIdentifier, anyhow::Error> {
            match self.status {
                GenesisCheckStatus::TemporaryFailure { checked_at }
                    if checked_at > Utc::now().sub(VALIDATION_ATTEMPT_TTL) =>
                {
                    unreachable!("should never check if ttl has not elapsed");
                }
                _ => Ok(NEW_CHAIN_IDENT.clone()),
            }
        }

        fn provider_name(&self) -> ProviderName {
            self.provider.clone()
        }
    }

    #[tokio::test]
    async fn test_provider_manager() {
        struct Case<'a> {
            name: &'a str,
            chain_id: &'a str,
            adapters: Vec<(ChainId, Vec<MockAdapter>)>,
            validator: Option<TestValidator>,
            expected: Result<Vec<&'a MockAdapter>, ProviderManagerError>,
        }

        let cases = vec![
            Case {
                name: "no adapters",
                chain_id: TEST_CHAIN_ID,
                adapters: vec![],
                validator: None,
                expected: Ok(vec![]),
            },
            Case {
                name: "no adapters",
                chain_id: TEST_CHAIN_ID,
                adapters: vec![(TEST_CHAIN_ID.into(), vec![TESTABLE_ADAPTER.clone()])],
                validator: Some(TestValidator {
                    check_result: Err(IdentValidatorError::UnsetIdent),
                    expected_new_ident: Some(NEW_CHAIN_IDENT.clone()),
                }),
                expected: Ok(vec![&TESTABLE_ADAPTER]),
            },
            Case {
                name: "adapter temporary failure with Ident unset",
                chain_id: TEST_CHAIN_ID,
                // UNTESTABLE_ADAPTER has failed ident, will be valid cause idents has None value
                adapters: vec![(TEST_CHAIN_ID.into(), vec![UNTESTABLE_ADAPTER.clone()])],
                validator: None,
                expected: Err(ProviderManagerError::NoProvidersAvailable(
                    TEST_CHAIN_ID.into(),
                )),
            },
            Case {
                name: "adapter temporary failure",
                chain_id: TEST_CHAIN_ID,
                adapters: vec![(TEST_CHAIN_ID.into(), vec![UNTESTABLE_ADAPTER.clone()])],
                validator: None,
                expected: Err(ProviderManagerError::NoProvidersAvailable(
                    TEST_CHAIN_ID.into(),
                )),
            },
            Case {
                name: "wrong chain ident",
                chain_id: TEST_CHAIN_ID,
                adapters: vec![(TEST_CHAIN_ID.into(), vec![FAILED_ADAPTER.clone()])],
                validator: Some(TestValidator {
                    check_result: Err(IdentValidatorError::ChangedNetVersion {
                        chain_id: TEST_CHAIN_ID.into(),
                        store_net_version: "".to_string(),
                        chain_net_version: "".to_string(),
                    }),
                    expected_new_ident: None,
                }),
                expected: Err(ProviderManagerError::AllProvidersFailed(
                    TEST_CHAIN_ID.into(),
                )),
            },
            Case {
                name: "all adapters ok or not checkable yet",
                chain_id: TEST_CHAIN_ID,
                adapters: vec![(
                    TEST_CHAIN_ID.into(),
                    vec![VALID_ADAPTER.clone(), FAILED_ADAPTER.clone()],
                )],
                // if a check is performed (which it shouldn't) the test will fail
                validator: Some(TestValidator {
                    check_result: Err(IdentValidatorError::ChangedNetVersion {
                        chain_id: TEST_CHAIN_ID.into(),
                        store_net_version: "".to_string(),
                        chain_net_version: "".to_string(),
                    }),
                    expected_new_ident: None,
                }),
                expected: Ok(vec![&VALID_ADAPTER]),
            },
            Case {
                name: "all adapters ok or checkable",
                chain_id: TEST_CHAIN_ID,
                adapters: vec![(
                    TEST_CHAIN_ID.into(),
                    vec![VALID_ADAPTER.clone(), TESTABLE_ADAPTER.clone()],
                )],
                validator: None,
                expected: Ok(vec![&VALID_ADAPTER, &TESTABLE_ADAPTER]),
            },
        ];

        for case in cases.into_iter() {
            let Case {
                name,
                chain_id,
                adapters,
                validator,
                expected,
            } = case;

            let logger = Logger::root(Discard, o!());
            let chain_id = chain_id.into();

            let validator: Arc<dyn IdentValidator> = match validator {
                None => Arc::new(MockIdentValidator {}),
                Some(validator) => Arc::new(validator),
            };

            let manager = ProviderManager::new(logger, adapters.clone().into_iter(), validator);

            for (_, adapters) in adapters.iter() {
                for adapter in adapters.iter() {
                    let provider = adapter.provider.clone();
                    let slot = manager
                        .inner
                        .status
                        .iter()
                        .find(|(ident, _)| ident.provider.eq(&provider))
                        .expect(&format!(
                            "case: {} - there should be a status for provider \"{}\"",
                            name, provider
                        ));
                    let mut s = slot.1.write().await;
                    *s = adapter.status.clone();
                }
            }

            let result = manager.get_all(&chain_id).await;
            match (expected, result) {
                (Ok(expected), Ok(result)) => assert_eq!(
                    expected, result,
                    "case {} failed. Result: {:?}",
                    name, result
                ),
                (Err(expected), Err(result)) => assert_eq!(
                    expected.to_string(),
                    result.to_string(),
                    "case {} failed. Result: {:?}",
                    name,
                    result
                ),
                (Ok(expected), Err(result)) => panic!(
                    "case {} failed. Result: {}, Expected: {:?}",
                    name, result, expected
                ),
                (Err(expected), Ok(result)) => panic!(
                    "case {} failed. Result: {:?}, Expected: {}",
                    name, result, expected
                ),
            }
        }
    }

    #[tokio::test]
    async fn test_provider_manager_updates_on_unset() {
        #[derive(Clone, Debug, Eq, PartialEq)]
        struct MockAdapter {}

        #[async_trait]
        impl NetIdentifiable for MockAdapter {
            async fn net_identifiers(&self) -> Result<ChainIdentifier, anyhow::Error> {
                Ok(NEW_CHAIN_IDENT.clone())
            }
            fn provider_name(&self) -> ProviderName {
                TEST_CHAIN_ID.into()
            }
        }

        struct TestValidator {
            called: AtomicBool,
            err: IdentValidatorError,
        }

        impl IdentValidator for TestValidator {
            fn check_ident(
                &self,
                _chain_id: &ChainId,
                _ident: &ChainIdentifier,
            ) -> Result<(), IdentValidatorError> {
                Err(self.err.clone())
            }

            fn update_ident(
                &self,
                _chain_id: &ChainId,
                ident: &ChainIdentifier,
            ) -> Result<(), anyhow::Error> {
                if NEW_CHAIN_IDENT.eq(ident) {
                    self.called.store(true, Ordering::SeqCst);
                    return Ok(());
                }

                unreachable!("unexpected call to update_ident ot unexpected ident passed");
            }
        }

        let logger = Logger::root(Discard, o!());
        let chain_id = TEST_CHAIN_ID.into();

        // Ensure the provider updates the chain ident when it wasn't set yet.
        let validator = Arc::new(TestValidator {
            called: AtomicBool::default(),
            err: IdentValidatorError::UnsetIdent,
        });
        let adapter = MockAdapter {};

        let manager = ProviderManager::new(
            logger,
            vec![(TEST_CHAIN_ID.into(), vec![adapter.clone()])].into_iter(),
            validator.clone(),
        );

        let mut result = manager.get_all(&chain_id).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(&adapter, result.pop().unwrap());
        assert_eq!(validator.called.load(Ordering::SeqCst), true);
    }
}
