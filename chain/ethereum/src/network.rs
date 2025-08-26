use anyhow::{anyhow, bail};
use graph::blockchain::ChainIdentifier;
use graph::components::network_provider::ChainName;
use graph::components::network_provider::NetworkDetails;
use graph::components::network_provider::ProviderManager;
use graph::components::network_provider::ProviderName;
use graph::endpoint::EndpointMetrics;
use graph::firehose::{AvailableCapacity, SubgraphLimit};
use graph::prelude::rand::{
    self,
    distr::{weighted::WeightedIndex, Distribution},
    seq::IteratorRandom,
    Rng,
};
use itertools::Itertools;
use std::sync::Arc;

pub use graph::impl_slog_value;
use graph::prelude::{async_trait, Error};

use crate::adapter::EthereumAdapter as _;
use crate::capabilities::NodeCapabilities;
use crate::EthereumAdapter;

pub const DEFAULT_ADAPTER_ERROR_RETEST_PERCENT: f64 = 0.2;

#[derive(Debug, Clone)]
pub struct EthereumNetworkAdapter {
    endpoint_metrics: Arc<EndpointMetrics>,
    pub capabilities: NodeCapabilities,
    pub adapter: Arc<EthereumAdapter>,
    /// The maximum number of times this adapter can be used. We use the
    /// strong_count on `adapter` to determine whether the adapter is above
    /// that limit. That's a somewhat imprecise but convenient way to
    /// determine the number of connections
    limit: SubgraphLimit,
    weight: f64,
}

#[async_trait]
impl NetworkDetails for EthereumNetworkAdapter {
    fn provider_name(&self) -> ProviderName {
        self.adapter.provider().into()
    }

    async fn chain_identifier(&self) -> Result<ChainIdentifier, Error> {
        self.adapter.net_identifiers().await
    }

    async fn provides_extended_blocks(&self) -> Result<bool, Error> {
        Ok(true)
    }
}

impl EthereumNetworkAdapter {
    pub fn new(
        endpoint_metrics: Arc<EndpointMetrics>,
        capabilities: NodeCapabilities,
        adapter: Arc<EthereumAdapter>,
        limit: SubgraphLimit,
        weight: f64,
    ) -> Self {
        Self {
            endpoint_metrics,
            capabilities,
            adapter,
            limit,
            weight,
        }
    }

    #[cfg(debug_assertions)]
    fn is_call_only(&self) -> bool {
        self.adapter.is_call_only()
    }

    pub fn get_capacity(&self) -> AvailableCapacity {
        self.limit.get_capacity(Arc::strong_count(&self.adapter))
    }

    pub fn current_error_count(&self) -> u64 {
        self.endpoint_metrics.get_count(&self.provider().into())
    }
    pub fn provider(&self) -> &str {
        self.adapter.provider()
    }
}

use crate::health::Health;

#[derive(Debug, Clone)]
pub struct EthereumNetworkAdapters {
    chain_id: ChainName,
    manager: ProviderManager<EthereumNetworkAdapter>,
    call_only_adapters: Vec<EthereumNetworkAdapter>,
    // Percentage of request that should be used to retest errored adapters.
    retest_percent: f64,
    weighted: bool,
    health_checkers: Vec<Arc<Health>>,
}

impl EthereumNetworkAdapters {
    pub fn empty_for_testing() -> Self {
        Self {
            chain_id: "".into(),
            manager: ProviderManager::default(),
            call_only_adapters: vec![],
            retest_percent: DEFAULT_ADAPTER_ERROR_RETEST_PERCENT,
            weighted: false,
            health_checkers: vec![],
        }
    }

    #[cfg(debug_assertions)]
    pub async fn for_testing(
        mut adapters: Vec<EthereumNetworkAdapter>,
        call_only: Vec<EthereumNetworkAdapter>,
    ) -> Self {
        use std::cmp::Ordering;

        use graph::components::network_provider::ProviderCheckStrategy;
        use graph::slog::{o, Discard, Logger};

        let chain_id: ChainName = "testing".into();
        adapters.sort_by(|a, b| {
            a.capabilities
                .partial_cmp(&b.capabilities)
                .unwrap_or(Ordering::Equal)
        });

        let provider = ProviderManager::new(
            Logger::root(Discard, o!()),
            vec![(chain_id.clone(), adapters)].into_iter(),
            ProviderCheckStrategy::MarkAsValid,
        );

        Self::new(chain_id, provider, call_only, None, false, vec![])
    }

    pub fn new(
        chain_id: ChainName,
        manager: ProviderManager<EthereumNetworkAdapter>,
        call_only_adapters: Vec<EthereumNetworkAdapter>,
        retest_percent: Option<f64>,
        weighted: bool,
        health_checkers: Vec<Arc<Health>>,
    ) -> Self {
        #[cfg(debug_assertions)]
        call_only_adapters.iter().for_each(|a| {
            a.is_call_only();
        });

        Self {
            chain_id,
            manager,
            call_only_adapters,
            retest_percent: retest_percent.unwrap_or(DEFAULT_ADAPTER_ERROR_RETEST_PERCENT),
            weighted,
            health_checkers,
        }
    }

    fn available_with_capabilities<'a>(
        input: Vec<&'a EthereumNetworkAdapter>,
        required_capabilities: &NodeCapabilities,
    ) -> impl Iterator<Item = &'a EthereumNetworkAdapter> + 'a {
        let cheapest_sufficient_capability = input
            .iter()
            .find(|adapter| &adapter.capabilities >= required_capabilities)
            .map(|adapter| &adapter.capabilities);

        input
            .into_iter()
            .filter(move |adapter| Some(&adapter.capabilities) == cheapest_sufficient_capability)
            .filter(|adapter| adapter.get_capacity() > AvailableCapacity::Unavailable)
    }

    /// returns all the available adapters that meet the required capabilities
    /// if no adapters are available at the time or none that meet the capabilities then
    /// an empty iterator is returned.
    pub async fn all_cheapest_with(
        &self,
        required_capabilities: &NodeCapabilities,
    ) -> impl Iterator<Item = &EthereumNetworkAdapter> + '_ {
        let all = self
            .manager
            .providers(&self.chain_id)
            .await
            .map(|adapters| adapters.collect_vec())
            .unwrap_or_default();

        Self::available_with_capabilities(all, required_capabilities)
    }

    // get all the adapters, don't trigger the ProviderManager's validations because we want
    // this function to remain sync. If no adapters are available an empty iterator is returned.
    pub(crate) fn all_unverified_cheapest_with(
        &self,
        required_capabilities: &NodeCapabilities,
    ) -> impl Iterator<Item = &EthereumNetworkAdapter> + '_ {
        let all = self
            .manager
            .providers_unchecked(&self.chain_id)
            .collect_vec();

        Self::available_with_capabilities(all, required_capabilities)
    }

    /// Main adapter selection entry point that handles both weight-based distribution
    /// and error retesting logic.
    ///
    /// The selection process:
    /// 1. First selects an adapter based on weights (if enabled) or random selection
    /// 2. Occasionally overrides the selection to retest adapters with errors
    ///
    /// The error retesting happens AFTER weight-based selection to minimize
    /// distribution skew while still allowing periodic health checks of errored endpoints.
    fn cheapest_from(
        &self,
        input: Vec<&EthereumNetworkAdapter>,
        required_capabilities: &NodeCapabilities,
    ) -> Result<Arc<EthereumAdapter>, Error> {
        // Select adapter based on weights or random strategy
        let selected_adapter = self.select_best_adapter(input.clone(), required_capabilities)?;

        // Occasionally override selection to retest errored adapters
        // This happens AFTER weight-based selection to minimize distribution skew
        let retest_rng: f64 = rand::rng().random();
        if retest_rng < self.retest_percent {
            // Find the adapter with the highest error count
            if let Some(most_errored) = input
                .iter()
                .max_by_key(|a| a.current_error_count())
                .filter(|a| a.current_error_count() > 0)
            {
                return Ok(most_errored.adapter.clone());
            }
        }

        Ok(selected_adapter)
    }

    /// Selects the best adapter based on the configured strategy (weighted or random).
    /// If weighted mode is enabled, uses weight-based probabilistic selection.
    /// Otherwise, falls back to random selection with error count consideration.
    fn select_best_adapter(
        &self,
        input: Vec<&EthereumNetworkAdapter>,
        required_capabilities: &NodeCapabilities,
    ) -> Result<Arc<EthereumAdapter>, Error> {
        if self.weighted {
            self.select_weighted_adapter(input, required_capabilities)
        } else {
            self.select_random_adapter(input, required_capabilities)
        }
    }

    /// Performs weighted random selection of adapters based on their configured weights.
    ///
    /// Weights are relative values between 0.0 and 1.0 that determine the probability
    /// of selecting each adapter. They don't need to sum to 1.0 as they're normalized
    /// internally by the WeightedIndex distribution.
    ///
    /// Falls back to random selection if weights are invalid (e.g., all zeros).
    fn select_weighted_adapter(
        &self,
        input: Vec<&EthereumNetworkAdapter>,
        required_capabilities: &NodeCapabilities,
    ) -> Result<Arc<EthereumAdapter>, Error> {
        if input.is_empty() {
            return Err(anyhow!(
                "A matching Ethereum network with {:?} was not found.",
                required_capabilities
            ));
        }

        let weights: Vec<_> = input
            .iter()
            .map(|a| {
                let health_checker = self
                    .health_checkers
                    .iter()
                    .find(|h| h.provider() == a.provider());
                let score = health_checker.map_or(1.0, |h| h.score());
                a.weight * score
            })
            .collect();

        if let Ok(dist) = WeightedIndex::new(&weights) {
            let idx = dist.sample(&mut rand::rng());
            Ok(input[idx].adapter.clone())
        } else {
            // Fallback to random selection if weights are invalid
            self.select_random_adapter(input, required_capabilities)
        }
    }

    /// Performs random selection of adapters with preference for those with fewer errors.
    ///
    /// Randomly selects up to 3 adapters from the available pool, then chooses the one
    /// with the lowest error count. This provides a balance between load distribution
    /// and avoiding problematic endpoints.
    fn select_random_adapter(
        &self,
        input: Vec<&EthereumNetworkAdapter>,
        required_capabilities: &NodeCapabilities,
    ) -> Result<Arc<EthereumAdapter>, Error> {
        let choices = input.into_iter().choose_multiple(&mut rand::rng(), 3);
        if let Some(adapter) = choices.iter().min_by_key(|a| a.current_error_count()) {
            Ok(adapter.adapter.clone())
        } else {
            Err(anyhow!(
                "A matching Ethereum network with {:?} was not found.",
                required_capabilities
            ))
        }
    }

    pub(crate) fn unverified_cheapest_with(
        &self,
        required_capabilities: &NodeCapabilities,
    ) -> Result<Arc<EthereumAdapter>, Error> {
        let cheapest = self
            .all_unverified_cheapest_with(required_capabilities)
            .collect_vec();

        self.cheapest_from(cheapest, required_capabilities)
    }

    /// This is the public entry point and should always use verified adapters
    pub async fn cheapest_with(
        &self,
        required_capabilities: &NodeCapabilities,
    ) -> Result<Arc<EthereumAdapter>, Error> {
        let cheapest = self
            .all_cheapest_with(required_capabilities)
            .await
            .collect_vec();

        self.cheapest_from(cheapest, required_capabilities)
    }

    pub async fn cheapest(&self) -> Option<Arc<EthereumAdapter>> {
        // EthereumAdapters are sorted by their NodeCapabilities when the EthereumNetworks
        // struct is instantiated so they do not need to be sorted here
        self.manager
            .providers(&self.chain_id)
            .await
            .map(|mut adapters| adapters.next())
            .unwrap_or_default()
            .map(|ethereum_network_adapter| ethereum_network_adapter.adapter.clone())
    }

    /// call_or_cheapest will bypass ProviderManagers' validation in order to remain non async.
    /// ideally this should only be called for already validated providers.
    pub fn call_or_cheapest(
        &self,
        capabilities: Option<&NodeCapabilities>,
    ) -> anyhow::Result<Arc<EthereumAdapter>> {
        // call_only_adapter can fail if we're out of capcity, this is fine since
        // we would want to fallback onto a full adapter
        // so we will ignore this error and return whatever comes out of `cheapest_with`
        match self.call_only_adapter() {
            Ok(Some(adapter)) => Ok(adapter),
            _ => {
                self.unverified_cheapest_with(capabilities.unwrap_or(&NodeCapabilities {
                    // Archive is required for call_only
                    archive: true,
                    traces: false,
                }))
            }
        }
    }

    pub fn call_only_adapter(&self) -> anyhow::Result<Option<Arc<EthereumAdapter>>> {
        if self.call_only_adapters.is_empty() {
            return Ok(None);
        }

        let adapters = self
            .call_only_adapters
            .iter()
            .min_by_key(|x| Arc::strong_count(&x.adapter))
            .ok_or(anyhow!("no available call only endpoints "))?;

        // TODO: This will probably blow up a lot sooner than [limit] amount of
        // subgraphs, since we probably use a few instances.
        if !adapters
            .limit
            .has_capacity(Arc::strong_count(&adapters.adapter))
        {
            bail!("call only adapter has reached the concurrency limit ");
        }

        // Cloning here ensure we have the correct count at any given time, if we return a reference it can be cloned later
        // which could cause a high number of endpoints to be given away before accounting for them.
        Ok(Some(adapters.adapter.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::Health;
    use graph::cheap_clone::CheapClone;
    use graph::components::network_provider::ProviderCheckStrategy;
    use graph::components::network_provider::ProviderManager;
    use graph::components::network_provider::ProviderName;
    use graph::data::value::Word;
    use graph::http::HeaderMap;
    use graph::slog::{o, Discard, Logger};
    use graph::{
        endpoint::EndpointMetrics, firehose::SubgraphLimit, prelude::MetricsRegistry, tokio,
        url::Url,
    };
    use std::sync::Arc;

    use crate::{EthereumAdapter, EthereumAdapterTrait, ProviderEthRpcMetrics, Transport};

    use super::{EthereumNetworkAdapter, EthereumNetworkAdapters, NodeCapabilities};

    #[test]
    fn ethereum_capabilities_comparison() {
        let archive = NodeCapabilities {
            archive: true,
            traces: false,
        };
        let traces = NodeCapabilities {
            archive: false,
            traces: true,
        };
        let archive_traces = NodeCapabilities {
            archive: true,
            traces: true,
        };
        let full = NodeCapabilities {
            archive: false,
            traces: false,
        };
        let full_traces = NodeCapabilities {
            archive: false,
            traces: true,
        };

        // Test all real combinations of capability comparisons
        assert_eq!(false, &full >= &archive);
        assert_eq!(false, &full >= &traces);
        assert_eq!(false, &full >= &archive_traces);
        assert_eq!(true, &full >= &full);
        assert_eq!(false, &full >= &full_traces);

        assert_eq!(true, &archive >= &archive);
        assert_eq!(false, &archive >= &traces);
        assert_eq!(false, &archive >= &archive_traces);
        assert_eq!(true, &archive >= &full);
        assert_eq!(false, &archive >= &full_traces);

        assert_eq!(false, &traces >= &archive);
        assert_eq!(true, &traces >= &traces);
        assert_eq!(false, &traces >= &archive_traces);
        assert_eq!(true, &traces >= &full);
        assert_eq!(true, &traces >= &full_traces);

        assert_eq!(true, &archive_traces >= &archive);
        assert_eq!(true, &archive_traces >= &traces);
        assert_eq!(true, &archive_traces >= &archive_traces);
        assert_eq!(true, &archive_traces >= &full);
        assert_eq!(true, &archive_traces >= &full_traces);

        assert_eq!(false, &full_traces >= &archive);
        assert_eq!(true, &full_traces >= &traces);
        assert_eq!(false, &full_traces >= &archive_traces);
        assert_eq!(true, &full_traces >= &full);
        assert_eq!(true, &full_traces >= &full_traces);
    }

    #[tokio::test]
    async fn adapter_selector_selects_eth_call() {
        let metrics = Arc::new(EndpointMetrics::mock());
        let logger = graph::log::logger(true);
        let mock_registry = Arc::new(MetricsRegistry::mock());
        let transport = Transport::new_rpc(
            Url::parse("http://127.0.0.1").unwrap(),
            HeaderMap::new(),
            metrics.clone(),
            "",
        );
        let provider_metrics = Arc::new(ProviderEthRpcMetrics::new(mock_registry.clone()));

        let eth_call_adapter = Arc::new(
            EthereumAdapter::new(
                logger.clone(),
                String::new(),
                transport.clone(),
                provider_metrics.clone(),
                true,
                true,
            )
            .await,
        );

        let eth_adapter = Arc::new(
            EthereumAdapter::new(
                logger.clone(),
                String::new(),
                transport.clone(),
                provider_metrics.clone(),
                true,
                false,
            )
            .await,
        );

        let mut adapters: EthereumNetworkAdapters = EthereumNetworkAdapters::for_testing(
            vec![EthereumNetworkAdapter::new(
                metrics.cheap_clone(),
                NodeCapabilities {
                    archive: true,
                    traces: false,
                },
                eth_adapter.clone(),
                SubgraphLimit::Limit(3),
                1.0,
            )],
            vec![EthereumNetworkAdapter::new(
                metrics.cheap_clone(),
                NodeCapabilities {
                    archive: true,
                    traces: false,
                },
                eth_call_adapter.clone(),
                SubgraphLimit::Limit(3),
                1.0,
            )],
        )
        .await;
        // one reference above and one inside adapters struct
        assert_eq!(Arc::strong_count(&eth_call_adapter), 2);
        assert_eq!(Arc::strong_count(&eth_adapter), 2);

        {
            // Not Found
            assert!(adapters
                .cheapest_with(&NodeCapabilities {
                    archive: false,
                    traces: true,
                })
                .await
                .is_err());

            // Check cheapest is not call only
            let adapter = adapters
                .cheapest_with(&NodeCapabilities {
                    archive: true,
                    traces: false,
                })
                .await
                .unwrap();
            assert_eq!(adapter.is_call_only(), false);
        }

        // Check limits
        {
            let adapter = adapters.call_or_cheapest(None).unwrap();
            assert!(adapter.is_call_only());
            assert_eq!(
                adapters.call_or_cheapest(None).unwrap().is_call_only(),
                false
            );
        }

        // Check empty falls back to call only
        {
            adapters.call_only_adapters = vec![];
            let adapter = adapters
                .call_or_cheapest(Some(&NodeCapabilities {
                    archive: true,
                    traces: false,
                }))
                .unwrap();
            assert_eq!(adapter.is_call_only(), false);
        }
    }

    #[tokio::test]
    async fn adapter_selector_unlimited() {
        let metrics = Arc::new(EndpointMetrics::mock());
        let logger = graph::log::logger(true);
        let mock_registry = Arc::new(MetricsRegistry::mock());
        let transport = Transport::new_rpc(
            Url::parse("http://127.0.0.1").unwrap(),
            HeaderMap::new(),
            metrics.clone(),
            "",
        );
        let provider_metrics = Arc::new(ProviderEthRpcMetrics::new(mock_registry.clone()));

        let eth_call_adapter = Arc::new(
            EthereumAdapter::new(
                logger.clone(),
                String::new(),
                transport.clone(),
                provider_metrics.clone(),
                true,
                true,
            )
            .await,
        );

        let eth_adapter = Arc::new(
            EthereumAdapter::new(
                logger.clone(),
                String::new(),
                transport.clone(),
                provider_metrics.clone(),
                true,
                false,
            )
            .await,
        );

        let adapters: EthereumNetworkAdapters = EthereumNetworkAdapters::for_testing(
            vec![EthereumNetworkAdapter::new(
                metrics.cheap_clone(),
                NodeCapabilities {
                    archive: true,
                    traces: false,
                },
                eth_call_adapter.clone(),
                SubgraphLimit::Unlimited,
                1.0,
            )],
            vec![EthereumNetworkAdapter::new(
                metrics.cheap_clone(),
                NodeCapabilities {
                    archive: true,
                    traces: false,
                },
                eth_adapter.clone(),
                SubgraphLimit::Limit(2),
                1.0,
            )],
        )
        .await;
        // one reference above and one inside adapters struct
        assert_eq!(Arc::strong_count(&eth_call_adapter), 2);
        assert_eq!(Arc::strong_count(&eth_adapter), 2);

        // verify that after all call_only were exhausted, we can still
        // get normal adapters
        let keep: Vec<Arc<EthereumAdapter>> = vec![0; 10]
            .iter()
            .map(|_| adapters.call_or_cheapest(None).unwrap())
            .collect();
        assert_eq!(keep.iter().any(|a| !a.is_call_only()), false);
    }

    #[tokio::test]
    async fn adapter_selector_disable_call_only_fallback() {
        let metrics = Arc::new(EndpointMetrics::mock());
        let logger = graph::log::logger(true);
        let mock_registry = Arc::new(MetricsRegistry::mock());
        let transport = Transport::new_rpc(
            Url::parse("http://127.0.0.1").unwrap(),
            HeaderMap::new(),
            metrics.clone(),
            "",
        );
        let provider_metrics = Arc::new(ProviderEthRpcMetrics::new(mock_registry.clone()));

        let eth_call_adapter = Arc::new(
            EthereumAdapter::new(
                logger.clone(),
                String::new(),
                transport.clone(),
                provider_metrics.clone(),
                true,
                true,
            )
            .await,
        );

        let eth_adapter = Arc::new(
            EthereumAdapter::new(
                logger.clone(),
                String::new(),
                transport.clone(),
                provider_metrics.clone(),
                true,
                false,
            )
            .await,
        );

        let adapters: EthereumNetworkAdapters = EthereumNetworkAdapters::for_testing(
            vec![EthereumNetworkAdapter::new(
                metrics.cheap_clone(),
                NodeCapabilities {
                    archive: true,
                    traces: false,
                },
                eth_call_adapter.clone(),
                SubgraphLimit::Disabled,
                1.0,
            )],
            vec![EthereumNetworkAdapter::new(
                metrics.cheap_clone(),
                NodeCapabilities {
                    archive: true,
                    traces: false,
                },
                eth_adapter.clone(),
                SubgraphLimit::Limit(3),
                1.0,
            )],
        )
        .await;
        // one reference above and one inside adapters struct
        assert_eq!(Arc::strong_count(&eth_call_adapter), 2);
        assert_eq!(Arc::strong_count(&eth_adapter), 2);
        assert_eq!(
            adapters.call_or_cheapest(None).unwrap().is_call_only(),
            false
        );
    }

    #[tokio::test]
    async fn adapter_selector_no_call_only_fallback() {
        let metrics = Arc::new(EndpointMetrics::mock());
        let logger = graph::log::logger(true);
        let mock_registry = Arc::new(MetricsRegistry::mock());
        let transport = Transport::new_rpc(
            Url::parse("http://127.0.0.1").unwrap(),
            HeaderMap::new(),
            metrics.clone(),
            "",
        );
        let provider_metrics = Arc::new(ProviderEthRpcMetrics::new(mock_registry.clone()));

        let eth_adapter = Arc::new(
            EthereumAdapter::new(
                logger.clone(),
                String::new(),
                transport.clone(),
                provider_metrics.clone(),
                true,
                false,
            )
            .await,
        );

        let adapters: EthereumNetworkAdapters = EthereumNetworkAdapters::for_testing(
            vec![EthereumNetworkAdapter::new(
                metrics.cheap_clone(),
                NodeCapabilities {
                    archive: true,
                    traces: false,
                },
                eth_adapter.clone(),
                SubgraphLimit::Limit(3),
                1.0,
            )],
            vec![],
        )
        .await;
        // one reference above and one inside adapters struct
        assert_eq!(Arc::strong_count(&eth_adapter), 2);
        assert_eq!(
            adapters.call_or_cheapest(None).unwrap().is_call_only(),
            false
        );
    }

    #[tokio::test]
    async fn eth_adapter_selection_multiple_adapters() {
        let logger = Logger::root(Discard, o!());
        let unavailable_provider = "unavailable-provider";
        let error_provider = "error-provider";
        let no_error_provider = "no-error-provider";

        let mock_registry = Arc::new(MetricsRegistry::mock());
        let metrics = Arc::new(EndpointMetrics::new(
            logger,
            &[unavailable_provider, error_provider, no_error_provider],
            mock_registry.clone(),
        ));
        let logger = graph::log::logger(true);
        let provider_metrics = Arc::new(ProviderEthRpcMetrics::new(mock_registry.clone()));
        let chain_id: Word = "chain_id".into();

        let adapters = vec![
            fake_adapter(
                &logger,
                &unavailable_provider,
                &provider_metrics,
                &metrics,
                false,
            )
            .await,
            fake_adapter(&logger, &error_provider, &provider_metrics, &metrics, false).await,
            fake_adapter(
                &logger,
                &no_error_provider,
                &provider_metrics,
                &metrics,
                false,
            )
            .await,
        ];

        // Set errors
        metrics.report_for_test(&ProviderName::from(error_provider), false);

        let mut no_retest_adapters = vec![];
        let mut always_retest_adapters = vec![];

        adapters.iter().cloned().for_each(|adapter| {
            let limit = if adapter.provider() == unavailable_provider {
                SubgraphLimit::Disabled
            } else {
                SubgraphLimit::Unlimited
            };

            no_retest_adapters.push(EthereumNetworkAdapter {
                endpoint_metrics: metrics.clone(),
                capabilities: NodeCapabilities {
                    archive: true,
                    traces: false,
                },
                adapter: adapter.clone(),
                limit: limit.clone(),
                weight: 1.0,
            });
            always_retest_adapters.push(EthereumNetworkAdapter {
                endpoint_metrics: metrics.clone(),
                capabilities: NodeCapabilities {
                    archive: true,
                    traces: false,
                },
                adapter,
                limit,
                weight: 1.0,
            });
        });
        let manager = ProviderManager::<EthereumNetworkAdapter>::new(
            logger,
            vec![(
                chain_id.clone(),
                no_retest_adapters
                    .iter()
                    .cloned()
                    .chain(always_retest_adapters.iter().cloned())
                    .collect(),
            )]
            .into_iter(),
            ProviderCheckStrategy::MarkAsValid,
        );

        let no_retest_adapters = EthereumNetworkAdapters::new(
            chain_id.clone(),
            manager.clone(),
            vec![],
            Some(0f64),
            false,
            vec![],
        );

        let always_retest_adapters = EthereumNetworkAdapters::new(
            chain_id,
            manager.clone(),
            vec![],
            Some(1f64),
            false,
            vec![],
        );

        assert_eq!(
            no_retest_adapters
                .cheapest_with(&NodeCapabilities {
                    archive: true,
                    traces: false,
                })
                .await
                .unwrap()
                .provider(),
            no_error_provider
        );
        assert_eq!(
            always_retest_adapters
                .cheapest_with(&NodeCapabilities {
                    archive: true,
                    traces: false,
                })
                .await
                .unwrap()
                .provider(),
            error_provider
        );
    }

    #[tokio::test]
    async fn eth_adapter_selection_single_adapter() {
        let logger = Logger::root(Discard, o!());
        let unavailable_provider = "unavailable-provider";
        let error_provider = "error-provider";
        let no_error_provider = "no-error-provider";

        let mock_registry = Arc::new(MetricsRegistry::mock());
        let metrics = Arc::new(EndpointMetrics::new(
            logger,
            &[unavailable_provider, error_provider, no_error_provider],
            mock_registry.clone(),
        ));
        let chain_id: Word = "chain_id".into();
        let logger = graph::log::logger(true);
        let provider_metrics = Arc::new(ProviderEthRpcMetrics::new(mock_registry.clone()));

        // Set errors
        metrics.report_for_test(&ProviderName::from(error_provider), false);

        let mut no_retest_adapters = vec![];
        no_retest_adapters.push(EthereumNetworkAdapter {
            endpoint_metrics: metrics.clone(),
            capabilities: NodeCapabilities {
                archive: true,
                traces: false,
            },
            adapter: fake_adapter(&logger, &error_provider, &provider_metrics, &metrics, false)
                .await,
            limit: SubgraphLimit::Unlimited,
            weight: 1.0,
        });

        let mut always_retest_adapters = vec![];
        always_retest_adapters.push(EthereumNetworkAdapter {
            endpoint_metrics: metrics.clone(),
            capabilities: NodeCapabilities {
                archive: true,
                traces: false,
            },
            adapter: fake_adapter(
                &logger,
                &no_error_provider,
                &provider_metrics,
                &metrics,
                false,
            )
            .await,
            limit: SubgraphLimit::Unlimited,
            weight: 1.0,
        });
        let manager = ProviderManager::<EthereumNetworkAdapter>::new(
            logger.clone(),
            always_retest_adapters
                .iter()
                .cloned()
                .map(|a| (chain_id.clone(), vec![a])),
            ProviderCheckStrategy::MarkAsValid,
        );

        let always_retest_adapters = EthereumNetworkAdapters::new(
            chain_id.clone(),
            manager.clone(),
            vec![],
            Some(1f64),
            false,
            vec![],
        );

        assert_eq!(
            always_retest_adapters
                .cheapest_with(&NodeCapabilities {
                    archive: true,
                    traces: false,
                })
                .await
                .unwrap()
                .provider(),
            no_error_provider
        );

        let manager = ProviderManager::<EthereumNetworkAdapter>::new(
            logger.clone(),
            no_retest_adapters
                .iter()
                .cloned()
                .map(|a| (chain_id.clone(), vec![a])),
            ProviderCheckStrategy::MarkAsValid,
        );

        let no_retest_adapters = EthereumNetworkAdapters::new(
            chain_id.clone(),
            manager,
            vec![],
            Some(0f64),
            false,
            vec![],
        );
        assert_eq!(
            no_retest_adapters
                .cheapest_with(&NodeCapabilities {
                    archive: true,
                    traces: false,
                })
                .await
                .unwrap()
                .provider(),
            error_provider
        );

        let mut no_available_adapter = vec![];
        no_available_adapter.push(EthereumNetworkAdapter {
            endpoint_metrics: metrics.clone(),
            capabilities: NodeCapabilities {
                archive: true,
                traces: false,
            },
            adapter: fake_adapter(
                &logger,
                &no_error_provider,
                &provider_metrics,
                &metrics,
                false,
            )
            .await,
            limit: SubgraphLimit::Disabled,
            weight: 1.0,
        });
        let manager = ProviderManager::new(
            logger,
            vec![(
                chain_id.clone(),
                no_available_adapter.iter().cloned().collect(),
            )]
            .into_iter(),
            ProviderCheckStrategy::MarkAsValid,
        );

        let no_available_adapter =
            EthereumNetworkAdapters::new(chain_id, manager, vec![], None, false, vec![]);
        let res = no_available_adapter
            .cheapest_with(&NodeCapabilities {
                archive: true,
                traces: false,
            })
            .await;
        assert!(res.is_err(), "{:?}", res);
    }

    async fn fake_adapter(
        logger: &Logger,
        provider: &str,
        provider_metrics: &Arc<ProviderEthRpcMetrics>,
        endpoint_metrics: &Arc<EndpointMetrics>,
        call_only: bool,
    ) -> Arc<EthereumAdapter> {
        let transport = Transport::new_rpc(
            Url::parse(&"http://127.0.0.1").unwrap(),
            HeaderMap::new(),
            endpoint_metrics.clone(),
            "",
        );

        Arc::new(
            EthereumAdapter::new(
                logger.clone(),
                provider.to_string(),
                transport.clone(),
                provider_metrics.clone(),
                true,
                call_only,
            )
            .await,
        )
    }

    #[tokio::test]
    async fn test_weighted_adapter_selection() {
        let metrics = Arc::new(EndpointMetrics::mock());
        let logger = graph::log::logger(true);
        let mock_registry = Arc::new(MetricsRegistry::mock());
        let transport = Transport::new_rpc(
            Url::parse("http://127.0.0.1").unwrap(),
            HeaderMap::new(),
            metrics.clone(),
            "",
        );
        let provider_metrics = Arc::new(ProviderEthRpcMetrics::new(mock_registry.clone()));

        let adapter1 = Arc::new(
            EthereumAdapter::new(
                logger.clone(),
                "adapter1".to_string(),
                transport.clone(),
                provider_metrics.clone(),
                true,
                false,
            )
            .await,
        );

        let adapter2 = Arc::new(
            EthereumAdapter::new(
                logger.clone(),
                "adapter2".to_string(),
                transport.clone(),
                provider_metrics.clone(),
                true,
                false,
            )
            .await,
        );

        let mut adapters = EthereumNetworkAdapters::for_testing(
            vec![
                EthereumNetworkAdapter::new(
                    metrics.cheap_clone(),
                    NodeCapabilities {
                        archive: true,
                        traces: false,
                    },
                    adapter1.clone(),
                    SubgraphLimit::Unlimited,
                    0.2,
                ),
                EthereumNetworkAdapter::new(
                    metrics.cheap_clone(),
                    NodeCapabilities {
                        archive: true,
                        traces: false,
                    },
                    adapter2.clone(),
                    SubgraphLimit::Unlimited,
                    0.8,
                ),
            ],
            vec![],
        )
        .await;

        let health_checker1 = Arc::new(Health::new(adapter1.clone()));
        let health_checker2 = Arc::new(Health::new(adapter2.clone()));

        adapters.health_checkers = vec![health_checker1.clone(), health_checker2.clone()];
        adapters.weighted = true;

        let mut adapter1_count = 0;
        let mut adapter2_count = 0;

        for _ in 0..1000 {
            let selected_adapter = adapters
                .cheapest_with(&NodeCapabilities {
                    archive: true,
                    traces: false,
                })
                .await
                .unwrap();

            if selected_adapter.provider() == "adapter1" {
                adapter1_count += 1;
            } else {
                adapter2_count += 1;
            }
        }

        // Check that the selection is roughly proportional to the weights.
        // Allow for a 10% tolerance.
        assert!(adapter1_count > 100 && adapter1_count < 300);
        assert!(adapter2_count > 700 && adapter2_count < 900);
    }
}
