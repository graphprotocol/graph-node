use anyhow::{anyhow, bail, Context};
use graph::cheap_clone::CheapClone;
use graph::firehose::SubgraphLimit;
use graph::prelude::rand::{self, seq::IteratorRandom};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

pub use graph::impl_slog_value;
use graph::prelude::Error;

use crate::adapter::EthereumAdapter as _;
use crate::capabilities::NodeCapabilities;
use crate::EthereumAdapter;

#[derive(Debug, Clone)]
pub struct EthereumNetworkAdapter {
    pub capabilities: NodeCapabilities,
    adapter: Arc<EthereumAdapter>,
    /// The maximum number of times this adapter can be used. We use the
    /// strong_count on `adapter` to determine whether the adapter is above
    /// that limit. That's a somewhat imprecise but convenient way to
    /// determine the number of connections
    limit: SubgraphLimit,
}

impl EthereumNetworkAdapter {
    fn is_call_only(&self) -> bool {
        self.adapter.is_call_only()
    }
}

#[derive(Debug, Clone, Default)]
pub struct EthereumNetworkAdapters {
    pub adapters: Vec<EthereumNetworkAdapter>,
    pub call_only_adapters: Vec<EthereumNetworkAdapter>,
}

impl EthereumNetworkAdapters {
    pub fn push_adapter(&mut self, adapter: EthereumNetworkAdapter) {
        if adapter.is_call_only() {
            self.call_only_adapters.push(adapter);
        } else {
            self.adapters.push(adapter);
        }
    }
    pub fn all_cheapest_with(
        &self,
        required_capabilities: &NodeCapabilities,
    ) -> impl Iterator<Item = Arc<EthereumAdapter>> + '_ {
        let cheapest_sufficient_capability = self
            .adapters
            .iter()
            .find(|adapter| &adapter.capabilities >= required_capabilities)
            .map(|adapter| &adapter.capabilities);

        self.adapters
            .iter()
            .filter(move |adapter| Some(&adapter.capabilities) == cheapest_sufficient_capability)
            .filter(|adapter| {
                adapter
                    .limit
                    .has_capacity(Arc::strong_count(&adapter.adapter))
            })
            .map(|adapter| adapter.adapter.cheap_clone())
    }

    pub fn cheapest_with(
        &self,
        required_capabilities: &NodeCapabilities,
    ) -> Result<Arc<EthereumAdapter>, Error> {
        // Select randomly from the cheapest adapters that have sufficent capabilities.
        self.all_cheapest_with(required_capabilities)
            .choose(&mut rand::thread_rng())
            .with_context(|| {
                anyhow!(
                    "A matching Ethereum network with {:?} was not found.",
                    required_capabilities
                )
            })
    }

    pub fn cheapest(&self) -> Option<Arc<EthereumAdapter>> {
        // EthereumAdapters are sorted by their NodeCapabilities when the EthereumNetworks
        // struct is instantiated so they do not need to be sorted here
        self.adapters
            .first()
            .map(|ethereum_network_adapter| ethereum_network_adapter.adapter.clone())
    }

    pub fn remove(&mut self, provider: &str) {
        self.adapters
            .retain(|adapter| adapter.adapter.provider() != provider);
    }

    pub fn call_or_cheapest(
        &self,
        capabilities: Option<&NodeCapabilities>,
    ) -> anyhow::Result<Arc<EthereumAdapter>> {
        // call_only_adapter can fail if we're out of capcity, this is fine since
        // we would want to fallback onto a full adapter
        // so we will ignore this error and return whatever comes out of `cheapest_with`
        match self.call_only_adapter() {
            Ok(Some(adapter)) => Ok(adapter),
            _ => self.cheapest_with(capabilities.unwrap_or(&NodeCapabilities {
                // Archive is required for call_only
                archive: true,
                traces: false,
            })),
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
            .ok_or(anyhow!("no available call only endpoints"))?;

        // TODO: This will probably blow up a lot sooner than [limit] amount of
        // subgraphs, since we probably use a few instances.
        if !adapters
            .limit
            .has_capacity(Arc::strong_count(&adapters.adapter))
        {
            bail!("call only adapter has reached the concurrency limit");
        }

        // Cloning here ensure we have the correct count at any given time, if we return a reference it can be cloned later
        // which could cause a high number of endpoints to be given away before accounting for them.
        Ok(Some(adapters.adapter.clone()))
    }
}

#[derive(Clone)]
pub struct EthereumNetworks {
    pub networks: HashMap<String, EthereumNetworkAdapters>,
}

impl EthereumNetworks {
    pub fn new() -> EthereumNetworks {
        EthereumNetworks {
            networks: HashMap::new(),
        }
    }

    pub fn insert(
        &mut self,
        name: String,
        capabilities: NodeCapabilities,
        adapter: Arc<EthereumAdapter>,
        limit: SubgraphLimit,
    ) {
        let network_adapters = self.networks.entry(name).or_default();

        network_adapters.push_adapter(EthereumNetworkAdapter {
            capabilities,
            adapter,
            limit,
        });
    }

    pub fn remove(&mut self, name: &str, provider: &str) {
        if let Some(adapters) = self.networks.get_mut(name) {
            adapters.remove(provider);
        }
    }

    pub fn extend(&mut self, other_networks: EthereumNetworks) {
        self.networks.extend(other_networks.networks);
    }

    pub fn flatten(&self) -> Vec<(String, NodeCapabilities, Arc<EthereumAdapter>)> {
        self.networks
            .iter()
            .flat_map(|(network_name, network_adapters)| {
                network_adapters
                    .adapters
                    .iter()
                    .map(move |network_adapter| {
                        (
                            network_name.clone(),
                            network_adapter.capabilities,
                            network_adapter.adapter.clone(),
                        )
                    })
            })
            .collect()
    }

    pub fn sort(&mut self) {
        for adapters in self.networks.values_mut() {
            adapters.adapters.sort_by(|a, b| {
                a.capabilities
                    .partial_cmp(&b.capabilities)
                    // We can't define a total ordering over node capabilities,
                    // so incomparable items are considered equal and end up
                    // near each other.
                    .unwrap_or(Ordering::Equal)
            })
        }
    }

    pub fn adapter_with_capabilities(
        &self,
        network_name: String,
        requirements: &NodeCapabilities,
    ) -> Result<Arc<EthereumAdapter>, Error> {
        self.networks
            .get(&network_name)
            .ok_or(anyhow!("network not supported: {}", &network_name))
            .and_then(|adapters| adapters.cheapest_with(requirements))
    }
}

#[cfg(test)]
mod tests {
    use graph::{firehose::SubgraphLimit, prelude::MetricsRegistry, tokio, url::Url};
    use http::HeaderMap;
    use std::sync::Arc;

    use crate::{EthereumAdapter, EthereumNetworks, ProviderEthRpcMetrics, Transport};

    use super::NodeCapabilities;

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
        let chain = "mainnet".to_string();
        let logger = graph::log::logger(true);
        let mock_registry: Arc<MetricsRegistry> = Arc::new(MetricsRegistry::mock());
        let transport =
            Transport::new_rpc(Url::parse("http://127.0.0.1").unwrap(), HeaderMap::new());
        let provider_metrics = Arc::new(ProviderEthRpcMetrics::new(mock_registry.clone()));

        let eth_call_adapter = Arc::new(
            EthereumAdapter::new(
                logger.clone(),
                String::new(),
                "http://127.0.0.1",
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
                "http://127.0.0.1",
                transport.clone(),
                provider_metrics.clone(),
                true,
                false,
            )
            .await,
        );

        let mut adapters = {
            let mut ethereum_networks = EthereumNetworks::new();
            ethereum_networks.insert(
                chain.clone(),
                NodeCapabilities {
                    archive: true,
                    traces: false,
                },
                eth_call_adapter.clone(),
                SubgraphLimit::Limit(3),
            );
            ethereum_networks.insert(
                chain.clone(),
                NodeCapabilities {
                    archive: true,
                    traces: false,
                },
                eth_adapter.clone(),
                SubgraphLimit::Limit(3),
            );
            ethereum_networks.networks.get(&chain).unwrap().clone()
        };
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
                .is_err());

            // Check cheapest is not call only
            let adapter = adapters
                .cheapest_with(&NodeCapabilities {
                    archive: true,
                    traces: false,
                })
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
        let chain = "mainnet".to_string();
        let logger = graph::log::logger(true);
        let mock_registry: Arc<MetricsRegistry> = Arc::new(MetricsRegistry::mock());
        let transport =
            Transport::new_rpc(Url::parse("http://127.0.0.1").unwrap(), HeaderMap::new());
        let provider_metrics = Arc::new(ProviderEthRpcMetrics::new(mock_registry.clone()));

        let eth_call_adapter = Arc::new(
            EthereumAdapter::new(
                logger.clone(),
                String::new(),
                "http://127.0.0.1",
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
                "http://127.0.0.1",
                transport.clone(),
                provider_metrics.clone(),
                true,
                false,
            )
            .await,
        );

        let adapters = {
            let mut ethereum_networks = EthereumNetworks::new();
            ethereum_networks.insert(
                chain.clone(),
                NodeCapabilities {
                    archive: true,
                    traces: false,
                },
                eth_call_adapter.clone(),
                SubgraphLimit::Unlimited,
            );
            ethereum_networks.insert(
                chain.clone(),
                NodeCapabilities {
                    archive: true,
                    traces: false,
                },
                eth_adapter.clone(),
                SubgraphLimit::Limit(3),
            );
            ethereum_networks.networks.get(&chain).unwrap().clone()
        };
        // one reference above and one inside adapters struct
        assert_eq!(Arc::strong_count(&eth_call_adapter), 2);
        assert_eq!(Arc::strong_count(&eth_adapter), 2);

        let keep: Vec<Arc<EthereumAdapter>> = vec![0; 10]
            .iter()
            .map(|_| adapters.call_or_cheapest(None).unwrap())
            .collect();
        assert_eq!(keep.iter().any(|a| !a.is_call_only()), false);
    }

    #[tokio::test]
    async fn adapter_selector_disable_call_only_fallback() {
        let chain = "mainnet".to_string();
        let logger = graph::log::logger(true);
        let mock_registry: Arc<MetricsRegistry> = Arc::new(MetricsRegistry::mock());
        let transport =
            Transport::new_rpc(Url::parse("http://127.0.0.1").unwrap(), HeaderMap::new());
        let provider_metrics = Arc::new(ProviderEthRpcMetrics::new(mock_registry.clone()));

        let eth_call_adapter = Arc::new(
            EthereumAdapter::new(
                logger.clone(),
                String::new(),
                "http://127.0.0.1",
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
                "http://127.0.0.1",
                transport.clone(),
                provider_metrics.clone(),
                true,
                false,
            )
            .await,
        );

        let adapters = {
            let mut ethereum_networks = EthereumNetworks::new();
            ethereum_networks.insert(
                chain.clone(),
                NodeCapabilities {
                    archive: true,
                    traces: false,
                },
                eth_call_adapter.clone(),
                SubgraphLimit::Disabled,
            );
            ethereum_networks.insert(
                chain.clone(),
                NodeCapabilities {
                    archive: true,
                    traces: false,
                },
                eth_adapter.clone(),
                SubgraphLimit::Limit(3),
            );
            ethereum_networks.networks.get(&chain).unwrap().clone()
        };
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
        let chain = "mainnet".to_string();
        let logger = graph::log::logger(true);
        let mock_registry: Arc<MetricsRegistry> = Arc::new(MetricsRegistry::mock());
        let transport =
            Transport::new_rpc(Url::parse("http://127.0.0.1").unwrap(), HeaderMap::new());
        let provider_metrics = Arc::new(ProviderEthRpcMetrics::new(mock_registry.clone()));

        let eth_adapter = Arc::new(
            EthereumAdapter::new(
                logger.clone(),
                String::new(),
                "http://127.0.0.1",
                transport.clone(),
                provider_metrics.clone(),
                true,
                false,
            )
            .await,
        );

        let adapters = {
            let mut ethereum_networks = EthereumNetworks::new();
            ethereum_networks.insert(
                chain.clone(),
                NodeCapabilities {
                    archive: true,
                    traces: false,
                },
                eth_adapter.clone(),
                SubgraphLimit::Limit(3),
            );
            ethereum_networks.networks.get(&chain).unwrap().clone()
        };
        // one reference above and one inside adapters struct
        assert_eq!(Arc::strong_count(&eth_adapter), 2);
        assert_eq!(
            adapters.call_or_cheapest(None).unwrap().is_call_only(),
            false
        );
    }
}
