use anyhow::{anyhow, Context};
use graph::cheap_clone::CheapClone;
use graph::prelude::rand::{self, seq::IteratorRandom};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

pub use graph::impl_slog_value;
use graph::prelude::Error;

use crate::adapter::EthereumAdapter as _;
use crate::capabilities::NodeCapabilities;
use crate::EthereumAdapter;

#[derive(Clone)]
pub struct EthereumNetworkAdapter {
    pub capabilities: NodeCapabilities,
    adapter: Arc<EthereumAdapter>,
    /// The maximum number of times this adapter can be used. We use the
    /// strong_count on `adapter` to determine whether the adapter is above
    /// that limit. That's a somewhat imprecise but convenient way to
    /// determine the number of connections
    limit: usize,
}

impl EthereumNetworkAdapter {
    pub fn call_only(&self) -> bool {
        self.adapter.call_only
    }
}

#[derive(Clone)]
pub struct EthereumNetworkAdapters {
    pub full_adapters: Vec<EthereumNetworkAdapter>,
    pub call_only_adapters: Vec<EthereumNetworkAdapter>,
}

impl EthereumNetworkAdapters {
    pub fn is_capabilities_fulfilled(&self, required_capabilities: &NodeCapabilities) -> bool {
        // Now that we have a split on adapters, we must perform some logic to determine if
        // capabilities can be fulfilled or not.
        match (required_capabilities.archive, required_capabilities.traces) {
            // Those 2 cases cannot be fulfilled by 'call_only', so it means we only inspect 'full_adapters'
            (false, _) => has_adapter_for(&self.full_adapters, required_capabilities),
            // This case could be provided by both set so we check both
            (true, false) => {
                has_adapter_for(&self.full_adapters, required_capabilities)
                    || has_adapter_for(&self.call_only_adapters, required_capabilities)
            }
            // The case '{ archive: true, traces: true }' is problematic, 'archive' could be provided
            // by 'call_only_adapters' while 'traces' by 'full_adapters'.
            (true, true) => {
                let only_archive = NodeCapabilities {
                    archive: true,
                    traces: false,
                };
                let only_traces = NodeCapabilities {
                    archive: false,
                    traces: true,
                };

                has_adapter_for(&self.full_adapters, &only_traces)
                    && (has_adapter_for(&self.full_adapters, &only_archive)
                        || has_adapter_for(&self.call_only_adapters, &only_archive))
            }
        }
    }

    pub fn all_cheapest_with(
        &self,
        required_capabilities: &NodeCapabilities,
    ) -> impl Iterator<Item = Arc<EthereumAdapter>> + '_ {
        all_cheapest_with(&self.full_adapters, required_capabilities)
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
        self.full_adapters
            .first()
            .map(|ethereum_network_adapter| ethereum_network_adapter.adapter.clone())
    }

    pub fn all_cheapest_call_only_with(
        &self,
        required_capabilities: &NodeCapabilities,
    ) -> impl Iterator<Item = Arc<EthereumAdapter>> + '_ {
        all_cheapest_with(&self.full_adapters, required_capabilities)
    }

    pub fn cheapest_call_only_with(
        &self,
        required_capabilities: &NodeCapabilities,
    ) -> Result<Arc<EthereumAdapter>, Error> {
        let adapters = all_cheapest_with(&self.call_only_adapters, required_capabilities);

        match adapters.choose(&mut rand::thread_rng()) {
            Some(adapter) => Ok(adapter),
            None => {
                // We fallback to all adapters if no call only adapters is available to retain backward compatibility
                self.cheapest_with(required_capabilities)
            }
        }
    }

    pub fn remove(&mut self, provider: &str) {
        self.full_adapters
            .retain(|adapter| adapter.adapter.provider() != provider);
    }
}

fn has_adapter_for<'a>(
    adapters: &'a Vec<EthereumNetworkAdapter>,
    required_capabilities: &NodeCapabilities,
) -> bool {
    all_cheapest_with(adapters, required_capabilities)
        .next()
        .is_some()
}

fn all_cheapest_with<'a>(
    adapters: &'a Vec<EthereumNetworkAdapter>,
    required_capabilities: &NodeCapabilities,
) -> impl Iterator<Item = Arc<EthereumAdapter>> + 'a {
    let cheapest_sufficient_capability = adapters
        .iter()
        .find(|adapter| &adapter.capabilities >= required_capabilities)
        .map(|adapter| &adapter.capabilities);

    adapters
        .iter()
        .filter(move |adapter| Some(&adapter.capabilities) == cheapest_sufficient_capability)
        .filter(|adapter| Arc::strong_count(&adapter.adapter) < adapter.limit)
        .map(|adapter| adapter.adapter.cheap_clone())
}

pub fn cheapest_with<'a>(
    adapters: impl Iterator<Item = Arc<EthereumAdapter>> + 'a,
    required_capabilities: &NodeCapabilities,
) -> Result<Arc<EthereumAdapter>, Error> {
    adapters.choose(&mut rand::thread_rng()).with_context(|| {
        anyhow!(
            "A matching Ethereum network with {:?} was not found.",
            required_capabilities
        )
    })
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
        limit: usize,
    ) {
        let network_adapters = self
            .networks
            .entry(name)
            .or_insert(EthereumNetworkAdapters {
                full_adapters: vec![],
                call_only_adapters: vec![],
            });

        let mut adapters = &mut network_adapters.full_adapters;
        if adapter.call_only {
            adapters = &mut network_adapters.call_only_adapters;
        }

        adapters.push(EthereumNetworkAdapter {
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
                    .full_adapters
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
            adapters.full_adapters.sort_by(|a, b| {
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

        // Test all real combinations of capability comparisons
        assert_eq!(false, &full >= &archive);
        assert_eq!(false, &full >= &traces);
        assert_eq!(false, &full >= &archive_traces);
        assert_eq!(true, &full >= &full);
        assert_eq!(false, &full >= &traces);

        assert_eq!(true, &archive >= &archive);
        assert_eq!(false, &archive >= &traces);
        assert_eq!(false, &archive >= &archive_traces);
        assert_eq!(true, &archive >= &full);
        assert_eq!(false, &archive >= &traces);

        assert_eq!(false, &traces >= &archive);
        assert_eq!(true, &traces >= &traces);
        assert_eq!(false, &traces >= &archive_traces);
        assert_eq!(true, &traces >= &full);
        assert_eq!(true, &traces >= &traces);

        assert_eq!(true, &archive_traces >= &archive);
        assert_eq!(true, &archive_traces >= &traces);
        assert_eq!(true, &archive_traces >= &archive_traces);
        assert_eq!(true, &archive_traces >= &full);
        assert_eq!(true, &archive_traces >= &traces);

        assert_eq!(false, &traces >= &archive);
        assert_eq!(true, &traces >= &traces);
        assert_eq!(false, &traces >= &archive_traces);
        assert_eq!(true, &traces >= &full);
        assert_eq!(true, &traces >= &traces);
    }
}
