use anyhow::anyhow;
use graph::{
    components::ethereum::NodeCapabilities,
    prelude::rand::{self, seq::IteratorRandom},
};
use std::collections::HashMap;
use std::sync::Arc;

pub use graph::impl_slog_value;
use graph::prelude::Error;

use crate::adapter::EthereumAdapter as _;
use crate::EthereumAdapter;

#[derive(Clone)]
pub struct EthereumNetworkAdapter {
    pub capabilities: NodeCapabilities,
    adapter: Arc<EthereumAdapter>,
}

#[derive(Clone)]
pub struct EthereumNetworkAdapters {
    pub adapters: Vec<EthereumNetworkAdapter>,
}

impl EthereumNetworkAdapters {
    pub fn cheapest_with(
        &self,
        required_capabilities: &NodeCapabilities,
    ) -> Result<&Arc<EthereumAdapter>, Error> {
        let sufficient_adapters: Vec<&EthereumNetworkAdapter> = self
            .adapters
            .iter()
            .filter(|adapter| &adapter.capabilities >= required_capabilities)
            .collect();
        if sufficient_adapters.is_empty() {
            return Err(anyhow!(
                "A matching Ethereum network with {:?} was not found.",
                required_capabilities
            ));
        }

        // Select from the matching adapters randomly
        let mut rng = rand::thread_rng();
        Ok(&sufficient_adapters.iter().choose(&mut rng).unwrap().adapter)
    }

    pub fn cheapest(&self) -> Option<Arc<EthereumAdapter>> {
        // EthereumAdapters are sorted by their NodeCapabilities when the EthereumNetworks
        // struct is instantiated so they do not need to be sorted here
        self.adapters
            .iter()
            .next()
            .map(|ethereum_network_adapter| ethereum_network_adapter.adapter.clone())
    }

    pub fn remove(&mut self, provider: &str) {
        self.adapters
            .retain(|adapter| adapter.adapter.provider() != provider);
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
    ) {
        let network_adapters = self
            .networks
            .entry(name)
            .or_insert(EthereumNetworkAdapters { adapters: vec![] });
        network_adapters.adapters.push(EthereumNetworkAdapter {
            capabilities,
            adapter: adapter.clone(),
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
                            network_adapter.capabilities.clone(),
                            network_adapter.adapter.clone(),
                        )
                    })
            })
            .collect()
    }

    pub fn sort(&mut self) {
        for adapters in self.networks.values_mut() {
            adapters
                .adapters
                .sort_by_key(|adapter| adapter.capabilities)
        }
    }

    pub fn adapter_with_capabilities(
        &self,
        network_name: String,
        requirements: &NodeCapabilities,
    ) -> Result<&Arc<EthereumAdapter>, Error> {
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
}
