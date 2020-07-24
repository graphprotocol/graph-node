use failure::{format_err, Error};
use std::cmp::Ord;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use crate::components::ethereum::EthereumAdapter;
pub use crate::impl_slog_value;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum NetworkCapability {
    Full,
    Archive,
    Traces,
}

impl FromStr for NetworkCapability {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "full" => NetworkCapability::Full,
            "archive" => NetworkCapability::Archive,
            "traces" => NetworkCapability::Traces,
            s => return Err(format_err!("invalid network capability provided: {}", s)),
        })
    }
}

impl fmt::Display for NetworkCapability {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            NetworkCapability::Full => write!(f, "full"),
            NetworkCapability::Archive => write!(f, "archive"),
            NetworkCapability::Traces => write!(f, "traces"),
        }
    }
}

impl NetworkCapability {
    pub fn sufficient_capability(
        network: &Vec<NetworkCapability>,
        required: &Vec<NetworkCapability>,
    ) -> bool {
        match required[..] {
            [NetworkCapability::Full] => true,
            [NetworkCapability::Traces] => network.contains(&NetworkCapability::Traces),
            [NetworkCapability::Full, NetworkCapability::Traces] => {
                network.contains(&NetworkCapability::Traces)
            }
            [NetworkCapability::Archive] => network.contains(&NetworkCapability::Archive),
            [NetworkCapability::Archive, NetworkCapability::Traces] => {
                network == &vec![NetworkCapability::Archive, NetworkCapability::Traces]
            }
            _ => false,
        }
    }

    pub fn cheapest() -> Self {
        NetworkCapability::Full
    }
}

impl_slog_value!(NetworkCapability, "{:?}");

#[derive(Clone)]
pub struct EthereumNetworkAdapters {
    pub adapters: BTreeMap<Vec<NetworkCapability>, Arc<dyn EthereumAdapter>>,
}

impl EthereumNetworkAdapters {
    pub fn cheapest_with(
        &self,
        required_capabilities: &Vec<NetworkCapability>,
    ) -> Result<&Arc<dyn EthereumAdapter>, Error> {
        let sufficient_adapters: BTreeMap<&Vec<NetworkCapability>, &Arc<dyn EthereumAdapter>> =
            self.adapters
                .iter()
                .filter(|(capabilities, _adapter)| {
                    NetworkCapability::sufficient_capability(capabilities, required_capabilities)
                })
                .collect();
        if sufficient_adapters.len() == 0 {
            return Err(format_err!(
                "A matching Ethereum network with {:?} was not found.",
                required_capabilities
            ));
        }
        Ok(sufficient_adapters.values().next().unwrap())
    }

    pub fn cheapest(&self) -> Option<&Arc<dyn EthereumAdapter>> {
        self.adapters.values().next()
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

    pub fn insert_or_update(
        &mut self,
        name: String,
        capabilities: Vec<NetworkCapability>,
        adapter: Arc<dyn EthereumAdapter>,
    ) {
        let network = self
            .networks
            .entry(name)
            .or_insert(EthereumNetworkAdapters {
                adapters: [(capabilities.clone(), adapter.clone())]
                    .iter()
                    .cloned()
                    .collect(),
            });
        network.adapters.insert(capabilities, adapter.clone());
    }

    pub fn extend(&mut self, other_networks: EthereumNetworks) {
        self.networks.extend(other_networks.networks);
    }

    pub fn flatten(&self) -> Vec<(String, Vec<NetworkCapability>, Arc<dyn EthereumAdapter>)> {
        self.networks
            .iter()
            .flat_map(|(network_name, network)| {
                network.adapters.iter().map(move |(capabilities, adapter)| {
                    (network_name.clone(), capabilities.clone(), adapter.clone())
                })
            })
            .collect()
    }

    pub fn get_adapter_with_requirements(
        &self,
        network_name: String,
        requirements: &Vec<NetworkCapability>,
    ) -> Result<&Arc<dyn EthereumAdapter>, Error> {
        self.networks
            .get(&network_name)
            .ok_or(format_err!("network not supported: {}", &network_name))
            .and_then(|adapters| {
                println!("{:?}", adapters.adapters.clone().keys());
                adapters.cheapest_with(requirements)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::NetworkCapability;

    #[test]
    fn ethereum_capabilities_comparison() {
        let archive = vec![NetworkCapability::Archive];
        let traces = vec![NetworkCapability::Traces];
        let archive_traces = vec![NetworkCapability::Archive, NetworkCapability::Traces];
        let full = vec![NetworkCapability::Full];
        let full_traces = vec![NetworkCapability::Full, NetworkCapability::Traces];

        // Test all real combinations of capability comparisons
        assert_eq!(
            false,
            NetworkCapability::sufficient_capability(&full, &archive)
        );
        assert_eq!(
            false,
            NetworkCapability::sufficient_capability(&full, &traces)
        );
        assert_eq!(
            false,
            NetworkCapability::sufficient_capability(&full, &archive_traces)
        );
        assert_eq!(true, NetworkCapability::sufficient_capability(&full, &full));
        assert_eq!(
            false,
            NetworkCapability::sufficient_capability(&full, &full_traces)
        );

        assert_eq!(
            true,
            NetworkCapability::sufficient_capability(&archive, &archive)
        );
        assert_eq!(
            false,
            NetworkCapability::sufficient_capability(&archive, &traces)
        );
        assert_eq!(
            false,
            NetworkCapability::sufficient_capability(&archive, &archive_traces)
        );
        assert_eq!(
            true,
            NetworkCapability::sufficient_capability(&archive, &full)
        );
        assert_eq!(
            false,
            NetworkCapability::sufficient_capability(&archive, &full_traces)
        );

        assert_eq!(
            false,
            NetworkCapability::sufficient_capability(&traces, &archive)
        );
        assert_eq!(
            true,
            NetworkCapability::sufficient_capability(&traces, &traces)
        );
        assert_eq!(
            false,
            NetworkCapability::sufficient_capability(&traces, &archive_traces)
        );
        assert_eq!(
            true,
            NetworkCapability::sufficient_capability(&traces, &full)
        );
        assert_eq!(
            true,
            NetworkCapability::sufficient_capability(&traces, &full_traces)
        );

        assert_eq!(
            true,
            NetworkCapability::sufficient_capability(&archive_traces, &archive)
        );
        assert_eq!(
            true,
            NetworkCapability::sufficient_capability(&archive_traces, &traces)
        );
        assert_eq!(
            true,
            NetworkCapability::sufficient_capability(&archive_traces, &archive_traces)
        );
        assert_eq!(
            true,
            NetworkCapability::sufficient_capability(&archive_traces, &full)
        );
        assert_eq!(
            true,
            NetworkCapability::sufficient_capability(&archive_traces, &full_traces)
        );

        assert_eq!(
            false,
            NetworkCapability::sufficient_capability(&full_traces, &archive)
        );
        assert_eq!(
            true,
            NetworkCapability::sufficient_capability(&full_traces, &traces)
        );
        assert_eq!(
            false,
            NetworkCapability::sufficient_capability(&full_traces, &archive_traces)
        );
        assert_eq!(
            true,
            NetworkCapability::sufficient_capability(&full_traces, &full)
        );
        assert_eq!(
            true,
            NetworkCapability::sufficient_capability(&full_traces, &full_traces)
        );
    }
}
