use failure::{format_err, Error};
use std::cmp::{Ord, Ordering, PartialOrd};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use crate::components::ethereum::EthereumAdapter;
pub use crate::impl_slog_value;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeCapabilities {
    pub archive: bool,
    pub traces: bool,
}

impl Ord for NodeCapabilities {
    fn cmp(&self, other: &Self) -> Ordering {
        match (
            self.archive.cmp(&other.archive),
            self.traces.cmp(&other.traces),
        ) {
            (Ordering::Greater, Ordering::Greater) => Ordering::Greater,
            (Ordering::Greater, Ordering::Equal) => Ordering::Greater,
            (Ordering::Equal, Ordering::Greater) => Ordering::Greater,
            (Ordering::Equal, Ordering::Equal) => Ordering::Equal,
            (Ordering::Less, _) => Ordering::Less,
            (_, Ordering::Less) => Ordering::Less,
        }
    }
}

impl PartialOrd for NodeCapabilities {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for NodeCapabilities {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            NodeCapabilities {
                archive: true,
                traces: true,
            } => write!(f, "archive, trace"),
            NodeCapabilities {
                archive: false,
                traces: true,
            } => write!(f, "full, trace"),
            NodeCapabilities {
                archive: false,
                traces: false,
            } => write!(f, "full"),
            NodeCapabilities {
                archive: true,
                traces: false,
            } => write!(f, "archive"),
        }
    }
}

impl_slog_value!(NodeCapabilities, "{:?}");

#[derive(Clone)]
pub struct EthereumNetworkAdapter {
    pub capabilities: NodeCapabilities,
    adapter: Arc<dyn EthereumAdapter>,
}

#[derive(Clone)]
pub struct EthereumNetworkAdapters {
    pub adapters: Vec<EthereumNetworkAdapter>,
}

impl EthereumNetworkAdapters {
    pub fn cheapest_with(
        &self,
        required_capabilities: &NodeCapabilities,
    ) -> Result<&Arc<dyn EthereumAdapter>, Error> {
        let sufficient_adapters: Vec<&EthereumNetworkAdapter> = self
            .adapters
            .iter()
            .filter(|adapter| &adapter.capabilities >= required_capabilities)
            .collect();
        if sufficient_adapters.is_empty() {
            return Err(format_err!(
                "A matching Ethereum network with {:?} was not found.",
                required_capabilities
            ));
        }
        // TODO: Randomly choose between capable nodes
        Ok(&sufficient_adapters.iter().next().unwrap().adapter)
    }

    pub fn cheapest(&self) -> Option<&Arc<dyn EthereumAdapter>> {
        self.adapters
            .iter()
            .next()
            .map(|ethereum_network_adapter| &ethereum_network_adapter.adapter)
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
        capabilities: NodeCapabilities,
        adapter: Arc<dyn EthereumAdapter>,
    ) {
        let network_adapters = self
            .networks
            .entry(name)
            .or_insert(EthereumNetworkAdapters {
                adapters: vec![EthereumNetworkAdapter {
                    capabilities,
                    adapter: adapter.clone(),
                }],
            });
        network_adapters.adapters.push(EthereumNetworkAdapter {
            capabilities,
            adapter: adapter.clone(),
        });
    }

    pub fn extend(&mut self, other_networks: EthereumNetworks) {
        self.networks.extend(other_networks.networks);
    }

    pub fn flatten(&self) -> Vec<(String, NodeCapabilities, Arc<dyn EthereumAdapter>)> {
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

    pub fn adapter_with_capabilities(
        &self,
        network_name: String,
        requirements: &NodeCapabilities,
    ) -> Result<&Arc<dyn EthereumAdapter>, Error> {
        self.networks
            .get(&network_name)
            .ok_or(format_err!("network not supported: {}", &network_name))
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
