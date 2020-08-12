use futures::{Future, Stream};
use mockall::predicate::*;
use rand::seq::IteratorRandom;
use std::cmp::{Ord, Ordering, PartialOrd};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use crate::components::ethereum::{
    EthereumAdapter, EthereumAdapterError, EthereumBlock, EthereumBlockPointer, EthereumCall,
    EthereumCallFilter, EthereumContractCall, EthereumContractCallError, EthereumLogFilter,
    EthereumNetworkIdentifier, LightEthereumBlock, SubgraphEthRpcMetrics,
};
pub use crate::impl_slog_value;
use crate::prelude::{
    ethabi, format_err,
    futures03::{compat::Stream01CompatExt, TryFutureExt, TryStreamExt},
    retry, stream, web3, ChainStore, CheapClone, Error, EthereumCallCache, Logger,
};
use ethabi::Token;
use web3::types::{Block, Log, H256};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeCapabilities {
    pub archive: bool,
    pub traces: bool,
}

// Take all NodeCapabilities fields into account when ordering
// A NodeCapabilities instance is considered equal or greater than another
// if all of its fields are equal or greater than the other
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

impl FromStr for NodeCapabilities {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let capabilities: Vec<&str> = s.split(",").collect();
        Ok(NodeCapabilities {
            archive: capabilities
                .iter()
                .find(|cap| cap.eq(&&"archive"))
                .is_some(),
            traces: capabilities.iter().find(|cap| cap.eq(&&"traces")).is_some(),
        })
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

impl_slog_value!(NodeCapabilities, "{}");

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

        // Select from the matching adapters randomly
        let mut rng = rand::thread_rng();
        Ok(&sufficient_adapters.iter().choose(&mut rng).unwrap().adapter)
    }

    pub fn sufficient_adapters(
        &self,
        required_capabilities: &NodeCapabilities,
    ) -> Result<EthereumNetworkAdapters, Error> {
        let sufficient_adapters: Vec<EthereumNetworkAdapter> = self
            .adapters
            .iter()
            .cloned()
            .filter(|adapter| &adapter.capabilities >= required_capabilities)
            .collect();
        if sufficient_adapters.is_empty() {
            return Err(format_err!(
                "A matching Ethereum network with {:?} was not found.",
                required_capabilities
            ));
        }

        Ok(EthereumNetworkAdapters {
            adapters: sufficient_adapters,
        })
    }

    pub fn cheapest(&self) -> Option<&Arc<dyn EthereumAdapter>> {
        // EthereumAdapters are sorted by their NodeCapabilities when the EthereumNetworks
        // struct is instantiated so they do not need to be sorted here
        self.adapters
            .iter()
            .next()
            .map(|ethereum_network_adapter| &ethereum_network_adapter.adapter)
    }
}

impl EthereumAdapter for EthereumNetworkAdapters {
    fn net_identifiers(
        &self,
        logger: &Logger,
    ) -> Box<dyn Future<Item = EthereumNetworkIdentifier, Error = Error> + Send> {
        let logger = logger.clone();
        let adapters = self.adapters.clone();
        let mut count = 0;

        Box::new(
            retry("NetworkAdapters: net_version RPC call", &logger)
                .no_limit()
                .timeout_secs(20)
                .run(move || {
                    count += 1;
                    adapters
                        .get(count % adapters.len())
                        .expect("Ethereum adapters access index should never be out of range")
                        .adapter
                        .net_identifiers(&logger)
                })
                .from_err(),
        )
    }

    fn latest_block_header(
        &self,
        logger: &Logger,
    ) -> Box<dyn Future<Item = web3::types::Block<H256>, Error = EthereumAdapterError> + Send> {
        let logger = logger.clone();
        let adapters = self.adapters.clone();
        let mut count = 0;

        Box::new(
            retry(
                "NetworkAdapters: eth_getBlockByNumber(latest) no txs RPC call",
                &logger,
            )
            .no_limit()
            .timeout_secs(20)
            .run(move || {
                count += 1;
                adapters
                    .get(count % adapters.len())
                    .expect("Ethereum adapters access index should never be out of range")
                    .adapter
                    .latest_block_header(&logger)
            })
            .map_err(move |e| {
                e.into_inner().unwrap_or_else(move || {
                    format_err!(
                        "All compatible Ethereum nodes took too long to return latest block header"
                    )
                    .into()
                })
            }),
        )
    }

    fn latest_block(
        &self,
        logger: &Logger,
    ) -> Box<dyn Future<Item = LightEthereumBlock, Error = EthereumAdapterError> + Send + Unpin>
    {
        let logger = logger.clone();
        let adapters = self.adapters.clone();
        let mut count = 0;

        Box::new(
            retry(
                "NetworkAdapters: eth_getBlockByNumber(latest) with txs RPC call",
                &logger,
            )
            .no_limit()
            .timeout_secs(20)
            .run(move || {
                count += 1;
                adapters
                    .get(count % adapters.len())
                    .expect("Ethereum adapters access index should never be out of range")
                    .adapter
                    .latest_block(&logger)
            })
            .map_err(move |e| {
                e.into_inner().unwrap_or_else(move || {
                    format_err!(
                        "All compatible Ethereum nodes took too long to return latest block"
                    )
                    .into()
                })
            }),
        )
    }

    fn load_block(
        &self,
        logger: &Logger,
        block_hash: H256,
    ) -> Box<dyn Future<Item = LightEthereumBlock, Error = Error> + Send> {
        let logger = logger.clone();
        let adapters = self.adapters.clone();
        let mut count = 0;

        Box::new(
            retry(
                "NetworkAdapters: eth_getBlockByNumber(latest) with txs RPC call",
                &logger,
            )
            .no_limit()
            .timeout_secs(20)
            .run(move || {
                count += 1;
                adapters
                    .get(count % adapters.len())
                    .expect("Ethereum adapters access index should never be out of range")
                    .adapter
                    .load_block(&logger, block_hash)
            })
            .from_err(),
        )
    }

    fn block_by_hash(
        &self,
        logger: &Logger,
        block_hash: H256,
    ) -> Box<dyn Future<Item = Option<LightEthereumBlock>, Error = Error> + Send> {
        let logger = logger.clone();
        let adapters = self.adapters.clone();
        let mut count = 0;

        Box::new(
            retry(
                "NetworkAdapters: eth_getBlockByNumber(latest) with txs RPC call",
                &logger,
            )
            .no_limit()
            .timeout_secs(20)
            .run(move || {
                count += 1;
                adapters
                    .get(count % adapters.len())
                    .expect("Ethereum adapters access index should never be out of range")
                    .adapter
                    .block_by_hash(&logger, block_hash)
            })
            .from_err(),
        )
    }

    fn block_by_number(
        &self,
        logger: &Logger,
        block_number: u64,
    ) -> Box<dyn Future<Item = Option<LightEthereumBlock>, Error = Error> + Send> {
        let logger = logger.clone();
        let adapters = self.adapters.clone();
        let mut count = 0;

        Box::new(
            retry(
                "NetworkAdapters: eth_getBlockByNumber(latest) with txs RPC call",
                &logger,
            )
            .no_limit()
            .timeout_secs(20)
            .run(move || {
                count += 1;
                adapters
                    .get(count % adapters.len())
                    .expect("Ethereum adapters access index should never be out of range")
                    .adapter
                    .block_by_number(&logger, block_number)
            })
            .from_err(),
        )
    }

    fn load_full_block(
        &self,
        logger: &Logger,
        block: LightEthereumBlock,
    ) -> Box<dyn Future<Item = EthereumBlock, Error = EthereumAdapterError> + Send> {
        let logger = logger.clone();
        let adapters = self.adapters.clone();
        let mut count = 0;

        Box::new(
            retry(
                "NetworkAdapters: batch eth_getTransactionReceipt RPC call",
                &logger,
            )
            .no_limit()
            .timeout_secs(20)
            .run(move || {
                let block = block.clone();
                count += 1;
                adapters
                    .get(count % adapters.len())
                    .expect("Ethereum adapters access index should never be out of range")
                    .adapter
                    .load_full_block(&logger, block)
            })
            .map_err(move |e| {
                e.into_inner().unwrap_or_else(move || {
                    format_err!("All compatible Ethereum nodes took too long to load full block")
                        .into()
                })
            }),
        )
    }

    fn block_pointer_from_number(
        &self,
        logger: &Logger,
        chain_store: Arc<dyn ChainStore>,
        block_number: u64,
    ) -> Box<dyn Future<Item = EthereumBlockPointer, Error = EthereumAdapterError> + Send> {
        let logger = logger.clone();
        let adapters = self.adapters.clone();
        let mut count = 0;

        Box::new(
            retry("NetworkAdapters: block pointer from number", &logger)
                .no_limit()
                    .timeout_secs(20)
                    .run(move || {
                        count += 1;
                        adapters.get(count % adapters.len())
                            .expect("Ethereum adapters access index should never be out of range")
                            .adapter
                            .block_pointer_from_number(&logger, chain_store.clone(), block_number)
                    }).map_err(move |e| {
                        e.into_inner().unwrap_or_else(move || {
                            format_err!("All compatible Ethereum nodes took too long to return block pointer from number").into()
                        })
                    })
        )
    }

    fn block_hash_by_block_number(
        &self,
        logger: &Logger,
        chain_store: Arc<dyn ChainStore>,
        block_number: u64,
        block_is_final: bool,
    ) -> Box<dyn Future<Item = Option<H256>, Error = Error> + Send> {
        let logger = logger.clone();
        let adapters = self.adapters.clone();
        let mut count = 0;
        Box::new(
            retry("NetworkAdapters: block hash by block number", &logger)
                .no_limit()
                .timeout_secs(20)
                .run(move || {
                    count += 1;
                    adapters
                        .get(count % adapters.len())
                        .expect("Ethereum adapters access index should never be out of range")
                        .adapter
                        .block_hash_by_block_number(
                            &logger,
                            chain_store.clone(),
                            block_number.clone(),
                            block_is_final.clone(),
                        )
                })
                .from_err(),
        )
    }

    fn uncles(
        &self,
        logger: &Logger,
        block: &LightEthereumBlock,
    ) -> Box<dyn Future<Item = Vec<Option<Block<H256>>>, Error = Error> + Send> {
        let logger = logger.clone();
        let adapters = self.adapters.clone();
        let block = block.clone();
        let mut count = 0;

        Box::new(
            retry(
                "NetworkAdapters: eth_getUncleByBlockHashAndIndex RPC call",
                &logger,
            )
            .no_limit()
            .timeout_secs(20)
            .run(move || {
                count += 1;
                adapters
                    .get(count % adapters.len())
                    .expect("Ethereum adapters access index should never be out of range")
                    .adapter
                    .uncles(&logger, &block)
            })
            .from_err(),
        )
    }

    fn is_on_main_chain(
        &self,
        logger: &Logger,
        subgraph_metrics: Arc<SubgraphEthRpcMetrics>,
        chain_store: Arc<dyn ChainStore>,
        block_ptr: EthereumBlockPointer,
    ) -> Box<dyn Future<Item = bool, Error = Error> + Send> {
        let logger = logger.clone();
        let adapters = self.adapters.clone();
        let mut count = 0;

        Box::new(
            retry("NetworkAdapters: is on main chain", &logger)
                .no_limit()
                .timeout_secs(20)
                .run(move || {
                    count += 1;
                    adapters
                        .get(count % adapters.len())
                        .expect("Ethereum adapters access index should never be out of range")
                        .adapter
                        .is_on_main_chain(
                            &logger,
                            subgraph_metrics.clone(),
                            chain_store.clone(),
                            block_ptr.clone(),
                        )
                })
                .from_err(),
        )
    }

    fn calls_in_block(
        &self,
        logger: &Logger,
        subgraph_metrics: Arc<SubgraphEthRpcMetrics>,
        block_number: u64,
        block_hash: H256,
    ) -> Box<dyn Future<Item = Vec<EthereumCall>, Error = Error> + Send> {
        let logger = logger.clone();
        let adapters = self.adapters.clone();
        let mut count = 0;

        Box::new(
            retry("NetworkAdapters: calls in block", &logger)
                .no_limit()
                .timeout_secs(20)
                .run(move || {
                    count += 1;
                    adapters
                        .get(count % adapters.len())
                        .expect("Ethereum adapters access index should never be out of range")
                        .adapter
                        .calls_in_block(
                            &logger,
                            subgraph_metrics.clone(),
                            block_number.clone(),
                            block_hash.clone(),
                        )
                })
                .from_err(),
        )
    }

    fn logs_in_block_range(
        &self,
        logger: &Logger,
        subgraph_metrics: Arc<SubgraphEthRpcMetrics>,
        from: u64,
        to: u64,
        log_filter: EthereumLogFilter,
    ) -> Box<dyn Future<Item = Vec<Log>, Error = Error> + Send> {
        let logger = logger.clone();
        let adapters = self.adapters.clone();
        let mut count = 0;

        Box::new(
            retry("NetworkAdapters: calls in block", &logger)
                .no_limit()
                .timeout_secs(20)
                .run(move || {
                    count += 1;
                    adapters
                        .get(count % adapters.len())
                        .expect("Ethereum adapters access index should never be out of range")
                        .adapter
                        .logs_in_block_range(
                            &logger,
                            subgraph_metrics.cheap_clone(),
                            from.clone(),
                            to.clone(),
                            log_filter.clone(),
                        )
                })
                .from_err(),
        )
    }

    fn calls_in_block_range(
        &self,
        logger: &Logger,
        subgraph_metrics: Arc<SubgraphEthRpcMetrics>,
        from: u64,
        to: u64,
        call_filter: EthereumCallFilter,
    ) -> Box<dyn Stream<Item = EthereumCall, Error = Error> + Send> {
        let logger = logger.clone();
        let adapters = self.adapters.clone();
        let mut count = 0;

        Box::new(
            retry("NetworkAdapters: calls in block", &logger)
                .no_limit()
                .timeout_secs(20)
                .run(move || {
                    count += 1;
                    adapters
                        .get(count % adapters.len())
                        .expect("Ethereum adapters access index should never be out of range")
                        .adapter
                        .calls_in_block_range(
                            &logger,
                            subgraph_metrics.clone(),
                            from.clone(),
                            to.clone(),
                            call_filter.clone(),
                        )
                        .compat()
                        .try_collect::<Vec<EthereumCall>>()
                        .compat()
                })
                .map(|i: Vec<EthereumCall>| stream::iter_ok::<_, Error>(i))
                .from_err()
                .flatten_stream(),
        )
    }

    fn contract_call(
        &self,
        logger: &Logger,
        call: EthereumContractCall,
        cache: Arc<dyn EthereumCallCache>,
    ) -> Box<dyn Future<Item = Vec<Token>, Error = EthereumContractCallError> + Send> {
        let logger = logger.clone();
        let adapters = self.adapters.clone();
        let mut count = 0;

        Box::new(
            retry("NetworkAdapters: contract call", &logger)
                .no_limit()
                .timeout_secs(20)
                .run(move || {
                    count += 1;
                    adapters
                        .get(count % adapters.len())
                        .expect("Ethereum adapters access index should never be out of range")
                        .adapter
                        .contract_call(&logger, call.clone(), cache.clone())
                })
                .map_err(move |e| {
                    e.into_inner().unwrap_or_else(move || {
                        format_err!(
                            "All compatible Ethereum nodes took too long to make contract call"
                        )
                        .into()
                    })
                }),
        )
    }

    /// Load Ethereum blocks in bulk, returning results as they come back as a Stream.
    fn load_blocks(
        &self,
        logger: Logger,
        chain_store: Arc<dyn ChainStore>,
        block_hashes: HashSet<H256>,
    ) -> Box<dyn Stream<Item = LightEthereumBlock, Error = Error> + Send> {
        let logger = logger.clone();
        let adapters = self.adapters.clone();
        let mut count = 0;

        Box::new(
            retry("NetworkAdapters: load blocks", &logger)
                .no_limit()
                .timeout_secs(20)
                .run(move || {
                    count += 1;
                    adapters
                        .get(count % adapters.len())
                        .expect("Ethereum adapters access index should never be out of range")
                        .adapter
                        .load_blocks(logger.clone(), chain_store.clone(), block_hashes.clone())
                        .compat()
                        .try_collect::<Vec<LightEthereumBlock>>()
                        .compat()
                })
                .map(|i: Vec<LightEthereumBlock>| stream::iter_ok::<_, Error>(i))
                .from_err()
                .flatten_stream(),
        )
    }

    /// Reorg safety: `to` must be a final block.
    fn block_range_to_ptrs(
        &self,
        logger: Logger,
        from: u64,
        to: u64,
    ) -> Box<dyn Future<Item = Vec<EthereumBlockPointer>, Error = Error> + Send> {
        let logger = logger.clone();
        let adapters = self.adapters.clone();
        let mut count = 0;

        Box::new(
            retry("NetworkAdapters: block range to ptrs", &logger)
                .no_limit()
                .timeout_secs(20)
                .run(move || {
                    count += 1;
                    adapters
                        .get(count % adapters.len())
                        .expect("Ethereum adapters access index should never be out of range")
                        .adapter
                        .block_range_to_ptrs(logger.clone(), from.clone(), to.clone())
                })
                .from_err(),
        )
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
        adapter: Arc<dyn EthereumAdapter>,
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

    pub fn sort(&mut self) {
        for adapters in self.networks.values_mut() {
            adapters
                .adapters
                .sort_by_key(|adapter| adapter.capabilities)
        }
    }

    pub fn adapters_with_capabilities(
        &self,
        network_name: String,
        requirements: &NodeCapabilities,
    ) -> Result<EthereumNetworkAdapters, Error> {
        self.networks
            .get(&network_name)
            .ok_or(format_err!("network not supported: {}", &network_name))
            .and_then(|adapters| adapters.sufficient_adapters(requirements))
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
