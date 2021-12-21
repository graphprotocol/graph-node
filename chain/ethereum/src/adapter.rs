use anyhow::Error;
use ethabi::{Error as ABIError, Function, ParamType, Token};
use futures::Future;
use graph::blockchain::ChainIdentifier;
use graph::env::env_var;
use mockall::automock;
use mockall::predicate::*;
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::marker::Unpin;
use thiserror::Error;
use tiny_keccak::keccak256;
use web3::types::{Address, Block, Log, H256};

use graph::prelude::*;
use graph::{
    blockchain as bc,
    components::metrics::{CounterVec, GaugeVec, HistogramVec},
    petgraph::{self, graphmap::GraphMap},
};

use crate::capabilities::NodeCapabilities;
use crate::data_source::BlockHandlerFilter;
use crate::{data_source::DataSource, Chain};

pub type EventSignature = H256;
pub type FunctionSelector = [u8; 4];

lazy_static! {
    static ref ETH_GET_LOGS_MAX_CONTRACTS: usize =
        env_var("GRAPH_ETH_GET_LOGS_MAX_CONTRACTS", 2000);
}

#[derive(Clone, Debug)]
pub struct EthereumContractCall {
    pub address: Address,
    pub block_ptr: BlockPtr,
    pub function: Function,
    pub args: Vec<Token>,
}

#[derive(Error, Debug)]
pub enum EthereumContractCallError {
    #[error("ABI error: {0}")]
    ABIError(ABIError),
    /// `Token` is not of expected `ParamType`
    #[error("type mismatch, token {0:?} is not of kind {1:?}")]
    TypeError(Token, ParamType),
    #[error("error encoding input call data: {0}")]
    EncodingError(ethabi::Error),
    #[error("call error: {0}")]
    Web3Error(web3::Error),
    #[error("call reverted: {0}")]
    Revert(String),
    #[error("ethereum node took too long to perform call")]
    Timeout,
}

impl From<ABIError> for EthereumContractCallError {
    fn from(e: ABIError) -> Self {
        EthereumContractCallError::ABIError(e)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Ord, PartialOrd, Hash)]
enum LogFilterNode {
    Contract(Address),
    Event(EventSignature),
}

/// Corresponds to an `eth_getLogs` call.
#[derive(Clone)]
pub struct EthGetLogsFilter {
    pub contracts: Vec<Address>,
    pub event_signatures: Vec<EventSignature>,
}

impl EthGetLogsFilter {
    fn from_contract(address: Address) -> Self {
        EthGetLogsFilter {
            contracts: vec![address],
            event_signatures: vec![],
        }
    }

    fn from_event(event: EventSignature) -> Self {
        EthGetLogsFilter {
            contracts: vec![],
            event_signatures: vec![event],
        }
    }
}

impl fmt::Display for EthGetLogsFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.contracts.len() == 1 {
            write!(
                f,
                "contract {:?}, {} events",
                self.contracts[0],
                self.event_signatures.len()
            )
        } else if self.event_signatures.len() == 1 {
            write!(
                f,
                "event {:?}, {} contracts",
                self.event_signatures[0],
                self.contracts.len()
            )
        } else {
            write!(f, "unreachable")
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct TriggerFilter {
    pub(crate) log: EthereumLogFilter,
    pub(crate) call: EthereumCallFilter,
    pub(crate) block: EthereumBlockFilter,
}

impl TriggerFilter {
    pub(crate) fn requires_traces(&self) -> bool {
        !self.call.is_empty() || self.block.requires_traces()
    }
}

impl bc::TriggerFilter<Chain> for TriggerFilter {
    fn extend<'a>(&mut self, data_sources: impl Iterator<Item = &'a DataSource> + Clone) {
        self.log
            .extend(EthereumLogFilter::from_data_sources(data_sources.clone()));
        self.call
            .extend(EthereumCallFilter::from_data_sources(data_sources.clone()));
        self.block
            .extend(EthereumBlockFilter::from_data_sources(data_sources));
    }

    fn node_capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
            archive: false,
            traces: self.requires_traces(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct EthereumLogFilter {
    /// Log filters can be represented as a bipartite graph between contracts and events. An edge
    /// exists between a contract and an event if a data source for the contract has a trigger for
    /// the event.
    contracts_and_events_graph: GraphMap<LogFilterNode, (), petgraph::Undirected>,

    // Event sigs with no associated address, matching on all addresses.
    wildcard_events: HashSet<EventSignature>,
}

impl EthereumLogFilter {
    /// Check if this filter matches the specified `Log`.
    pub fn matches(&self, log: &Log) -> bool {
        // First topic should be event sig
        match log.topics.first() {
            None => false,

            Some(sig) => {
                // The `Log` matches the filter either if the filter contains
                // a (contract address, event signature) pair that matches the
                // `Log`, or if the filter contains wildcard event that matches.
                let contract = LogFilterNode::Contract(log.address);
                let event = LogFilterNode::Event(*sig);
                self.contracts_and_events_graph
                    .all_edges()
                    .any(|(s, t, ())| {
                        (s == contract && t == event) || (t == contract && s == event)
                    })
                    || self.wildcard_events.contains(sig)
            }
        }
    }

    pub fn from_data_sources<'a>(iter: impl IntoIterator<Item = &'a DataSource>) -> Self {
        let mut this = EthereumLogFilter::default();
        for ds in iter {
            for event_sig in ds.mapping.event_handlers.iter().map(|e| e.topic0()) {
                match ds.source.address {
                    Some(contract) => {
                        this.contracts_and_events_graph.add_edge(
                            LogFilterNode::Contract(contract),
                            LogFilterNode::Event(event_sig),
                            (),
                        );
                    }
                    None => {
                        this.wildcard_events.insert(event_sig);
                    }
                }
            }
        }
        this
    }

    /// Extends this log filter with another one.
    pub fn extend(&mut self, other: EthereumLogFilter) {
        // Destructure to make sure we're checking all fields.
        let EthereumLogFilter {
            contracts_and_events_graph,
            wildcard_events,
        } = other;
        for (s, t, ()) in contracts_and_events_graph.all_edges() {
            self.contracts_and_events_graph.add_edge(s, t, ());
        }
        self.wildcard_events.extend(wildcard_events);
    }

    /// An empty filter is one that never matches.
    pub fn is_empty(&self) -> bool {
        // Destructure to make sure we're checking all fields.
        let EthereumLogFilter {
            contracts_and_events_graph,
            wildcard_events,
        } = self;
        contracts_and_events_graph.edge_count() == 0 && wildcard_events.is_empty()
    }

    /// Filters for `eth_getLogs` calls. The filters will not return false positives. This attempts
    /// to balance between having granular filters but too many calls and having few calls but too
    /// broad filters causing the Ethereum endpoint to timeout.
    pub fn eth_get_logs_filters(self) -> impl Iterator<Item = EthGetLogsFilter> {
        let mut filters = Vec::new();

        // First add the wildcard event filters.
        for wildcard_event in self.wildcard_events {
            filters.push(EthGetLogsFilter::from_event(wildcard_event))
        }

        // The current algorithm is to repeatedly find the maximum cardinality vertex and turn all
        // of its edges into a filter. This is nice because it is neutral between filtering by
        // contract or by events, if there are many events that appear on only one data source
        // we'll filter by many events on a single contract, but if there is an event that appears
        // on a lot of data sources we'll filter by many contracts with a single event.
        //
        // From a theoretical standpoint we're finding a vertex cover, and this is not the optimal
        // algorithm to find a minimum vertex cover, but should be fine as an approximation.
        //
        // One optimization we're not doing is to merge nodes that have the same neighbors into a
        // single node. For example if a subgraph has two data sources, each with the same two
        // events, we could cover that with a single filter and no false positives. However that
        // might cause the filter to become too broad, so at the moment it seems excessive.
        let mut g = self.contracts_and_events_graph;
        while g.edge_count() > 0 {
            let mut push_filter = |filter: EthGetLogsFilter| {
                // Sanity checks:
                // - The filter is not a wildcard because all nodes have neighbors.
                // - The graph is bipartite.
                assert!(filter.contracts.len() > 0 && filter.event_signatures.len() > 0);
                assert!(filter.contracts.len() == 1 || filter.event_signatures.len() == 1);
                filters.push(filter);
            };

            // If there are edges, there are vertexes.
            let max_vertex = g.nodes().max_by_key(|&n| g.neighbors(n).count()).unwrap();
            let mut filter = match max_vertex {
                LogFilterNode::Contract(address) => EthGetLogsFilter::from_contract(address),
                LogFilterNode::Event(event_sig) => EthGetLogsFilter::from_event(event_sig),
            };
            for neighbor in g.neighbors(max_vertex) {
                match neighbor {
                    LogFilterNode::Contract(address) => {
                        if filter.contracts.len() == *ETH_GET_LOGS_MAX_CONTRACTS {
                            // The batch size was reached, register the filter and start a new one.
                            let event = filter.event_signatures[0];
                            push_filter(filter);
                            filter = EthGetLogsFilter::from_event(event);
                        }
                        filter.contracts.push(address);
                    }
                    LogFilterNode::Event(event_sig) => filter.event_signatures.push(event_sig),
                }
            }

            push_filter(filter);
            g.remove_node(max_vertex);
        }
        filters.into_iter()
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct EthereumCallFilter {
    // Each call filter has a map of filters keyed by address, each containing a tuple with
    // start_block and the set of function signatures
    pub contract_addresses_function_signatures:
        HashMap<Address, (BlockNumber, HashSet<FunctionSelector>)>,
}

impl EthereumCallFilter {
    pub fn matches(&self, call: &EthereumCall) -> bool {
        // Calls returned by Firehose actually contains pure transfers and smart
        // contract calls. If the input is less than 4 bytes, we assume it's a pure transfer
        // and discards those.
        if call.input.0.len() < 4 {
            return false;
        }

        // Ensure the call is to a contract the filter expressed an interest in
        match self.contract_addresses_function_signatures.get(&call.to) {
            None => false,
            Some(v) => {
                let signature = &v.1;

                // If the call is to a contract with no specified functions, keep the call
                //
                // Allows the ability to genericly match on all calls to a contract.
                // Caveat is this catch all clause limits you from matching with a specific call
                // on the same address
                if signature.is_empty() {
                    true
                } else {
                    // Ensure the call is to run a function the filter expressed an interest in
                    signature.contains(&call.input.0[..4])
                }
            }
        }
    }

    pub fn from_data_sources<'a>(iter: impl IntoIterator<Item = &'a DataSource>) -> Self {
        iter.into_iter()
            .filter_map(|data_source| data_source.source.address.map(|addr| (addr, data_source)))
            .map(|(contract_addr, data_source)| {
                let start_block = data_source.source.start_block;
                data_source
                    .mapping
                    .call_handlers
                    .iter()
                    .map(move |call_handler| {
                        let sig = keccak256(call_handler.function.as_bytes());
                        (start_block, contract_addr, [sig[0], sig[1], sig[2], sig[3]])
                    })
            })
            .flatten()
            .collect()
    }

    /// Extends this call filter with another one.
    pub fn extend(&mut self, other: EthereumCallFilter) {
        // Extend existing address / function signature key pairs
        // Add new address / function signature key pairs from the provided EthereumCallFilter
        for (address, (proposed_start_block, new_sigs)) in
            other.contract_addresses_function_signatures.into_iter()
        {
            match self
                .contract_addresses_function_signatures
                .get_mut(&address)
            {
                Some((existing_start_block, existing_sigs)) => {
                    *existing_start_block = cmp::min(proposed_start_block, *existing_start_block);
                    existing_sigs.extend(new_sigs);
                }
                None => {
                    self.contract_addresses_function_signatures
                        .insert(address, (proposed_start_block, new_sigs));
                }
            }
        }
    }

    /// An empty filter is one that never matches.
    pub fn is_empty(&self) -> bool {
        // Destructure to make sure we're checking all fields.
        let EthereumCallFilter {
            contract_addresses_function_signatures,
        } = self;
        contract_addresses_function_signatures.is_empty()
    }
}

impl FromIterator<(BlockNumber, Address, FunctionSelector)> for EthereumCallFilter {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (BlockNumber, Address, FunctionSelector)>,
    {
        let mut lookup: HashMap<Address, (BlockNumber, HashSet<FunctionSelector>)> = HashMap::new();
        iter.into_iter()
            .for_each(|(start_block, address, function_signature)| {
                if !lookup.contains_key(&address) {
                    lookup.insert(address, (start_block, HashSet::default()));
                }
                lookup.get_mut(&address).map(|set| {
                    if set.0 > start_block {
                        set.0 = start_block
                    }
                    set.1.insert(function_signature);
                    set
                });
            });
        EthereumCallFilter {
            contract_addresses_function_signatures: lookup,
        }
    }
}

impl From<&EthereumBlockFilter> for EthereumCallFilter {
    fn from(ethereum_block_filter: &EthereumBlockFilter) -> Self {
        Self {
            contract_addresses_function_signatures: ethereum_block_filter
                .contract_addresses
                .iter()
                .map(|(start_block_opt, address)| {
                    (address.clone(), (*start_block_opt, HashSet::default()))
                })
                .collect::<HashMap<Address, (BlockNumber, HashSet<FunctionSelector>)>>(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct EthereumBlockFilter {
    pub contract_addresses: HashSet<(BlockNumber, Address)>,
    pub trigger_every_block: bool,
}

impl EthereumBlockFilter {
    pub fn from_data_sources<'a>(iter: impl IntoIterator<Item = &'a DataSource>) -> Self {
        iter.into_iter()
            .filter(|data_source| data_source.source.address.is_some())
            .fold(Self::default(), |mut filter_opt, data_source| {
                let has_block_handler_with_call_filter = data_source
                    .mapping
                    .block_handlers
                    .clone()
                    .into_iter()
                    .any(|block_handler| match block_handler.filter {
                        Some(ref filter) if *filter == BlockHandlerFilter::Call => true,
                        _ => false,
                    });

                let has_block_handler_without_filter = data_source
                    .mapping
                    .block_handlers
                    .clone()
                    .into_iter()
                    .any(|block_handler| block_handler.filter.is_none());

                filter_opt.extend(Self {
                    trigger_every_block: has_block_handler_without_filter,
                    contract_addresses: if has_block_handler_with_call_filter {
                        vec![(
                            data_source.source.start_block,
                            data_source.source.address.unwrap().to_owned(),
                        )]
                        .into_iter()
                        .collect()
                    } else {
                        HashSet::default()
                    },
                });
                filter_opt
            })
    }

    pub fn extend(&mut self, other: EthereumBlockFilter) {
        self.trigger_every_block = self.trigger_every_block || other.trigger_every_block;
        self.contract_addresses = self.contract_addresses.iter().cloned().fold(
            HashSet::new(),
            |mut addresses, (start_block, address)| {
                match other
                    .contract_addresses
                    .iter()
                    .cloned()
                    .find(|(_, other_address)| &address == other_address)
                {
                    Some((other_start_block, address)) => {
                        addresses.insert((cmp::min(other_start_block, start_block), address));
                    }
                    None => {
                        addresses.insert((start_block, address));
                    }
                }
                addresses
            },
        );
    }

    fn requires_traces(&self) -> bool {
        !self.contract_addresses.is_empty()
    }

    /// An empty filter is one that never matches.
    pub fn is_empty(&self) -> bool {
        // If we are triggering every block, we are of course not empty
        if self.trigger_every_block {
            return false;
        }

        self.contract_addresses.is_empty()
    }
}

#[derive(Clone)]
pub struct ProviderEthRpcMetrics {
    request_duration: Box<HistogramVec>,
    errors: Box<CounterVec>,
}

impl ProviderEthRpcMetrics {
    pub fn new(registry: Arc<impl MetricsRegistry>) -> Self {
        let request_duration = registry
            .new_histogram_vec(
                "eth_rpc_request_duration",
                "Measures eth rpc request duration",
                vec![String::from("method"), String::from("provider")],
                vec![0.05, 0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4, 12.8, 25.6],
            )
            .unwrap();
        let errors = registry
            .new_counter_vec(
                "eth_rpc_errors",
                "Counts eth rpc request errors",
                vec![String::from("method"), String::from("provider")],
            )
            .unwrap();
        Self {
            request_duration,
            errors,
        }
    }

    pub fn observe_request(&self, duration: f64, method: &str, provider: &str) {
        self.request_duration
            .with_label_values(&[method, provider])
            .observe(duration);
    }

    pub fn add_error(&self, method: &str, provider: &str) {
        self.errors.with_label_values(&[method, provider]).inc();
    }
}

#[derive(Clone)]
pub struct SubgraphEthRpcMetrics {
    request_duration: Box<GaugeVec>,
    errors: Box<CounterVec>,
}

impl SubgraphEthRpcMetrics {
    pub fn new(registry: Arc<dyn MetricsRegistry>, subgraph_hash: &str) -> Self {
        let request_duration = registry
            .new_deployment_gauge_vec(
                "deployment_eth_rpc_request_duration",
                "Measures eth rpc request duration for a subgraph deployment",
                &subgraph_hash,
                vec![String::from("method"), String::from("provider")],
            )
            .unwrap();
        let errors = registry
            .new_deployment_counter_vec(
                "deployment_eth_rpc_errors",
                "Counts eth rpc request errors for a subgraph deployment",
                &subgraph_hash,
                vec![String::from("method"), String::from("provider")],
            )
            .unwrap();
        Self {
            request_duration,
            errors,
        }
    }

    pub fn observe_request(&self, duration: f64, method: &str, provider: &str) {
        self.request_duration
            .with_label_values(&[method, provider])
            .set(duration);
    }

    pub fn add_error(&self, method: &str, provider: &str) {
        self.errors.with_label_values(&[method, provider]).inc();
    }
}

/// Common trait for components that watch and manage access to Ethereum.
///
/// Implementations may be implemented against an in-process Ethereum node
/// or a remote node over RPC.
#[automock]
#[async_trait]
pub trait EthereumAdapter: Send + Sync + 'static {
    fn url_hostname(&self) -> &str;

    /// The `provider.label` from the adapter's configuration
    fn provider(&self) -> &str;

    /// Ask the Ethereum node for some identifying information about the Ethereum network it is
    /// connected to.
    async fn net_identifiers(&self) -> Result<ChainIdentifier, Error>;

    /// Get the latest block, including full transactions.
    fn latest_block(
        &self,
        logger: &Logger,
    ) -> Box<dyn Future<Item = LightEthereumBlock, Error = bc::IngestorError> + Send + Unpin>;

    /// Get the latest block, with only the header and transaction hashes.
    fn latest_block_header(
        &self,
        logger: &Logger,
    ) -> Box<dyn Future<Item = web3::types::Block<H256>, Error = bc::IngestorError> + Send>;

    fn load_block(
        &self,
        logger: &Logger,
        block_hash: H256,
    ) -> Box<dyn Future<Item = LightEthereumBlock, Error = Error> + Send>;

    /// Load Ethereum blocks in bulk, returning results as they come back as a Stream.
    /// May use the `chain_store` as a cache.
    fn load_blocks(
        &self,
        logger: Logger,
        chain_store: Arc<dyn ChainStore>,
        block_hashes: HashSet<H256>,
    ) -> Box<dyn Stream<Item = Arc<LightEthereumBlock>, Error = Error> + Send>;

    /// Find a block by its hash.
    fn block_by_hash(
        &self,
        logger: &Logger,
        block_hash: H256,
    ) -> Box<dyn Future<Item = Option<LightEthereumBlock>, Error = Error> + Send>;

    fn block_by_number(
        &self,
        logger: &Logger,
        block_number: BlockNumber,
    ) -> Box<dyn Future<Item = Option<LightEthereumBlock>, Error = Error> + Send>;

    /// Load full information for the specified `block` (in particular, transaction receipts).
    fn load_full_block(
        &self,
        logger: &Logger,
        block: LightEthereumBlock,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<EthereumBlock, bc::IngestorError>> + Send>>;

    /// Load block pointer for the specified `block number`.
    fn block_pointer_from_number(
        &self,
        logger: &Logger,
        block_number: BlockNumber,
    ) -> Box<dyn Future<Item = BlockPtr, Error = bc::IngestorError> + Send>;

    /// Find a block by its number, according to the Ethereum node.
    ///
    /// Careful: don't use this function without considering race conditions.
    /// Chain reorgs could happen at any time, and could affect the answer received.
    /// Generally, it is only safe to use this function with blocks that have received enough
    /// confirmations to guarantee no further reorgs, **and** where the Ethereum node is aware of
    /// those confirmations.
    /// If the Ethereum node is far behind in processing blocks, even old blocks can be subject to
    /// reorgs.
    fn block_hash_by_block_number(
        &self,
        logger: &Logger,
        block_number: BlockNumber,
    ) -> Box<dyn Future<Item = Option<H256>, Error = Error> + Send>;

    /// Obtain all uncle blocks for a given block hash.
    fn uncles(
        &self,
        logger: &Logger,
        block: &LightEthereumBlock,
    ) -> Box<dyn Future<Item = Vec<Option<Block<H256>>>, Error = Error> + Send>;

    /// Call the function of a smart contract.
    fn contract_call(
        &self,
        logger: &Logger,
        call: EthereumContractCall,
        cache: Arc<dyn EthereumCallCache>,
    ) -> Box<dyn Future<Item = Vec<Token>, Error = EthereumContractCallError> + Send>;
}

#[cfg(test)]
mod tests {
    use super::EthereumCallFilter;

    use graph::prelude::web3::types::Address;
    use graph::prelude::web3::types::Bytes;
    use graph::prelude::EthereumCall;

    use std::collections::{HashMap, HashSet};
    use std::iter::FromIterator;

    #[test]
    fn matching_ethereum_call_filter() {
        let address = |id: u64| Address::from_low_u64_be(id);
        let bytes = |value: Vec<u8>| Bytes::from(value);
        let call = |to: Address, input: Vec<u8>| EthereumCall {
            to,
            input: bytes(input),
            ..Default::default()
        };

        let filter = EthereumCallFilter {
            contract_addresses_function_signatures: HashMap::from_iter(vec![
                (address(0), (0, HashSet::from_iter(vec![[0u8; 4]]))),
                (address(1), (1, HashSet::from_iter(vec![[1u8; 4]]))),
                (address(2), (2, HashSet::new())),
            ]),
        };

        assert_eq!(
            false,
            filter.matches(&call(address(2), vec![])),
            "call with empty bytes are always ignore, whatever the condition"
        );

        assert_eq!(
            false,
            filter.matches(&call(address(4), vec![1; 36])),
            "call with incorrect address should be ignored"
        );

        assert_eq!(
            true,
            filter.matches(&call(address(1), vec![1; 36])),
            "call with correct address & signature should match"
        );

        assert_eq!(
            false,
            filter.matches(&call(address(1), vec![4u8; 36])),
            "call with correct address but incorrect signature for a specific contract filter (i.e. matches some signatures) should be ignored"
        );
    }

    #[test]
    fn extending_ethereum_call_filter() {
        let mut base = EthereumCallFilter {
            contract_addresses_function_signatures: HashMap::from_iter(vec![
                (
                    Address::from_low_u64_be(0),
                    (0, HashSet::from_iter(vec![[0u8; 4]])),
                ),
                (
                    Address::from_low_u64_be(1),
                    (1, HashSet::from_iter(vec![[1u8; 4]])),
                ),
            ]),
        };
        let extension = EthereumCallFilter {
            contract_addresses_function_signatures: HashMap::from_iter(vec![
                (
                    Address::from_low_u64_be(0),
                    (2, HashSet::from_iter(vec![[2u8; 4]])),
                ),
                (
                    Address::from_low_u64_be(3),
                    (3, HashSet::from_iter(vec![[3u8; 4]])),
                ),
            ]),
        };
        base.extend(extension);

        assert_eq!(
            base.contract_addresses_function_signatures
                .get(&Address::from_low_u64_be(0)),
            Some(&(0, HashSet::from_iter(vec![[0u8; 4], [2u8; 4]])))
        );
        assert_eq!(
            base.contract_addresses_function_signatures
                .get(&Address::from_low_u64_be(3)),
            Some(&(3, HashSet::from_iter(vec![[3u8; 4]])))
        );
        assert_eq!(
            base.contract_addresses_function_signatures
                .get(&Address::from_low_u64_be(1)),
            Some(&(1, HashSet::from_iter(vec![[1u8; 4]])))
        );
    }
}

// Tests `eth_get_logs_filters` in instances where all events are filtered on by all contracts.
// This represents, for example, the relationship between dynamic data sources and their events.
#[test]
fn complete_log_filter() {
    use std::collections::BTreeSet;

    // Test a few combinations of complete graphs.
    for i in [1, 2] {
        let events: BTreeSet<_> = (0..i).map(H256::from_low_u64_le).collect();

        for j in [1, 1000, 2000, 3000] {
            let contracts: BTreeSet<_> = (0..j).map(Address::from_low_u64_le).collect();

            // Construct the complete bipartite graph with i events and j contracts.
            let mut contracts_and_events_graph = GraphMap::new();
            for &contract in &contracts {
                for &event in &events {
                    contracts_and_events_graph.add_edge(
                        LogFilterNode::Contract(contract),
                        LogFilterNode::Event(event),
                        (),
                    );
                }
            }

            // Run `eth_get_logs_filters`, which is what we want to test.
            let logs_filters: Vec<_> = EthereumLogFilter {
                contracts_and_events_graph,
                wildcard_events: HashSet::new(),
            }
            .eth_get_logs_filters()
            .collect();

            // Assert that a contract or event is filtered on iff it was present in the graph.
            assert_eq!(
                logs_filters
                    .iter()
                    .map(|l| l.contracts.iter())
                    .flatten()
                    .copied()
                    .collect::<BTreeSet<_>>(),
                contracts
            );
            assert_eq!(
                logs_filters
                    .iter()
                    .map(|l| l.event_signatures.iter())
                    .flatten()
                    .copied()
                    .collect::<BTreeSet<_>>(),
                events
            );

            // Assert that chunking works.
            for filter in logs_filters {
                assert!(filter.contracts.len() <= *ETH_GET_LOGS_MAX_CONTRACTS);
            }
        }
    }
}
