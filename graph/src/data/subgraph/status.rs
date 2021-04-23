//! Support for the indexing status API

use super::schema::{SubgraphError, SubgraphHealth};
use crate::data::graphql::{object, IntoValue};
use crate::prelude::{q, web3::types::H256, BlockPtr, Value};

pub enum Filter {
    /// Get all versions for the named subgraph
    SubgraphName(String),
    /// Get the current (`true`) or pending (`false`) version of the named
    /// subgraph
    SubgraphVersion(String, bool),
    /// Get the status of all deployments whose ids are given
    Deployments(Vec<String>),
}

/// Light wrapper around `EthereumBlockPointer` that is compatible with GraphQL values.
#[derive(Debug)]
pub struct EthereumBlock(BlockPtr);

impl EthereumBlock {
    pub fn new(hash: H256, number: u64) -> Self {
        EthereumBlock(BlockPtr::from((hash, number)))
    }

    pub fn to_ptr(self) -> BlockPtr {
        self.0
    }

    pub fn number(&self) -> i32 {
        self.0.number
    }
}

impl IntoValue for EthereumBlock {
    fn into_value(self) -> q::Value {
        object! {
            __typename: "EthereumBlock",
            hash: self.0.hash_hex(),
            number: format!("{}", self.0.number),
        }
    }
}

impl From<BlockPtr> for EthereumBlock {
    fn from(ptr: BlockPtr) -> Self {
        Self(ptr)
    }
}

/// Indexing status information related to the chain. Right now, we only
/// support Ethereum, but once we support more chains, we'll have to turn this into
/// an enum
#[derive(Debug)]
pub struct ChainInfo {
    /// The network name (e.g. `mainnet`, `ropsten`, `rinkeby`, `kovan` or `goerli`).
    pub network: String,
    /// The current head block of the chain.
    pub chain_head_block: Option<EthereumBlock>,
    /// The earliest block available for this subgraph.
    pub earliest_block: Option<EthereumBlock>,
    /// The latest block that the subgraph has synced to.
    pub latest_block: Option<EthereumBlock>,
}

impl IntoValue for ChainInfo {
    fn into_value(self) -> q::Value {
        let ChainInfo {
            network,
            chain_head_block,
            earliest_block,
            latest_block,
        } = self;
        object! {
            // `__typename` is needed for the `ChainIndexingStatus` interface
            // in GraphQL to work.
            __typename: "EthereumIndexingStatus",
            network: network,
            chainHeadBlock: chain_head_block,
            earliestBlock: earliest_block,
            latestBlock: latest_block,
        }
    }
}

#[derive(Debug)]
pub struct Info {
    /// The subgraph ID.
    pub subgraph: String,

    /// Whether or not the subgraph has synced all the way to the current chain head.
    pub synced: bool,
    pub health: SubgraphHealth,
    pub fatal_error: Option<SubgraphError>,
    pub non_fatal_errors: Vec<SubgraphError>,

    /// Indexing status on different chains involved in the subgraph's data sources.
    pub chains: Vec<ChainInfo>,

    pub entity_count: u64,

    /// ID of the Graph Node that the subgraph is indexed by.
    pub node: Option<String>,
}

impl IntoValue for Info {
    fn into_value(self) -> q::Value {
        let Info {
            subgraph,
            chains,
            entity_count,
            fatal_error,
            health,
            node,
            non_fatal_errors,
            synced,
        } = self;

        fn subgraph_error_to_value(subgraph_error: SubgraphError) -> q::Value {
            let SubgraphError {
                subgraph_id,
                message,
                block_ptr,
                handler,
                deterministic,
            } = subgraph_error;

            object! {
                __typename: "SubgraphError",
                subgraphId: subgraph_id.to_string(),
                message: message,
                handler: handler,
                block: object! {
                    __typename: "Block",
                    number: block_ptr.as_ref().map(|x| x.number),
                    hash: block_ptr.map(|x| q::Value::from(Value::Bytes(x.hash.into()))),
                },
                deterministic: deterministic,
            }
        }

        let non_fatal_errors: Vec<q::Value> = non_fatal_errors
            .into_iter()
            .map(subgraph_error_to_value)
            .collect();
        let fatal_error_val = fatal_error.map_or(q::Value::Null, subgraph_error_to_value);

        object! {
            __typename: "SubgraphIndexingStatus",
            subgraph: subgraph,
            synced: synced,
            health: q::Value::from(health),
            fatalError: fatal_error_val,
            nonFatalErrors: non_fatal_errors,
            chains: chains.into_iter().map(|chain| chain.into_value()).collect::<Vec<_>>(),
            entityCount: format!("{}", entity_count),
            node: node,
        }
    }
}
