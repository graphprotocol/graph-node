//! Support for the indexing status API

use super::schema::{SubgraphError, SubgraphHealth};
use crate::blockchain::BlockHash;
use crate::components::store::{BlockNumber, DeploymentId};
use crate::data::graphql::{object, IntoValue};
use crate::prelude::{r, BlockPtr, Value};

pub enum Filter {
    /// Get all versions for the named subgraph
    SubgraphName(String),
    /// Get the current (`true`) or pending (`false`) version of the named
    /// subgraph
    SubgraphVersion(String, bool),
    /// Get the status of all deployments whose the given given IPFS hashes
    Deployments(Vec<String>),
    /// Get the status of all deployments with the given ids
    DeploymentIds(Vec<DeploymentId>),
}

/// Light wrapper around `EthereumBlockPointer` that is compatible with GraphQL values.
#[derive(Clone, Debug)]
pub struct EthereumBlock(BlockPtr);

impl EthereumBlock {
    pub fn new(hash: BlockHash, number: BlockNumber) -> Self {
        EthereumBlock(BlockPtr::new(hash, number))
    }

    pub fn to_ptr(self) -> BlockPtr {
        self.0
    }

    pub fn number(&self) -> i32 {
        self.0.number
    }
}

impl IntoValue for EthereumBlock {
    fn into_value(self) -> r::Value {
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
#[derive(Clone, Debug)]
pub struct ChainInfo {
    /// The network name (e.g. `mainnet`, `ropsten`, `rinkeby`, `kovan` or `goerli`).
    pub network: String,
    /// The current head block of the chain.
    pub chain_head_block: Option<EthereumBlock>,
    /// The earliest block available for this subgraph (only the number).
    pub earliest_block_number: BlockNumber,
    /// The latest block that the subgraph has synced to.
    pub latest_block: Option<EthereumBlock>,
}

impl IntoValue for ChainInfo {
    fn into_value(self) -> r::Value {
        let ChainInfo {
            network,
            chain_head_block,
            earliest_block_number,
            latest_block,
        } = self;
        object! {
            // `__typename` is needed for the `ChainIndexingStatus` interface
            // in GraphQL to work.
            __typename: "EthereumIndexingStatus",
            network: network,
            chainHeadBlock: chain_head_block,
            earliestBlock: object! {
                __typename: "EarliestBlock",
                number: earliest_block_number,
                hash: "0x0"
            },
            latestBlock: latest_block,
        }
    }
}

#[derive(Debug, Default)]
pub struct SubgraphSize {
    pub row_estimate: i64,
    pub table_bytes: i64,
    pub index_bytes: i64,
    pub toast_bytes: i64,
    pub total_bytes: i64,
}

impl IntoValue for SubgraphSize {
    fn into_value(self) -> r::Value {
        let SubgraphSize {
            row_estimate,
            table_bytes,
            index_bytes,
            toast_bytes,
            total_bytes,
        } = self;
        object! {
            __typename: "SubgraphSize",
            rowEstimate: format!("{}", row_estimate),
            tableSize: format!("{}", table_bytes),
            indexSize: format!("{}", index_bytes),
            toastSize: format!("{}", toast_bytes),
            totalSize: format!("{}", total_bytes),
        }
    }
}

#[derive(Debug)]
pub struct Info {
    pub id: DeploymentId,

    /// The deployment hash
    pub subgraph: String,

    /// Whether or not the subgraph has synced all the way to the current chain head.
    pub synced: bool,
    pub health: SubgraphHealth,
    pub fatal_error: Option<SubgraphError>,
    pub non_fatal_errors: Vec<SubgraphError>,
    pub paused: Option<bool>,

    /// Indexing status on different chains involved in the subgraph's data sources.
    pub chains: Vec<ChainInfo>,

    pub entity_count: u64,

    /// ID of the Graph Node that the subgraph is indexed by.
    pub node: Option<String>,

    pub history_blocks: i32,

    /// The size of all entity tables in a deployment's namespace in bytes.
    pub subgraph_size: SubgraphSize,
}

impl IntoValue for Info {
    fn into_value(self) -> r::Value {
        let Info {
            id: _,
            subgraph,
            chains,
            entity_count,
            fatal_error,
            health,
            paused,
            node,
            non_fatal_errors,
            synced,
            history_blocks,
            subgraph_size,
        } = self;

        let non_fatal_errors: Vec<_> = non_fatal_errors
            .into_iter()
            .map(subgraph_error_to_value)
            .collect();
        let fatal_error_val = fatal_error.map_or(r::Value::Null, subgraph_error_to_value);

        object! {
            __typename: "SubgraphIndexingStatus",
            subgraph: subgraph,
            synced: synced,
            health: r::Value::from(health),
            paused: paused,
            fatalError: fatal_error_val,
            nonFatalErrors: non_fatal_errors,
            chains: chains.into_iter().map(|chain| chain.into_value()).collect::<Vec<_>>(),
            entityCount: format!("{}", entity_count),
            node: node,
            historyBlocks: history_blocks,
            subgraphSize: subgraph_size.into_value(),
        }
    }
}

fn subgraph_error_to_value(subgraph_error: SubgraphError) -> r::Value {
    let SubgraphError {
        subgraph_id,
        message,
        block_ptr,
        handler,
        deterministic,
    } = subgraph_error;

    let block_value = block_ptr
        .map(|ptr| {
            object! {
                __typename: "Block",
                number: ptr.number,
                hash: r::Value::from(Value::Bytes(ptr.hash.into())),
            }
        })
        .unwrap_or(r::Value::Null);

    object! {
        __typename: "SubgraphError",
        subgraphId: subgraph_id.to_string(),
        message: message,
        handler: handler,
        block: block_value,
        deterministic: deterministic,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::DeploymentHash;
    use web3::types::H256;

    #[test]
    fn subgraph_error_block_is_null_without_pointer() {
        let deployment =
            DeploymentHash::new("QmXoypizjW3WknFiJnKLwHCnL72vedxjQkDDP1mXWo6uco").unwrap();
        let value = subgraph_error_to_value(SubgraphError {
            subgraph_id: deployment,
            message: "boom".to_string(),
            block_ptr: None,
            handler: None,
            deterministic: true,
        });

        match value {
            r::Value::Object(map) => {
                assert_eq!(map.get("block"), Some(&r::Value::Null));
            }
            _ => panic!("expected object"),
        }
    }

    #[test]
    fn subgraph_error_block_contains_data_when_present() {
        let deployment =
            DeploymentHash::new("QmXoypizjW3WknFiJnKLwHCnL72vedxjQkDDP1mXWo6uco").unwrap();
        let ptr = BlockPtr::new(H256::zero().into(), 42);
        let value = subgraph_error_to_value(SubgraphError {
            subgraph_id: deployment,
            message: "boom".to_string(),
            block_ptr: Some(ptr),
            handler: None,
            deterministic: true,
        });

        match value {
            r::Value::Object(map) => match map.get("block").expect("block field present") {
                r::Value::Object(block) => {
                    assert_eq!(block.get("number"), Some(&r::Value::Int(42.into())));
                }
                other => panic!("unexpected block value {other:?}"),
            },
            _ => panic!("expected object"),
        }
    }
}
