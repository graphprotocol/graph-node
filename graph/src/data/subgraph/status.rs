//! Support for the indexing status API
use failure::Error;
use graphql_parser::query as q;

use super::schema::{SubgraphError, SubgraphHealth};
use crate::{
    data::graphql::ValueList,
    prelude::{web3::types::H256, BigInt, EthereumBlockPointer, Value, ValueMap},
};
use crate::{
    data::graphql::{object, IntoValue},
    prelude::TryFromValue,
};

/// Light wrapper around `EthereumBlockPointer` that is compatible with GraphQL values.
#[derive(Debug)]
pub struct EthereumBlock(EthereumBlockPointer);

impl EthereumBlock {
    pub fn new(hash: H256, number: u64) -> Self {
        EthereumBlock(EthereumBlockPointer { hash, number })
    }
}

impl From<EthereumBlock> for q::Value {
    fn from(block: EthereumBlock) -> Self {
        object! {
            __typename: "EthereumBlock",
            hash: block.0.hash_hex(),
            number: format!("{}", block.0.number),
        }
    }
}

impl IntoValue for EthereumBlock {
    fn into_value(self) -> q::Value {
        self.into()
    }
}

/// The indexing status of a subgraph on an Ethereum network (like mainnet or ropsten).
#[derive(Debug)]
pub struct EthereumInfo {
    /// The network name (e.g. `mainnet`, `ropsten`, `rinkeby`, `kovan` or `goerli`).
    network: String,
    /// The current head block of the chain.
    chain_head_block: Option<EthereumBlock>,
    /// The earliest block available for this subgraph.
    earliest_block: Option<EthereumBlock>,
    /// The latest block that the subgraph has synced to.
    latest_block: Option<EthereumBlock>,
}

/// Indexing status information for different chains (only Ethereum right now).
#[derive(Debug)]
pub enum ChainInfo {
    Ethereum(EthereumInfo),
}

impl From<ChainInfo> for q::Value {
    fn from(status: ChainInfo) -> Self {
        match status {
            ChainInfo::Ethereum(inner) => object! {
                // `__typename` is needed for the `ChainIndexingStatus` interface
                // in GraphQL to work.
                __typename: "EthereumIndexingStatus",
                network: inner.network,
                chainHeadBlock: inner.chain_head_block,
                earliestBlock: inner.earliest_block,
                latestBlock: inner.latest_block,
            },
        }
    }
}

/// The ID of a subgraph deployment assignment.
#[derive(Debug)]
pub struct DeploymentAssignment {
    /// ID of the subgraph.
    subgraph: String,
    /// ID of the Graph Node that indexes the subgraph.
    node: String,
}

impl TryFromValue for DeploymentAssignment {
    fn try_from_value(value: &q::Value) -> Result<Self, Error> {
        Ok(Self {
            subgraph: value.get_required("id")?,
            node: value.get_required("nodeId")?,
        })
    }
}

#[derive(Debug)]
pub struct Info {
    /// The subgraph ID.
    subgraph: String,

    /// Whether or not the subgraph has synced all the way to the current chain head.
    synced: bool,
    health: SubgraphHealth,
    fatal_error: Option<SubgraphError>,
    non_fatal_errors: Vec<SubgraphError>,

    /// Indexing status on different chains involved in the subgraph's data sources.
    chains: Vec<ChainInfo>,

    /// ID of the Graph Node that the subgraph is indexed by.
    node: String,
}

impl Info {
    /// Adds a Graph Node ID to the indexing status.
    pub fn with_node(self, node: String) -> Self {
        Self {
            subgraph: self.subgraph,
            synced: self.synced,
            health: self.health,
            fatal_error: self.fatal_error,
            non_fatal_errors: self.non_fatal_errors,
            chains: self.chains,
            node,
        }
    }

    /// Attempts to parse `${prefix}Hash` and `${prefix}Number` fields on a
    /// GraphQL object value into an `EthereumBlock`.
    fn block_from_value(
        value: &q::Value,
        prefix: &'static str,
    ) -> Result<Option<EthereumBlock>, Error> {
        let hash_key = format!("{}Hash", prefix);
        let number_key = format!("{}Number", prefix);

        match (
            value.get_optional::<H256>(hash_key.as_ref())?,
            value
                .get_optional::<BigInt>(number_key.as_ref())?
                .map(|n| n.to_u64()),
        ) {
            // Only return an Ethereum block if we can parse both the block hash and number
            (Some(hash), Some(number)) => Ok(Some(EthereumBlock::new(hash, number))),
            _ => Ok(None),
        }
    }
}

impl TryFromValue for Info {
    fn try_from_value(value: &q::Value) -> Result<Self, Error> {
        Ok(Self {
            subgraph: value.get_required("id")?,
            synced: value.get_required("synced")?,
            health: value.get_required("health")?,
            fatal_error: value.get_optional("fatalError")?,
            non_fatal_errors: value.get_required("nonFatalErrors")?,
            chains: vec![ChainInfo::Ethereum(EthereumInfo {
                network: value
                    .get_required::<q::Value>("manifest")?
                    .get_required::<q::Value>("dataSources")?
                    .get_values::<q::Value>()?[0]
                    .get_required("network")?,
                chain_head_block: Self::block_from_value(value, "ethereumHeadBlock")?,
                earliest_block: Self::block_from_value(value, "earliestEthereumBlock")?,
                latest_block: Self::block_from_value(value, "latestEthereumBlock")?,
            })],
            node: "THIS WILL GO AWAY WHEN WE QUERY THE DATABASE DIRECTLY".to_string(),
        })
    }
}

impl From<Info> for q::Value {
    fn from(status: Info) -> Self {
        let Info {
            subgraph,
            chains,
            fatal_error,
            health,
            node,
            non_fatal_errors,
            synced,
        } = status;

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
                    number: block_ptr.map(|x| x.number),
                    hash: block_ptr.map(|x| q::Value::from(Value::Bytes(x.hash.as_ref().into()))),
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
            chains: chains.into_iter().map(q::Value::from).collect::<Vec<_>>(),
            node: node,
        }
    }
}

pub struct Statuses(Vec<Info>);

impl Statuses {
    pub fn to_vec(self) -> Vec<Info> {
        self.0
    }
}

impl From<q::Value> for Statuses {
    fn from(data: q::Value) -> Self {
        // Extract deployment assignment IDs from the query result
        let assignments = data
            .get_required::<q::Value>("subgraphDeploymentAssignments")
            .expect("no subgraph deployment assignments in the result")
            .get_values::<DeploymentAssignment>()
            .expect("failed to parse subgraph deployment assignments");

        Statuses(
            // Parse indexing statuses from deployments
            data.get_required::<q::Value>("subgraphDeployments")
                .expect("no subgraph deployments in the result")
                .get_values()
                .expect("failed to parse subgraph deployments")
                .into_iter()
                // Filter out those deployments for which there is no active assignment
                .filter_map(|status: Info| {
                    assignments
                        .iter()
                        .find(|assignment| assignment.subgraph == status.subgraph)
                        .map(|assignment| status.with_node(assignment.node.clone()))
                })
                .collect(),
        )
    }
}

impl From<Statuses> for q::Value {
    fn from(statuses: Statuses) -> Self {
        q::Value::List(statuses.0.into_iter().map(q::Value::from).collect())
    }
}
