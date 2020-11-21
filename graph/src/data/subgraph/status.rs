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

pub enum Filter {
    SubgraphName(String),
}

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

impl From<ChainInfo> for q::Value {
    fn from(info: ChainInfo) -> Self {
        let ChainInfo {
            network,
            chain_head_block,
            earliest_block,
            latest_block,
        } = info;
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
    pub subgraph: String,

    /// Whether or not the subgraph has synced all the way to the current chain head.
    pub synced: bool,
    pub health: SubgraphHealth,
    pub fatal_error: Option<SubgraphError>,
    pub non_fatal_errors: Vec<SubgraphError>,

    /// Indexing status on different chains involved in the subgraph's data sources.
    pub chains: Vec<ChainInfo>,

    /// ID of the Graph Node that the subgraph is indexed by.
    pub node: Option<String>,
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
            node: Some(node),
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
            chains: vec![ChainInfo {
                network: value
                    .get_required::<q::Value>("manifest")?
                    .get_required::<q::Value>("dataSources")?
                    .get_values::<q::Value>()?[0]
                    .get_required("network")?,
                chain_head_block: Self::block_from_value(value, "ethereumHeadBlock")?,
                earliest_block: Self::block_from_value(value, "earliestEthereumBlock")?,
                latest_block: Self::block_from_value(value, "latestEthereumBlock")?,
            }],
            node: None,
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

pub struct Infos(Vec<Info>);

impl Infos {
    pub fn to_vec(self) -> Vec<Info> {
        self.0
    }
}

impl From<Vec<Info>> for Infos {
    fn from(infos: Vec<Info>) -> Self {
        Infos(infos)
    }
}

impl From<q::Value> for Infos {
    fn from(data: q::Value) -> Self {
        // Extract deployment assignment IDs from the query result
        let assignments = data
            .get_required::<q::Value>("subgraphDeploymentAssignments")
            .expect("no subgraph deployment assignments in the result")
            .get_values::<DeploymentAssignment>()
            .expect("failed to parse subgraph deployment assignments");

        Infos(
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

impl From<Infos> for q::Value {
    fn from(infos: Infos) -> Self {
        q::Value::List(infos.0.into_iter().map(q::Value::from).collect())
    }
}
