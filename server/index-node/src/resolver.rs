use graphql_parser::{query as q, query::Name, schema as s, schema::ObjectType};
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;

use graph::data::graphql::{TryFromValue, ValueList, ValueMap};
use graph::data::subgraph::schema::SUBGRAPHS_ID;
use graph::prelude::*;
use graph_graphql::prelude::{object_value, ObjectOrInterface, Resolver};

use web3::types::H256;

/// Resolver for the index node GraphQL API.
pub struct IndexNodeResolver<R, S> {
    logger: Logger,
    graphql_runner: Arc<R>,
    store: Arc<S>,
}

/// Light wrapper around `EthereumBlockPointer` that is compatible with GraphQL values.
struct EthereumBlock(EthereumBlockPointer);

impl From<EthereumBlock> for q::Value {
    fn from(block: EthereumBlock) -> Self {
        object_value(vec![
            ("hash", q::Value::String(block.0.hash_hex())),
            ("number", q::Value::String(format!("{}", block.0.number))),
        ])
    }
}

/// The indexing status of a subgraph on an Ethereum network (like mainnet or ropsten).
struct EthereumIndexingStatus {
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
enum ChainIndexingStatus {
    Ethereum(EthereumIndexingStatus),
}

impl From<ChainIndexingStatus> for q::Value {
    fn from(status: ChainIndexingStatus) -> Self {
        match status {
            ChainIndexingStatus::Ethereum(inner) => object_value(vec![
                // `__typename` is needed for the `ChainIndexingStatus` interface
                // in GraphQL to work.
                (
                    "__typename",
                    q::Value::String(String::from("EthereumIndexingStatus")),
                ),
                ("network", q::Value::String(inner.network)),
                (
                    "chainHeadBlock",
                    inner
                        .chain_head_block
                        .map_or(q::Value::Null, q::Value::from),
                ),
                (
                    "earliestBlock",
                    inner.earliest_block.map_or(q::Value::Null, q::Value::from),
                ),
                (
                    "latestBlock",
                    inner.latest_block.map_or(q::Value::Null, q::Value::from),
                ),
            ]),
        }
    }
}

/// The overall indexing status of a subgraph.
struct IndexingStatus {
    /// The subgraph ID.
    subgraph: String,
    /// Whether or not the subgraph has synced all the way to the current chain head.
    synced: bool,
    /// Whether or not the subgraph has failed syncing.
    failed: bool,
    /// If it has failed, an optional error.
    error: Option<String>,
    /// Indexing status on different chains involved in the subgraph's data sources.
    chains: Vec<ChainIndexingStatus>,
}

impl IndexingStatus {
    /// Attempts to parse `${prefix}Hash` and `${prefix}Number` fields on a
    /// GraphQL object value into an `EthereumBlock`.
    fn block_from_value(
        value: &q::Value,
        prefix: &'static str,
    ) -> Result<Option<EthereumBlock>, Error> {
        let hash_key = format!("{}Hash", prefix);
        let number_key = format!("{}Number", prefix);

        match (
            value
                .get_optional::<q::Value>(hash_key.as_ref())?
                .and_then(|value| match value {
                    q::Value::String(s) => Some(s),
                    _ => None,
                })
                .map(|s| H256::from_str(s.as_ref()))
                .transpose()?,
            value
                .get_optional::<q::Value>(number_key.as_ref())?
                .and_then(|value| match value {
                    q::Value::String(s) => Some(s),
                    _ => None,
                })
                .map(|s| BigInt::from_str(s.as_ref()))
                .transpose()?
                .map(|n| n.to_u64()),
        ) {
            // Only return an Ethereum block if we can parse both the block hash and number
            (Some(hash), Some(number)) => Ok(Some(EthereumBlock(EthereumBlockPointer {
                hash: hash,
                number: number,
            }))),
            _ => Ok(None),
        }
    }
}

impl TryFromValue for IndexingStatus {
    fn try_from_value(value: &q::Value) -> Result<Self, Error> {
        Ok(Self {
            subgraph: value.get_required("id")?,
            synced: value.get_required("synced")?,
            failed: value.get_required("failed")?,
            error: None,
            chains: vec![ChainIndexingStatus::Ethereum(EthereumIndexingStatus {
                network: value
                    .get_required::<q::Value>("manifest")?
                    .get_required::<q::Value>("dataSources")?
                    .get_values::<q::Value>()?[0]
                    .get_required("network")?,
                chain_head_block: Self::block_from_value(value, "ethereumHeadBlock")?,
                earliest_block: Self::block_from_value(value, "earliestEthereumBlock")?,
                latest_block: Self::block_from_value(value, "latestEthereumBlock")?,
            })],
        })
    }
}

impl From<IndexingStatus> for q::Value {
    fn from(status: IndexingStatus) -> Self {
        object_value(vec![
            (
                "__typename",
                q::Value::String(String::from("SubgraphIndexingStatus")),
            ),
            ("subgraph", q::Value::String(status.subgraph)),
            ("synced", q::Value::Boolean(status.synced)),
            ("failed", q::Value::Boolean(status.failed)),
            (
                "error",
                status.error.map_or(q::Value::Null, q::Value::String),
            ),
            (
                "chains",
                q::Value::List(status.chains.into_iter().map(q::Value::from).collect()),
            ),
        ])
    }
}

struct IndexingStatuses(Vec<IndexingStatus>);

impl From<&QueryResult> for IndexingStatuses {
    fn from(result: &QueryResult) -> Self {
        IndexingStatuses(result.data.as_ref().map_or(vec![], |value| {
            value
                .get_required::<q::Value>("subgraphDeployments")
                .expect("no subgraph deployments in the result")
                .get_values()
                .expect("failed to parse subgraph deployments")
        }))
    }
}

impl From<IndexingStatuses> for q::Value {
    fn from(statuses: IndexingStatuses) -> Self {
        q::Value::List(statuses.0.into_iter().map(q::Value::from).collect())
    }
}

impl<R, S> IndexNodeResolver<R, S>
where
    R: GraphQlRunner,
    S: Store + SubgraphDeploymentStore,
{
    pub fn new(logger: &Logger, graphql_runner: Arc<R>, store: Arc<S>) -> Self {
        let logger = logger.new(o!("component" => "IndexNodeResolver"));
        Self {
            logger,
            graphql_runner,
            store,
        }
    }

    fn resolve_indexing_statuses(
        &self,
        arguments: &HashMap<&q::Name, q::Value>,
    ) -> Result<q::Value, QueryExecutionError> {
        // Build a query for matching subgraph deployments
        let query = Query {
            // The query is against the subgraph of subgraphs
            schema: self
                .store
                .subgraph_schema(&SUBGRAPHS_ID)
                .map_err(QueryExecutionError::StoreError)?,

            // We're querying all deployments that match the provided filter
            document: q::parse_query(
                r#"
                query deployments($where: SubgraphDeployment_filter!) {
                  subgraphDeployments(where: $where) {
                    id
                    synced
                    failed
                    ethereumHeadBlockNumber
                    ethereumHeadBlockHash
                    earliestEthereumBlockHash
                    earliestEthereumBlockNumber
                    latestEthereumBlockHash
                    latestEthereumBlockNumber
                    manifest {
                      dataSources(first: 1) {
                        network
                      }
                    }
                  }
                }
                "#,
            )
            .unwrap(),

            // If the `subgraphs` argument was provided, build a suitable `where`
            // filter to match the IDs; otherwise leave the `where` filter empty
            variables: Some(QueryVariables::new(HashMap::from_iter(
                vec![(
                    "where".into(),
                    object_value(arguments.get(&String::from("subgraphs")).map_or(
                        vec![],
                        |value| match value {
                            ids @ q::Value::List(_) => vec![("id_in", ids.clone())],
                            _ => unreachable!(),
                        },
                    )),
                )]
                .into_iter(),
            ))),
        };

        // Execute the query
        let result = self
            .graphql_runner
            .run_query_with_complexity(query, None, None, Some(std::u32::MAX))
            .wait()
            .expect("error querying subgraph deployments");

        Ok(IndexingStatuses::from(&result).into())
    }
}

impl<R, S> Clone for IndexNodeResolver<R, S>
where
    R: GraphQlRunner,
    S: Store + SubgraphDeploymentStore,
{
    fn clone(&self) -> Self {
        Self {
            logger: self.logger.clone(),
            graphql_runner: self.graphql_runner.clone(),
            store: self.store.clone(),
        }
    }
}

impl<R, S> Resolver for IndexNodeResolver<R, S>
where
    R: GraphQlRunner,
    S: Store + SubgraphDeploymentStore,
{
    fn resolve_objects(
        &self,
        parent: &Option<q::Value>,
        field: &q::Name,
        field_definition: &s::Field,
        object_type: ObjectOrInterface<'_>,
        arguments: &HashMap<&q::Name, q::Value>,
        _types_for_interface: &BTreeMap<Name, Vec<ObjectType>>,
        _max_first: u32,
    ) -> Result<q::Value, QueryExecutionError> {
        match (parent, object_type.name(), field.as_str()) {
            // The top-level `indexingStatuses` field
            (None, "SubgraphIndexingStatus", "indexingStatuses") => {
                self.resolve_indexing_statuses(arguments)
            }

            // The `chains` field of `ChainIndexingStatus` values
            (Some(status), "ChainIndexingStatus", "chains") => match status {
                q::Value::Object(map) => Ok(map
                    .get("chains")
                    .expect("subgraph indexing status without `chains`")
                    .clone()),
                _ => unreachable!(),
            },

            // Unknown fields on the `Query` type
            (None, _, name) => Err(QueryExecutionError::UnknownField(
                field_definition.position.clone(),
                "Query".into(),
                name.into(),
            )),

            // Unknown fields on any other types
            (_, type_name, name) => Err(QueryExecutionError::UnknownField(
                field_definition.position.clone(),
                type_name.into(),
                name.into(),
            )),
        }
    }

    fn resolve_object(
        &self,
        parent: &Option<q::Value>,
        field: &q::Field,
        field_definition: &s::Field,
        object_type: ObjectOrInterface<'_>,
        _arguments: &HashMap<&q::Name, q::Value>,
        _types_for_interface: &BTreeMap<Name, Vec<ObjectType>>,
    ) -> Result<q::Value, QueryExecutionError> {
        match (parent, object_type.name(), field.name.as_str()) {
            (Some(status), "EthereumBlock", "chainHeadBlock") => Ok(status
                .get_optional("chainHeadBlock")
                .map_err(|e| QueryExecutionError::StoreError(e))?
                .unwrap_or(q::Value::Null)),
            (Some(status), "EthereumBlock", "earliestBlock") => Ok(status
                .get_optional("earliestBlock")
                .map_err(|e| QueryExecutionError::StoreError(e))?
                .unwrap_or(q::Value::Null)),
            (Some(status), "EthereumBlock", "latestBlock") => Ok(status
                .get_optional("latestBlock")
                .map_err(|e| QueryExecutionError::StoreError(e))?
                .unwrap_or(q::Value::Null)),

            // Unknown fields on other types
            (_, type_name, name) => Err(QueryExecutionError::UnknownField(
                field_definition.position.clone(),
                type_name.into(),
                name.into(),
            )),
        }
    }
}
