use graphql_parser::{query as q, schema as s};
use std::collections::HashMap;

use graph::data::graphql::{
    object, IntoValue, ObjectOrInterface, TryFromValue, ValueList, ValueMap,
};
use graph::data::subgraph::schema::{SubgraphError, SubgraphHealth, SUBGRAPHS_ID};
use graph::prelude::*;
use graph_graphql::prelude::{ExecutionContext, Resolver};
use std::convert::TryInto;
use web3::types::{Address, H256};

static DEPLOYMENT_STATUS_FRAGMENT: &str = r#"
    fragment deploymentStatus on SubgraphDeploymentDetail {
        id
        synced
        health
        fatalError {
            subgraphId
            message
            blockNumber
            blockHash
            handler
            deterministic
        }
        nonFatalErrors(first: 1000, orderBy: blockNumber) {
            subgraphId
            message
            blockNumber
            blockHash
            handler
            deterministic
        }
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
  "#;

/// Resolver for the index node GraphQL API.
pub struct IndexNodeResolver<R, S> {
    logger: Logger,
    graphql_runner: Arc<R>,
    store: Arc<S>,
}

/// The ID of a subgraph deployment assignment.
#[derive(Debug)]
struct DeploymentAssignment {
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

/// Light wrapper around `EthereumBlockPointer` that is compatible with GraphQL values.
#[derive(Debug)]
struct EthereumBlock(EthereumBlockPointer);

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
#[derive(Debug)]
enum ChainIndexingStatus {
    Ethereum(EthereumIndexingStatus),
}

impl From<ChainIndexingStatus> for q::Value {
    fn from(status: ChainIndexingStatus) -> Self {
        match status {
            ChainIndexingStatus::Ethereum(inner) => object! {
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

/// The overall indexing status of a subgraph.
#[derive(Debug)]
struct IndexingStatusWithoutNode {
    /// The subgraph ID.
    subgraph: String,

    /// Whether or not the subgraph has synced all the way to the current chain head.
    synced: bool,
    health: SubgraphHealth,
    fatal_error: Option<SubgraphError>,
    non_fatal_errors: Vec<SubgraphError>,

    /// Indexing status on different chains involved in the subgraph's data sources.
    chains: Vec<ChainIndexingStatus>,
}

#[derive(Debug)]
struct IndexingStatus {
    /// The subgraph ID.
    subgraph: String,

    /// Whether or not the subgraph has synced all the way to the current chain head.
    synced: bool,
    health: SubgraphHealth,
    fatal_error: Option<SubgraphError>,
    non_fatal_errors: Vec<SubgraphError>,

    /// Indexing status on different chains involved in the subgraph's data sources.
    chains: Vec<ChainIndexingStatus>,

    /// ID of the Graph Node that the subgraph is indexed by.
    node: String,
}

impl IndexingStatusWithoutNode {
    /// Adds a Graph Node ID to the indexing status.
    fn with_node(self, node: String) -> IndexingStatus {
        IndexingStatus {
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
            (Some(hash), Some(number)) => {
                Ok(Some(EthereumBlock(EthereumBlockPointer { hash, number })))
            }
            _ => Ok(None),
        }
    }
}

impl TryFromValue for IndexingStatusWithoutNode {
    fn try_from_value(value: &q::Value) -> Result<Self, Error> {
        Ok(Self {
            subgraph: value.get_required("id")?,
            synced: value.get_required("synced")?,
            health: value.get_required("health")?,
            fatal_error: value.get_optional("fatalError")?,
            non_fatal_errors: value.get_required("nonFatalErrors")?,
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
        let IndexingStatus {
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

struct IndexingStatuses(Vec<IndexingStatus>);

impl From<q::Value> for IndexingStatuses {
    fn from(data: q::Value) -> Self {
        // Extract deployment assignment IDs from the query result
        let assignments = data
            .get_required::<q::Value>("subgraphDeploymentAssignments")
            .expect("no subgraph deployment assignments in the result")
            .get_values::<DeploymentAssignment>()
            .expect("failed to parse subgraph deployment assignments");

        IndexingStatuses(
            // Parse indexing statuses from deployments
            data.get_required::<q::Value>("subgraphDeployments")
                .expect("no subgraph deployments in the result")
                .get_values()
                .expect("failed to parse subgraph deployments")
                .into_iter()
                // Filter out those deployments for which there is no active assignment
                .filter_map(|status: IndexingStatusWithoutNode| {
                    assignments
                        .iter()
                        .find(|assignment| assignment.subgraph == status.subgraph)
                        .map(|assignment| status.with_node(assignment.node.clone()))
                })
                .collect(),
        )
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
        // Extract optional "subgraphs" argument
        let subgraphs = arguments
            .get(&String::from("subgraphs"))
            .map(|value| match value {
                ids @ q::Value::List(_) => ids.clone(),
                _ => unreachable!(),
            });

        // Build a `where` filter that both subgraph deployments and subgraph deployment
        // assignments have to match
        let where_filter = match subgraphs {
            Some(ref ids) => object! { id_in: ids.clone() },
            None => object! {},
        };

        // Build a query for matching subgraph deployments
        let query = Query::new(
            // The query is against the subgraph of subgraphs
            self.store
                .api_schema(&SUBGRAPHS_ID)
                .map_err(|e| QueryExecutionError::StoreError(e.into()))?,
            // We're querying all deployments that match the provided filter
            q::parse_query(&format!(
                "{}{}",
                DEPLOYMENT_STATUS_FRAGMENT,
                r#"
                query deployments(
                  $whereDeployments: SubgraphDeployment_filter!,
                  $whereAssignments: SubgraphDeploymentAssignment_filter!
                ) {
                  subgraphDeployments: subgraphDeploymentDetails(where: $whereDeployments, first: 1000000) {
                    ...deploymentStatus
                  }
                  subgraphDeploymentAssignments(where: $whereAssignments, first: 1000000) {
                    id
                    nodeId
                  }
                }
                "#,
            ))
            .unwrap(),
            // If the `subgraphs` argument was provided, build a suitable `where`
            // filter to match the IDs; otherwise leave the `where` filter empty
            Some(QueryVariables::new(HashMap::from_iter(
                vec![
                    ("whereDeployments".into(), where_filter.clone()),
                    ("whereAssignments".into(), where_filter),
                ]
                .into_iter(),
            ))),
            None,
        );

        // Execute the query. We are in a blocking context so we may just block.
        let result = graph::block_on(self.graphql_runner.cheap_clone().run_query_with_complexity(
            query,
            DeploymentState::meta(),
            None,
            None,
            Some(std::u32::MAX),
            Some(std::u32::MAX),
            true,
        ));

        // Metadata queries are not cached.
        let result = Arc::try_unwrap(result).unwrap();

        let data = match result.to_result() {
            Err(errors) => {
                error!(
                    self.logger,
                    "Failed to query subgraph deployments";
                    "subgraphs" => format!("{:?}", subgraphs),
                    "errors" => format!("{:?}", errors)
                );
                return Ok(q::Value::List(vec![]));
            }
            Ok(None) => {
                return Ok(q::Value::List(vec![]));
            }
            Ok(Some(data)) => data,
        };

        Ok(IndexingStatuses::from(data).into())
    }

    fn resolve_indexing_statuses_for_subgraph_name(
        &self,
        arguments: &HashMap<&q::Name, q::Value>,
    ) -> Result<q::Value, QueryExecutionError> {
        // Get the subgraph name from the arguments; we can safely use `expect` here
        // because the argument will already have been validated prior to the resolver
        // being called
        let subgraph_name = arguments
            .get_required::<String>("subgraphName")
            .expect("subgraphName not provided");

        debug!(
            self.logger,
            "Resolve indexing statuses for subgraph name";
            "name" => &subgraph_name
        );

        // Build a `where` filter that the subgraph has to match
        let where_filter = object! { name: subgraph_name.clone() };

        // Build a query for matching subgraph deployments
        let query = Query::new(
            // The query is against the subgraph of subgraphs
            self.store
                .api_schema(&SUBGRAPHS_ID)
                .map_err(|e| QueryExecutionError::StoreError(e.into()))?,
            // We're querying all deployments that match the provided filter
            q::parse_query(&format!(
                "{}{}",
                DEPLOYMENT_STATUS_FRAGMENT,
                r#"
                query subgraphs($where: Subgraph_filter!) {
                  subgraphs(where: $where, first: 1000000) {
                    versions(orderBy: createdAt, orderDirection: asc, first: 1000000) {
                      deployment {
                       ...deploymentStatus
                      }
                    }
                  }
                  subgraphDeploymentAssignments(first: 1000000) {
                    id
                    nodeId
                  }
                }
                "#,
            ))
            .unwrap(),
            // If the `subgraphs` argument was provided, build a suitable `where`
            // filter to match the IDs; otherwise leave the `where` filter empty
            Some(QueryVariables::new(HashMap::from_iter(
                vec![("where".into(), where_filter)].into_iter(),
            ))),
            None,
        );

        // Execute the query. We are in a blocking context so we may just block.
        let result = graph::block_on(self.graphql_runner.cheap_clone().run_query_with_complexity(
            query,
            DeploymentState::meta(),
            None,
            None,
            Some(std::u32::MAX),
            Some(std::u32::MAX),
            true,
        ));

        // Metadata queries are not cached.
        let result = Arc::try_unwrap(result).unwrap();

        let data = match result.to_result() {
            Err(errors) => {
                error!(
                    self.logger,
                    "Failed to query subgraph deployments";
                    "subgraph" => subgraph_name,
                    "errors" => format!("{:?}", errors)
                );
                return Ok(q::Value::List(vec![]));
            }
            Ok(None) => {
                return Ok(q::Value::List(vec![]));
            }
            Ok(Some(data)) => data,
        };

        let subgraphs = match data
            .get_optional::<q::Value>("subgraphs")
            .expect("invalid subgraphs")
        {
            Some(subgraphs) => subgraphs,
            None => return Ok(q::Value::List(vec![])),
        };

        let subgraphs = subgraphs
            .get_values::<q::Value>()
            .expect("invalid subgraph values");

        let subgraph = if subgraphs.len() > 0 {
            subgraphs[0].clone()
        } else {
            return Ok(q::Value::List(vec![]));
        };

        let deployments = subgraph
            .get_required::<q::Value>("versions")
            .expect("missing subgraph versions")
            .get_values::<q::Value>()
            .expect("invalid subgraph versions")
            .into_iter()
            .map(|version| {
                version
                    .get_required::<q::Value>("deployment")
                    .expect("missing deployment")
            })
            .collect::<Vec<_>>();

        let transformed_data = object! {
            subgraphDeployments: deployments,
            subgraphDeploymentAssignments:
                data.get_required::<q::Value>("subgraphDeploymentAssignments")
                    .expect("missing deployment assignments"),
        };

        Ok(IndexingStatuses::from(transformed_data).into())
    }

    fn resolve_proof_of_indexing(
        &self,
        argument_values: &HashMap<&q::Name, q::Value>,
    ) -> Result<q::Value, QueryExecutionError> {
        let deployment_id = argument_values
            .get_required::<SubgraphDeploymentId>("subgraph")
            .expect("Valid subgraphId required");

        let block_hash = argument_values
            .get_required::<H256>("blockHash")
            .expect("Valid blockHash required")
            .try_into()
            .unwrap();

        let indexer = argument_values
            .get_optional::<Address>("indexer")
            .expect("Invalid indexer");

        let poi_fut = self
            .store
            .get_proof_of_indexing(&deployment_id, &indexer, block_hash);
        let poi = match futures::executor::block_on(poi_fut) {
            Ok(Some(poi)) => q::Value::String(format!("0x{}", hex::encode(&poi))),
            Ok(None) => q::Value::Null,
            Err(e) => {
                error!(
                    self.logger,
                    "Failed to query proof of indexing";
                    "subgraph" => deployment_id,
                    "error" => format!("{:?}", e)
                );
                q::Value::Null
            }
        };

        Ok(poi)
    }

    fn resolve_indexing_status_for_version(
        &self,
        arguments: &HashMap<&q::Name, q::Value>,

        // If `true` return the current version, if `false` return the pending version.
        current_version: bool,
    ) -> Result<q::Value, QueryExecutionError> {
        // We can safely unwrap because the argument is non-nullable and has been validated.
        let subgraph_name = arguments.get_required::<String>("subgraphName").unwrap();

        debug!(
            self.logger,
            "Resolve indexing statuses for subgraph name";
            "name" => &subgraph_name
        );

        // Build a `where` filter that the subgraph has to match
        let where_filter = object!(name: subgraph_name.clone());

        let query = Query::new(
            self.store
                .api_schema(&SUBGRAPHS_ID)
                .map_err(|e| QueryExecutionError::StoreError(e.into()))?,
            q::parse_query(&format!(
                "{}{}",
                DEPLOYMENT_STATUS_FRAGMENT,
                r#"
                query subgraphs($where: Subgraph_filter!, $currentVersion: Boolean!) {
                  subgraphs(where: $where, first: 1) {
                    currentVersion @include(if: $currentVersion) {
                        deployment {
                            ...deploymentStatus
                        }
                    }

                    pendingVersion @skip(if: $currentVersion) {
                        deployment {
                            ...deploymentStatus
                        }
                    }
                  }

                  subgraphDeploymentAssignments(first: 1000000) {
                    id
                    nodeId
                  }
                }
                "#,
            ))
            .unwrap(),
            Some(QueryVariables::new(HashMap::from_iter(
                vec![
                    ("where".into(), where_filter),
                    ("currentVersion".into(), q::Value::Boolean(current_version)),
                ]
                .into_iter(),
            ))),
            None,
        );

        // Execute the query. We are in a blocking context so we may just block.
        let result = graph::block_on(self.graphql_runner.cheap_clone().run_query_with_complexity(
            query,
            DeploymentState::meta(),
            None,
            None,
            Some(std::u32::MAX),
            Some(std::u32::MAX),
            true,
        ));

        // Metadata queries are not cached.
        let result = Arc::try_unwrap(result).unwrap();

        let data = match result.to_result() {
            Err(errors) => {
                error!(
                    self.logger,
                    "Failed to query subgraph deployments";
                    "subgraph" => subgraph_name,
                    "errors" => format!("{:?}", errors)
                );
                return Ok(q::Value::Null);
            }
            Ok(None) => return Ok(q::Value::Null),
            Ok(Some(data)) => data,
        };

        let subgraphs = match data
            .get_optional::<q::Value>("subgraphs")
            .expect("invalid subgraphs")
        {
            Some(subgraphs) => subgraphs,
            None => return Ok(q::Value::Null),
        };

        let subgraphs = subgraphs
            .get_values::<q::Value>()
            .expect("invalid subgraph values");

        let subgraph = match subgraphs.into_iter().next() {
            Some(subgraph) => subgraph,
            None => return Ok(q::Value::Null),
        };

        let field_name = match current_version {
            true => "currentVersion",
            false => "pendingVersion",
        };

        let deployments = subgraph
            .get_optional::<q::Value>(field_name)
            .unwrap()
            .map(|version| {
                q::Value::List(vec![version
                    .get_required::<q::Value>("deployment")
                    .expect("missing deployment")])
            })
            .unwrap_or(q::Value::List(vec![]));

        let transformed_data = object!(
            subgraphDeployments: deployments,
            subgraphDeploymentAssignments:
                data.get_required::<q::Value>("subgraphDeploymentAssignments")
                    .expect("missing deployment assignments"),
        );

        Ok(IndexingStatuses::from(transformed_data)
            .0
            .into_iter()
            .next()
            .map(Into::into)
            .unwrap_or(q::Value::Null))
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
    const CACHEABLE: bool = false;

    fn prefetch(
        &self,
        _: &ExecutionContext<Self>,
        _: &q::SelectionSet,
    ) -> Result<Option<q::Value>, Vec<QueryExecutionError>> {
        Ok(None)
    }

    /// Resolves a scalar value for a given scalar type.
    fn resolve_scalar_value(
        &self,
        parent_object_type: &s::ObjectType,
        field: &q::Field,
        scalar_type: &s::ScalarType,
        value: Option<q::Value>,
        argument_values: &HashMap<&q::Name, q::Value>,
    ) -> Result<q::Value, QueryExecutionError> {
        // Check if we are resolving the proofOfIndexing bytes
        if &parent_object_type.name == "Query"
            && &field.name == "proofOfIndexing"
            && &scalar_type.name == "Bytes"
        {
            return self.resolve_proof_of_indexing(argument_values);
        }

        // Fallback to the same as is in the default trait implementation. There
        // is no way to call back into the default implementation for the trait.
        // So, note that this is duplicated.
        // See also c2112309-44fd-4a84-92a0-5a651e6ed548
        Ok(value.unwrap_or(q::Value::Null))
    }

    fn resolve_objects(
        &self,
        prefetched_objects: Option<q::Value>,
        field: &q::Field,
        _field_definition: &s::Field,
        object_type: ObjectOrInterface<'_>,
        arguments: &HashMap<&q::Name, q::Value>,
    ) -> Result<q::Value, QueryExecutionError> {
        match (prefetched_objects, object_type.name(), field.name.as_str()) {
            // The top-level `indexingStatuses` field
            (None, "SubgraphIndexingStatus", "indexingStatuses") => {
                self.resolve_indexing_statuses(arguments)
            }

            // The top-level `indexingStatusesForSubgraphName` field
            (None, "SubgraphIndexingStatus", "indexingStatusesForSubgraphName") => {
                self.resolve_indexing_statuses_for_subgraph_name(arguments)
            }

            // Resolve fields of `Object` values (e.g. the `chains` field of `ChainIndexingStatus`)
            (value, _, _) => Ok(value.unwrap_or(q::Value::Null)),
        }
    }

    fn resolve_object(
        &self,
        prefetched_object: Option<q::Value>,
        field: &q::Field,
        _field_definition: &s::Field,
        _object_type: ObjectOrInterface<'_>,
        arguments: &HashMap<&q::Name, q::Value>,
    ) -> Result<q::Value, QueryExecutionError> {
        match (prefetched_object, field.name.as_str()) {
            // The top-level `indexingStatusForCurrentVersion` field
            (None, "indexingStatusForCurrentVersion") => {
                self.resolve_indexing_status_for_version(arguments, true)
            }

            // The top-level `indexingStatusForPendingVersion` field
            (None, "indexingStatusForPendingVersion") => {
                self.resolve_indexing_status_for_version(arguments, false)
            }

            // Resolve fields of `Object` values (e.g. the `latestBlock` field of `EthereumBlock`)
            (value, _) => Ok(value.unwrap_or(q::Value::Null)),
        }
    }
}
