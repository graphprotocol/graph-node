use graphql_parser::{query as q, schema as s};
use std::collections::HashMap;

use graph::data::graphql::{object, ObjectOrInterface, ValueList, ValueMap};
use graph::data::subgraph::schema::SUBGRAPHS_ID;
use graph::data::subgraph::status;
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

        Ok(status::Infos::from(data).into())
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

        let infos = self
            .store
            .cheap_clone()
            .query_store(false)
            .status(status::Filter::SubgraphName(subgraph_name))?;

        Ok(status::Infos::from(infos).into())
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

        Ok(status::Infos::from(transformed_data)
            .to_vec()
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
