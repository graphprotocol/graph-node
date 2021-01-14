use std::collections::HashMap;

use graph::data::graphql::{IntoValue, ObjectOrInterface, ValueMap};
use graph::data::subgraph::status;
use graph::prelude::*;
use graph_graphql::prelude::{ExecutionContext, Resolver};
use std::convert::TryInto;
use web3::types::{Address, H256};

/// Resolver for the index node GraphQL API.
pub struct IndexNodeResolver<R, S> {
    logger: Logger,
    graphql_runner: Arc<R>,
    store: Arc<S>,
}

impl<R, S> IndexNodeResolver<R, S>
where
    R: GraphQlRunner,
    S: SubgraphStore,
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
        arguments: &HashMap<&String, q::Value>,
    ) -> Result<q::Value, QueryExecutionError> {
        let deployments = arguments
            .get(&String::from("subgraphs"))
            .map(|value| match value {
                q::Value::List(ids) => ids
                    .into_iter()
                    .map(|id| match id {
                        s::Value::String(s) => s.clone(),
                        _ => unreachable!(),
                    })
                    .collect(),
                _ => unreachable!(),
            })
            .unwrap_or_else(|| Vec::new());

        let infos = self
            .store
            .status(status::Filter::Deployments(deployments))?;
        Ok(infos.into_value())
    }

    fn resolve_indexing_statuses_for_subgraph_name(
        &self,
        arguments: &HashMap<&String, q::Value>,
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
            .status(status::Filter::SubgraphName(subgraph_name))?;

        Ok(infos.into_value())
    }

    fn resolve_proof_of_indexing(
        &self,
        argument_values: &HashMap<&String, q::Value>,
    ) -> Result<q::Value, QueryExecutionError> {
        let deployment_id = argument_values
            .get_required::<SubgraphDeploymentId>("subgraph")
            .expect("Valid subgraphId required");

        let block_number: u64 = argument_values
            .get_required::<u64>("blockNumber")
            .expect("Valid blockNumber required")
            .try_into()
            .unwrap();

        let block_hash = argument_values
            .get_required::<H256>("blockHash")
            .expect("Valid blockHash required")
            .try_into()
            .unwrap();

        let block = EthereumBlockPointer::from((block_hash, block_number));

        let indexer = argument_values
            .get_optional::<Address>("indexer")
            .expect("Invalid indexer");

        let poi_fut = self
            .store
            .clone()
            .get_proof_of_indexing(&deployment_id, &indexer, block);
        let poi = match futures::executor::block_on(poi_fut) {
            Ok(Some(poi)) => q::Value::String(format!("0x{}", hex::encode(&poi))),
            Ok(None) => q::Value::Null,
            Err(e) => {
                error!(
                    self.logger,
                    "Failed to query proof of indexing";
                    "subgraph" => deployment_id,
                    "block" => format!("{}", block),
                    "error" => format!("{:?}", e)
                );
                q::Value::Null
            }
        };

        Ok(poi)
    }

    fn resolve_indexing_status_for_version(
        &self,
        arguments: &HashMap<&String, q::Value>,

        // If `true` return the current version, if `false` return the pending version.
        current_version: bool,
    ) -> Result<q::Value, QueryExecutionError> {
        // We can safely unwrap because the argument is non-nullable and has been validated.
        let subgraph_name = arguments.get_required::<String>("subgraphName").unwrap();

        debug!(
            self.logger,
            "Resolve indexing status for subgraph name";
            "name" => &subgraph_name,
            "current_version" => current_version,
        );

        let infos = self.store.status(status::Filter::SubgraphVersion(
            subgraph_name,
            current_version,
        ))?;

        Ok(infos
            .into_iter()
            .next()
            .map(|info| info.into_value())
            .unwrap_or(q::Value::Null))
    }
}

impl<R, S> Clone for IndexNodeResolver<R, S>
where
    R: GraphQlRunner,
    S: SubgraphStore,
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
    S: SubgraphStore,
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
        argument_values: &HashMap<&String, q::Value>,
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
        arguments: &HashMap<&String, q::Value>,
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
        arguments: &HashMap<&String, q::Value>,
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
