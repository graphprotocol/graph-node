use graphql_parser::{query as q, schema as s};
use std::collections::HashMap;
use std::result;
use std::sync::Arc;

use graph::components::store::*;
use graph::prelude::*;

use crate::prelude::*;
use crate::query::ext::BlockConstraint;
use crate::schema::ast as sast;

use crate::store::query::{collect_entities_from_query_field, parse_subgraph_id};

/// A resolver that fetches entities from a `Store`.
#[derive(Clone)]
pub struct StoreResolver {
    logger: Logger,
    pub(crate) store: Arc<dyn QueryStore>,
    pub(crate) block: BlockNumber,
}

impl CheapClone for StoreResolver {}

impl StoreResolver {
    /// Create a resolver that looks up entities at whatever block is the
    /// latest when the query is run. That means that multiple calls to find
    /// entities into this resolver might return entities from different
    /// blocks
    pub fn for_subscription(logger: &Logger, store: Arc<impl Store>) -> Self {
        StoreResolver {
            logger: logger.new(o!("component" => "StoreResolver")),
            store: store.query_store(true),
            block: BLOCK_NUMBER_MAX,
        }
    }

    /// Create a resolver that looks up entities at the block specified
    /// by `bc`. Any calls to find objects will always return entities as
    /// of that block. Note that if `bc` is `BlockConstraint::Latest` we use
    /// whatever the latest block for the subgraph was when the resolver was
    /// created
    pub async fn at_block(
        logger: &Logger,
        store: Arc<impl Store + SubgraphDeploymentStore>,
        bc: BlockConstraint,
        subgraph: SubgraphDeploymentId,
    ) -> Result<(Self, EthereumBlockPointer), QueryExecutionError> {
        let store_clone = store.cheap_clone();
        let block_ptr = graph::spawn_blocking_allow_panic(move || {
            Self::locate_block(store_clone.as_ref(), bc, subgraph)
        })
        .await
        .map_err(|e| QueryExecutionError::Panic(e.to_string()))
        .and_then(|x| x)?; // Propagate panics.
        let resolver = StoreResolver {
            logger: logger.new(o!("component" => "StoreResolver")),
            store: store.query_store(false),
            block: block_ptr.number as i32,
        };
        Ok((resolver, block_ptr))
    }

    fn locate_block(
        store: &(impl Store + SubgraphDeploymentStore),
        bc: BlockConstraint,
        subgraph: SubgraphDeploymentId,
    ) -> Result<EthereumBlockPointer, QueryExecutionError> {
        if store
            .uses_relational_schema(&subgraph)
            .map_err(StoreError::from)?
            && !subgraph.is_meta()
        {
            // Relational storage (most subgraphs); block constraints fully
            // supported
            match bc {
                BlockConstraint::Number(number) => store
                    .block_ptr(subgraph.clone())
                    .map_err(|e| StoreError::from(e).into())
                    .and_then(|ptr| {
                        let ptr =
                            ptr.expect("we should have already checked that the subgraph exists");
                        if ptr.number < number as u64 {
                            Err(QueryExecutionError::ValueParseError(
                                "block.number".to_owned(),
                                format!(
                                    "subgraph {} has only indexed up to block number {} \
                                 and data for block number {} is therefore not yet available",
                                    subgraph, ptr.number, number
                                ),
                            ))
                        } else {
                            // We don't have a way here to look the block hash up from
                            // the database, and even if we did, there is no guarantee
                            // that we have the block in our cache. We therefore
                            // always return an all zeroes hash when users specify
                            // a block number
                            Ok(EthereumBlockPointer::from((
                                web3::types::H256::zero(),
                                number as u64,
                            )))
                        }
                    }),
                BlockConstraint::Hash(hash) => store
                    .block_number(&subgraph, hash)
                    .map_err(|e| e.into())
                    .and_then(|number| {
                        number
                            .ok_or_else(|| {
                                QueryExecutionError::ValueParseError(
                                    "block.hash".to_owned(),
                                    "no block with that hash found".to_owned(),
                                )
                            })
                            .map(|number| EthereumBlockPointer::from((hash, number as u64)))
                    }),
                BlockConstraint::Latest => store
                    .block_ptr(subgraph.clone())
                    .map_err(|e| StoreError::from(e).into())
                    .and_then(|ptr| {
                        let ptr =
                            ptr.expect("we should have already checked that the subgraph exists");
                        Ok(ptr)
                    }),
            }
        } else {
            // JSONB storage or subgraph metadata; only allow BlockConstraint::Latest
            if matches!(bc, BlockConstraint::Latest) {
                Ok(EthereumBlockPointer::from((
                    web3::types::H256::zero(),
                    BLOCK_NUMBER_MAX as u64,
                )))
            } else {
                Err(QueryExecutionError::NotSupported(
                    "This subgraph uses JSONB storage, which does not \
            support querying at a specific block height. Redeploy \
            a new version of this subgraph to enable this feature."
                        .to_owned(),
                ))
            }
        }
    }
}

impl Resolver for StoreResolver {
    const CACHEABLE: bool = true;

    fn prefetch(
        &self,
        ctx: &ExecutionContext<Self>,
        selection_set: &q::SelectionSet,
    ) -> Result<Option<q::Value>, Vec<QueryExecutionError>> {
        super::prefetch::run(&self, ctx, selection_set).map(|value| Some(value))
    }

    fn resolve_objects(
        &self,
        prefetched_objects: Option<q::Value>,
        field: &q::Field,
        _field_definition: &s::Field,
        object_type: ObjectOrInterface<'_>,
        _arguments: &HashMap<&q::Name, q::Value>,
    ) -> Result<q::Value, QueryExecutionError> {
        if let Some(child) = prefetched_objects {
            Ok(child)
        } else {
            Err(QueryExecutionError::ResolveEntitiesError(format!(
                "internal error resolving {}.{}: \
                 expected prefetched result, but found nothing",
                object_type.name(),
                &field.name,
            )))
        }
    }

    fn resolve_object(
        &self,
        prefetched_object: Option<q::Value>,
        field: &q::Field,
        field_definition: &s::Field,
        object_type: ObjectOrInterface<'_>,
        _arguments: &HashMap<&q::Name, q::Value>,
    ) -> Result<q::Value, QueryExecutionError> {
        if let Some(q::Value::List(children)) = prefetched_object {
            if children.len() > 1 {
                let derived_from_field =
                    sast::get_derived_from_field(object_type, field_definition)
                        .expect("only derived fields can lead to multiple children here");

                return Err(QueryExecutionError::AmbiguousDerivedFromResult(
                    field.position.clone(),
                    field.name.to_owned(),
                    object_type.name().to_owned(),
                    derived_from_field.name.to_owned(),
                ));
            } else {
                return Ok(children.into_iter().next().unwrap_or(q::Value::Null));
            }
        } else {
            return Err(QueryExecutionError::ResolveEntitiesError(format!(
                "internal error resolving {}.{}: \
                 expected prefetched result, but found nothing",
                object_type.name(),
                &field.name,
            )));
        }
    }

    fn resolve_field_stream<'a, 'b>(
        &self,
        schema: &'a s::Document,
        object_type: &'a s::ObjectType,
        field: &'b q::Field,
    ) -> result::Result<StoreEventStreamBox, QueryExecutionError> {
        // Fail if the field does not exist on the object type
        if sast::get_field(object_type, &field.name).is_none() {
            return Err(QueryExecutionError::UnknownField(
                field.position,
                object_type.name.clone(),
                field.name.clone(),
            ));
        }

        // Collect all entities involved in the query field
        let entities = collect_entities_from_query_field(schema, object_type, field);

        // Subscribe to the store and return the entity change stream
        let deployment_id = parse_subgraph_id(object_type)?;
        Ok(self.store.subscribe(entities).throttle_while_syncing(
            &self.logger,
            self.store.clone(),
            deployment_id,
            *SUBSCRIPTION_THROTTLE_INTERVAL,
        ))
    }
}
