use std::collections::{BTreeMap, HashMap};
use std::result;
use std::sync::Arc;

use graph::data::{
    graphql::{object, ObjectOrInterface},
    schema::META_FIELD_TYPE,
};
use graph::prelude::*;
use graph::{components::store::*, data::schema::BLOCK_FIELD_TYPE};

use crate::query::ext::BlockConstraint;
use crate::schema::ast as sast;
use crate::{prelude::*, schema::api::ErrorPolicy};

use crate::store::query::collect_entities_from_query_field;

/// A resolver that fetches entities from a `Store`.
#[derive(Clone)]
pub struct StoreResolver {
    logger: Logger,
    pub(crate) store: Arc<dyn QueryStore>,
    subscription_manager: Arc<dyn SubscriptionManager>,
    pub(crate) block_ptr: Option<BlockPtr>,
    deployment: DeploymentHash,
    has_non_fatal_errors: bool,
    error_policy: ErrorPolicy,
}

impl CheapClone for StoreResolver {}

impl StoreResolver {
    /// Create a resolver that looks up entities at whatever block is the
    /// latest when the query is run. That means that multiple calls to find
    /// entities into this resolver might return entities from different
    /// blocks
    pub fn for_subscription(
        logger: &Logger,
        deployment: DeploymentHash,
        store: Arc<dyn QueryStore>,
        subscription_manager: Arc<dyn SubscriptionManager>,
    ) -> Self {
        StoreResolver {
            logger: logger.new(o!("component" => "StoreResolver")),
            store,
            subscription_manager,
            block_ptr: None,
            deployment,

            // Checking for non-fatal errors does not work with subscriptions.
            has_non_fatal_errors: false,
            error_policy: ErrorPolicy::Deny,
        }
    }

    /// Create a resolver that looks up entities at the block specified
    /// by `bc`. Any calls to find objects will always return entities as
    /// of that block. Note that if `bc` is `BlockConstraint::Latest` we use
    /// whatever the latest block for the subgraph was when the resolver was
    /// created
    pub async fn at_block(
        logger: &Logger,
        store: Arc<dyn QueryStore>,
        subscription_manager: Arc<dyn SubscriptionManager>,
        bc: BlockConstraint,
        error_policy: ErrorPolicy,
        deployment: DeploymentHash,
    ) -> Result<Self, QueryExecutionError> {
        let store_clone = store.cheap_clone();
        let deployment2 = deployment.clone();
        let block_ptr = graph::spawn_blocking_allow_panic(move || {
            Self::locate_block(store_clone.as_ref(), bc, deployment2.clone())
        })
        .await
        .map_err(|e| QueryExecutionError::Panic(e.to_string()))
        .and_then(|x| x)?; // Propagate panics.

        let has_non_fatal_errors = store
            .has_non_fatal_errors(Some(block_ptr.block_number()))
            .await?;

        let resolver = StoreResolver {
            logger: logger.new(o!("component" => "StoreResolver")),
            store,
            subscription_manager,
            block_ptr: Some(block_ptr),
            deployment,
            has_non_fatal_errors,
            error_policy,
        };
        Ok(resolver)
    }

    pub fn block_number(&self) -> BlockNumber {
        self.block_ptr
            .as_ref()
            .map(|ptr| ptr.number as BlockNumber)
            .unwrap_or(BLOCK_NUMBER_MAX)
    }

    fn locate_block(
        store: &dyn QueryStore,
        bc: BlockConstraint,
        subgraph: DeploymentHash,
    ) -> Result<BlockPtr, QueryExecutionError> {
        match bc {
            BlockConstraint::Number(number) => store
                .block_ptr()
                .map_err(|e| StoreError::from(e).into())
                .and_then(|ptr| {
                    let ptr = ptr.expect("we should have already checked that the subgraph exists");
                    if ptr.number < number {
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
                        // See 7a7b9708-adb7-4fc2-acec-88680cb07ec1
                        Ok(BlockPtr::from((web3::types::H256::zero(), number as u64)))
                    }
                }),
            BlockConstraint::Hash(hash) => {
                store
                    .block_number(hash)
                    .map_err(Into::into)
                    .and_then(|number| {
                        number
                            .ok_or_else(|| {
                                QueryExecutionError::ValueParseError(
                                    "block.hash".to_owned(),
                                    "no block with that hash found".to_owned(),
                                )
                            })
                            .map(|number| BlockPtr::from((hash, number as u64)))
                    })
            }
            BlockConstraint::Latest => store
                .block_ptr()
                .map_err(|e| StoreError::from(e).into())
                .and_then(|ptr| {
                    let ptr = ptr.expect("we should have already checked that the subgraph exists");
                    Ok(ptr)
                }),
        }
    }

    fn handle_meta(
        &self,
        prefetched_object: Option<q::Value>,
        object_type: &ObjectOrInterface<'_>,
    ) -> Result<(Option<q::Value>, Option<q::Value>), QueryExecutionError> {
        // Pretend that the whole `_meta` field was loaded by prefetch. Eager
        // loading this is ok until we add more information to this field
        // that would force us to query the database; when that happens, we
        // need to switch to loading on demand
        if object_type.is_meta() {
            let hash = self
                .block_ptr
                .as_ref()
                .and_then(|ptr| {
                    // locate_block indicates that we do not have a block hash
                    // by setting the hash to `zero`
                    // See 7a7b9708-adb7-4fc2-acec-88680cb07ec1
                    let hash_h256 = ptr.hash_as_h256();
                    if hash_h256 == web3::types::H256::zero() {
                        None
                    } else {
                        Some(q::Value::String(format!("0x{:x}", hash_h256)))
                    }
                })
                .unwrap_or(q::Value::Null);
            let number = self
                .block_ptr
                .as_ref()
                .map(|ptr| q::Value::Int((ptr.number as i32).into()))
                .unwrap_or(q::Value::Null);
            let mut map = BTreeMap::new();
            let block = object! {
                hash: hash,
                number: number,
                __typename: BLOCK_FIELD_TYPE
            };
            map.insert("prefetch:block".to_string(), q::Value::List(vec![block]));
            map.insert(
                "deployment".to_string(),
                q::Value::String(self.deployment.to_string()),
            );
            map.insert(
                "hasIndexingErrors".to_string(),
                q::Value::Boolean(self.has_non_fatal_errors),
            );
            map.insert(
                "__typename".to_string(),
                q::Value::String(META_FIELD_TYPE.to_string()),
            );
            return Ok((None, Some(q::Value::Object(map))));
        }
        return Ok((prefetched_object, None));
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
        _arguments: &HashMap<&str, q::Value>,
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
        _arguments: &HashMap<&str, q::Value>,
    ) -> Result<q::Value, QueryExecutionError> {
        let (prefetched_object, meta) = self.handle_meta(prefetched_object, &object_type)?;
        if let Some(meta) = meta {
            return Ok(meta);
        }
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
        // Collect all entities involved in the query field
        let entities = collect_entities_from_query_field(schema, object_type, field);

        // Subscribe to the store and return the entity change stream
        Ok(self
            .subscription_manager
            .subscribe(entities)
            .throttle_while_syncing(
                &self.logger,
                self.store.clone(),
                *SUBSCRIPTION_THROTTLE_INTERVAL,
            ))
    }

    fn post_process(&self, result: &mut QueryResult) -> Result<(), anyhow::Error> {
        // Post-processing is only necessary for queries with indexing errors, and no query errors.
        if !self.has_non_fatal_errors || result.has_errors() {
            return Ok(());
        }

        // Add the "indexing_error" to the response.
        assert!(result.errors_mut().is_empty());
        *result.errors_mut() = vec![QueryError::IndexingError];

        match self.error_policy {
            // If indexing errors are denied, we omit results, except for the `_meta` response.
            // Note that the meta field could have been queried under a different response key,
            // or a different field queried under the response key `_meta`.
            ErrorPolicy::Deny => {
                let data = result.take_data();
                let meta = data.and_then(|mut d| d.remove_entry("_meta"));
                result.set_data(meta.map(|m| BTreeMap::from_iter(Some(m))));
            }
            ErrorPolicy::Allow => (),
        }
        Ok(())
    }
}
