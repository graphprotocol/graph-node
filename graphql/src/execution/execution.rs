use super::cache::{QueryBlockCache, QueryCache};
use crossbeam::atomic::AtomicCell;
use graph::{
    data::{schema::META_FIELD_NAME, value::Object},
    prelude::{s, CheapClone},
    util::timed_rw_lock::TimedMutex,
};
use lazy_static::lazy_static;
use stable_hash::crypto::SetHasher;
use stable_hash::prelude::*;
use stable_hash::utils::stable_hash;
use std::time::Instant;
use std::{borrow::ToOwned, collections::HashSet};

use graph::data::graphql::*;
use graph::data::query::CacheStatus;
use graph::prelude::*;
use graph::util::lfu_cache::LfuCache;

use super::QueryHash;
use crate::execution::ast as a;
use crate::introspection::{is_introspection_field, INTROSPECTION_QUERY_TYPE};
use crate::prelude::*;
use crate::schema::ast as sast;

lazy_static! {
    // Comma separated subgraph ids to cache queries for.
    // If `*` is present in the list, queries are cached for all subgraphs.
    // Defaults to "*".
    static ref CACHED_SUBGRAPH_IDS: Vec<String> = {
        std::env::var("GRAPH_CACHED_SUBGRAPH_IDS")
        .unwrap_or("*".to_string())
        .split(',')
        .map(ToOwned::to_owned)
        .collect()
    };

    static ref CACHE_ALL: bool = CACHED_SUBGRAPH_IDS.contains(&"*".to_string());

    // How many blocks per network should be kept in the query cache. When the limit is reached,
    // older blocks are evicted. This should be kept small since a lookup to the cache is O(n) on
    // this value, and the cache memory usage also increases with larger number. Set to 0 to disable
    // the cache. Defaults to 2.
    static ref QUERY_CACHE_BLOCKS: usize = {
        std::env::var("GRAPH_QUERY_CACHE_BLOCKS")
        .unwrap_or("2".to_string())
        .parse::<usize>()
        .expect("Invalid value for GRAPH_QUERY_CACHE_BLOCKS environment variable")
    };

    /// Maximum total memory to be used by the cache. Each block has a max size of
    /// `QUERY_CACHE_MAX_MEM` / (`QUERY_CACHE_BLOCKS` * `GRAPH_QUERY_BLOCK_CACHE_SHARDS`).
    /// The env var is in MB.
    static ref QUERY_CACHE_MAX_MEM: usize = {
        1_000_000 *
        std::env::var("GRAPH_QUERY_CACHE_MAX_MEM")
        .unwrap_or("1000".to_string())
        .parse::<usize>()
        .expect("Invalid value for GRAPH_QUERY_CACHE_MAX_MEM environment variable")
    };

    static ref QUERY_CACHE_STALE_PERIOD: u64 = {
        std::env::var("GRAPH_QUERY_CACHE_STALE_PERIOD")
        .unwrap_or("100".to_string())
        .parse::<u64>()
        .expect("Invalid value for GRAPH_QUERY_CACHE_STALE_PERIOD environment variable")
    };

    /// In how many shards (mutexes) the query block cache is split.
    /// Ideally this should divide 256 so that the distribution of queries to shards is even.
    static ref QUERY_BLOCK_CACHE_SHARDS: u8 = {
        std::env::var("GRAPH_QUERY_BLOCK_CACHE_SHARDS")
        .unwrap_or("128".to_string())
        .parse::<u8>()
        .expect("Invalid value for GRAPH_QUERY_BLOCK_CACHE_SHARDS environment variable, max is 255")
    };

    static ref QUERY_LFU_CACHE_SHARDS: u8 = {
        std::env::var("GRAPH_QUERY_LFU_CACHE_SHARDS")
        .map(|s| {
            s.parse::<u8>()
             .expect("Invalid value for GRAPH_QUERY_LFU_CACHE_SHARDS environment variable, max is 255")
        })
        .unwrap_or(*QUERY_BLOCK_CACHE_SHARDS)
    };



    // Sharded query results cache for recent blocks by network.
    // The `VecDeque` works as a ring buffer with a capacity of `QUERY_CACHE_BLOCKS`.
    static ref QUERY_BLOCK_CACHE: Vec<TimedMutex<QueryBlockCache>> = {
            let shards = *QUERY_BLOCK_CACHE_SHARDS;
            let blocks = *QUERY_CACHE_BLOCKS;

            // The memory budget is evenly divided among blocks and their shards.
            let max_weight = *QUERY_CACHE_MAX_MEM / (blocks * shards as usize);
            let mut caches = Vec::new();
            for i in 0..shards {
                let id = format!("query_block_cache_{}", i);
                caches.push(TimedMutex::new(QueryBlockCache::new(blocks, i, max_weight), id))
            }
            caches
    };
    static ref QUERY_HERD_CACHE: QueryCache<Arc<QueryResult>> = QueryCache::new("query_herd_cache");
    static ref QUERY_LFU_CACHE: Vec<TimedMutex<LfuCache<QueryHash, WeightedResult>>> = {
        std::iter::repeat_with(|| TimedMutex::new(LfuCache::new(), "query_lfu_cache"))
                    .take(*QUERY_LFU_CACHE_SHARDS as usize).collect()
    };
}

struct WeightedResult {
    result: Arc<QueryResult>,
    weight: usize,
}

impl CacheWeight for WeightedResult {
    fn indirect_weight(&self) -> usize {
        self.weight
    }
}

impl Default for WeightedResult {
    fn default() -> Self {
        WeightedResult {
            result: Arc::new(QueryResult::new(Object::default())),
            weight: 0,
        }
    }
}

struct HashableQuery<'a> {
    query_schema_id: &'a DeploymentHash,
    selection_set: &'a a::SelectionSet,
    block_ptr: &'a BlockPtr,
}

/// Note that the use of StableHash here is a little bit loose. In particular,
/// we are converting items to a string inside here as a quick-and-dirty
/// implementation. This precludes the ability to add new fields (unlikely
/// anyway). So, this hash isn't really Stable in the way that the StableHash
/// crate defines it. Since hashes are only persisted for this process, we don't
/// need that property. The reason we are using StableHash is to get collision
/// resistance and use it's foolproof API to prevent easy mistakes instead.
///
/// This is also only as collision resistant insofar as the to_string impls are
/// collision resistant. It is highly likely that this is ok, since these come
/// from an ast.
///
/// It is possible that multiple asts that are effectively the same query with
/// different representations. This is considered not an issue. The worst
/// possible outcome is that the same query will have multiple cache entries.
/// But, the wrong result should not be served.
impl StableHash for HashableQuery<'_> {
    fn stable_hash<H: StableHasher>(&self, mut sequence_number: H::Seq, state: &mut H) {
        self.query_schema_id
            .stable_hash(sequence_number.next_child(), state);

        // Not stable! Uses to_string
        format!("{:?}", self.selection_set).stable_hash(sequence_number.next_child(), state);

        self.block_ptr
            .stable_hash(sequence_number.next_child(), state);
    }
}

// The key is: subgraph id + selection set + variables + fragment definitions
fn cache_key(
    ctx: &ExecutionContext<impl Resolver>,
    selection_set: &a::SelectionSet,
    block_ptr: &BlockPtr,
) -> QueryHash {
    // It is very important that all data used for the query is included.
    // Otherwise, incorrect results may be returned.
    let query = HashableQuery {
        query_schema_id: ctx.query.schema.id(),
        selection_set,
        block_ptr,
    };
    stable_hash::<SetHasher, _>(&query)
}

/// Contextual information passed around during query execution.
pub struct ExecutionContext<R>
where
    R: Resolver,
{
    /// The logger to use.
    pub logger: Logger,

    /// The query to execute.
    pub query: Arc<crate::execution::Query>,

    /// The resolver to use.
    pub resolver: R,

    /// Time at which the query times out.
    pub deadline: Option<Instant>,

    /// Max value for `first`.
    pub max_first: u32,

    /// Max value for `skip`
    pub max_skip: u32,

    /// Records whether this was a cache hit, used for logging.
    pub(crate) cache_status: AtomicCell<CacheStatus>,
}

pub(crate) fn get_field<'a>(
    object_type: impl Into<ObjectOrInterface<'a>>,
    name: &str,
) -> Option<s::Field> {
    if name == "__schema" || name == "__type" {
        let object_type = &*INTROSPECTION_QUERY_TYPE;
        sast::get_field(object_type, name).cloned()
    } else {
        sast::get_field(object_type, name).cloned()
    }
}

impl<R> ExecutionContext<R>
where
    R: Resolver,
{
    pub fn as_introspection_context(&self) -> ExecutionContext<IntrospectionResolver> {
        let introspection_resolver =
            IntrospectionResolver::new(&self.logger, self.query.schema.schema());

        ExecutionContext {
            logger: self.logger.cheap_clone(),
            resolver: introspection_resolver,
            query: self.query.cheap_clone(),
            deadline: self.deadline,
            max_first: std::u32::MAX,
            max_skip: std::u32::MAX,

            // `cache_status` is a dead value for the introspection context.
            cache_status: AtomicCell::new(CacheStatus::Miss),
        }
    }
}

pub(crate) fn execute_root_selection_set_uncached(
    ctx: &ExecutionContext<impl Resolver>,
    selection_set: &a::SelectionSet,
    root_type: &sast::ObjectType,
) -> Result<Object, Vec<QueryExecutionError>> {
    // Split the top-level fields into introspection fields and
    // regular data fields
    let mut data_set = a::SelectionSet::empty_from(selection_set);
    let mut intro_set = a::SelectionSet::empty_from(selection_set);
    let mut meta_items = Vec::new();

    for field in selection_set.fields_for(root_type)? {
        // See if this is an introspection or data field. We don't worry about
        // non-existent fields; those will cause an error later when we execute
        // the data_set SelectionSet
        if is_introspection_field(&field.name) {
            intro_set.push(field)?
        } else if &field.name == META_FIELD_NAME {
            meta_items.push(field)
        } else {
            data_set.push(field)?
        }
    }

    // If we are getting regular data, prefetch it from the database
    let mut values = if data_set.is_empty() && meta_items.is_empty() {
        Object::default()
    } else {
        let initial_data = ctx.resolver.prefetch(&ctx, &data_set)?;
        data_set.push_fields(meta_items)?;
        execute_selection_set_to_map(&ctx, &data_set, root_type, initial_data)?
    };

    // Resolve introspection fields, if there are any
    if !intro_set.is_empty() {
        let ictx = ctx.as_introspection_context();

        values.extend(execute_selection_set_to_map(
            &ictx,
            ctx.query.selection_set.as_ref(),
            &*INTROSPECTION_QUERY_TYPE,
            None,
        )?);
    }

    Ok(values)
}

/// Executes the root selection set of a query.
pub(crate) async fn execute_root_selection_set<R: Resolver>(
    ctx: Arc<ExecutionContext<R>>,
    selection_set: Arc<a::SelectionSet>,
    root_type: sast::ObjectType,
    block_ptr: Option<BlockPtr>,
) -> Arc<QueryResult> {
    // Cache the cache key to not have to calculate it twice - once for lookup
    // and once for insert.
    let mut key: Option<QueryHash> = None;

    if R::CACHEABLE && (*CACHE_ALL || CACHED_SUBGRAPH_IDS.contains(ctx.query.schema.id())) {
        if let (Some(block_ptr), Some(network)) = (block_ptr.as_ref(), &ctx.query.network) {
            // JSONB and metadata queries use `BLOCK_NUMBER_MAX`. Ignore this case for two reasons:
            // - Metadata queries are not cacheable.
            // - Caching `BLOCK_NUMBER_MAX` would make this cache think all other blocks are old.
            if block_ptr.number != BLOCK_NUMBER_MAX {
                // Calculate the hash outside of the lock
                let cache_key = cache_key(&ctx, &selection_set, &block_ptr);
                let shard = (cache_key[0] as usize) % QUERY_BLOCK_CACHE.len();

                // Check if the response is cached, first in the recent blocks cache,
                // and then in the LfuCache for historical queries
                // The blocks are used to delimit how long locks need to be held
                {
                    let cache = QUERY_BLOCK_CACHE[shard].lock(&ctx.logger);
                    if let Some(result) = cache.get(network, &block_ptr, &cache_key) {
                        ctx.cache_status.store(CacheStatus::Hit);
                        return result;
                    }
                }
                {
                    let mut cache = QUERY_LFU_CACHE[shard].lock(&ctx.logger);
                    if let Some(weighted) = cache.get(&cache_key) {
                        ctx.cache_status.store(CacheStatus::Hit);
                        return weighted.result.cheap_clone();
                    }
                }
                key = Some(cache_key);
            }
        }
    }

    let execute_ctx = ctx.cheap_clone();
    let execute_selection_set = selection_set.cheap_clone();
    let execute_root_type = root_type.cheap_clone();
    let run_query = async move {
        let _permit = execute_ctx.resolver.query_permit().await;

        let logger = execute_ctx.logger.clone();
        let query_text = execute_ctx.query.query_text.cheap_clone();
        let variables_text = execute_ctx.query.variables_text.cheap_clone();
        match graph::spawn_blocking_allow_panic(move || {
            let mut query_res = QueryResult::from(execute_root_selection_set_uncached(
                &execute_ctx,
                &execute_selection_set,
                &execute_root_type,
            ));

            // Unwrap: In practice should never fail, but if it does we will catch the panic.
            execute_ctx.resolver.post_process(&mut query_res).unwrap();
            query_res.deployment = Some(execute_ctx.query.schema.id().clone());
            Arc::new(query_res)
        })
        .await
        {
            Ok(result) => result,
            Err(e) => {
                let e = e.into_panic();
                let e = match e
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or(e.downcast_ref::<&'static str>().copied())
                {
                    Some(e) => e.to_string(),
                    None => "panic is not a string".to_string(),
                };
                error!(
                    logger,
                    "panic when processing graphql query";
                    "panic" => e.to_string(),
                    "query" => query_text,
                    "variables" => variables_text,
                );
                Arc::new(QueryResult::from(QueryExecutionError::Panic(e)))
            }
        }
    };

    let (result, herd_hit) = if let Some(key) = key {
        QUERY_HERD_CACHE
            .cached_query(key, run_query, &ctx.logger)
            .await
    } else {
        (run_query.await, false)
    };
    if herd_hit {
        ctx.cache_status.store(CacheStatus::Shared);
    }

    // Check if this query should be cached.
    // Share errors from the herd cache, but don't store them in generational cache.
    // In particular, there is a problem where asking for a block pointer beyond the chain
    // head can cause the legitimate cache to be thrown out.
    // It would be redundant to insert herd cache hits.
    let no_cache = herd_hit || result.has_errors();
    if let (false, Some(key), Some(block_ptr), Some(network)) =
        (no_cache, key, block_ptr, &ctx.query.network)
    {
        // Calculate the weight outside the lock.
        let weight = result.weight();
        let shard = (key[0] as usize) % QUERY_BLOCK_CACHE.len();
        let inserted = QUERY_BLOCK_CACHE[shard].lock(&ctx.logger).insert(
            network,
            block_ptr.clone(),
            key,
            result.cheap_clone(),
            weight,
            ctx.logger.cheap_clone(),
        );

        if inserted {
            ctx.cache_status.store(CacheStatus::Insert);
        } else {
            // Results that are too old for the QUERY_BLOCK_CACHE go into the QUERY_LFU_CACHE
            let mut cache = QUERY_LFU_CACHE[shard].lock(&ctx.logger);
            let max_mem = *QUERY_CACHE_MAX_MEM / (*QUERY_BLOCK_CACHE_SHARDS as usize);
            cache.evict_with_period(max_mem, *QUERY_CACHE_STALE_PERIOD);
            cache.insert(
                key,
                WeightedResult {
                    result: result.cheap_clone(),
                    weight,
                },
            );
            ctx.cache_status.store(CacheStatus::Insert);
        }
    }

    result
}

/// Executes a selection set, requiring the result to be of the given object type.
///
/// Allows passing in a parent value during recursive processing of objects and their fields.
fn execute_selection_set<'a>(
    ctx: &'a ExecutionContext<impl Resolver>,
    selection_set: &'a a::SelectionSet,
    object_type: &sast::ObjectType,
    prefetched_value: Option<r::Value>,
) -> Result<r::Value, Vec<QueryExecutionError>> {
    Ok(r::Value::Object(execute_selection_set_to_map(
        ctx,
        selection_set,
        object_type,
        prefetched_value,
    )?))
}

fn execute_selection_set_to_map<'a>(
    ctx: &'a ExecutionContext<impl Resolver>,
    selection_set: &'a a::SelectionSet,
    object_type: &sast::ObjectType,
    prefetched_value: Option<r::Value>,
) -> Result<Object, Vec<QueryExecutionError>> {
    let mut prefetched_object = match prefetched_value {
        Some(r::Value::Object(object)) => Some(object),
        Some(_) => unreachable!(),
        None => None,
    };
    let mut errors: Vec<QueryExecutionError> = Vec::new();
    let mut result_map = Object::new();

    // Gather fields that appear more than once with the same response key.
    let multiple_response_keys = {
        let mut multiple_response_keys = HashSet::new();
        let mut fields = HashSet::new();
        for field in selection_set.fields_for(object_type)? {
            if !fields.insert(field.name.as_str()) {
                multiple_response_keys.insert(field.name.as_str());
            }
        }
        multiple_response_keys
    };

    // Process all field groups in order
    for field in selection_set.fields_for(object_type)? {
        match ctx.deadline {
            Some(deadline) if deadline < Instant::now() => {
                errors.push(QueryExecutionError::Timeout);
                break;
            }
            _ => (),
        }

        let response_key = field.response_key();

        // Unwrap: The query was validated to contain only valid fields.
        let field_type = sast::get_field(object_type, &field.name).unwrap();

        // Check if we have the value already.
        let field_value = prefetched_object
            .as_mut()
            .map(|o| {
                // Prefetched objects are associated to `prefetch:response_key`.
                if let Some(val) = o.remove(&format!("prefetch:{}", response_key)) {
                    return Some(val);
                }

                // Scalars and scalar lists are associated to the field name.
                // If the field has more than one response key, we have to clone.
                match multiple_response_keys.contains(field.name.as_str()) {
                    false => o.remove(&field.name),
                    true => o.get(&field.name).cloned(),
                }
            })
            .flatten();
        match execute_field(&ctx, object_type, field_value, field, field_type) {
            Ok(v) => {
                result_map.insert(response_key.to_owned(), v);
            }
            Err(mut e) => {
                errors.append(&mut e);
            }
        }
    }

    if errors.is_empty() {
        Ok(result_map)
    } else {
        Err(errors)
    }
}

/// Executes a field.
fn execute_field(
    ctx: &ExecutionContext<impl Resolver>,
    object_type: &s::ObjectType,
    field_value: Option<r::Value>,
    field: &a::Field,
    field_definition: &s::Field,
) -> Result<r::Value, Vec<QueryExecutionError>> {
    resolve_field_value(
        ctx,
        object_type,
        field_value,
        field,
        field_definition,
        &field_definition.field_type,
    )
    .and_then(|value| complete_value(ctx, field, &field_definition.field_type, value))
}

/// Resolves the value of a field.
fn resolve_field_value(
    ctx: &ExecutionContext<impl Resolver>,
    object_type: &s::ObjectType,
    field_value: Option<r::Value>,
    field: &a::Field,
    field_definition: &s::Field,
    field_type: &s::Type,
) -> Result<r::Value, Vec<QueryExecutionError>> {
    match field_type {
        s::Type::NonNullType(inner_type) => resolve_field_value(
            ctx,
            object_type,
            field_value,
            field,
            field_definition,
            inner_type.as_ref(),
        ),

        s::Type::NamedType(ref name) => resolve_field_value_for_named_type(
            ctx,
            object_type,
            field_value,
            field,
            field_definition,
            name,
        ),

        s::Type::ListType(inner_type) => resolve_field_value_for_list_type(
            ctx,
            object_type,
            field_value,
            field,
            field_definition,
            inner_type.as_ref(),
        ),
    }
}

/// Resolves the value of a field that corresponds to a named type.
fn resolve_field_value_for_named_type(
    ctx: &ExecutionContext<impl Resolver>,
    object_type: &s::ObjectType,
    field_value: Option<r::Value>,
    field: &a::Field,
    field_definition: &s::Field,
    type_name: &str,
) -> Result<r::Value, Vec<QueryExecutionError>> {
    // Try to resolve the type name into the actual type
    let named_type = ctx
        .query
        .schema
        .get_named_type(type_name)
        .ok_or_else(|| QueryExecutionError::NamedTypeError(type_name.to_string()))?;
    match named_type {
        // Let the resolver decide how the field (with the given object type) is resolved
        s::TypeDefinition::Object(t) => {
            ctx.resolver
                .resolve_object(field_value, field, field_definition, t.into())
        }

        // Let the resolver decide how values in the resolved object value
        // map to values of GraphQL enums
        s::TypeDefinition::Enum(t) => ctx.resolver.resolve_enum_value(field, t, field_value),

        // Let the resolver decide how values in the resolved object value
        // map to values of GraphQL scalars
        s::TypeDefinition::Scalar(t) => {
            ctx.resolver
                .resolve_scalar_value(object_type, field, t, field_value)
        }

        s::TypeDefinition::Interface(i) => {
            ctx.resolver
                .resolve_object(field_value, field, field_definition, i.into())
        }

        s::TypeDefinition::Union(_) => Err(QueryExecutionError::Unimplemented("unions".to_owned())),

        s::TypeDefinition::InputObject(_) => unreachable!("input objects are never resolved"),
    }
    .map_err(|e| vec![e])
}

/// Resolves the value of a field that corresponds to a list type.
fn resolve_field_value_for_list_type(
    ctx: &ExecutionContext<impl Resolver>,
    object_type: &s::ObjectType,
    field_value: Option<r::Value>,
    field: &a::Field,
    field_definition: &s::Field,
    inner_type: &s::Type,
) -> Result<r::Value, Vec<QueryExecutionError>> {
    match inner_type {
        s::Type::NonNullType(inner_type) => resolve_field_value_for_list_type(
            ctx,
            object_type,
            field_value,
            field,
            field_definition,
            inner_type,
        ),

        s::Type::NamedType(ref type_name) => {
            let named_type = ctx
                .query
                .schema
                .get_named_type(type_name)
                .ok_or_else(|| QueryExecutionError::NamedTypeError(type_name.to_string()))?;

            match named_type {
                // Let the resolver decide how the list field (with the given item object type)
                // is resolved into a entities based on the (potential) parent object
                s::TypeDefinition::Object(t) => ctx
                    .resolver
                    .resolve_objects(field_value, field, field_definition, t.into())
                    .map_err(|e| vec![e]),

                // Let the resolver decide how values in the resolved object value
                // map to values of GraphQL enums
                s::TypeDefinition::Enum(t) => {
                    ctx.resolver.resolve_enum_values(field, &t, field_value)
                }

                // Let the resolver decide how values in the resolved object value
                // map to values of GraphQL scalars
                s::TypeDefinition::Scalar(t) => {
                    ctx.resolver.resolve_scalar_values(field, &t, field_value)
                }

                s::TypeDefinition::Interface(t) => ctx
                    .resolver
                    .resolve_objects(field_value, field, field_definition, t.into())
                    .map_err(|e| vec![e]),

                s::TypeDefinition::Union(_) => Err(vec![QueryExecutionError::Unimplemented(
                    "unions".to_owned(),
                )]),

                s::TypeDefinition::InputObject(_) => {
                    unreachable!("input objects are never resolved")
                }
            }
        }

        // We don't support nested lists yet
        s::Type::ListType(_) => Err(vec![QueryExecutionError::Unimplemented(
            "nested list types".to_owned(),
        )]),
    }
}

/// Ensures that a value matches the expected return type.
fn complete_value(
    ctx: &ExecutionContext<impl Resolver>,
    field: &a::Field,
    field_type: &s::Type,
    resolved_value: r::Value,
) -> Result<r::Value, Vec<QueryExecutionError>> {
    match field_type {
        // Fail if the field type is non-null but the value is null
        s::Type::NonNullType(inner_type) => {
            return match complete_value(ctx, field, inner_type, resolved_value)? {
                r::Value::Null => Err(vec![QueryExecutionError::NonNullError(
                    field.position,
                    field.name.to_string(),
                )]),

                v => Ok(v),
            };
        }

        // If the resolved value is null, return null
        _ if resolved_value.is_null() => Ok(resolved_value),

        // Complete list values
        s::Type::ListType(inner_type) => {
            match resolved_value {
                // Complete list values individually
                r::Value::List(mut values) => {
                    let mut errors = Vec::new();

                    // To avoid allocating a new vector this completes the values in place.
                    for value_place in &mut values {
                        // Put in a placeholder, complete the value, put the completed value back.
                        let value = std::mem::replace(value_place, r::Value::Null);
                        match complete_value(ctx, field, inner_type, value) {
                            Ok(value) => {
                                *value_place = value;
                            }
                            Err(errs) => errors.extend(errs),
                        }
                    }
                    match errors.is_empty() {
                        true => Ok(r::Value::List(values)),
                        false => Err(errors),
                    }
                }

                // Return field error if the resolved value for the list is not a list
                _ => Err(vec![QueryExecutionError::ListValueError(
                    field.position,
                    field.name.to_string(),
                )]),
            }
        }

        s::Type::NamedType(name) => {
            let named_type = ctx.query.schema.get_named_type(name).unwrap();

            match named_type {
                // Complete scalar values
                s::TypeDefinition::Scalar(scalar_type) => {
                    resolved_value.coerce_scalar(scalar_type).map_err(|value| {
                        vec![QueryExecutionError::ScalarCoercionError(
                            field.position,
                            field.name.to_owned(),
                            value.into(),
                            scalar_type.name.to_owned(),
                        )]
                    })
                }

                // Complete enum values
                s::TypeDefinition::Enum(enum_type) => {
                    resolved_value.coerce_enum(enum_type).map_err(|value| {
                        vec![QueryExecutionError::EnumCoercionError(
                            field.position,
                            field.name.to_owned(),
                            value.into(),
                            enum_type.name.to_owned(),
                            enum_type
                                .values
                                .iter()
                                .map(|value| value.name.to_owned())
                                .collect(),
                        )]
                    })
                }

                // Complete object types recursively
                s::TypeDefinition::Object(object_type) => {
                    let object_type = ctx.query.schema.object_type(object_type).into();
                    execute_selection_set(
                        ctx,
                        &field.selection_set,
                        &object_type,
                        Some(resolved_value),
                    )
                }

                // Resolve interface types using the resolved value and complete the value recursively
                s::TypeDefinition::Interface(_) => {
                    let object_type = resolve_abstract_type(ctx, named_type, &resolved_value)?;

                    execute_selection_set(
                        ctx,
                        &field.selection_set,
                        &object_type,
                        Some(resolved_value),
                    )
                }

                // Resolve union types using the resolved value and complete the value recursively
                s::TypeDefinition::Union(_) => {
                    let object_type = resolve_abstract_type(ctx, named_type, &resolved_value)?;

                    execute_selection_set(
                        ctx,
                        &field.selection_set,
                        &object_type,
                        Some(resolved_value),
                    )
                }

                s::TypeDefinition::InputObject(_) => {
                    unreachable!("input objects are never resolved")
                }
            }
        }
    }
}

/// Resolves an abstract type (interface, union) into an object type based on the given value.
fn resolve_abstract_type<'a>(
    ctx: &'a ExecutionContext<impl Resolver>,
    abstract_type: &s::TypeDefinition,
    object_value: &r::Value,
) -> Result<sast::ObjectType, Vec<QueryExecutionError>> {
    // Let the resolver handle the type resolution, return an error if the resolution
    // yields nothing
    let obj_type = ctx
        .resolver
        .resolve_abstract_type(&ctx.query.schema, abstract_type, object_value)
        .ok_or_else(|| {
            vec![QueryExecutionError::AbstractTypeError(
                sast::get_type_name(abstract_type).to_string(),
            )]
        })?;
    Ok(ctx.query.schema.object_type(obj_type).into())
}
