use super::cache::{QueryBlockCache, QueryCache};
use crossbeam::atomic::AtomicCell;
use graph::{
    data::schema::META_FIELD_NAME,
    prelude::{s, CheapClone},
    util::timed_rw_lock::TimedMutex,
};
use indexmap::IndexMap;
use lazy_static::lazy_static;
use stable_hash::crypto::SetHasher;
use stable_hash::prelude::*;
use stable_hash::utils::stable_hash;
use std::borrow::ToOwned;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::iter;
use std::time::Instant;

use graph::data::graphql::*;
use graph::data::query::CacheStatus;
use graph::prelude::*;
use graph::util::lfu_cache::LfuCache;

use super::QueryHash;
use crate::introspection::{
    is_introspection_field, INTROSPECTION_DOCUMENT, INTROSPECTION_QUERY_TYPE,
};
use crate::prelude::*;
use crate::query::ast as qast;
use crate::schema::ast as sast;
use crate::values::coercion;

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
            result: Arc::new(QueryResult::new(BTreeMap::default())),
            weight: 0,
        }
    }
}

struct HashableQuery<'a> {
    query_schema_id: &'a DeploymentHash,
    query_variables: &'a HashMap<String, q::Value>,
    query_fragments: &'a HashMap<String, q::FragmentDefinition>,
    selection_set: &'a q::SelectionSet,
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

        // Not stable! Uses to_string()
        self.query_variables
            .iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect::<HashMap<_, _>>()
            .stable_hash(sequence_number.next_child(), state);

        // Not stable! Uses to_string()
        self.query_fragments
            .iter()
            .map(|(k, v)| (k, v.to_string()))
            .collect::<HashMap<_, _>>()
            .stable_hash(sequence_number.next_child(), state);

        // Not stable! Uses to_string
        self.selection_set
            .to_string()
            .stable_hash(sequence_number.next_child(), state);

        self.block_ptr
            .stable_hash(sequence_number.next_child(), state);
    }
}

// The key is: subgraph id + selection set + variables + fragment definitions
fn cache_key(
    ctx: &ExecutionContext<impl Resolver>,
    selection_set: &q::SelectionSet,
    block_ptr: &BlockPtr,
) -> QueryHash {
    // It is very important that all data used for the query is included.
    // Otherwise, incorrect results may be returned.
    let query = HashableQuery {
        query_schema_id: ctx.query.schema.id(),
        query_variables: &ctx.query.variables,
        query_fragments: &ctx.query.fragments,
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

    pub load_manager: Arc<dyn QueryLoadManager>,

    /// Set if this query is being executed in another resolver and therefore reentering functions
    /// such as `execute_root_selection_set`.
    pub nested_resolver: bool,
}

// Helpers to look for types and fields on both the introspection and regular schemas.
pub(crate) fn get_named_type(schema: &s::Document, name: &str) -> Option<s::TypeDefinition> {
    if name.starts_with("__") {
        sast::get_named_type(&INTROSPECTION_DOCUMENT, name).cloned()
    } else {
        sast::get_named_type(schema, name).cloned()
    }
}

pub(crate) fn get_field<'a>(
    object_type: impl Into<ObjectOrInterface<'a>>,
    name: &str,
) -> Option<s::Field> {
    if name == "__schema" || name == "__type" {
        let object_type = *INTROSPECTION_QUERY_TYPE;
        sast::get_field(object_type, name).cloned()
    } else {
        sast::get_field(object_type, name).cloned()
    }
}

pub(crate) fn object_or_interface<'a>(
    schema: &'a s::Document,
    name: &str,
) -> Option<ObjectOrInterface<'a>> {
    if name.starts_with("__") {
        INTROSPECTION_DOCUMENT.object_or_interface(name)
    } else {
        schema.object_or_interface(name)
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
            query: self.query.as_introspection_query(),
            deadline: self.deadline,
            max_first: std::u32::MAX,
            max_skip: std::u32::MAX,

            // `cache_status` and `load_manager` are dead values for the introspection context.
            cache_status: AtomicCell::new(CacheStatus::Miss),
            load_manager: self.load_manager.cheap_clone(),
            nested_resolver: self.nested_resolver,
        }
    }
}

pub fn execute_root_selection_set_uncached(
    ctx: &ExecutionContext<impl Resolver>,
    selection_set: &q::SelectionSet,
    root_type: &s::ObjectType,
) -> Result<BTreeMap<String, q::Value>, Vec<QueryExecutionError>> {
    // Split the top-level fields into introspection fields and
    // regular data fields
    let mut data_set = q::SelectionSet {
        span: selection_set.span.clone(),
        items: Vec::new(),
    };
    let mut intro_set = q::SelectionSet {
        span: selection_set.span.clone(),
        items: Vec::new(),
    };
    let mut meta_items = Vec::new();

    for (_, fields) in collect_fields(ctx, root_type, iter::once(selection_set)) {
        let name = fields[0].name.clone();
        let selections = fields.into_iter().map(|f| q::Selection::Field(f.clone()));
        // See if this is an introspection or data field. We don't worry about
        // non-existent fields; those will cause an error later when we execute
        // the data_set SelectionSet
        if is_introspection_field(&name) {
            intro_set.items.extend(selections)
        } else if &name == META_FIELD_NAME {
            meta_items.extend(selections)
        } else {
            data_set.items.extend(selections)
        }
    }

    // If we are getting regular data, prefetch it from the database
    let mut values = if data_set.items.is_empty() && meta_items.is_empty() {
        BTreeMap::default()
    } else {
        let initial_data = ctx.resolver.prefetch(&ctx, &data_set)?;
        data_set.items.extend(meta_items);
        execute_selection_set_to_map(&ctx, iter::once(&data_set), root_type, initial_data)?
    };

    // Resolve introspection fields, if there are any
    if !intro_set.items.is_empty() {
        let ictx = ctx.as_introspection_context();

        values.extend(execute_selection_set_to_map(
            &ictx,
            iter::once(&intro_set),
            &*INTROSPECTION_QUERY_TYPE,
            None,
        )?);
    }

    Ok(values)
}

/// Executes the root selection set of a query.
pub async fn execute_root_selection_set<R: Resolver>(
    ctx: Arc<ExecutionContext<R>>,
    selection_set: Arc<q::SelectionSet>,
    root_type: Arc<s::ObjectType>,
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
    let nested_resolver = ctx.nested_resolver;
    let run_query = async move {
        // Limiting the cuncurrent queries prevents increase in resource usage when the DB is
        // contended and queries start queing up. This semaphore organizes the queueing so that
        // waiting queries consume few resources.
        //
        // Do not request a permit in a nested resolver, since it is already holding a permit and
        // requesting another could deadlock.
        let _permit = if !nested_resolver {
            execute_ctx.load_manager.query_permit().await
        } else {
            // Acquire a dummy semaphore. Unwrap: a semaphore that was just created can be acquired.
            Arc::new(tokio::sync::Semaphore::new(1))
                .acquire_owned()
                .await
        };

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
                    .or(e.downcast_ref::<&'static str>().map(|&s| s))
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
    selection_sets: impl Iterator<Item = &'a q::SelectionSet>,
    object_type: &s::ObjectType,
    prefetched_value: Option<q::Value>,
) -> Result<q::Value, Vec<QueryExecutionError>> {
    Ok(q::Value::Object(execute_selection_set_to_map(
        ctx,
        selection_sets,
        object_type,
        prefetched_value,
    )?))
}

fn execute_selection_set_to_map<'a>(
    ctx: &'a ExecutionContext<impl Resolver>,
    selection_sets: impl Iterator<Item = &'a q::SelectionSet>,
    object_type: &s::ObjectType,
    prefetched_value: Option<q::Value>,
) -> Result<BTreeMap<String, q::Value>, Vec<QueryExecutionError>> {
    let mut prefetched_object = match prefetched_value {
        Some(q::Value::Object(object)) => Some(object),
        Some(_) => unreachable!(),
        None => None,
    };
    let mut errors: Vec<QueryExecutionError> = Vec::new();
    let mut result_map: BTreeMap<String, q::Value> = BTreeMap::new();

    // Group fields with the same response key, so we can execute them together
    let grouped_field_set = collect_fields(ctx, object_type, selection_sets);

    // Gather fields that appear more than once with the same response key.
    let multiple_response_keys = {
        let mut multiple_response_keys = HashSet::new();
        let mut fields = HashSet::new();
        for field in grouped_field_set.iter().map(|(_, f)| f.iter()).flatten() {
            if !fields.insert(field.name.as_str()) {
                multiple_response_keys.insert(field.name.as_str());
            }
        }
        multiple_response_keys
    };

    // Process all field groups in order
    for (response_key, fields) in grouped_field_set {
        match ctx.deadline {
            Some(deadline) if deadline < Instant::now() => {
                errors.push(QueryExecutionError::Timeout);
                break;
            }
            _ => (),
        }

        // Unwrap: The query was validated to contain only valid fields.
        let field = sast::get_field(object_type, &fields[0].name).unwrap();

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
                match multiple_response_keys.contains(fields[0].name.as_str()) {
                    false => o.remove(&fields[0].name),
                    true => o.get(&fields[0].name).cloned(),
                }
            })
            .flatten();
        match execute_field(&ctx, object_type, field_value, &fields[0], field, fields) {
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

/// Collects fields from selection sets. Returns a map from response key to fields. There will
/// typically be a single field for a response key. If there are multiple, the overall execution
/// logic will effectively merged them into the output for the response key.
pub fn collect_fields<'a>(
    ctx: &'a ExecutionContext<impl Resolver>,
    object_type: &s::ObjectType,
    selection_sets: impl Iterator<Item = &'a q::SelectionSet>,
) -> IndexMap<&'a str, Vec<&'a q::Field>> {
    let mut grouped_fields = IndexMap::new();
    collect_fields_inner(
        ctx,
        object_type,
        selection_sets,
        &mut HashSet::new(),
        &mut grouped_fields,
    );
    grouped_fields
}

pub fn collect_fields_inner<'a>(
    ctx: &'a ExecutionContext<impl Resolver>,
    object_type: &s::ObjectType,
    selection_sets: impl Iterator<Item = &'a q::SelectionSet>,
    visited_fragments: &mut HashSet<&'a str>,
    output: &mut IndexMap<&'a str, Vec<&'a q::Field>>,
) {
    for selection_set in selection_sets {
        // Only consider selections that are not skipped and should be included
        let selections = selection_set
            .items
            .iter()
            .filter(|selection| !qast::skip_selection(selection, &ctx.query.variables))
            .filter(|selection| qast::include_selection(selection, &ctx.query.variables));

        for selection in selections {
            match selection {
                q::Selection::Field(ref field) => {
                    let response_key = qast::get_response_key(field);
                    output.entry(response_key).or_default().push(field);
                }

                q::Selection::FragmentSpread(spread) => {
                    // Only consider the fragment if it hasn't already been included,
                    // as would be the case if the same fragment spread ...Foo appeared
                    // twice in the same selection set.
                    //
                    // Note: This will skip both duplicate fragments and will break cycles,
                    // so we support fragments even though the GraphQL spec prohibits them.
                    if visited_fragments.insert(&spread.fragment_name) {
                        let fragment = ctx.query.get_fragment(&spread.fragment_name);
                        if does_fragment_type_apply(ctx, object_type, &fragment.type_condition) {
                            // We have a fragment that applies to the current object type,
                            // collect fields recursively
                            collect_fields_inner(
                                ctx,
                                object_type,
                                iter::once(&fragment.selection_set),
                                visited_fragments,
                                output,
                            );
                        }
                    }
                }

                q::Selection::InlineFragment(fragment) => {
                    let applies = match &fragment.type_condition {
                        Some(cond) => does_fragment_type_apply(ctx, object_type, &cond),
                        None => true,
                    };

                    if applies {
                        collect_fields_inner(
                            ctx,
                            object_type,
                            iter::once(&fragment.selection_set),
                            visited_fragments,
                            output,
                        )
                    }
                }
            };
        }
    }
}

/// Determines whether a fragment is applicable to the given object type.
fn does_fragment_type_apply(
    ctx: &ExecutionContext<impl Resolver>,
    object_type: &s::ObjectType,
    fragment_type: &q::TypeCondition,
) -> bool {
    // This is safe to do, as TypeCondition only has a single `On` variant.
    let q::TypeCondition::On(ref name) = fragment_type;

    // Resolve the type the fragment applies to based on its name
    let named_type = sast::get_named_type(ctx.query.schema.document(), name);

    match named_type {
        // The fragment applies to the object type if its type is the same object type
        Some(s::TypeDefinition::Object(ot)) => object_type == ot,

        // The fragment also applies to the object type if its type is an interface
        // that the object type implements
        Some(s::TypeDefinition::Interface(it)) => {
            object_type.implements_interfaces.contains(&it.name)
        }

        // The fragment also applies to an object type if its type is a union that
        // the object type is one of the possible types for
        Some(s::TypeDefinition::Union(ut)) => ut.types.contains(&object_type.name),

        // In all other cases, the fragment does not apply
        _ => false,
    }
}

/// Executes a field.
fn execute_field(
    ctx: &ExecutionContext<impl Resolver>,
    object_type: &s::ObjectType,
    field_value: Option<q::Value>,
    field: &q::Field,
    field_definition: &s::Field,
    fields: Vec<&q::Field>,
) -> Result<q::Value, Vec<QueryExecutionError>> {
    coerce_argument_values(&ctx.query, object_type, field)
        .and_then(|argument_values| {
            resolve_field_value(
                ctx,
                object_type,
                field_value,
                field,
                field_definition,
                &field_definition.field_type,
                &argument_values,
            )
        })
        .and_then(|value| complete_value(ctx, field, &field_definition.field_type, &fields, value))
}

/// Resolves the value of a field.
fn resolve_field_value(
    ctx: &ExecutionContext<impl Resolver>,
    object_type: &s::ObjectType,
    field_value: Option<q::Value>,
    field: &q::Field,
    field_definition: &s::Field,
    field_type: &s::Type,
    argument_values: &HashMap<&str, q::Value>,
) -> Result<q::Value, Vec<QueryExecutionError>> {
    match field_type {
        s::Type::NonNullType(inner_type) => resolve_field_value(
            ctx,
            object_type,
            field_value,
            field,
            field_definition,
            inner_type.as_ref(),
            argument_values,
        ),

        s::Type::NamedType(ref name) => resolve_field_value_for_named_type(
            ctx,
            object_type,
            field_value,
            field,
            field_definition,
            name,
            argument_values,
        ),

        s::Type::ListType(inner_type) => resolve_field_value_for_list_type(
            ctx,
            object_type,
            field_value,
            field,
            field_definition,
            inner_type.as_ref(),
            argument_values,
        ),
    }
}

/// Resolves the value of a field that corresponds to a named type.
fn resolve_field_value_for_named_type(
    ctx: &ExecutionContext<impl Resolver>,
    object_type: &s::ObjectType,
    field_value: Option<q::Value>,
    field: &q::Field,
    field_definition: &s::Field,
    type_name: &str,
    argument_values: &HashMap<&str, q::Value>,
) -> Result<q::Value, Vec<QueryExecutionError>> {
    // Try to resolve the type name into the actual type
    let named_type = sast::get_named_type(ctx.query.schema.document(), type_name)
        .ok_or_else(|| QueryExecutionError::NamedTypeError(type_name.to_string()))?;
    match named_type {
        // Let the resolver decide how the field (with the given object type) is resolved
        s::TypeDefinition::Object(t) => ctx.resolver.resolve_object(
            field_value,
            field,
            field_definition,
            t.into(),
            argument_values,
        ),

        // Let the resolver decide how values in the resolved object value
        // map to values of GraphQL enums
        s::TypeDefinition::Enum(t) => ctx.resolver.resolve_enum_value(field, t, field_value),

        // Let the resolver decide how values in the resolved object value
        // map to values of GraphQL scalars
        s::TypeDefinition::Scalar(t) => {
            ctx.resolver
                .resolve_scalar_value(object_type, field, t, field_value, argument_values)
        }

        s::TypeDefinition::Interface(i) => ctx.resolver.resolve_object(
            field_value,
            field,
            field_definition,
            i.into(),
            argument_values,
        ),

        s::TypeDefinition::Union(_) => Err(QueryExecutionError::Unimplemented("unions".to_owned())),

        s::TypeDefinition::InputObject(_) => unreachable!("input objects are never resolved"),
    }
    .map_err(|e| vec![e])
}

/// Resolves the value of a field that corresponds to a list type.
fn resolve_field_value_for_list_type(
    ctx: &ExecutionContext<impl Resolver>,
    object_type: &s::ObjectType,
    field_value: Option<q::Value>,
    field: &q::Field,
    field_definition: &s::Field,
    inner_type: &s::Type,
    argument_values: &HashMap<&str, q::Value>,
) -> Result<q::Value, Vec<QueryExecutionError>> {
    match inner_type {
        s::Type::NonNullType(inner_type) => resolve_field_value_for_list_type(
            ctx,
            object_type,
            field_value,
            field,
            field_definition,
            inner_type,
            argument_values,
        ),

        s::Type::NamedType(ref type_name) => {
            let named_type = sast::get_named_type(ctx.query.schema.document(), type_name)
                .ok_or_else(|| QueryExecutionError::NamedTypeError(type_name.to_string()))?;

            match named_type {
                // Let the resolver decide how the list field (with the given item object type)
                // is resolved into a entities based on the (potential) parent object
                s::TypeDefinition::Object(t) => ctx
                    .resolver
                    .resolve_objects(
                        field_value,
                        field,
                        field_definition,
                        t.into(),
                        argument_values,
                    )
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
                    .resolve_objects(
                        field_value,
                        field,
                        field_definition,
                        t.into(),
                        argument_values,
                    )
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
    field: &q::Field,
    field_type: &s::Type,
    fields: &Vec<&q::Field>,
    resolved_value: q::Value,
) -> Result<q::Value, Vec<QueryExecutionError>> {
    match field_type {
        // Fail if the field type is non-null but the value is null
        s::Type::NonNullType(inner_type) => {
            return match complete_value(ctx, field, inner_type, fields, resolved_value)? {
                q::Value::Null => Err(vec![QueryExecutionError::NonNullError(
                    field.position,
                    field.name.to_string(),
                )]),

                v => Ok(v),
            };
        }

        // If the resolved value is null, return null
        _ if resolved_value == q::Value::Null => {
            return Ok(resolved_value);
        }

        // Complete list values
        s::Type::ListType(inner_type) => {
            match resolved_value {
                // Complete list values individually
                q::Value::List(mut values) => {
                    let mut errors = Vec::new();

                    // To avoid allocating a new vector this completes the values in place.
                    for value_place in &mut values {
                        // Put in a placeholder, complete the value, put the completed value back.
                        let value = std::mem::replace(value_place, q::Value::Null);
                        match complete_value(ctx, field, inner_type, fields, value) {
                            Ok(value) => {
                                *value_place = value;
                            }
                            Err(errs) => errors.extend(errs),
                        }
                    }
                    match errors.is_empty() {
                        true => Ok(q::Value::List(values)),
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
            let named_type = sast::get_named_type(ctx.query.schema.document(), name).unwrap();

            match named_type {
                // Complete scalar values
                s::TypeDefinition::Scalar(scalar_type) => {
                    resolved_value.coerce(scalar_type).map_err(|value| {
                        vec![QueryExecutionError::ScalarCoercionError(
                            field.position.clone(),
                            field.name.to_owned(),
                            value,
                            scalar_type.name.to_owned(),
                        )]
                    })
                }

                // Complete enum values
                s::TypeDefinition::Enum(enum_type) => {
                    resolved_value.coerce(enum_type).map_err(|value| {
                        vec![QueryExecutionError::EnumCoercionError(
                            field.position.clone(),
                            field.name.to_owned(),
                            value,
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
                s::TypeDefinition::Object(object_type) => execute_selection_set(
                    ctx,
                    fields.iter().map(|f| &f.selection_set),
                    object_type,
                    Some(resolved_value),
                ),

                // Resolve interface types using the resolved value and complete the value recursively
                s::TypeDefinition::Interface(_) => {
                    let object_type = resolve_abstract_type(ctx, named_type, &resolved_value)?;

                    execute_selection_set(
                        ctx,
                        fields.iter().map(|f| &f.selection_set),
                        object_type,
                        Some(resolved_value),
                    )
                }

                // Resolve union types using the resolved value and complete the value recursively
                s::TypeDefinition::Union(_) => {
                    let object_type = resolve_abstract_type(ctx, named_type, &resolved_value)?;

                    execute_selection_set(
                        ctx,
                        fields.iter().map(|f| &f.selection_set),
                        object_type,
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
    object_value: &q::Value,
) -> Result<&'a s::ObjectType, Vec<QueryExecutionError>> {
    // Let the resolver handle the type resolution, return an error if the resolution
    // yields nothing
    ctx.resolver
        .resolve_abstract_type(ctx.query.schema.document(), abstract_type, object_value)
        .ok_or_else(|| {
            vec![QueryExecutionError::AbstractTypeError(
                sast::get_type_name(abstract_type).to_string(),
            )]
        })
}

/// Coerces argument values into GraphQL values.
pub fn coerce_argument_values<'a>(
    query: &crate::execution::Query,
    ty: impl Into<ObjectOrInterface<'a>>,
    field: &q::Field,
) -> Result<HashMap<&'a str, q::Value>, Vec<QueryExecutionError>> {
    let mut coerced_values = HashMap::new();
    let mut errors = vec![];

    let resolver = |name: &str| sast::get_named_type(&query.schema.document(), name);

    for argument_def in sast::get_argument_definitions(ty, &field.name)
        .into_iter()
        .flatten()
    {
        let value = qast::get_argument_value(&field.arguments, &argument_def.name).cloned();
        match coercion::coerce_input_value(value, &argument_def, &resolver, &query.variables) {
            Ok(Some(value)) => {
                if argument_def.name == "text".to_string() {
                    coerced_values.insert(
                        argument_def.name.as_str(),
                        q::Value::Object(BTreeMap::from_iter(vec![(field.name.clone(), value)])),
                    );
                } else {
                    coerced_values.insert(&argument_def.name, value);
                }
            }
            Ok(None) => {}
            Err(e) => errors.push(e),
        }
    }

    if errors.is_empty() {
        Ok(coerced_values)
    } else {
        Err(errors)
    }
}
