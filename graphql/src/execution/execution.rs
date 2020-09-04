use super::cache::QueryCache;
use crossbeam::atomic::AtomicCell;
use graph::prelude::CheapClone;
use graphql_parser::query as q;
use graphql_parser::schema as s;
use indexmap::IndexMap;
use lazy_static::lazy_static;
use stable_hash::crypto::SetHasher;
use stable_hash::prelude::*;
use stable_hash::utils::stable_hash;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::iter;
use std::sync::{Mutex, RwLock};
use std::time::Instant;

use graph::data::query::CacheStatus;
use graph::prelude::*;
use graph::util::lfu_cache::LfuCache;

use crate::introspection::{
    is_introspection_field, INTROSPECTION_DOCUMENT, INTROSPECTION_QUERY_TYPE,
};
use crate::prelude::*;
use crate::query::ast as qast;
use crate::schema::ast as sast;
use crate::values::coercion;

type QueryHash = <SetHasher as StableHasher>::Out;

#[derive(Debug)]
struct CacheByBlock {
    block: EthereumBlockPointer,
    max_weight: usize,
    weight: usize,
    cache: HashMap<QueryHash, Arc<QueryResult>>,
}

impl CacheByBlock {
    fn new(block: EthereumBlockPointer, max_weight: usize) -> Self {
        CacheByBlock {
            block,
            max_weight,
            weight: 0,
            cache: HashMap::new(),
        }
    }

    /// Returns `true` if the insert was successful or `false` if the cache was full.
    fn insert(&mut self, key: QueryHash, value: Arc<QueryResult>, weight: usize) -> bool {
        // We never try to insert errors into this cache, and always resolve some value.
        assert!(value.errors.is_none());
        let fits_in_cache = self.weight + weight <= self.max_weight;
        if fits_in_cache {
            self.weight += weight;
            self.cache.insert(key, value);
        }
        fits_in_cache
    }
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
            result: Arc::new(QueryResult::new(Some(q::Value::Null))),
            weight: 0,
        }
    }
}

/// Organize block caches by network names. Since different networks
/// will be at different block heights, we need to keep their `CacheByBlock`
/// separate
struct QueryBlockCache(Vec<(String, VecDeque<CacheByBlock>)>);

impl QueryBlockCache {
    fn insert(
        &mut self,
        network: &str,
        block_ptr: EthereumBlockPointer,
        key: QueryHash,
        result: Arc<QueryResult>,
        weight: usize,
    ) -> bool {
        // Get or insert the cache for this network.
        let cache = match self
            .0
            .iter_mut()
            .find(|(n, _)| n == network)
            .map(|(_, c)| c)
        {
            Some(c) => c,
            None => {
                self.0.push((network.to_owned(), VecDeque::new()));
                &mut self.0.last_mut().unwrap().1
            }
        };

        // If there is already a cache by the block of this query, just add it there.
        let mut cache_insert = false;
        if let Some(cache_by_block) = cache.iter_mut().find(|c| c.block == block_ptr) {
            cache_insert = cache_by_block.insert(key, result.cheap_clone(), weight);
        } else if *QUERY_CACHE_BLOCKS > 0 {
            // We're creating a new `CacheByBlock` if:
            // - There are none yet, this is the first query being cached, or
            // - `block_ptr` is of higher or equal number than the most recent block in the cache.
            // Otherwise this is a historical query that does not belong in
            // the block cache
            let should_insert = match cache.iter().next() {
                None => true,
                Some(highest) => highest.block.number <= block_ptr.number,
            };

            if should_insert {
                if cache.len() == *QUERY_CACHE_BLOCKS {
                    // At capacity, so pop the oldest block.
                    cache.pop_back();
                }

                // Create a new cache by block, insert this entry, and add it to the QUERY_CACHE.
                let max_weight = *QUERY_CACHE_MAX_MEM / *QUERY_CACHE_BLOCKS;
                let mut cache_by_block = CacheByBlock::new(block_ptr, max_weight);
                cache_insert = cache_by_block.insert(key, result.cheap_clone(), weight);
                cache.push_front(cache_by_block);
            }
        }
        cache_insert
    }

    fn get(
        &self,
        network: &str,
        block_ptr: &EthereumBlockPointer,
        key: &QueryHash,
    ) -> Option<Arc<QueryResult>> {
        if let Some(cache) = self.0.iter().find(|(n, _)| n == network).map(|(_, c)| c) {
            // Iterate from the most recent block looking for a block that matches.
            if let Some(cache_by_block) = cache.iter().find(|c| &c.block == block_ptr) {
                if let Some(response) = cache_by_block.cache.get(key) {
                    return Some(response.cheap_clone());
                }
            }
        }
        None
    }
}

lazy_static! {
    // Comma separated subgraph ids to cache queries for.
    // If `*` is present in the list, queries are cached for all subgraphs.
    // Defaults to "*".
    static ref CACHED_SUBGRAPH_IDS: Vec<String> = {
        std::env::var("GRAPH_CACHED_SUBGRAPH_IDS")
        .unwrap_or("*".to_string())
        .split(',')
        .map(|s| s.to_owned())
        .collect()
    };

    static ref CACHE_ALL: bool = CACHED_SUBGRAPH_IDS.contains(&"*".to_string());

    // How many blocks per network should be kept in the query cache. When the limit is reached,
    // older blocks are evicted. This should be kept small since a lookup to the cache is O(n) on
    // this value, and the cache memory usage also increases with larger number. Set to 0 to disable
    // the cache. Defaults to 1.
    static ref QUERY_CACHE_BLOCKS: usize = {
        std::env::var("GRAPH_QUERY_CACHE_BLOCKS")
        .unwrap_or("1".to_string())
        .parse::<usize>()
        .expect("Invalid value for GRAPH_QUERY_CACHE_BLOCKS environment variable")
    };

    /// Maximum total memory to be used by the cache. Each block has a max size of
    /// `QUERY_CACHE_MAX_MEM` / `QUERY_CACHE_BLOCKS`. The env var is in MB.
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

    // Cache query results for recent blocks by network.
    // The `VecDeque` works as a ring buffer with a capacity of `QUERY_CACHE_BLOCKS`.
    static ref QUERY_BLOCK_CACHE: RwLock<QueryBlockCache> = RwLock::new(QueryBlockCache(vec![]));
    static ref QUERY_HERD_CACHE: QueryCache<Arc<QueryResult>> = QueryCache::new();
    static ref QUERY_LFU_CACHE: Mutex<LfuCache<QueryHash, WeightedResult>> = Mutex::new(LfuCache::new());
}

struct HashableQuery<'a> {
    query_schema_id: &'a SubgraphDeploymentId,
    query_variables: &'a HashMap<q::Name, q::Value>,
    query_fragments: &'a HashMap<String, q::FragmentDefinition>,
    selection_set: &'a q::SelectionSet,
    block_ptr: &'a EthereumBlockPointer,
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
    block_ptr: &EthereumBlockPointer,
) -> QueryHash {
    // It is very important that all data used for the query is included.
    // Otherwise, incorrect results may be returned.
    let query = HashableQuery {
        query_schema_id: &ctx.query.schema.id,
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

    /// Records whether this was a cache hit, used for logging.
    pub(crate) cache_status: AtomicCell<CacheStatus>,
}

// Helpers to look for types and fields on both the introspection and regular schemas.
pub(crate) fn get_named_type(schema: &s::Document, name: &Name) -> Option<s::TypeDefinition> {
    if name.starts_with("__") {
        sast::get_named_type(&INTROSPECTION_DOCUMENT, name).cloned()
    } else {
        sast::get_named_type(schema, name).cloned()
    }
}

pub(crate) fn get_field<'a>(
    object_type: impl Into<ObjectOrInterface<'a>>,
    name: &Name,
) -> Option<s::Field> {
    if name == "__schema" || name == "__type" {
        let object_type = sast::get_root_query_type(&INTROSPECTION_DOCUMENT).unwrap();
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
        let introspection_resolver = IntrospectionResolver::new(&self.logger, &self.query.schema);

        ExecutionContext {
            logger: self.logger.cheap_clone(),
            resolver: introspection_resolver,
            query: self.query.as_introspection_query(),
            deadline: self.deadline,
            max_first: std::u32::MAX,
            cache_status: AtomicCell::new(CacheStatus::Miss),
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

    for (_, fields) in collect_fields(ctx, root_type, iter::once(selection_set), None) {
        let name = fields[0].name.clone();
        let selections = fields.into_iter().map(|f| q::Selection::Field(f.clone()));
        // See if this is an introspection or data field. We don't worry about
        // non-existent fields; those will cause an error later when we execute
        // the data_set SelectionSet
        if is_introspection_field(&name) {
            intro_set.items.extend(selections)
        } else {
            data_set.items.extend(selections)
        }
    }

    // If we are getting regular data, prefetch it from the database
    let mut values = if data_set.items.is_empty() {
        BTreeMap::default()
    } else {
        let initial_data = ctx.resolver.prefetch(&ctx, selection_set)?;
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
pub fn execute_root_selection_set<R: Resolver>(
    ctx: &ExecutionContext<R>,
    selection_set: &q::SelectionSet,
    root_type: &s::ObjectType,
    block_ptr: Option<EthereumBlockPointer>,
) -> Arc<QueryResult> {
    // Cache the cache key to not have to calculate it twice - once for lookup
    // and once for insert.
    let mut key: Option<QueryHash> = None;

    if R::CACHEABLE && (*CACHE_ALL || CACHED_SUBGRAPH_IDS.contains(&ctx.query.schema.id)) {
        if let (Some(block_ptr), Some(network)) = (block_ptr, &ctx.query.network) {
            // JSONB and metadata queries use `BLOCK_NUMBER_MAX`. Ignore this case for two reasons:
            // - Metadata queries are not cacheable.
            // - Caching `BLOCK_NUMBER_MAX` would make this cache think all other blocks are old.
            if block_ptr.number != BLOCK_NUMBER_MAX as u64 {
                // Calculate the hash outside of the lock
                let cache_key = cache_key(ctx, selection_set, &block_ptr);

                // Check if the response is cached, first in the recent blocks cache,
                // and then in the LfuCache for historical queries
                // The blocks are used to delimit how long locks need to be held
                {
                    let cache = QUERY_BLOCK_CACHE.read().unwrap();
                    if let Some(result) = cache.get(network, &block_ptr, &cache_key) {
                        ctx.cache_status.store(CacheStatus::Hit);
                        return result;
                    }
                }
                {
                    let mut cache = QUERY_LFU_CACHE.lock().unwrap();
                    if let Some(weighted) = cache.get(&cache_key) {
                        ctx.cache_status.store(CacheStatus::Hit);
                        return weighted.result.cheap_clone();
                    }
                }
                key = Some(cache_key);
            }
        }
    }

    let mut herd_hit = true;
    let mut run_query = || {
        herd_hit = false;
        Arc::new(QueryResult::from(execute_root_selection_set_uncached(
            ctx,
            selection_set,
            root_type,
        )))
    };
    let result = if let Some(key) = key {
        QUERY_HERD_CACHE.cached_query(key, run_query)
    } else {
        run_query()
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
        let weight = result.data.as_ref().unwrap().weight();
        let mut cache = QUERY_BLOCK_CACHE.write().unwrap();

        // Get or insert the cache for this network.
        if cache.insert(network, block_ptr, key, result.cheap_clone(), weight) {
            ctx.cache_status.store(CacheStatus::Insert);
        } else {
            // Results that are too old for the QUERY_BLOCK_CACHE go into the QUERY_LFU_CACHE
            let mut cache = QUERY_LFU_CACHE.lock().unwrap();
            cache.evict_with_period(*QUERY_CACHE_MAX_MEM, *QUERY_CACHE_STALE_PERIOD);
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
    let grouped_field_set = collect_fields(ctx, object_type, selection_sets, None);

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

        // If the field exists on the object, execute it and add its result to the result map
        if let Some(ref field) = sast::get_field(object_type, &fields[0].name) {
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
        } else {
            errors.push(QueryExecutionError::UnknownField(
                fields[0].position,
                object_type.name.clone(),
                fields[0].name.clone(),
            ))
        }
    }

    if errors.is_empty() && !result_map.is_empty() {
        Ok(result_map)
    } else {
        if errors.is_empty() {
            errors.push(QueryExecutionError::EmptySelectionSet(
                object_type.name.clone(),
            ));
        }
        Err(errors)
    }
}

/// Collects fields from selection sets.
pub fn collect_fields<'a>(
    ctx: &'a ExecutionContext<impl Resolver>,
    object_type: &s::ObjectType,
    selection_sets: impl Iterator<Item = &'a q::SelectionSet>,
    visited_fragments: Option<HashSet<&'a q::Name>>,
) -> IndexMap<&'a String, Vec<&'a q::Field>> {
    let mut visited_fragments = visited_fragments.unwrap_or_default();
    let mut grouped_fields: IndexMap<_, Vec<_>> = IndexMap::new();

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
                    // Obtain the response key for the field
                    let response_key = qast::get_response_key(field);

                    // Create a field group for this response key on demand and
                    // append the selection field to this group.
                    grouped_fields.entry(response_key).or_default().push(field);
                }

                q::Selection::FragmentSpread(spread) => {
                    // Only consider the fragment if it hasn't already been included,
                    // as would be the case if the same fragment spread ...Foo appeared
                    // twice in the same selection set
                    if !visited_fragments.contains(&spread.fragment_name) {
                        visited_fragments.insert(&spread.fragment_name);

                        // Resolve the fragment using its name and, if it applies, collect
                        // fields for the fragment and group them
                        ctx.query
                            .get_fragment(&spread.fragment_name)
                            .and_then(|fragment| {
                                // We have a fragment, only pass it on if it applies to the
                                // current object type
                                if does_fragment_type_apply(
                                    ctx,
                                    object_type,
                                    &fragment.type_condition,
                                ) {
                                    Some(fragment)
                                } else {
                                    None
                                }
                            })
                            .map(|fragment| {
                                // We have a fragment that applies to the current object type,
                                // collect its fields into response key groups
                                let fragment_grouped_field_set = collect_fields(
                                    ctx,
                                    object_type,
                                    iter::once(&fragment.selection_set),
                                    Some(visited_fragments.clone()),
                                );

                                // Add all items from each fragments group to the field group
                                // with the corresponding response key
                                for (response_key, mut fragment_group) in fragment_grouped_field_set
                                {
                                    grouped_fields
                                        .entry(response_key)
                                        .or_default()
                                        .append(&mut fragment_group);
                                }
                            });
                    }
                }

                q::Selection::InlineFragment(fragment) => {
                    let applies = match &fragment.type_condition {
                        Some(cond) => does_fragment_type_apply(ctx, object_type, &cond),
                        None => true,
                    };

                    if applies {
                        let fragment_grouped_field_set = collect_fields(
                            ctx,
                            object_type,
                            iter::once(&fragment.selection_set),
                            Some(visited_fragments.clone()),
                        );

                        for (response_key, mut fragment_group) in fragment_grouped_field_set {
                            grouped_fields
                                .entry(response_key)
                                .or_default()
                                .append(&mut fragment_group);
                        }
                    }
                }
            };
        }
    }

    grouped_fields
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
    let named_type = sast::get_named_type(&ctx.query.schema.document, name);

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
    coerce_argument_values(ctx, object_type, field)
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
    argument_values: &HashMap<&q::Name, q::Value>,
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
    type_name: &s::Name,
    argument_values: &HashMap<&q::Name, q::Value>,
) -> Result<q::Value, Vec<QueryExecutionError>> {
    // Try to resolve the type name into the actual type
    let named_type = sast::get_named_type(&ctx.query.schema.document, type_name)
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
    argument_values: &HashMap<&q::Name, q::Value>,
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
            let named_type = sast::get_named_type(&ctx.query.schema.document, type_name)
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
            let named_type = sast::get_named_type(&ctx.query.schema.document, name).unwrap();

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
        .resolve_abstract_type(&ctx.query.schema.document, abstract_type, object_value)
        .ok_or_else(|| {
            vec![QueryExecutionError::AbstractTypeError(
                sast::get_type_name(abstract_type).to_string(),
            )]
        })
}

/// Coerces argument values into GraphQL values.
pub fn coerce_argument_values<'a>(
    ctx: &ExecutionContext<impl Resolver>,
    object_type: &'a s::ObjectType,
    field: &q::Field,
) -> Result<HashMap<&'a q::Name, q::Value>, Vec<QueryExecutionError>> {
    let mut coerced_values = HashMap::new();
    let mut errors = vec![];

    let resolver = |name: &Name| sast::get_named_type(&ctx.query.schema.document, name);

    for argument_def in sast::get_argument_definitions(object_type, &field.name)
        .into_iter()
        .flatten()
    {
        let value = qast::get_argument_value(&field.arguments, &argument_def.name).cloned();
        match coercion::coerce_input_value(value, &argument_def, &resolver, &ctx.query.variables) {
            Ok(Some(value)) => {
                if argument_def.name == "text".to_string() {
                    coerced_values.insert(
                        &argument_def.name,
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
