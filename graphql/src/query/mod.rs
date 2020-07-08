use graph::prelude::{info, o, EthereumBlockPointer, Logger, QueryExecutionError, QueryResult};
use graphql_parser::query as q;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Instant;

use graph::data::graphql::effort::LoadManager;

use crate::execution::*;
use crate::schema::ast as sast;

/// Utilities for working with GraphQL query ASTs.
pub mod ast;

/// Extension traits
pub mod ext;

/// Options available for query execution.
pub struct QueryExecutionOptions<R> {
    /// The logger to use during query execution.
    pub logger: Logger,

    /// The resolver to use.
    pub resolver: R,

    /// Time at which the query times out.
    pub deadline: Option<Instant>,

    /// Maximum value for the `first` argument.
    pub max_first: u32,

    pub load_manager: Arc<LoadManager>,
}

/// Executes a query and returns a result.
/// If the query is not cacheable, the `Arc` may be unwrapped.
pub fn execute_query<R>(
    query: Arc<Query>,
    selection_set: Option<&q::SelectionSet>,
    block_ptr: Option<EthereumBlockPointer>,
    options: QueryExecutionOptions<R>,
) -> Arc<QueryResult>
where
    R: Resolver,
{
    let query_hash = {
        let mut hasher = DefaultHasher::new();
        query.query_text.hash(&mut hasher);
        hasher.finish()
    };
    let query_id = format!("{:x}-{:x}", query.shape_hash, query_hash);
    let query_logger = options.logger.new(o!(
        "subgraph_id" => (*query.schema.id).clone(),
        "query_id" => query_id
    ));

    // Create a fresh execution context
    let ctx = ExecutionContext {
        logger: query_logger.clone(),
        resolver: options.resolver,
        query: query.clone(),
        deadline: options.deadline,
        max_first: options.max_first,
        cache_status: Default::default(),
    };

    if !query.is_query() {
        return Arc::new(
            QueryExecutionError::NotSupported("Only queries are supported".to_string()).into(),
        );
    }
    let selection_set = selection_set.unwrap_or(&query.selection_set);

    // Obtain the root Query type and fail if there isn't one
    let query_type = match sast::get_root_query_type(&ctx.query.schema.document) {
        Some(t) => t,
        None => return Arc::new(QueryExecutionError::NoRootQueryObjectType.into()),
    };

    // Execute top-level `query { ... }` and `{ ... }` expressions.
    let start = Instant::now();
    let result = execute_root_selection_set(&ctx, selection_set, query_type, block_ptr);
    let elapsed = start.elapsed();
    options.load_manager.add_query(query.shape_hash, elapsed);
    if *graph::log::LOG_GQL_TIMING {
        info!(
            query_logger,
            "Query timing (GraphQL)";
            "query" => &query.query_text,
            "variables" => &query.variables_text,
            "query_time_ms" => elapsed.as_millis(),
            "cached" => ctx.cache_status.load().to_string(),
            "block" => block_ptr.map(|b| b.number).unwrap_or(0),
            "complexity" => &query.complexity
        );
    }
    result
}
