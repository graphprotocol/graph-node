use graph::prelude::{info, o, EthereumBlockPointer, Logger, QueryExecutionError};
use graphql_parser::query as q;
use std::collections::BTreeMap;
use std::sync::{atomic::AtomicBool, Arc};
use std::time::Instant;
use uuid::Uuid;

use crate::execution::*;
use crate::schema::ast as sast;

/// Utilities for working with GraphQL query ASTs.
pub mod ast;

/// Extension traits
pub mod ext;

/// Hashing the 'shape' of a query
pub mod shape_hash;

/// Options available for query execution.
pub struct QueryExecutionOptions<R>
where
    R: Resolver,
{
    /// The logger to use during query execution.
    pub logger: Logger,

    /// The resolver to use.
    pub resolver: R,

    /// Time at which the query times out.
    pub deadline: Option<Instant>,

    /// Maximum value for the `first` argument.
    pub max_first: u32,
}

/// Executes a query and returns a result.
pub fn execute_query<R>(
    query: Arc<Query>,
    selection_set: Option<&q::SelectionSet>,
    block_ptr: Option<EthereumBlockPointer>,
    options: QueryExecutionOptions<R>,
) -> Result<BTreeMap<String, q::Value>, Vec<QueryExecutionError>>
where
    R: Resolver,
{
    let query_id = Uuid::new_v4().to_string();
    let query_logger = options.logger.new(o!(
        "subgraph_id" => (*query.schema.id).clone(),
        "query_id" => query_id
    ));

    // Create a fresh execution context
    let ctx = ExecutionContext {
        logger: query_logger.clone(),
        resolver: Arc::new(options.resolver),
        query: query.clone(),
        deadline: options.deadline,
        max_first: options.max_first,
        cached: AtomicBool::new(false),
    };

    if !query.is_query() {
        return Err(vec![QueryExecutionError::NotSupported(
            "Only queries are supported".to_string(),
        )]);
    }
    let selection_set = selection_set.unwrap_or(&query.selection_set);

    // Obtain the root Query type and fail if there isn't one
    let query_type = match sast::get_root_query_type(&ctx.query.schema.document) {
        Some(t) => t,
        None => return Err(vec![QueryExecutionError::NoRootQueryObjectType]),
    };

    // Execute top-level `query { ... }` and `{ ... }` expressions.
    let start = Instant::now();
    let result = execute_root_selection_set(&ctx, selection_set, query_type, block_ptr);
    if *graph::log::LOG_GQL_TIMING {
        info!(
            query_logger,
            "Query timing (GraphQL)";
            "query" => &query.query_text,
            "variables" => &query.variables_text,
            "query_time_ms" => start.elapsed().as_millis(),
            "cached" => ctx.cached.load(std::sync::atomic::Ordering::SeqCst),
            "block" => block_ptr.map(|b| b.number).unwrap_or(0),
            "complexity" => &query.complexity
        );
    }
    result
}
