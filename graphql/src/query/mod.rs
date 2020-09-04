use graph::prelude::{EthereumBlockPointer, QueryExecutionError, QueryResult};
use graphql_parser::query as q;
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
    // Create a fresh execution context
    let ctx = ExecutionContext {
        logger: query.logger.clone(),
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

    // Obtain the root Query type and fail if there isn't one
    let query_type = match sast::get_root_query_type(&ctx.query.schema.document) {
        Some(t) => t,
        None => return Arc::new(QueryExecutionError::NoRootQueryObjectType.into()),
    };

    // Execute top-level `query { ... }` and `{ ... }` expressions.
    let start = Instant::now();
    let result = execute_root_selection_set(
        &ctx,
        selection_set.unwrap_or(&query.selection_set),
        query_type,
        block_ptr,
    );
    let elapsed = start.elapsed();
    let cache_status = ctx.cache_status.load();
    options
        .load_manager
        .record_work(query.shape_hash, elapsed, cache_status);
    query.log_cache_status(
        selection_set,
        block_ptr.map(|b| b.number).unwrap_or(0),
        start,
        cache_status.to_string(),
    );
    result
}
