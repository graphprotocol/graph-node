use graph::prelude::{Query as GraphDataQuery, *};
use graphql_parser::{query as q, Style};
use std::time::Instant;
use uuid::Uuid;

use crate::execution::*;

/// Utilities for working with GraphQL query ASTs.
pub mod ast;

/// Extension traits
pub mod ext;

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

    /// Maximum complexity for a query.
    pub max_complexity: Option<u64>,

    /// Maximum depth for a query.
    pub max_depth: u8,

    /// Maximum value for the `first` argument.
    pub max_first: u32,
}

/// Executes a query and returns a result.
pub fn execute_query<R>(query: GraphDataQuery, options: QueryExecutionOptions<R>) -> QueryResult
where
    R: Resolver,
{
    match execute_query_inner(query, options) {
        Ok(v) => QueryResult::new(Some(v)),
        Err(errors) => QueryResult::from(errors),
    }
}

/// Executes a query and returns a result.
fn execute_query_inner<R>(
    query: GraphDataQuery,
    options: QueryExecutionOptions<R>,
) -> Result<q::Value, Vec<QueryExecutionError>>
where
    R: Resolver,
{
    let query_id = Uuid::new_v4().to_string();
    let query_logger = options.logger.new(o!(
        "subgraph_id" => (*query.schema.id).clone(),
        "query_id" => query_id
    ));

    let (query_text, variables_text) = if *graph::log::LOG_GQL_TIMING {
        (
            query
                .document
                .format(&Style::default().indent(0))
                .replace('\n', " "),
            serde_json::to_string(&query.variables).unwrap_or_default(),
        )
    } else {
        ("".to_owned(), "".to_owned())
    };

    let query = crate::execution::Query::new(query, options.max_complexity, options.max_depth)?;

    let mode = if query.verify {
        ExecutionMode::Verify
    } else {
        ExecutionMode::Prefetch
    };

    // Create a fresh execution context
    let ctx = ExecutionContext {
        logger: query_logger.clone(),
        resolver: Arc::new(options.resolver),
        query: query.clone(),
        fields: vec![],
        deadline: options.deadline,
        max_first: options.max_first,
        block: BLOCK_NUMBER_MAX,
        mode,
    };

    if !query.is_query() {
        return Err(vec![QueryExecutionError::NotSupported(
            "Only queries are supported".to_string(),
        )]);
    }

    // Execute top-level `query { ... }` and `{ ... }` expressions.
    let start = Instant::now();
    let result = execute_root_selection_set(&ctx, &query.selection_set);
    if *graph::log::LOG_GQL_TIMING {
        info!(
            query_logger,
            "Query timing (GraphQL)";
            "query" => query_text,
            "variables" => variables_text,
            "query_time_ms" => start.elapsed().as_millis(),
        );
    }
    result
}
