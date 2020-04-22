use graph::prelude::{Query as GraphDataQuery, *};
use graphql_parser::Style;
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

    let query = match crate::execution::Query::new(query) {
        Ok(query) => query,
        Err(e) => return QueryResult::from(e),
    };

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
        return QueryResult::from(vec![QueryExecutionError::NotSupported(
            "Only queries are supported".to_string(),
        )]);
    }

    // Execute top-level `query { ... }` and `{ ... }` expressions.
    let validation_errors = query.validate_fields();
    if !validation_errors.is_empty() {
        return QueryResult::from(validation_errors);
    }

    let complexity = query.complexity(options.max_depth);

    let start = Instant::now();
    let result = match (complexity, options.max_complexity) {
        (Err(e), _) => Err(vec![e]),
        (Ok(complexity), Some(max_complexity)) if complexity > max_complexity => {
            Err(vec![QueryExecutionError::TooComplex(
                complexity,
                max_complexity,
            )])
        }
        (Ok(_), _) => execute_root_selection_set(&ctx, &query.selection_set),
    };
    if *graph::log::LOG_GQL_TIMING {
        info!(
            query_logger,
            "Query timing (GraphQL)";
            "query" => query_text,
            "variables" => variables_text,
            "query_time_ms" => start.elapsed().as_millis(),
        );
    }

    match result {
        Ok(value) => QueryResult::new(Some(value)),
        Err(e) => QueryResult::from(e),
    }
}
