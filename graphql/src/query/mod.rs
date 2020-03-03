use graph::prelude::*;
use graphql_parser::{query as q, Style};
use std::time::Instant;
use uuid::Uuid;

use crate::execution::*;
use crate::query::ast as qast;
use crate::schema::ast as sast;

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
pub fn execute_query<R>(query: Query, options: QueryExecutionOptions<R>) -> QueryResult
where
    R: Resolver,
{
    let query_id = Uuid::new_v4().to_string();
    let query_logger = options.logger.new(o!(
        "subgraph_id" => (*query.schema.id).clone(),
        "query_id" => query_id
    ));

    // Obtain the only operation of the query (fail if there is none or more than one)
    let operation = match qast::get_operation(&query.document, None) {
        Ok(op) => op,
        Err(e) => return QueryResult::from(e),
    };

    // Parse variable values
    let coerced_variable_values =
        match coerce_variable_values(&query.schema, operation, &query.variables) {
            Ok(values) => values,
            Err(errors) => return QueryResult::from(errors),
        };

    let mode = if let q::OperationDefinition::Query(query) = operation {
        if query.directives.iter().any(|dir| dir.name == "verify") {
            ExecutionMode::Verify
        } else {
            ExecutionMode::Prefetch
        }
    } else {
        ExecutionMode::Prefetch
    };

    // Create a fresh execution context
    let ctx = ExecutionContext {
        logger: query_logger.clone(),
        resolver: Arc::new(options.resolver),
        schema: query.schema.clone(),
        document: query.document.clone(),
        fields: vec![],
        variable_values: Arc::new(coerced_variable_values),
        deadline: options.deadline,
        max_first: options.max_first,
        block: BLOCK_NUMBER_MAX,
        mode,
    };

    let result = match operation {
        // Execute top-level `query { ... }` and `{ ... }` expressions.
        q::OperationDefinition::Query(q::Query { selection_set, .. })
        | q::OperationDefinition::SelectionSet(selection_set) => {
            let root_type = sast::get_root_query_type_def(&ctx.schema.document).unwrap();
            let validation_errors =
                ctx.validate_fields(&"Query".to_owned(), root_type, selection_set);
            if !validation_errors.is_empty() {
                return QueryResult::from(validation_errors);
            }

            let complexity = ctx.root_query_complexity(root_type, selection_set, options.max_depth);

            let start = Instant::now();
            let result =
                match (complexity, options.max_complexity) {
                    (Err(e), _) => Err(vec![e]),
                    (Ok(complexity), Some(max_complexity)) if complexity > max_complexity => Err(
                        vec![QueryExecutionError::TooComplex(complexity, max_complexity)],
                    ),
                    (Ok(_), _) => execute_root_selection_set(&ctx, selection_set),
                };
            info!(
                query_logger,
                "Query timing (GraphQL)";
                "query" => query.document.format(&Style::default().indent(0)).replace('\n', " "),
                "variables" => serde_json::to_string(&query.variables).unwrap_or_default(),
                "query_time_ms" => start.elapsed().as_millis(),
            );
            result
        }
        // Everything else (e.g. mutations) is unsupported
        _ => Err(vec![QueryExecutionError::NotSupported(
            "Only queries are supported".to_string(),
        )]),
    };

    match result {
        Ok(value) => QueryResult::new(Some(value)),
        Err(e) => QueryResult::from(e),
    }
}
