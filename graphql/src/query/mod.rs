use graph::prelude::*;
use graphql_parser::query as q;
use std::time::Instant;

use execution::*;
use prelude::*;
use query::ast as qast;

/// Utilities for working with GraphQL query ASTs.
pub mod ast;

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
}

/// Executes a query and returns a result.
pub fn execute_query<R>(query: &Query, options: QueryExecutionOptions<R>) -> QueryResult
where
    R: Resolver,
{
    info!(options.logger, "Execute query");

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

    // Create an introspection type store and resolver
    let introspection_schema = introspection_schema();
    let introspection_resolver = IntrospectionResolver::new(&options.logger, &query.schema);

    // Create a fresh execution context
    let ctx = ExecutionContext {
        logger: options.logger,
        resolver: Arc::new(options.resolver),
        schema: &query.schema,
        introspection_resolver: Arc::new(introspection_resolver),
        introspection_schema: &introspection_schema,
        introspecting: false,
        document: &query.document,
        fields: vec![],
        variable_values: Arc::new(coerced_variable_values),
        deadline: options.deadline,
    };

    let result = match *operation {
        // Execute top-level `query { ... }` expressions
        q::OperationDefinition::Query(q::Query {
            ref selection_set, ..
        }) => execute_root_selection_set(ctx, selection_set, &None),

        // Execute top-level `{ ... }` expressions
        q::OperationDefinition::SelectionSet(ref selection_set) => {
            execute_root_selection_set(ctx, selection_set, &None)
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
