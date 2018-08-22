use graphql_parser::query as q;

use graph::prelude::*;

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
    pub logger: slog::Logger,
    /// The resolver to use.
    pub resolver: R,
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
    };

    match operation {
        // Execute top-level `query { ... }` expressions
        &q::OperationDefinition::Query(q::Query {
            ref selection_set, ..
        }) => execute_root_selection_set(ctx, selection_set, &None),

        // Execute top-level `{ ... }` expressions
        &q::OperationDefinition::SelectionSet(ref selection_set) => {
            execute_root_selection_set(ctx, selection_set, &None)
        }

        // Everything else (e.g. mutations) is unsupported
        _ => QueryResult::from(QueryExecutionError::NotSupported(
            "Only queries are supported".to_string(),
        )),
    }
}
