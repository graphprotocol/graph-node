pub extern crate graphql_parser;

/// Utilities for working with GraphQL schemas.
pub mod schema;

/// Utilities for schema introspection.
pub mod introspection;

/// Utilities for executing GraphQL.
mod execution;

/// Utilities for executing GraphQL queries and working with query ASTs.
pub mod query;

/// Utilities for executing GraphQL subscriptions.
pub mod subscription;

/// Utilities for working with GraphQL values.
mod values;

/// Utilities for querying `Store` components.
mod store;

/// The external interface for actually running queries
mod runner;

/// Utilities for working with Prometheus.
mod metrics;

/// Prelude that exports the most important traits and types.
pub mod prelude {
    pub use graph::prelude::s::ObjectType;

    pub use super::execution::{ast as a, ExecutionContext, Query, Resolver};
    pub use super::introspection::IntrospectionResolver;
    pub use super::metrics::GraphQLMetrics;
    pub use super::query::ext::BlockConstraint;
    pub use super::query::{execute_query, QueryExecutionOptions};
    pub use super::runner::GraphQlRunner;
    pub use super::schema::{api_schema, APISchemaError};
    pub use super::store::StoreResolver;
    pub use super::subscription::SubscriptionExecutionOptions;
    pub use super::values::MaybeCoercible;
}

#[cfg(debug_assertions)]
pub mod test_support {
    pub use super::metrics::GraphQLMetrics;
    pub use super::runner::INITIAL_DEPLOYMENT_STATE_FOR_TESTS;
}
