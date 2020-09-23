pub extern crate graphql_parser;

use graph::prelude::failure;

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

/// Prelude that exports the most important traits and types.
pub mod prelude {
    pub use super::execution::{ExecutionContext, Query, Resolver};
    pub use super::introspection::{introspection_schema, IntrospectionResolver};
    pub use super::query::{execute_query, ext::BlockConstraint, QueryExecutionOptions};
    pub use super::schema::{api_schema, ast::validate_entity, APISchemaError};
    pub use super::store::{build_query, StoreResolver};
    pub use super::subscription::{execute_subscription, SubscriptionExecutionOptions};
    pub use super::values::{object_value, IntoValue, MaybeCoercible};

    pub use super::graphql_parser::{query::Name, schema::ObjectType};
    pub use super::runner::GraphQlRunner;

    pub use crate::object;
}
