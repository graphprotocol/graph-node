/// Generate a merged schema from a schema and a set of imported schemas
pub mod merge;

/// Generate full-fledged API schemas from existing GraphQL schemas.
pub mod api;

/// Utilities for working with GraphQL schema ASTs.
pub mod ast;

pub use self::api::{api_schema, APISchemaError};
