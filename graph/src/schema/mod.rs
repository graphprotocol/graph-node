/// Generate full-fledged API schemas from existing GraphQL schemas.
mod api;

/// Utilities for working with GraphQL schema ASTs.
pub mod ast;

mod fulltext;
mod input_schema;

pub use api::{api_schema, APISchemaError};

pub use api::{ApiSchema, ErrorPolicy};
pub use fulltext::{FulltextAlgorithm, FulltextConfig, FulltextDefinition, FulltextLanguage};
pub use input_schema::InputSchema;
