//! Code generation for subgraph AssemblyScript types.
//!
//! This module generates AssemblyScript types from:
//! - GraphQL schema (entity classes)
//! - Contract ABIs (event and call bindings)
//! - Data source templates

mod schema;
mod types;
mod typescript;

pub use schema::SchemaCodeGenerator;
pub use typescript::{
    ArrayType, Class, ClassMember, Method, ModuleImports, NamedType, NullableType, Param,
    StaticMethod, GENERATED_FILE_NOTE,
};
