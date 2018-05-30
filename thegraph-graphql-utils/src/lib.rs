extern crate graphql_parser;
extern crate indexmap;
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate slog;
extern crate thegraph;

/// Utilities for working with query and schema ASTs.
pub mod ast;

/// Utilities for schema introspection.
pub mod introspection;

/// Utilities for executing GraphQL queries.
pub mod execution;

/// Mocks.
pub mod mocks;

mod coercion;
mod resolver;
mod serialize;

pub use self::coercion::{MaybeCoercible, MaybeCoercibleValue};
pub use self::resolver::Resolver;
pub use self::serialize::SerializableValue;
