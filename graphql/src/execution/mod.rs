/// Implementation of the GraphQL execution algorithm.
mod execution;

/// Common trait for field resolvers used in the execution.
mod resolver;

pub use self::execution::*;
pub use self::resolver::Resolver;
