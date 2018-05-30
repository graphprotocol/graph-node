extern crate futures;
extern crate graphql_parser;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;
extern crate tokio;
extern crate tokio_core;
#[macro_use]
extern crate diesel;

/// Traits and types for all system components.
pub mod components;

/// Common data types used throughout The Graph.
pub mod data;

/// Utilities.
pub mod util;

/// A prelude that makes all system component traits and data types available.
///
/// Add the following code to import all traits and data types listed below at once.
///
/// ```
/// use thegraph::prelude::*;
/// ```
pub mod prelude {
    pub use components::data_sources::DataSourceProvider;
    pub use components::query::QueryRunner;
    pub use components::schema::SchemaProvider;
    pub use components::server::GraphQLServer;
    pub use components::store::{Store, StoreFilter, StoreOrder, StoreQuery, StoreRange};

    pub use data::query::{Query, QueryError, QueryExecutionError, QueryResult, QueryVariableValue,
                          QueryVariables};
    pub use data::schema::Schema;
    pub use data::store::{Attribute, Entity, Value};
}
