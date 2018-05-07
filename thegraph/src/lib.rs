extern crate futures;
extern crate graphql_parser;
extern crate hyper;
extern crate serde_json;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;
extern crate tokio;

/// Mock implementations for all system components.
pub mod mock;

/// Common traits and types for all system components.
pub mod common;

/// A prelude that makes all system component traits available.
///
/// Add the following code to import all traits listed below at once.
///
/// ```
/// use thegraph::prelude::*;
/// ```
pub mod prelude {
    pub use common::data_sources::DataSourceProvider;
    pub use common::query::QueryRunner;
    pub use common::schema::SchemaProvider;
    pub use common::server::GraphQLServer;
    pub use common::store::Store;
}
