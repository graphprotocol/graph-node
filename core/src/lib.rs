extern crate futures;
extern crate graphql_parser;
#[macro_use]
extern crate slog;
extern crate serde;
extern crate serde_yaml;
extern crate graph;
extern crate graph_graphql;
extern crate graph_runtime_wasm;
extern crate tokio_core;

mod data_sources;
mod query;
mod schema;

pub use data_sources::RuntimeManager;
pub use query::QueryRunner;
pub use schema::SchemaProvider;
