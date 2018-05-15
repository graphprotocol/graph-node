extern crate futures;
extern crate graphql_parser;
#[macro_use]
extern crate slog;
extern crate thegraph;
extern crate tokio_core;

mod schema;

pub use schema::SchemaProvider;
