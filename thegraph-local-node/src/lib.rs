extern crate futures;
extern crate graphql_parser;
#[macro_use]
extern crate slog;
extern crate thegraph;
extern crate thegraph_core;
extern crate tokio_core;

mod data_sources;

pub use self::data_sources::DataSourceProvider;
