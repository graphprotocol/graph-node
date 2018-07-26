extern crate futures;
extern crate graphql_parser;
#[macro_use]
extern crate slog;
extern crate graph;
extern crate graph_core;
extern crate tokio_core;

mod data_sources;

pub use self::data_sources::DataSourceProvider;
