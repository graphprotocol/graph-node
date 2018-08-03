extern crate futures;
extern crate graphql_parser;
#[macro_use]
extern crate slog;
extern crate graph;
extern crate graph_core;

mod subgraph;

pub use self::subgraph::SubgraphProvider;
