extern crate failure;
extern crate futures;
extern crate graph;
extern crate graph_graphql;
extern crate graphql_parser;
extern crate rand;

mod block_stream;
mod store;

pub use self::block_stream::{MockBlockStream, MockBlockStreamBuilder};
pub use self::store::{FakeStore, MockStore};
