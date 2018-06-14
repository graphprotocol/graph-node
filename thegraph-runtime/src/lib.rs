extern crate futures;
#[macro_use]
extern crate slog;
extern crate thegraph;
extern crate tokio_core;

mod adapter;

pub use self::adapter::{RuntimeAdapter, RuntimeAdapterConfig};
