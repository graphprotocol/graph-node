extern crate futures;
#[macro_use]
extern crate slog;
extern crate parity_wasm;
extern crate thegraph;
extern crate tokio_core;
extern crate wasmi;

mod adapter;
mod interpreter;

pub use self::adapter::{RuntimeAdapter, RuntimeAdapterConfig};
