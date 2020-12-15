#[cfg(debug_assertions)]
#[macro_use]
extern crate diesel;

#[cfg(debug_assertions)]
pub mod block_store;
#[cfg(debug_assertions)]
pub mod store;
#[cfg(debug_assertions)]
pub use crate::store::*;
