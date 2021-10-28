#[path = "dfuse.bstream.v1.rs"]
mod pbbstream;

pub mod endpoints;

pub mod bstream {
    pub use super::pbbstream::*;
}
