#[path = "dfuse.bstream.v1.rs"]
mod pbbstream;

pub mod endpoints;
mod helpers;

pub mod bstream {
    pub use super::pbbstream::*;
}

pub use helpers::decode_firehose_block;
