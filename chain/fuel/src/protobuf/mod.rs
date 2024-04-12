#[rustfmt::skip]
#[path = "sf.fuel.r#type.v1.rs"]
pub mod pbcodec;

pub use graph_runtime_wasm::asc_abi::class::{Array, Uint8Array};

pub use crate::runtime::abi::*;
pub use pbcodec::*;
