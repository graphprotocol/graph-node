#[rustfmt::skip]
#[path = "sf.cosmos.r#type.v1.rs"]
pub mod pbcodec;


pub use graph_runtime_derive::AscType;
pub use graph::runtime::{
     AscIndexId, AscPtr, AscType, AscValue, DeterministicHostError, IndexForAscTypeId,
    asc_new, gas::GasCounter, AscHeap,
    ToAscObj,

};
pub use graph::semver::Version;
pub use graph_runtime_wasm::asc_abi::class::{Array, AscEnum, Uint8Array};

pub use crate::runtime::generated::*;
pub use pbcodec::*;
//use graph_runtime_wasm::asc_abi::v0_0_5::*;
pub use graph_runtime_wasm::asc_abi::class::*;

#[derive(graph_runtime_derive::ToAscType)]
#[graph_runtime_derive::generate_network_type_id(Cosmos)]
#[graph_runtime_derive::generate_from_rust_type()]
pub struct Consensus {
    pub block: u64,
    pub app: u64,
}

#[cfg(test)]
mod x {
    use super::pbcodec::*;

    #[test]
    fn x(){
        let b = super::pbcodec::Timestamp{
            seconds:0,
            nanos:0
        };
   }
}