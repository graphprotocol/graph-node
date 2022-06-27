#[rustfmt::skip]
#[path = "sf.cosmos.r#type.v1.rs"]
pub mod pbcodec;

//pub use graph::semver::Version;
pub use graph_runtime_wasm::asc_abi::class::{Array, AscEnum, AscString, Uint8Array};

pub use crate::runtime::utils::*;
pub use pbcodec::*;

#[graph_runtime_derive::generate_asc_type(__required__{max_age_duration: Duration})]
#[graph_runtime_derive::generate_network_type_id(Cosmos)]
#[graph_runtime_derive::generate_from_rust_type(__required__{max_age_duration: Duration})]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MyBlock {
    #[prost(int64, tag = "1")]
    pub max_age_num_blocks: i64,
    #[prost(message, optional, tag = "2")]
    pub max_age_duration: ::core::option::Option<Duration>,
    #[prost(int64, tag = "3")]
    pub max_bytes: i64,
}
