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





#[graph_runtime_derive::generate_asc_type(
    one{duplicate_vote_evidence: DuplicateVoteEvidence,light_client_attack_evidence: LightClientAttackEvidence}
)]  //AscType
//#[graph_runtime_derive::generate_network_type_id(Cosmos)]
//#[graph_runtime_derive::generate_from_rust_type()]  //impl ToAscObj<#asc_name> for #name
pub struct MyConsensus {
    pub sum: ::core::option::Option<evidence::Sum>,
}

impl ToAscObj<AscMyConsensus> for MyConsensus {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscMyConsensus, DeterministicHostError> {
        use codec::evidence::Sum;

        let sum = self
            .sum
            .as_ref()
            .ok_or_else(|| missing_field_error("Evidence", "sum"))?;

        let (duplicate_vote_evidence, light_client_attack_evidence) = match sum {
            Sum::DuplicateVoteEvidence(d) => (asc_new(heap, d, gas)?, AscPtr::null()),
            Sum::LightClientAttackEvidence(l) => (AscPtr::null(), asc_new(heap, l, gas)?),
        };

        Ok(AscMyConsensus {
            duplicate_vote_evidence:      if let Sum::DuplicateVoteEvidence(v) = self.sum.as_ref().unwrap() {asc_new(heap, v, gas)? } else {AscPtr::null()},
            light_client_attack_evidence: if let Sum::LightClientAttackEvidence(v) = self.sum.as_ref().unwrap() { asc_new(heap, v, gas)? } else {AscPtr::null()} 
        })
    }
}
