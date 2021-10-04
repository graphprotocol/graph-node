use crate::trigger::TendermintBlockData;
use graph::prelude::BigInt;
use graph::runtime::{asc_new, AscPtr, DeterministicHostError, ToAscObj};
use graph::runtime::{AscHeap, AscIndexId, AscType, IndexForAscTypeId};
use graph::semver;
use graph_runtime_derive::AscType;
use graph_runtime_wasm::asc_abi::class::{AscBigInt, Uint8Array};
use std::mem::size_of;

type AscH256 = Uint8Array;

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscTendermintBlock {
    pub hash: AscPtr<AscH256>,
    pub parent_hash: AscPtr<AscH256>,
    pub number: AscPtr<AscBigInt>,
    pub timestamp: AscPtr<AscBigInt>,
}

impl AscIndexId for AscTendermintBlock {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::EthereumBlock;
}

impl ToAscObj<AscTendermintBlock> for TendermintBlockData {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscTendermintBlock, DeterministicHostError> {
        Ok(AscTendermintBlock {
            hash: asc_new(heap, &self.hash)?,
            number: asc_new(heap, &BigInt::from(self.number))?,
            timestamp: asc_new(heap, &BigInt::from(self.timestamp))?,
            parent_hash: self
                .parent_hash
                .map(|parent_hash| asc_new(heap, &parent_hash))
                .unwrap_or(Ok(AscPtr::null()))?,
        })
    }
}
