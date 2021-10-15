use crate::trigger::TendermintBlockData;
use graph::prelude::BigInt;
use graph::runtime::{asc_new, AscPtr, DeterministicHostError, ToAscObj};
use graph::runtime::{AscHeap, AscIndexId, AscType, IndexForAscTypeId};
use graph::semver;
use graph::{semver::Version};
use graph_runtime_derive::AscType;
use graph_runtime_wasm::asc_abi::class::{AscBigInt, Uint8Array};


use std::mem::size_of;

type AscHash = Uint8Array;

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscTendermintBlock {
    pub hash: AscPtr<AscHash>,
    pub parent_hash: AscPtr<AscHash>,
    pub number: AscPtr<AscBigInt>,
    pub time_sec: AscPtr<AscBigInt>,
    pub time_nano: AscPtr<AscBigInt>,
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
            hash: asc_new(heap, self.hash.as_bytes())?,
            number: asc_new(heap, &BigInt::from(self.number))?,
            time_sec: asc_new(heap, &BigInt::from(self.time_sec))?,
            time_nano: asc_new(heap, &BigInt::from(self.time_nano))?,
            parent_hash: self
                .parent_hash
                .map(|parent_hash| asc_new(heap, parent_hash.as_bytes()))
                .unwrap_or(Ok(AscPtr::null()))?,
        })
    }
}
