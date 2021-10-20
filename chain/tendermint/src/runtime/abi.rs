/*use graph::prelude::BigInt;
use graph::runtime::{asc_new, AscPtr, DeterministicHostError, ToAscObj};
use graph::runtime::{AscHeap, AscIndexId, AscType, IndexForAscTypeId};
use graph::semver;
use graph::{semver::Version};
use graph_runtime_derive::AscType;
use graph_runtime_wasm::asc_abi::class::{AscBigInt, Uint8Array};


use std::mem::size_of;

type AscHash = Uint8Array;
*/

#[path = "../protobuf/fig.tendermint.codec.v1.rs"]
mod pbcodec;

use crate::codec;
use graph::runtime::{asc_new, DeterministicHostError, ToAscObj};
use graph::runtime::{AscHeap, AscPtr};
use graph_runtime_wasm::asc_abi::class::{Array, AscEnum, EnumPayload, Uint8Array};

pub(crate) use super::generated::*;


impl ToAscObj<AscEventList> for codec::EventList {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEventList, DeterministicHostError> {
        Ok(AscEventList {
            newblock: asc_new(heap, self.newblock.as_ref().unwrap())?,
            evidence: asc_new(heap, self.evidence.as_ref().unwrap())?,
            transaction: asc_new(heap, self.transaction.as_ref().unwrap())?,
            vote: asc_new(heap, self.vote.as_ref().unwrap())?,
            roundstate: asc_new(heap, self.roundstate.as_ref().unwrap())?,
            newround: asc_new(heap, self.newround.as_ref().unwrap())?,
            completeproposal: asc_new(heap, self.completeproposal.as_ref().unwrap())?,
            validatorsetupdates: asc_new(heap, self.validatorsetupdates.as_ref().unwrap())?,
            string: asc_new(heap, self.string.as_ref().unwrap())?,
            blocksyncstatus: asc_new(heap, self.blocksyncstatus.as_ref().unwrap())?,
            statesyncstatus: asc_new(heap, self.statesyncstatus.as_ref().unwrap())?,
        })
    }
}



impl ToAscObj<AscEventDataNewBlock> for codec::EventDataNewBlock {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEventDataNewBlock, DeterministicHostError> {
        Ok(AscEventDataNewBlock {
            block: asc_new(heap, self.block.as_ref().unwrap())?,
            block_id: asc_new(heap, self.block_id.as_ref().unwrap())?,
            result_begin_block: asc_new(heap, self.result_begin_block.as_ref().unwrap())?,
            result_end_block: asc_new(heap, self.result_end_block.as_ref().unwrap())?,
        })
    }
}


impl ToAscObj<AscBlock> for codec::pbcodec::Block {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscBlock, DeterministicHostError> {
        Ok(AscBlock {
            header: asc_new(heap, self.header.as_ref().unwrap())?,
            data: asc_new(heap, self.data.as_ref().unwrap())?,
            evidence: asc_new(heap, self.evidence.as_ref().unwrap())?,
            last_commit: asc_new(heap, self.last_commit.as_ref().unwrap())?,
        })
    }
}

//&Bytes(

impl ToAscObj<AscHeader> for codec::Header {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscHeader, DeterministicHostError> {
        Ok(AscHeader {
            version: asc_new(heap, self.version.as_ref().unwrap())?,
            chain_id:  asc_new(heap, &self.chain_id)?,
            height: self.height,
            time: asc_new(heap, self.time.as_ref().unwrap())?,
            last_block_id: asc_new(heap, self.last_block_id.as_ref().unwrap())?,
            last_commit_hash:  asc_new(heap, self.last_commit_hash.as_ref().unwrap())?,
            data_hash:  asc_new(heap, self.data_hash.as_ref().unwrap())?,
            validators_hash:  asc_new(heap, self.validators_hash.as_ref().unwrap())?,
            next_validators_hash:  asc_new(heap, self.next_validators_hash.as_ref().unwrap())?,
            consensus_hash: asc_new(heap, self.consensus_hash.as_ref().unwrap())?,
            app_hash: asc_new(heap, self.app_hash.as_ref().unwrap())?,
            last_results_hash: asc_new(heap, self.last_results_hash.as_ref().unwrap())?,
            evidence_hash: asc_new(heap, self.evidence_hash.as_ref().unwrap())?,
            proposer_address: asc_new(heap, self.proposer_address.as_ref().unwrap())?,
        })
    }
}

impl ToAscObj<AscTimestamp> for codec::Timestamp {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscTimestamp, DeterministicHostError> {
        Ok(AscTimestamp {
            seconds: self.seconds,
            nanos: self.nanos,
        })
    }
}

impl ToAscObj<AscBlockID> for codec::BlockId {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscBlockID, DeterministicHostError> {
        Ok(AscBlockID {
            hash: asc_new(heap, self.hash.as_ref().unwrap())?,
            part_set_header: asc_new(heap, self.part_set_header.as_ref().unwrap())?,
        })
    }
}


impl ToAscObj<AscPartSetHeader> for codec::PartSetHeader {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscPartSetHeader, DeterministicHostError> {
        Ok(AscPartSetHeader {
            total:  self.total,
            hash: asc_new(heap, self.hash.as_ref().unwrap())?,
        })
    }
}


impl ToAscObj<AscConsensus> for codec::Consensus {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscConsensus, DeterministicHostError> {
        Ok(AscConsensus {
            block: self.block,
            app: self.app,
        })
    }
}

impl ToAscObj<AscData> for codec::Data {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscData, DeterministicHostError> {
        Ok(AscData {
            txs: asc_new(heap, self.txs.as_ref().unwrap())?,
        })
    }
}

impl ToAscObj<AscEvidenceList> for codec::EvidenceList {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEvidenceList, DeterministicHostError> {
        Ok(AscEvidenceList {
            evidence: asc_new(heap, self.evidence.as_ref().unwrap())?,
        })
    }
}


impl ToAscObj<AscResponseBeginBlock> for codec::ResponseBeginBlock {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscResponseBeginBlock, DeterministicHostError> {
        Ok(AscResponseBeginBlock {
            events: asc_new(heap, self.events.as_ref().unwrap())?,
        })
    }
}

impl ToAscObj<AscEvent> for codec::Event {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEvent, DeterministicHostError> {
        Ok(AscEvent {
            eventtype: self.r#type;
            attributes: todo!(),
        })
    }
}
impl ToAscObj<AscEventArray> for Vec<codec::Event> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEventArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x)).collect();
        let content = content?;
        Ok(AscEventArray(Array::new(&*content, heap)?))
    }
}
