#[path = "../protobuf/fig.tendermint.codec.v1.rs"]
mod pbcodec;

use crate::codec::{self};
use graph::prelude::BigInt;
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
            transaction: asc_new(heap, &self.transaction)?,
            vote: self
                .vote
                .clone()
                .map(|d| asc_new(heap, &d))
                .unwrap_or(Ok(AscPtr::null()))?,
            roundstate: self
                .roundstate
                .clone()
                .map(|d| asc_new(heap, &d))
                .unwrap_or(Ok(AscPtr::null()))?,
            newround: self
                .newround
                .clone()
                .map(|d| asc_new(heap, &d))
                .unwrap_or(Ok(AscPtr::null()))?,
            completeproposal: self
                .completeproposal
                .clone()
                .map(|d| asc_new(heap, &d))
                .unwrap_or(Ok(AscPtr::null()))?,
            validatorsetupdates: self
                .validatorsetupdates
                .clone()
                .map(|d| asc_new(heap, &d))
                .unwrap_or(Ok(AscPtr::null()))?,
            eventdatastring: self
                .eventdatastring
                .clone()
                .map(|d| asc_new(heap, &d))
                .unwrap_or(Ok(AscPtr::null()))?,
            blocksyncstatus: self
                .blocksyncstatus
                .clone()
                .map(|d| asc_new(heap, &d))
                .unwrap_or(Ok(AscPtr::null()))?,
            statesyncstatus: self
                .statesyncstatus
                .clone()
                .map(|d| asc_new(heap, &d))
                .unwrap_or(Ok(AscPtr::null()))?,
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
            result_begin_block: self
                .result_begin_block
                .clone()
                .map(|d| asc_new(heap, &d))
                .unwrap_or(Ok(AscPtr::null()))?,
            result_end_block: self
                .result_end_block
                .clone()
                .map(|d| asc_new(heap, &d))
                .unwrap_or(Ok(AscPtr::null()))?,
        })
    }
}

impl ToAscObj<AscBlock> for codec::Block {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscBlock, DeterministicHostError> {
        Ok(AscBlock {
            header: asc_new(heap, self.header.as_ref().unwrap())?,
            data: self
                .data
                .clone()
                .map(|d| asc_new(heap, &d))
                .unwrap_or(Ok(AscPtr::null()))?,
            evidence: self
                .evidence
                .clone()
                .map(|d| asc_new(heap, &d))
                .unwrap_or(Ok(AscPtr::null()))?,
            last_commit: asc_new(heap, self.last_commit.as_ref().unwrap())?,
        })
    }
}

impl ToAscObj<AscHeader> for codec::Header {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscHeader, DeterministicHostError> {
        Ok(AscHeader {
            version: asc_new(heap, self.version.as_ref().unwrap())?,
            chain_id: asc_new(heap, &self.chain_id)?,
            height: asc_new(heap, &BigInt::from(self.height))?,
            time: asc_new(heap, self.time.as_ref().unwrap())?,
            last_block_id: asc_new(heap, self.last_block_id.as_ref().unwrap())?,
            last_commit_hash: asc_new(heap, &Bytes(&self.last_commit_hash))?,
            data_hash: asc_new(heap, &Bytes(&self.data_hash))?,
            validators_hash: asc_new(heap, &Bytes(&self.validators_hash))?,
            next_validators_hash: asc_new(heap, &Bytes(&self.next_validators_hash))?,
            consensus_hash: asc_new(heap, &Bytes(&self.consensus_hash))?,
            app_hash: asc_new(heap, &Bytes(&self.app_hash))?,
            last_results_hash: asc_new(heap, &Bytes(&self.last_results_hash))?,
            evidence_hash: asc_new(heap, &Bytes(&self.evidence_hash))?,
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
            seconds: asc_new(heap, &BigInt::from(self.seconds))?,
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
            hash: asc_new(heap, &Bytes(&self.hash))?,
            part_set_header: self
                .part_set_header
                .clone()
                .map(|d| asc_new(heap, &d))
                .unwrap_or(Ok(AscPtr::null()))?,
        })
    }
}

impl ToAscObj<AscPartSetHeader> for codec::PartSetHeader {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscPartSetHeader, DeterministicHostError> {
        Ok(AscPartSetHeader {
            total: self.total,
            hash: asc_new(heap, &Bytes(&self.hash))?,
        })
    }
}

impl ToAscObj<AscConsensus> for codec::Consensus {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscConsensus, DeterministicHostError> {
        Ok(AscConsensus {
            block: asc_new(heap, &BigInt::from(self.block))?,
            app: asc_new(heap, &BigInt::from(self.app))?,
        })
    }
}

impl ToAscObj<AscData> for codec::Data {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscData, DeterministicHostError> {
        Ok(AscData {
            txs: asc_new(heap, &self.txs)?,
        })
    }
}
impl ToAscObj<AscBytesArray> for Vec<Vec<u8>> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscBytesArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, &Bytes(&x))).collect();
        let content = content?;
        Ok(AscBytesArray(Array::new(&*content, heap)?))
    }
}

impl ToAscObj<AscEvidenceList> for codec::EvidenceList {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEvidenceList, DeterministicHostError> {
        Ok(AscEvidenceList {
            evidence: asc_new(heap, &self.evidence)?,
        })
    }
}

impl ToAscObj<AscResponseBeginBlock> for codec::ResponseBeginBlock {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscResponseBeginBlock, DeterministicHostError> {
        Ok(AscResponseBeginBlock {
            events: asc_new(heap, &self.events)?,
        })
    }
}

impl ToAscObj<AscEvent> for codec::Event {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEvent, DeterministicHostError> {
        Ok(AscEvent {
            eventtype: asc_new(heap, &self.eventtype)?,
            attributes: asc_new(heap, &self.attributes)?,
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

impl ToAscObj<AscEventAttribute> for codec::EventAttribute {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEventAttribute, DeterministicHostError> {
        Ok(AscEventAttribute {
            key: asc_new(heap, &self.key)?,
            value: asc_new(heap, &self.value)?,
            // index:  self.index.clone(),  TODO(l): seems broken ?
        })
    }
}

impl ToAscObj<AscEventAttributeArray> for Vec<codec::EventAttribute> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEventAttributeArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x)).collect();
        let content = content?;
        Ok(AscEventAttributeArray(Array::new(&*content, heap)?))
    }
}

impl ToAscObj<AscResponseEndBlock> for codec::ResponseEndBlock {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscResponseEndBlock, DeterministicHostError> {
        Ok(AscResponseEndBlock {
            events: asc_new(heap, &self.events)?,
            validator_updates: asc_new(heap, &self.validator_updates)?,
            consensus_param_updates: asc_new(heap, self.consensus_param_updates.as_ref().unwrap())?,
        })
    }
}

impl ToAscObj<AscConsensusParams> for codec::ConsensusParams {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscConsensusParams, DeterministicHostError> {
        Ok(AscConsensusParams {
            block: self
                .block
                .clone()
                .map(|d| asc_new(heap, &d))
                .unwrap_or(Ok(AscPtr::null()))?,
            evidence: self
                .evidence
                .clone()
                .map(|d| asc_new(heap, &d))
                .unwrap_or(Ok(AscPtr::null()))?,
            validator: self
                .validator
                .clone()
                .map(|d| asc_new(heap, &d))
                .unwrap_or(Ok(AscPtr::null()))?,
            version: self
                .version
                .clone()
                .map(|d| asc_new(heap, &d))
                .unwrap_or(Ok(AscPtr::null()))?,
        })
    }
}

impl ToAscObj<AscVersion> for codec::Version {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscVersion, DeterministicHostError> {
        Ok(AscVersion {
            app_version: asc_new(heap, &BigInt::from(self.app_version))?,
        })
    }
}

impl ToAscObj<AscCommit> for codec::Commit {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscCommit, DeterministicHostError> {
        Ok(AscCommit {
            height: asc_new(heap, &BigInt::from(self.height))?,
            round: self.round,
            block_id: asc_new(heap, self.block_id.as_ref().unwrap())?,
            signatures: asc_new(heap, &self.signatures)?,
        })
    }
}

impl ToAscObj<AscCommitSig> for codec::CommitSig {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscCommitSig, DeterministicHostError> {
        Ok(AscCommitSig {
            block_id_flag: asc_new(heap, &BlockIDKind(self.block_id_flag))?,
            validator_address: asc_new(heap, self.validator_address.as_ref().unwrap())?,
            timestamp: asc_new(heap, self.timestamp.as_ref().unwrap())?,
            signature: asc_new(heap, &Bytes(&self.signature))?,
        })
    }
}

impl ToAscObj<AscCommitSigArray> for Vec<codec::CommitSig> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscCommitSigArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x)).collect();
        let content = content?;
        Ok(AscCommitSigArray(Array::new(&*content, heap)?))
    }
}

/*
impl ToAscObj<AscBytesArray> for Vec<codec::Bytes> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscBytesArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x)).collect();
        let content = content?;
        Ok(AscBytesArray(Array::new(&*content, heap)?))
    }
}
*/

impl ToAscObj<AscValidatorArray> for Vec<codec::Validator> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscValidatorArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x)).collect();
        let content = content?;
        Ok(AscValidatorArray(Array::new(&*content, heap)?))
    }
}

impl ToAscObj<AscValidator> for codec::Validator {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscValidator, DeterministicHostError> {
        Ok(AscValidator {
            address: asc_new(heap, &Bytes(&self.address))?,
            pub_key: asc_new(heap, self.pub_key.as_ref().unwrap())?,
            voting_power: self.voting_power,
            proposer_priority: self.proposer_priority,
        })
    }
}

impl ToAscObj<AscEvidence> for codec::Evidence {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEvidence, DeterministicHostError> {
        let ev = self.sum.as_ref().unwrap();

        Ok(AscEvidence {
            duplicate_vote_evidence: AscPtr::null(), //asc_new(heap,self.sum.as_ref().unwrap().duplicate_vote_evidence.as_ref().unwrap())?,
            light_client_attack_evidence: AscPtr::null(), // asc_new(heap,self.sum.unwrap().light_client_attack_evidence.as_ref().unwrap())?,
        })
    }
}

impl ToAscObj<AscPublicKey> for codec::PublicKey {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscPublicKey, DeterministicHostError> {
        Ok(AscPublicKey {
            ed25519: AscPtr::null(),
            secp256k1: AscPtr::null(),
            sr25519: AscPtr::null(),
        })
    }
}

impl ToAscObj<AscDuplicateVoteEvidence> for codec::DuplicateVoteEvidence {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscDuplicateVoteEvidence, DeterministicHostError> {
        Ok(AscDuplicateVoteEvidence {
            vote_a: asc_new(heap, self.vote_a.as_ref().unwrap())?,
            vote_b: asc_new(heap, self.vote_b.as_ref().unwrap())?,
            total_voting_power: self.total_voting_power,
            validator_power: self.total_voting_power,
            timestamp: asc_new(heap, self.timestamp.as_ref().unwrap())?,
        })
    }
}

impl ToAscObj<AscEventDataVote> for codec::EventDataVote {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEventDataVote, DeterministicHostError> {
        Ok(AscEventDataVote {
            height: asc_new(heap, &BigInt::from(self.height))?,
            round: self.round,
            block_id: asc_new(heap, self.block_id.as_ref().unwrap())?,
            timestamp: asc_new(heap, self.timestamp.as_ref().unwrap())?,
            validator_address: asc_new(heap, self.validator_address.as_ref().unwrap())?,
            validator_index: self.validator_index,
            signature: asc_new(heap, &Bytes(&self.signature))?,
            eventvotetype: asc_new(heap, &SignedMessageTypeKind(self.eventvotetype))?,
        })
    }
}

impl ToAscObj<AscEvidenceArray> for Vec<codec::Evidence> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEvidenceArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x)).collect();
        let content = content?;
        Ok(AscEvidenceArray(Array::new(&*content, heap)?))
    }
}

impl ToAscObj<AscEventDataTxArray> for Vec<codec::EventDataTx> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEventDataTxArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x)).collect();
        let content = content?;
        Ok(AscEventDataTxArray(Array::new(&*content, heap)?))
    }
}

impl ToAscObj<AscEventDataTx> for codec::EventDataTx {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEventDataTx, DeterministicHostError> {
        Ok(AscEventDataTx {
            tx_result: asc_new(heap, self.tx_result.as_ref().unwrap())?,
        })
    }
}

impl ToAscObj<AscTxResult> for codec::TxResult {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscTxResult, DeterministicHostError> {
        Ok(AscTxResult {
            height: asc_new(heap, &BigInt::from(self.height))?,
            index: self.index,
            tx: asc_new(heap, &Bytes(&self.tx))?,
            result: asc_new(heap, self.result.as_ref().unwrap())?,
        })
    }
}

impl ToAscObj<AscResponseDeliverTx> for codec::ResponseDeliverTx {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscResponseDeliverTx, DeterministicHostError> {
        Ok(AscResponseDeliverTx {
            code: self.code,
            data: asc_new(heap, &Bytes(&self.data))?,
            log: asc_new(heap, &self.log.clone())?,
            info: asc_new(heap, &self.info.clone())?,
            gas_wanted: asc_new(heap, &BigInt::from(self.gas_wanted))?,
            gas_used: asc_new(heap, &BigInt::from(self.gas_used))?,
            events: asc_new(heap, &self.events)?,
            codespace: asc_new(heap, &self.codespace.clone())?,
        })
    }
}

impl ToAscObj<AscEventDataRoundState> for codec::EventDataRoundState {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEventDataRoundState, DeterministicHostError> {
        Ok(AscEventDataRoundState {
            height: asc_new(heap, &BigInt::from(self.height))?,
            round: self.round,
            step: asc_new(heap, &self.step)?,
        })
    }
}

impl ToAscObj<AscEventDataNewRound> for codec::EventDataNewRound {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEventDataNewRound, DeterministicHostError> {
        Ok(AscEventDataNewRound {
            height: asc_new(heap, &BigInt::from(self.height))?,
            round: self.round,
            step: asc_new(heap, &self.step)?,
            proposer: asc_new(heap, self.proposer.as_ref().unwrap())?,
        })
    }
}

impl ToAscObj<AscValidatorInfo> for codec::ValidatorInfo {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscValidatorInfo, DeterministicHostError> {
        Ok(AscValidatorInfo {
            address: asc_new(heap, self.address.as_ref().unwrap())?,
            index: self.index,
        })
    }
}

impl ToAscObj<AscAddress> for codec::Address {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscAddress, DeterministicHostError> {
        Ok(AscAddress {
            address: asc_new(heap, &Bytes(&self.address))?,
        })
    }
}

impl ToAscObj<AscEventDataCompleteProposal> for codec::EventDataCompleteProposal {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEventDataCompleteProposal, DeterministicHostError> {
        Ok(AscEventDataCompleteProposal {
            height: asc_new(heap, &BigInt::from(self.height))?,
            round: self.round,
            step: asc_new(heap, &self.step)?,
            block_id: asc_new(heap, self.block_id.as_ref().unwrap())?,
        })
    }
}

impl ToAscObj<AscEventDataValidatorSetUpdates> for codec::EventDataValidatorSetUpdates {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEventDataValidatorSetUpdates, DeterministicHostError> {
        Ok(AscEventDataValidatorSetUpdates {
            validator_updates: asc_new(heap, &self.validator_updates)?,
        })
    }
}

impl ToAscObj<AscEventDataString> for codec::EventDataString {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEventDataString, DeterministicHostError> {
        Ok(AscEventDataString {
            eventdatastring: asc_new(heap, &self.eventdatastring)?,
        })
    }
}

impl ToAscObj<AscEventDataBlockSyncStatus> for codec::EventDataBlockSyncStatus {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEventDataBlockSyncStatus, DeterministicHostError> {
        Ok(AscEventDataBlockSyncStatus {
            complete: self.complete,
            height: asc_new(heap, &BigInt::from(self.height))?,
        })
    }
}

impl ToAscObj<AscEventDataStateSyncStatus> for codec::EventDataStateSyncStatus {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEventDataStateSyncStatus, DeterministicHostError> {
        Ok(AscEventDataStateSyncStatus {
            complete: self.complete,
            height: asc_new(heap, &BigInt::from(self.height))?,
        })
    }
}

struct BlockIDKind(i32);

impl ToAscObj<AscBlockIDFlagEnum> for BlockIDKind {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        _heap: &mut H,
    ) -> Result<AscBlockIDFlagEnum, DeterministicHostError> {
        let value = match self.0 {
            0 => AscBlockIDFlag::BlockIdFlagUnknown,
            1 => AscBlockIDFlag::BlockIdFlagAbsent,
            2 => AscBlockIDFlag::BlockIdFlagCommit,
            2 => AscBlockIDFlag::BlockIdFlagNil,
            _ => {
                return Err(DeterministicHostError(anyhow::format_err!(
                    "Invalid direction value {}",
                    self.0
                )))
            }
        };

        Ok(AscBlockIDFlagEnum(AscEnum {
            _padding: 0,
            kind: value,
            payload: EnumPayload(self.0 as u64),
        }))
    }
}

struct SignedMessageTypeKind(i32);

impl ToAscObj<AscSignedMsgTypeEnum> for SignedMessageTypeKind {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        _heap: &mut H,
    ) -> Result<AscSignedMsgTypeEnum, DeterministicHostError> {
        let value = match self.0 {
            0 => AscSignedMsgType::SignedMsgTypeUnknown,
            1 => AscSignedMsgType::SignedMsgTypePrevote,
            2 => AscSignedMsgType::SignedMsgTypePrecommit,
            3 => AscSignedMsgType::SignedMsgTypeProposal,
            _ => {
                return Err(DeterministicHostError(anyhow::format_err!(
                    "Invalid direction value {}",
                    self.0
                )))
            }
        };

        Ok(AscSignedMsgTypeEnum(AscEnum {
            _padding: 0,
            kind: value,
            payload: EnumPayload(self.0 as u64),
        }))
    }
}

struct Bytes<'a>(&'a Vec<u8>);

impl ToAscObj<Uint8Array> for Bytes<'_> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscHash, DeterministicHostError> {
        self.0.to_asc_obj(heap)
    }
}

/*
impl ToAscObj<Uint8Array> for Bytes<codec::Address> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscHash, DeterministicHostError> {
        self.0.to_asc_obj(heap)
    }
}
*/
