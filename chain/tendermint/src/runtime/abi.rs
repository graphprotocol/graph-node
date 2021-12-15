#[path = "../protobuf/fig.tendermint.codec.v1.rs"]
mod pbcodec;

use crate::codec;
use crate::trigger::EventData;
use graph::runtime::{
    asc_new, AscHeap, AscIndexId, AscPtr, AscType, DeterministicHostError, ToAscObj,
};
use graph_runtime_wasm::asc_abi::class::{Array, AscEnum, EnumPayload, Uint8Array};

pub(crate) use super::generated::*;

impl ToAscObj<AscEventData> for EventData {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEventData, DeterministicHostError> {
        Ok(AscEventData {
            event: asc_new(heap, &self.event)?,
            block: asc_new(heap, self.block.as_ref())?,
        })
    }
}

impl ToAscObj<AscEventList> for codec::EventList {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEventList, DeterministicHostError> {
        Ok(AscEventList {
            newblock: asc_new_or_null(heap, &self.newblock)?,
            transaction: asc_new(heap, &self.transaction)?,
            validatorsetupdates: asc_new_or_null(heap, &self.validatorsetupdates)?,
        })
    }
}

impl ToAscObj<AscEventBlock> for codec::EventBlock {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEventBlock, DeterministicHostError> {
        Ok(AscEventBlock {
            block: asc_new_or_null(heap, &self.block)?,
            block_id: asc_new_or_null(heap, &self.block_id)?,
            result_begin_block: asc_new_or_null(heap, &self.result_begin_block)?,
            result_end_block: asc_new_or_null(heap, &self.result_end_block)?,
        })
    }
}

impl ToAscObj<AscBlock> for codec::Block {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscBlock, DeterministicHostError> {
        Ok(AscBlock {
            header: asc_new_or_missing(heap, &self.header, "Block", "header")?,
            data: asc_new_or_missing(heap, &self.data, "Block", "data")?,
            evidence: asc_new_or_missing(heap, &self.evidence, "Block", "evidence")?,
            last_commit: asc_new_or_null(heap, &self.last_commit)?,
        })
    }
}

impl ToAscObj<AscHeader> for codec::Header {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscHeader, DeterministicHostError> {
        Ok(AscHeader {
            version: asc_new_or_missing(heap, &self.version, "Header", "version")?,
            chain_id: asc_new(heap, &self.chain_id)?,
            height: self.height,
            time: asc_new_or_missing(heap, &self.time, "Header", "time")?,
            last_block_id: asc_new_or_missing(
                heap,
                &self.last_block_id,
                "Header",
                "last_block_id",
            )?,
            last_commit_hash: asc_new(heap, &Bytes(&self.last_commit_hash))?,
            data_hash: asc_new(heap, &Bytes(&self.data_hash))?,
            validators_hash: asc_new(heap, &Bytes(&self.validators_hash))?,
            next_validators_hash: asc_new(heap, &Bytes(&self.next_validators_hash))?,
            consensus_hash: asc_new(heap, &Bytes(&self.consensus_hash))?,
            app_hash: asc_new(heap, &Bytes(&self.app_hash))?,
            last_results_hash: asc_new(heap, &Bytes(&self.last_results_hash))?,
            evidence_hash: asc_new(heap, &Bytes(&self.evidence_hash))?,
            proposer_address: asc_new_or_null(heap, &self.proposer_address)?,
            _padding: 0,
        })
    }
}

impl ToAscObj<AscTimestamp> for codec::Timestamp {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        _heap: &mut H,
    ) -> Result<AscTimestamp, DeterministicHostError> {
        Ok(AscTimestamp {
            seconds: self.seconds,
            nanos: self.nanos,
            _padding: 0,
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
            part_set_header: asc_new_or_missing(
                heap,
                &self.part_set_header,
                "BlockId",
                "part_set_header",
            )?,
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
        _heap: &mut H,
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

        Ok(AscBytesArray(Array::new(&content?, heap)?))
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

        Ok(AscEventArray(Array::new(&content?, heap)?))
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
            index: self.index,
            _padding: false,
            _padding2: 0,
        })
    }
}

impl ToAscObj<AscEventAttributeArray> for Vec<codec::EventAttribute> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEventAttributeArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x)).collect();

        Ok(AscEventAttributeArray(Array::new(&content?, heap)?))
    }
}

impl ToAscObj<AscResponseEndBlock> for codec::ResponseEndBlock {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscResponseEndBlock, DeterministicHostError> {
        Ok(AscResponseEndBlock {
            validator_updates: asc_new(heap, &self.validator_updates)?,
            consensus_param_updates: asc_new_or_null(heap, &self.consensus_param_updates)?,
            events: asc_new(heap, &self.events)?,
        })
    }
}

impl ToAscObj<AscConsensusParams> for codec::ConsensusParams {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscConsensusParams, DeterministicHostError> {
        Ok(AscConsensusParams {
            block: asc_new_or_null(heap, &self.block)?,
            evidence: asc_new_or_null(heap, &self.evidence)?,
            validator: asc_new_or_null(heap, &self.validator)?,
            version: asc_new_or_null(heap, &self.version)?,
        })
    }
}

impl ToAscObj<AscVersion> for codec::Version {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        _heap: &mut H,
    ) -> Result<AscVersion, DeterministicHostError> {
        Ok(AscVersion {
            app_version: self.app_version,
        })
    }
}

impl ToAscObj<AscCommit> for codec::Commit {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscCommit, DeterministicHostError> {
        Ok(AscCommit {
            height: self.height,
            round: self.round,
            block_id: asc_new_or_missing(heap, &self.block_id, "Commit", "block_id")?,
            signatures: asc_new(heap, &self.signatures)?,
            _padding: 0,
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
            validator_address: asc_new_or_null(heap, &self.validator_address)?,
            timestamp: asc_new_or_missing(heap, &self.timestamp, "CommitSig", "timestamp")?,
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

        Ok(AscValidatorArray(Array::new(&content?, heap)?))
    }
}

impl ToAscObj<AscValidator> for codec::Validator {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscValidator, DeterministicHostError> {
        Ok(AscValidator {
            address: asc_new(heap, &Bytes(&self.address))?,
            pub_key: asc_new_or_missing(heap, &self.pub_key, "Validator", "pub_key")?,
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
        use codec::evidence::Sum;

        let sum = self
            .sum
            .as_ref()
            .ok_or(missing_field_error("Evidence", "sum"))?;

        let (duplicate_vote_evidence, light_client_attack_evidence) = match sum {
            Sum::DuplicateVoteEvidence(d) => (asc_new(heap, d)?, AscPtr::null()),
            Sum::LightClientAttackEvidence(l) => (AscPtr::null(), asc_new(heap, l)?),
        };

        Ok(AscEvidence {
            duplicate_vote_evidence,
            light_client_attack_evidence,
        })
    }
}

impl ToAscObj<AscPublicKey> for codec::PublicKey {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscPublicKey, DeterministicHostError> {
        use codec::public_key::Sum;

        let sum = self
            .sum
            .as_ref()
            .ok_or(missing_field_error("PublicKey", "sum"))?;

        let (ed25519, secp256k1, sr25519) = match sum {
            Sum::Ed25519(e) => (asc_new(heap, &Bytes(&e))?, AscPtr::null(), AscPtr::null()),
            Sum::Secp256k1(s) => (AscPtr::null(), asc_new(heap, &Bytes(&s))?, AscPtr::null()),
            Sum::Sr25519(s) => (AscPtr::null(), AscPtr::null(), asc_new(heap, &Bytes(&s))?),
        };

        Ok(AscPublicKey {
            ed25519,
            secp256k1,
            sr25519,
        })
    }
}

impl ToAscObj<AscDuplicateVoteEvidence> for codec::DuplicateVoteEvidence {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscDuplicateVoteEvidence, DeterministicHostError> {
        Ok(AscDuplicateVoteEvidence {
            vote_a: asc_new_or_null(heap, &self.vote_a)?,
            vote_b: asc_new_or_null(heap, &self.vote_b)?,
            total_voting_power: self.total_voting_power,
            validator_power: self.total_voting_power,
            timestamp: asc_new_or_missing(
                heap,
                &self.timestamp,
                "DuplicateVoteEvidence",
                "timestamp",
            )?,
        })
    }
}

impl ToAscObj<AscLightClientAttackEvidence> for codec::LightClientAttackEvidence {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscLightClientAttackEvidence, DeterministicHostError> {
        Ok(AscLightClientAttackEvidence {
            conflicting_block: asc_new_or_null(heap, &self.conflicting_block)?,
            common_height: self.common_height,
            byzantine_validators: asc_new(heap, &self.byzantine_validators)?,
            total_voting_power: self.total_voting_power,
            timestamp: asc_new_or_missing(
                heap,
                &self.timestamp,
                "LightClientAttackEvidence",
                "timestamp",
            )?,
        })
    }
}

impl ToAscObj<AscLightBlock> for codec::LightBlock {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscLightBlock, DeterministicHostError> {
        Ok(AscLightBlock {
            signed_header: asc_new_or_missing(
                heap,
                &self.signed_header,
                "LightBlock",
                "signed_header",
            )?,
            validator_set: asc_new_or_missing(
                heap,
                &self.validator_set,
                "LightBlock",
                "validator_set",
            )?,
        })
    }
}

impl ToAscObj<AscSignedHeader> for codec::SignedHeader {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscSignedHeader, DeterministicHostError> {
        Ok(AscSignedHeader {
            header: asc_new_or_null(heap, &self.header)?,
            commit: asc_new_or_null(heap, &self.commit)?,
        })
    }
}

impl ToAscObj<AscValidatorSet> for codec::ValidatorSet {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscValidatorSet, DeterministicHostError> {
        Ok(AscValidatorSet {
            validators: asc_new(heap, &self.validators)?,
            proposer: asc_new_or_missing(heap, &self.proposer, "ValidatorSet", "proposer")?,
            total_voting_power: self.total_voting_power,
        })
    }
}

impl ToAscObj<AscEventVote> for codec::EventVote {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEventVote, DeterministicHostError> {
        Ok(AscEventVote {
            eventvotetype: asc_new(heap, &SignedMessageTypeKind(self.eventvotetype))?,
            height: self.height,
            round: self.round,
            block_id: asc_new_or_missing(heap, &self.block_id, "EventVote", "block_id")?,
            timestamp: asc_new_or_missing(heap, &self.timestamp, "EventVote", "timestamp")?,
            validator_address: asc_new_or_null(heap, &self.validator_address)?,
            validator_index: self.validator_index,
            signature: asc_new(heap, &Bytes(&self.signature))?,
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

impl ToAscObj<AscEventTxArray> for Vec<codec::EventTx> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEventTxArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x)).collect();
        let content = content?;
        Ok(AscEventTxArray(Array::new(&*content, heap)?))
    }
}

impl ToAscObj<AscEventTx> for codec::EventTx {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEventTx, DeterministicHostError> {
        Ok(AscEventTx {
            tx_result: asc_new_or_null(heap, &self.tx_result)?,
        })
    }
}

impl ToAscObj<AscTxResult> for codec::TxResult {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscTxResult, DeterministicHostError> {
        Ok(AscTxResult {
            height: self.height,
            index: self.index,
            tx: asc_new(heap, &Bytes(&self.tx))?,
            result: asc_new_or_null(heap, &self.result)?,
            _padding: 0,
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
            gas_wanted: self.gas_wanted,
            gas_used: self.gas_used,
            events: asc_new(heap, &self.events)?,
            codespace: asc_new(heap, &self.codespace.clone())?,
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
impl ToAscObj<AscEventValidatorSetUpdates> for codec::EventValidatorSetUpdates {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
    ) -> Result<AscEventValidatorSetUpdates, DeterministicHostError> {
        Ok(AscEventValidatorSetUpdates {
            validator_updates: asc_new(heap, &self.validator_updates)?,
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
            3 => AscBlockIDFlag::BlockIdFlagNil,
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

/// Map an optional object to its Asc equivalent if Some, otherwise return null.
fn asc_new_or_null<H, O, A>(
    heap: &mut H,
    object: &Option<O>,
) -> Result<AscPtr<A>, DeterministicHostError>
where
    H: AscHeap + ?Sized,
    O: ToAscObj<A>,
    A: AscType + AscIndexId,
{
    match object {
        Some(o) => asc_new(heap, o),
        None => Ok(AscPtr::null()),
    }
}

/// Create an error for a missing field in a type.
fn missing_field_error(type_name: &str, field_name: &str) -> DeterministicHostError {
    DeterministicHostError(anyhow::anyhow!("{} missing {}", type_name, field_name))
}

/// Map an optional object to its Asc equivalent if Some, otherwise return a missing field error.
fn asc_new_or_missing<H, O, A>(
    heap: &mut H,
    object: &Option<O>,
    type_name: &str,
    field_name: &str,
) -> Result<AscPtr<A>, DeterministicHostError>
where
    H: AscHeap + ?Sized,
    O: ToAscObj<A>,
    A: AscType + AscIndexId,
{
    match object {
        Some(o) => asc_new(heap, o),
        None => Err(missing_field_error(type_name, field_name)),
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
