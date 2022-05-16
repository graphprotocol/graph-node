#[rustfmt::skip]
#[path = "../protobuf/sf.cosmos.r#type.v1.rs"]
mod pbcosmos;

use anyhow::anyhow;
use graph::runtime::{
    asc_new, gas::GasCounter, AscHeap, AscIndexId, AscPtr, AscType, DeterministicHostError,
    ToAscObj,
};
use graph_runtime_wasm::asc_abi::class::{Array, Uint8Array};

use crate::codec;

pub(crate) use super::generated::*;

impl ToAscObj<AscBlock> for codec::Block {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscBlock, DeterministicHostError> {
        Ok(AscBlock {
            header: asc_new_or_missing(heap, &self.header, gas, "Block", "header")?,
            evidence: asc_new_or_null(heap, &self.evidence, gas)?,
            last_commit: asc_new_or_null(heap, &self.last_commit, gas)?,
            result_begin_block: asc_new_or_missing(
                heap,
                &self.result_begin_block,
                gas,
                "Block",
                "result_begin_block",
            )?,
            result_end_block: asc_new_or_missing(
                heap,
                &self.result_end_block,
                gas,
                "Block",
                "result_end_block",
            )?,
            transactions: asc_new(heap, &self.transactions, gas)?,
            validator_updates: asc_new(heap, &self.validator_updates, gas)?,
        })
    }
}

impl ToAscObj<AscHeaderOnlyBlock> for codec::HeaderOnlyBlock {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscHeaderOnlyBlock, DeterministicHostError> {
        Ok(AscHeaderOnlyBlock {
            header: asc_new_or_missing(heap, &self.header, gas, "HeaderOnlyBlock", "header")?,
        })
    }
}

impl ToAscObj<AscEventData> for codec::EventData {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEventData, DeterministicHostError> {
        Ok(AscEventData {
            event: asc_new_or_missing(heap, &self.event, gas, "EventData", "event")?,
            block: asc_new_or_missing(heap, &self.block, gas, "EventData", "block")?,
        })
    }
}

impl ToAscObj<AscTransactionData> for codec::TransactionData {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscTransactionData, DeterministicHostError> {
        Ok(AscTransactionData {
            tx: asc_new_or_missing(heap, &self.tx, gas, "TransactionData", "tx")?,
            block: asc_new_or_missing(heap, &self.block, gas, "TransactionData", "block")?,
        })
    }
}

impl ToAscObj<AscHeader> for codec::Header {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscHeader, DeterministicHostError> {
        Ok(AscHeader {
            version: asc_new_or_null(heap, &self.version, gas)?,
            chain_id: asc_new(heap, &self.chain_id, gas)?,
            height: self.height,
            time: asc_new_or_null(heap, &self.time, gas)?,
            last_block_id: asc_new_or_missing(
                heap,
                &self.last_block_id,
                gas,
                "Header",
                "last_block_id",
            )?,
            last_commit_hash: asc_new(heap, &Bytes(&self.last_commit_hash), gas)?,
            data_hash: asc_new(heap, &Bytes(&self.data_hash), gas)?,
            validators_hash: asc_new(heap, &Bytes(&self.validators_hash), gas)?,
            next_validators_hash: asc_new(heap, &Bytes(&self.next_validators_hash), gas)?,
            consensus_hash: asc_new(heap, &Bytes(&self.consensus_hash), gas)?,
            app_hash: asc_new(heap, &Bytes(&self.app_hash), gas)?,
            last_results_hash: asc_new(heap, &Bytes(&self.last_results_hash), gas)?,
            evidence_hash: asc_new(heap, &Bytes(&self.evidence_hash), gas)?,
            proposer_address: asc_new(heap, &Bytes(&self.proposer_address), gas)?,
            hash: asc_new(heap, &Bytes(&self.hash), gas)?,
        })
    }
}

impl ToAscObj<AscConsensus> for codec::Consensus {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        _heap: &mut H,
        _gas: &GasCounter,
    ) -> Result<AscConsensus, DeterministicHostError> {
        Ok(AscConsensus {
            block: self.block,
            app: self.app,
        })
    }
}

impl ToAscObj<AscTimestamp> for codec::Timestamp {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        _heap: &mut H,
        _gas: &GasCounter,
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
        gas: &GasCounter,
    ) -> Result<AscBlockID, DeterministicHostError> {
        Ok(AscBlockID {
            hash: asc_new(heap, &Bytes(&self.hash), gas)?,
            part_set_header: asc_new_or_null(heap, &self.part_set_header, gas)?,
        })
    }
}

impl ToAscObj<AscPartSetHeader> for codec::PartSetHeader {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscPartSetHeader, DeterministicHostError> {
        Ok(AscPartSetHeader {
            total: self.total,
            hash: asc_new(heap, &Bytes(&self.hash), gas)?,
        })
    }
}

impl ToAscObj<AscEvidenceList> for codec::EvidenceList {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEvidenceList, DeterministicHostError> {
        Ok(AscEvidenceList {
            evidence: asc_new(heap, &self.evidence, gas)?,
        })
    }
}

impl ToAscObj<AscEvidence> for codec::Evidence {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEvidence, DeterministicHostError> {
        use codec::evidence::Sum;

        let sum = self
            .sum
            .as_ref()
            .ok_or_else(|| missing_field_error("Evidence", "sum"))?;

        let (duplicate_vote_evidence, light_client_attack_evidence) = match sum {
            Sum::DuplicateVoteEvidence(d) => (asc_new(heap, d, gas)?, AscPtr::null()),
            Sum::LightClientAttackEvidence(l) => (AscPtr::null(), asc_new(heap, l, gas)?),
        };

        Ok(AscEvidence {
            duplicate_vote_evidence,
            light_client_attack_evidence,
        })
    }
}

impl ToAscObj<AscEvidenceArray> for Vec<codec::Evidence> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEvidenceArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x, gas)).collect();

        Ok(AscEvidenceArray(Array::new(&content?, heap, gas)?))
    }
}

impl ToAscObj<AscDuplicateVoteEvidence> for codec::DuplicateVoteEvidence {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscDuplicateVoteEvidence, DeterministicHostError> {
        Ok(AscDuplicateVoteEvidence {
            vote_a: asc_new_or_null(heap, &self.vote_a, gas)?,
            vote_b: asc_new_or_null(heap, &self.vote_b, gas)?,
            total_voting_power: self.total_voting_power,
            validator_power: self.total_voting_power,
            timestamp: asc_new_or_null(heap, &self.timestamp, gas)?,
            _padding: 0,
        })
    }
}

impl ToAscObj<AscEventVote> for codec::EventVote {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEventVote, DeterministicHostError> {
        Ok(AscEventVote {
            event_vote_type: match self.event_vote_type {
                0 => 0,
                1 => 1,
                2 => 2,
                32 => 32,
                value => {
                    return Err(DeterministicHostError::from(anyhow!(
                        "Invalid event vote type: {}",
                        value
                    )))
                }
            },
            _padding: 0,
            height: self.height,
            round: self.round,
            block_id: asc_new_or_null(heap, &self.block_id, gas)?,
            timestamp: asc_new_or_null(heap, &self.timestamp, gas)?,
            validator_address: asc_new(heap, &Bytes(&self.validator_address), gas)?,
            validator_index: self.validator_index,
            signature: asc_new(heap, &Bytes(&self.signature), gas)?,
        })
    }
}

impl ToAscObj<AscLightClientAttackEvidence> for codec::LightClientAttackEvidence {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscLightClientAttackEvidence, DeterministicHostError> {
        Ok(AscLightClientAttackEvidence {
            conflicting_block: asc_new_or_null(heap, &self.conflicting_block, gas)?,
            _padding: 0,
            common_height: self.common_height,
            total_voting_power: self.total_voting_power,
            byzantine_validators: asc_new(heap, &self.byzantine_validators, gas)?,
            timestamp: asc_new_or_null(heap, &self.timestamp, gas)?,
        })
    }
}

impl ToAscObj<AscLightBlock> for codec::LightBlock {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscLightBlock, DeterministicHostError> {
        Ok(AscLightBlock {
            signed_header: asc_new_or_null(heap, &self.signed_header, gas)?,
            validator_set: asc_new_or_null(heap, &self.validator_set, gas)?,
        })
    }
}

impl ToAscObj<AscSignedHeader> for codec::SignedHeader {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscSignedHeader, DeterministicHostError> {
        Ok(AscSignedHeader {
            header: asc_new_or_null(heap, &self.header, gas)?,
            commit: asc_new_or_null(heap, &self.commit, gas)?,
        })
    }
}

impl ToAscObj<AscCommit> for codec::Commit {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscCommit, DeterministicHostError> {
        Ok(AscCommit {
            height: self.height,
            round: self.round,
            block_id: asc_new_or_null(heap, &self.block_id, gas)?,
            signatures: asc_new(heap, &self.signatures, gas)?,
            _padding: 0,
        })
    }
}

impl ToAscObj<AscCommitSig> for codec::CommitSig {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscCommitSig, DeterministicHostError> {
        Ok(AscCommitSig {
            block_id_flag: match self.block_id_flag {
                0 => 0,
                1 => 1,
                2 => 2,
                3 => 3,
                value => {
                    return Err(DeterministicHostError::from(anyhow!(
                        "Invalid block ID flag: {}",
                        value
                    )))
                }
            },
            validator_address: asc_new(heap, &Bytes(&self.validator_address), gas)?,
            timestamp: asc_new_or_null(heap, &self.timestamp, gas)?,
            signature: asc_new(heap, &Bytes(&self.signature), gas)?,
        })
    }
}

impl ToAscObj<AscCommitSigArray> for Vec<codec::CommitSig> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscCommitSigArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x, gas)).collect();

        Ok(AscCommitSigArray(Array::new(&content?, heap, gas)?))
    }
}

impl ToAscObj<AscValidatorSet> for codec::ValidatorSet {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscValidatorSet, DeterministicHostError> {
        Ok(AscValidatorSet {
            validators: asc_new(heap, &self.validators, gas)?,
            proposer: asc_new_or_null(heap, &self.proposer, gas)?,
            total_voting_power: self.total_voting_power,
        })
    }
}

impl ToAscObj<AscValidator> for codec::Validator {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscValidator, DeterministicHostError> {
        Ok(AscValidator {
            address: asc_new(heap, &Bytes(&self.address), gas)?,
            pub_key: asc_new_or_null(heap, &self.pub_key, gas)?,
            voting_power: self.voting_power,
            proposer_priority: self.proposer_priority,
        })
    }
}

impl ToAscObj<AscValidatorArray> for Vec<codec::Validator> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscValidatorArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x, gas)).collect();

        Ok(AscValidatorArray(Array::new(&content?, heap, gas)?))
    }
}

impl ToAscObj<AscPublicKey> for codec::PublicKey {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscPublicKey, DeterministicHostError> {
        use codec::public_key::Sum;

        let sum = self
            .sum
            .as_ref()
            .ok_or_else(|| missing_field_error("PublicKey", "sum"))?;

        let (ed25519, secp256k1) = match sum {
            Sum::Ed25519(e) => (asc_new(heap, &Bytes(e), gas)?, AscPtr::null()),
            Sum::Secp256k1(s) => (AscPtr::null(), asc_new(heap, &Bytes(s), gas)?),
        };

        Ok(AscPublicKey { ed25519, secp256k1 })
    }
}

impl ToAscObj<AscResponseBeginBlock> for codec::ResponseBeginBlock {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscResponseBeginBlock, DeterministicHostError> {
        Ok(AscResponseBeginBlock {
            events: asc_new(heap, &self.events, gas)?,
        })
    }
}

impl ToAscObj<AscEvent> for codec::Event {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEvent, DeterministicHostError> {
        Ok(AscEvent {
            event_type: asc_new(heap, &self.event_type, gas)?,
            attributes: asc_new(heap, &self.attributes, gas)?,
        })
    }
}

impl ToAscObj<AscEventArray> for Vec<codec::Event> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEventArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x, gas)).collect();

        Ok(AscEventArray(Array::new(&content?, heap, gas)?))
    }
}

impl ToAscObj<AscEventAttribute> for codec::EventAttribute {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEventAttribute, DeterministicHostError> {
        Ok(AscEventAttribute {
            key: asc_new(heap, &self.key, gas)?,
            value: asc_new(heap, &self.value, gas)?,
            index: self.index,
            _padding: 0,
            _padding2: 0,
        })
    }
}

impl ToAscObj<AscEventAttributeArray> for Vec<codec::EventAttribute> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEventAttributeArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x, gas)).collect();

        Ok(AscEventAttributeArray(Array::new(&content?, heap, gas)?))
    }
}

impl ToAscObj<AscResponseEndBlock> for codec::ResponseEndBlock {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscResponseEndBlock, DeterministicHostError> {
        Ok(AscResponseEndBlock {
            validator_updates: asc_new(heap, &self.validator_updates, gas)?,
            consensus_param_updates: asc_new_or_null(heap, &self.consensus_param_updates, gas)?,
            events: asc_new(heap, &self.events, gas)?,
        })
    }
}

impl ToAscObj<AscValidatorUpdate> for codec::ValidatorUpdate {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscValidatorUpdate, DeterministicHostError> {
        Ok(AscValidatorUpdate {
            address: asc_new(heap, &Bytes(&self.address), gas)?,
            pub_key: asc_new_or_null(heap, &self.pub_key, gas)?,
            power: self.power,
        })
    }
}

impl ToAscObj<AscValidatorUpdateArray> for Vec<codec::ValidatorUpdate> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscValidatorUpdateArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x, gas)).collect();

        Ok(AscValidatorUpdateArray(Array::new(&content?, heap, gas)?))
    }
}

impl ToAscObj<AscConsensusParams> for codec::ConsensusParams {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscConsensusParams, DeterministicHostError> {
        Ok(AscConsensusParams {
            block: asc_new_or_null(heap, &self.block, gas)?,
            evidence: asc_new_or_null(heap, &self.evidence, gas)?,
            validator: asc_new_or_null(heap, &self.validator, gas)?,
            version: asc_new_or_null(heap, &self.version, gas)?,
        })
    }
}

impl ToAscObj<AscBlockParams> for codec::BlockParams {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        _heap: &mut H,
        _gas: &GasCounter,
    ) -> Result<AscBlockParams, DeterministicHostError> {
        Ok(AscBlockParams {
            max_bytes: self.max_bytes,
            max_gas: self.max_gas,
        })
    }
}

impl ToAscObj<AscEvidenceParams> for codec::EvidenceParams {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscEvidenceParams, DeterministicHostError> {
        Ok(AscEvidenceParams {
            max_age_num_blocks: self.max_age_num_blocks,
            max_age_duration: asc_new_or_null(heap, &self.max_age_duration, gas)?,
            _padding: 0,
            max_bytes: self.max_bytes,
        })
    }
}

impl ToAscObj<AscDuration> for codec::Duration {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        _heap: &mut H,
        _gas: &GasCounter,
    ) -> Result<AscDuration, DeterministicHostError> {
        Ok(AscDuration {
            seconds: self.seconds,
            nanos: self.nanos,
            _padding: 0,
        })
    }
}

impl ToAscObj<AscValidatorParams> for codec::ValidatorParams {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscValidatorParams, DeterministicHostError> {
        Ok(AscValidatorParams {
            pub_key_types: asc_new(heap, &self.pub_key_types, gas)?,
        })
    }
}

impl ToAscObj<AscVersionParams> for codec::VersionParams {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        _heap: &mut H,
        _gas: &GasCounter,
    ) -> Result<AscVersionParams, DeterministicHostError> {
        Ok(AscVersionParams {
            app_version: self.app_version,
        })
    }
}

impl ToAscObj<AscTxResult> for codec::TxResult {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscTxResult, DeterministicHostError> {
        Ok(AscTxResult {
            height: self.height,
            index: self.index,
            tx: asc_new_or_missing(heap, &self.tx, gas, "TxResult", "tx")?,
            result: asc_new_or_missing(heap, &self.result, gas, "TxResult", "result")?,
            hash: asc_new(heap, &Bytes(&self.hash), gas)?,
        })
    }
}

impl ToAscObj<AscTxResultArray> for Vec<codec::TxResult> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscTxResultArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x, gas)).collect();

        Ok(AscTxResultArray(Array::new(&content?, heap, gas)?))
    }
}

impl ToAscObj<AscTx> for codec::Tx {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscTx, DeterministicHostError> {
        Ok(AscTx {
            body: asc_new_or_missing(heap, &self.body, gas, "Tx", "body")?,
            auth_info: asc_new_or_null(heap, &self.auth_info, gas)?,
            signatures: asc_new(heap, &self.signatures, gas)?,
        })
    }
}

impl ToAscObj<AscTxBody> for codec::TxBody {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscTxBody, DeterministicHostError> {
        Ok(AscTxBody {
            messages: asc_new(heap, &self.messages, gas)?,
            memo: asc_new(heap, &self.memo, gas)?,
            timeout_height: self.timeout_height,
            extension_options: asc_new(heap, &self.extension_options, gas)?,
            non_critical_extension_options: asc_new(
                heap,
                &self.non_critical_extension_options,
                gas,
            )?,
        })
    }
}

impl ToAscObj<AscAny> for prost_types::Any {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscAny, DeterministicHostError> {
        Ok(AscAny {
            type_url: asc_new(heap, &self.type_url, gas)?,
            value: asc_new(heap, &Bytes(&self.value), gas)?,
        })
    }
}

impl ToAscObj<AscAnyArray> for Vec<prost_types::Any> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscAnyArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x, gas)).collect();

        Ok(AscAnyArray(Array::new(&content?, heap, gas)?))
    }
}

impl ToAscObj<AscAuthInfo> for codec::AuthInfo {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscAuthInfo, DeterministicHostError> {
        Ok(AscAuthInfo {
            signer_infos: asc_new(heap, &self.signer_infos, gas)?,
            fee: asc_new_or_null(heap, &self.fee, gas)?,
            tip: asc_new_or_null(heap, &self.tip, gas)?,
        })
    }
}

impl ToAscObj<AscSignerInfo> for codec::SignerInfo {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscSignerInfo, DeterministicHostError> {
        Ok(AscSignerInfo {
            public_key: asc_new_or_null(heap, &self.public_key, gas)?,
            mode_info: asc_new_or_null(heap, &self.mode_info, gas)?,
            sequence: self.sequence,
        })
    }
}

impl ToAscObj<AscSignerInfoArray> for Vec<codec::SignerInfo> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscSignerInfoArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x, gas)).collect();

        Ok(AscSignerInfoArray(Array::new(&content?, heap, gas)?))
    }
}

impl ToAscObj<AscModeInfo> for codec::ModeInfo {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscModeInfo, DeterministicHostError> {
        use codec::mode_info::Sum;

        let sum = self
            .sum
            .as_ref()
            .ok_or_else(|| missing_field_error("ModeInfo", "sum"))?;

        let (single, multi) = match sum {
            Sum::Single(s) => (asc_new(heap, s, gas)?, AscPtr::null()),
            Sum::Multi(m) => (AscPtr::null(), asc_new(heap, m, gas)?),
        };

        Ok(AscModeInfo { single, multi })
    }
}

impl ToAscObj<AscModeInfoArray> for Vec<codec::ModeInfo> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscModeInfoArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x, gas)).collect();

        Ok(AscModeInfoArray(Array::new(&content?, heap, gas)?))
    }
}

impl ToAscObj<AscModeInfoSingle> for codec::ModeInfoSingle {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        _heap: &mut H,
        _gas: &GasCounter,
    ) -> Result<AscModeInfoSingle, DeterministicHostError> {
        Ok(AscModeInfoSingle {
            mode: match self.mode {
                0 => 0,
                1 => 1,
                2 => 2,
                127 => 127,
                value => {
                    return Err(DeterministicHostError::from(anyhow!(
                        "Invalid sign mode: {}",
                        value,
                    )))
                }
            },
        })
    }
}

impl ToAscObj<AscModeInfoMulti> for codec::ModeInfoMulti {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscModeInfoMulti, DeterministicHostError> {
        Ok(AscModeInfoMulti {
            bitarray: asc_new_or_null(heap, &self.bitarray, gas)?,
            mode_infos: asc_new(heap, &self.mode_infos, gas)?,
        })
    }
}

impl ToAscObj<AscCompactBitArray> for codec::CompactBitArray {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscCompactBitArray, DeterministicHostError> {
        Ok(AscCompactBitArray {
            extra_bits_stored: self.extra_bits_stored,
            elems: asc_new(heap, &Bytes(&self.elems), gas)?,
        })
    }
}

impl ToAscObj<AscFee> for codec::Fee {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscFee, DeterministicHostError> {
        Ok(AscFee {
            amount: asc_new(heap, &self.amount, gas)?,
            _padding: 0,
            gas_limit: self.gas_limit,
            payer: asc_new(heap, &self.payer, gas)?,
            granter: asc_new(heap, &self.granter, gas)?,
        })
    }
}

impl ToAscObj<AscCoin> for codec::Coin {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscCoin, DeterministicHostError> {
        Ok(AscCoin {
            denom: asc_new(heap, &self.denom, gas)?,
            amount: asc_new(heap, &self.amount, gas)?,
        })
    }
}

impl ToAscObj<AscCoinArray> for Vec<codec::Coin> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscCoinArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> = self.iter().map(|x| asc_new(heap, x, gas)).collect();

        Ok(AscCoinArray(Array::new(&content?, heap, gas)?))
    }
}

impl ToAscObj<AscTip> for codec::Tip {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscTip, DeterministicHostError> {
        Ok(AscTip {
            amount: asc_new(heap, &self.amount, gas)?,
            tipper: asc_new(heap, &self.tipper, gas)?,
        })
    }
}

impl ToAscObj<AscBytesArray> for Vec<Vec<u8>> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscBytesArray, DeterministicHostError> {
        let content: Result<Vec<_>, _> =
            self.iter().map(|x| asc_new(heap, &Bytes(x), gas)).collect();

        Ok(AscBytesArray(Array::new(&content?, heap, gas)?))
    }
}

impl ToAscObj<AscResponseDeliverTx> for codec::ResponseDeliverTx {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscResponseDeliverTx, DeterministicHostError> {
        Ok(AscResponseDeliverTx {
            code: self.code,
            data: asc_new(heap, &Bytes(&self.data), gas)?,
            log: asc_new(heap, &self.log.clone(), gas)?,
            info: asc_new(heap, &self.info.clone(), gas)?,
            gas_wanted: self.gas_wanted,
            gas_used: self.gas_used,
            events: asc_new(heap, &self.events, gas)?,
            codespace: asc_new(heap, &self.codespace.clone(), gas)?,
        })
    }
}

impl ToAscObj<AscValidatorSetUpdates> for codec::ValidatorSetUpdates {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<AscValidatorSetUpdates, DeterministicHostError> {
        Ok(AscValidatorSetUpdates {
            validator_updates: asc_new(heap, &self.validator_updates, gas)?,
        })
    }
}

struct Bytes<'a>(&'a Vec<u8>);

impl ToAscObj<Uint8Array> for Bytes<'_> {
    fn to_asc_obj<H: AscHeap + ?Sized>(
        &self,
        heap: &mut H,
        gas: &GasCounter,
    ) -> Result<Uint8Array, DeterministicHostError> {
        self.0.to_asc_obj(heap, gas)
    }
}

/// Map an optional object to its Asc equivalent if Some, otherwise return null.
fn asc_new_or_null<H, O, A>(
    heap: &mut H,
    object: &Option<O>,
    gas: &GasCounter,
) -> Result<AscPtr<A>, DeterministicHostError>
where
    H: AscHeap + ?Sized,
    O: ToAscObj<A>,
    A: AscType + AscIndexId,
{
    match object {
        Some(o) => asc_new(heap, o, gas),
        None => Ok(AscPtr::null()),
    }
}

/// Create an error for a missing field in a type.
fn missing_field_error(type_name: &str, field_name: &str) -> DeterministicHostError {
    DeterministicHostError::from(anyhow!("{} missing {}", type_name, field_name))
}

/// Map an optional object to its Asc equivalent if Some, otherwise return a missing field error.
fn asc_new_or_missing<H, O, A>(
    heap: &mut H,
    object: &Option<O>,
    gas: &GasCounter,
    type_name: &str,
    field_name: &str,
) -> Result<AscPtr<A>, DeterministicHostError>
where
    H: AscHeap + ?Sized,
    O: ToAscObj<A>,
    A: AscType + AscIndexId,
{
    match object {
        Some(o) => asc_new(heap, o, gas),
        None => Err(missing_field_error(type_name, field_name)),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use graph_runtime_wasm::asc_abi::class::AscString;

    /// A macro that takes an ASc struct value definition and asserts that the struct size
    /// (mem::size_of) is the same as the sum of all its field ASc byte representation
    /// (to_asc_bytes) sizes.
    macro_rules! assert_asc_size {
        ($struct_name:ident {
            $($field:ident : $field_value:expr),+
            $(,)? // trailing
        }) => {
            let value = $struct_name {
                $($field: $field_value),+
            };

            let mem_size = std::mem::size_of::<$struct_name>();
            let mut bytes = Vec::with_capacity(mem_size);

            $(bytes.extend_from_slice(
                    &value.$field.to_asc_bytes()
                    .expect("failed to turn {} {} field value to asc bytes"));)+

            assert_eq!(
                mem_size,
                bytes.len(),
                "Expected {} to have size {}, but it was {}. \
                Field reordering or padding needed",
                stringify!($struct_name),
                bytes.len(),
                mem_size,
            );
        };
    }

    #[test]
    fn test_asc_type_alignment() {
        // TODO: automatically generate these tests for each struct in generated module that derives
        // AscType

        assert_asc_size!(AscBlock {
            header: AscPtr::<AscHeader>::null(),
            evidence: AscPtr::<AscEvidenceList>::null(),
            last_commit: AscPtr::<AscCommit>::null(),
            result_begin_block: AscPtr::<AscResponseBeginBlock>::null(),
            result_end_block: AscPtr::<AscResponseEndBlock>::null(),
            transactions: AscPtr::<AscTxResultArray>::null(),
            validator_updates: AscPtr::<AscValidatorArray>::null(),
        });

        assert_asc_size!(AscHeaderOnlyBlock {
            header: AscPtr::<AscHeader>::null(),
        });

        assert_asc_size!(AscEventData {
            event: AscPtr::<AscEvent>::null(),
            block: AscPtr::<AscHeaderOnlyBlock>::null(),
        });

        assert_asc_size!(AscTransactionData {
            tx: AscPtr::<AscTxResult>::null(),
            block: AscPtr::<AscHeaderOnlyBlock>::null(),
        });

        assert_asc_size!(AscHeader {
            version: AscPtr::<AscConsensus>::null(),
            chain_id: AscPtr::<AscString>::null(),
            height: 0,
            time: AscPtr::<AscTimestamp>::null(),
            last_block_id: AscPtr::<AscBlockID>::null(),
            last_commit_hash: AscPtr::<Uint8Array>::null(),
            data_hash: AscPtr::<Uint8Array>::null(),
            validators_hash: AscPtr::<Uint8Array>::null(),
            next_validators_hash: AscPtr::<Uint8Array>::null(),
            consensus_hash: AscPtr::<Uint8Array>::null(),
            app_hash: AscPtr::<Uint8Array>::null(),
            last_results_hash: AscPtr::<Uint8Array>::null(),
            evidence_hash: AscPtr::<Uint8Array>::null(),
            proposer_address: AscPtr::<Uint8Array>::null(),
            hash: AscPtr::<Uint8Array>::null(),
        });

        assert_asc_size!(AscConsensus { block: 0, app: 0 });

        assert_asc_size!(AscTimestamp {
            seconds: 0,
            nanos: 0,
            _padding: 0,
        });

        assert_asc_size!(AscBlockID {
            hash: AscPtr::<Uint8Array>::null(),
            part_set_header: AscPtr::<AscPartSetHeader>::null(),
        });

        assert_asc_size!(AscPartSetHeader {
            total: 0,
            hash: AscPtr::<Uint8Array>::null(),
        });

        assert_asc_size!(AscEvidenceList {
            evidence: AscPtr::<AscEvidenceArray>::null(),
        });

        assert_asc_size!(AscEvidence {
            duplicate_vote_evidence: AscPtr::<AscDuplicateVoteEvidence>::null(),
            light_client_attack_evidence: AscPtr::<AscLightClientAttackEvidence>::null(),
        });

        assert_asc_size!(AscDuplicateVoteEvidence {
            vote_a: AscPtr::<AscEventVote>::null(),
            vote_b: AscPtr::<AscEventVote>::null(),
            total_voting_power: 0,
            validator_power: 0,
            timestamp: AscPtr::<AscTimestamp>::null(),
            _padding: 0,
        });

        assert_asc_size!(AscEventVote {
            event_vote_type: 0,
            _padding: 0,
            height: 0,
            round: 0,
            block_id: AscPtr::<AscBlockID>::null(),
            timestamp: AscPtr::<AscTimestamp>::null(),
            validator_address: AscPtr::<Uint8Array>::null(),
            validator_index: 0,
            signature: AscPtr::<Uint8Array>::null(),
        });

        assert_asc_size!(AscLightClientAttackEvidence {
            conflicting_block: AscPtr::<AscLightBlock>::null(),
            _padding: 0,
            common_height: 0,
            total_voting_power: 0,
            byzantine_validators: AscPtr::<AscValidatorArray>::null(),
            timestamp: AscPtr::<AscTimestamp>::null(),
        });

        assert_asc_size!(AscLightBlock {
            signed_header: AscPtr::<AscSignedHeader>::null(),
            validator_set: AscPtr::<AscValidatorSet>::null(),
        });

        assert_asc_size!(AscSignedHeader {
            header: AscPtr::<AscHeader>::null(),
            commit: AscPtr::<AscCommit>::null(),
        });

        assert_asc_size!(AscCommit {
            height: 0,
            round: 0,
            block_id: AscPtr::<AscBlockID>::null(),
            signatures: AscPtr::<AscCommitSigArray>::null(),
            _padding: 0,
        });

        assert_asc_size!(AscCommitSig {
            block_id_flag: 0,
            validator_address: AscPtr::<Uint8Array>::null(),
            timestamp: AscPtr::<AscTimestamp>::null(),
            signature: AscPtr::<Uint8Array>::null(),
        });

        assert_asc_size!(AscValidatorSet {
            validators: AscPtr::<AscValidatorArray>::null(),
            proposer: AscPtr::<AscValidator>::null(),
            total_voting_power: 0,
        });

        assert_asc_size!(AscValidator {
            address: AscPtr::<Uint8Array>::null(),
            pub_key: AscPtr::<AscPublicKey>::null(),
            voting_power: 0,
            proposer_priority: 0,
        });

        assert_asc_size!(AscPublicKey {
            ed25519: AscPtr::<Uint8Array>::null(),
            secp256k1: AscPtr::<Uint8Array>::null(),
        });

        assert_asc_size!(AscResponseBeginBlock {
            events: AscPtr::<AscEventArray>::null(),
        });

        assert_asc_size!(AscEvent {
            event_type: AscPtr::<AscString>::null(),
            attributes: AscPtr::<AscEventAttributeArray>::null(),
        });

        assert_asc_size!(AscEventAttribute {
            key: AscPtr::<AscString>::null(),
            value: AscPtr::<AscString>::null(),
            index: false,
            _padding: 0,
            _padding2: 0
        });

        assert_asc_size!(AscResponseEndBlock {
            validator_updates: AscPtr::<AscValidatorUpdateArray>::null(),
            consensus_param_updates: AscPtr::<AscConsensusParams>::null(),
            events: AscPtr::<AscEventArray>::null(),
        });

        assert_asc_size!(AscValidatorUpdate {
            address: AscPtr::<Uint8Array>::null(),
            pub_key: AscPtr::<AscPublicKey>::null(),
            power: 0,
        });

        assert_asc_size!(AscConsensusParams {
            block: AscPtr::<AscBlockParams>::null(),
            evidence: AscPtr::<AscEvidenceParams>::null(),
            validator: AscPtr::<AscValidatorParams>::null(),
            version: AscPtr::<AscVersionParams>::null(),
        });

        assert_asc_size!(AscBlockParams {
            max_bytes: 0,
            max_gas: 0,
        });

        assert_asc_size!(AscEvidenceParams {
            max_age_num_blocks: 0,
            max_age_duration: AscPtr::<AscDuration>::null(),
            _padding: 0,
            max_bytes: 0,
        });

        assert_asc_size!(AscDuration {
            seconds: 0,
            nanos: 0,
            _padding: 0,
        });

        assert_asc_size!(AscValidatorParams {
            pub_key_types: AscPtr::<Array<AscPtr<AscString>>>::null(),
        });

        assert_asc_size!(AscVersionParams { app_version: 0 });

        assert_asc_size!(AscTxResult {
            height: 0,
            index: 0,
            tx: AscPtr::<AscTx>::null(),
            result: AscPtr::<AscResponseDeliverTx>::null(),
            hash: AscPtr::<Uint8Array>::null(),
        });

        assert_asc_size!(AscTx {
            body: AscPtr::<AscTxBody>::null(),
            auth_info: AscPtr::<AscAuthInfo>::null(),
            signatures: AscPtr::<AscBytesArray>::null(),
        });

        assert_asc_size!(AscTxBody {
            messages: AscPtr::<AscAnyArray>::null(),
            memo: AscPtr::<AscString>::null(),
            timeout_height: 0,
            extension_options: AscPtr::<AscAnyArray>::null(),
            non_critical_extension_options: AscPtr::<AscAnyArray>::null(),
        });

        assert_asc_size!(AscAny {
            type_url: AscPtr::<AscString>::null(),
            value: AscPtr::<Uint8Array>::null(),
        });

        assert_asc_size!(AscAuthInfo {
            signer_infos: AscPtr::<AscSignerInfoArray>::null(),
            fee: AscPtr::<AscFee>::null(),
            tip: AscPtr::<AscTip>::null(),
        });

        assert_asc_size!(AscSignerInfo {
            public_key: AscPtr::<AscAny>::null(),
            mode_info: AscPtr::<AscModeInfo>::null(),
            sequence: 0,
        });

        assert_asc_size!(AscModeInfo {
            single: AscPtr::<AscModeInfoSingle>::null(),
            multi: AscPtr::<AscModeInfoMulti>::null(),
        });

        assert_asc_size!(AscModeInfoSingle { mode: 0 });

        assert_asc_size!(AscModeInfoMulti {
            bitarray: AscPtr::<AscCompactBitArray>::null(),
            mode_infos: AscPtr::<AscModeInfoArray>::null(),
        });

        assert_asc_size!(AscCompactBitArray {
            extra_bits_stored: 0,
            elems: AscPtr::<Uint8Array>::null(),
        });

        assert_asc_size!(AscFee {
            amount: AscPtr::<AscCoinArray>::null(),
            _padding: 0,
            gas_limit: 0,
            payer: AscPtr::<AscString>::null(),
            granter: AscPtr::<AscString>::null(),
        });

        assert_asc_size!(AscCoin {
            denom: AscPtr::<AscString>::null(),
            amount: AscPtr::<AscString>::null(),
        });

        assert_asc_size!(AscTip {
            amount: AscPtr::<AscCoinArray>::null(),
            tipper: AscPtr::<AscString>::null(),
        });

        assert_asc_size!(AscResponseDeliverTx {
            code: 0,
            data: AscPtr::<Uint8Array>::null(),
            log: AscPtr::<AscString>::null(),
            info: AscPtr::<AscString>::null(),
            gas_wanted: 0,
            gas_used: 0,
            events: AscPtr::<AscEventArray>::null(),
            codespace: AscPtr::<AscString>::null(),
        });

        assert_asc_size!(AscValidatorSetUpdates {
            validator_updates: AscPtr::<AscValidatorArray>::null(),
        });
    }
}
