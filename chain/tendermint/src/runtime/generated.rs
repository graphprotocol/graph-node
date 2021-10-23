use graph_runtime_derive::AscType;
use graph::runtime::{AscType, AscIndexId, DeterministicHostError, IndexForAscTypeId, AscPtr, AscValue};
use graph::{semver, anyhow, semver::Version};
use std::mem::size_of;
use graph_runtime_wasm::asc_abi::class::{Uint8Array, AscEnum, Array, AscString, AscBigInt};

pub(crate) type AscHash = Uint8Array;

pub struct AscEventDataTxArray(pub(crate) Array<AscPtr<AscEventDataTx>>);

impl AscType for AscEventDataTxArray {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(asc_obj: &[u8], api_version: &Version) -> Result<Self, DeterministicHostError> {
        Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
    }
}

impl AscIndexId for AscEventDataTxArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintArrayEventDataTx;
}

pub struct AscEventArray(pub(crate) Array<AscPtr<AscEvent>>);

impl AscType for AscEventArray {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(asc_obj: &[u8], api_version: &Version) -> Result<Self, DeterministicHostError> {
        Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
    }
}

impl AscIndexId for AscEventArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintArrayEvent;
}

pub struct AscValidatorArray(pub(crate) Array<AscPtr<AscValidator>>);

impl AscType for AscValidatorArray {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(asc_obj: &[u8], api_version: &Version) -> Result<Self, DeterministicHostError> {
        Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
    }
}

impl AscIndexId for AscValidatorArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintArrayValidator;
}

pub struct AscCommitSigArray(pub(crate) Array<AscPtr<AscCommitSig>>);

impl AscType for AscCommitSigArray {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(asc_obj: &[u8], api_version: &Version) -> Result<Self, DeterministicHostError> {
        Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
    }
}

impl AscIndexId for AscCommitSigArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintArrayCommitSig;
}

pub struct AscBytesArray(pub(crate) Array<AscPtr<Uint8Array>>);

impl AscType for AscBytesArray {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(asc_obj: &[u8], api_version: &Version) -> Result<Self, DeterministicHostError> {
        Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
    }
}

impl AscIndexId for AscBytesArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintArrayBytes;
}

pub struct AscEvidenceArray(pub(crate) Array<AscPtr<AscEvidence>>);

impl AscType for AscEvidenceArray {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(asc_obj: &[u8], api_version: &Version) -> Result<Self, DeterministicHostError> {
        Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
    }
}

impl AscIndexId for AscEvidenceArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintArrayEvidence;
}

pub struct AscEventAttributeArray(pub(crate) Array<AscPtr<AscEventAttribute>>);

impl AscType for AscEventAttributeArray {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(asc_obj: &[u8], api_version: &Version) -> Result<Self, DeterministicHostError> {
        Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
    }
}

impl AscIndexId for AscEventAttributeArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintArrayEventAttribute;
}


pub struct AscBlockIDFlagEnum(pub(crate) AscEnum<AscBlockIDFlag>);

impl AscType for AscBlockIDFlagEnum {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(asc_obj: &[u8], api_version: &Version) -> Result<Self, DeterministicHostError> {
        Ok(Self(AscEnum::from_asc_bytes(asc_obj, api_version)?))
    }
}

impl AscIndexId for AscBlockIDFlagEnum {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintBlockIDFlagEnum;
}

pub struct AscSignedMsgTypeEnum(pub(crate) AscEnum<AscSignedMsgType>);

impl AscType for AscSignedMsgTypeEnum {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(asc_obj: &[u8], api_version: &Version) -> Result<Self, DeterministicHostError> {
        Ok(Self(AscEnum::from_asc_bytes(asc_obj, api_version)?))
    }
}

impl AscIndexId for AscSignedMsgTypeEnum {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintSignedMsgTypeEnum;
}


#[repr(u32)]
#[derive(AscType, Copy, Clone)]
pub(crate) enum AscSignedMsgType {
    SignedMsgTypeUnknown,
    SignedMsgTypePrevote,
    SignedMsgTypePrecommit,
    SignedMsgTypeProposal,
}

impl AscValue for AscSignedMsgType {}

impl Default for AscSignedMsgType {
    fn default() -> Self {
        Self::SignedMsgTypeUnknown
    }
}


#[repr(u32)]
#[derive(AscType, Copy, Clone)]
pub(crate) enum AscBlockIDFlag {
    BlockIdFlagUnknown,
    BlockIdFlagAbsent,
    BlockIdFlagCommit,
    BlockIdFlagNil,
}

impl AscValue for AscBlockIDFlag {}

impl Default for AscBlockIDFlag {
    fn default() -> Self {
        Self::BlockIdFlagUnknown
    }
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEventList {
    pub newblock: AscPtr<AscEventDataNewBlock>,
    pub transaction: AscPtr<AscEventDataTxArray>,
    pub vote: AscPtr<AscEventDataVote>,
    pub roundstate: AscPtr<AscEventDataRoundState>,
    pub newround: AscPtr<AscEventDataNewRound>,
    pub completeproposal: AscPtr<AscEventDataCompleteProposal>,
    pub validatorsetupdates: AscPtr<AscEventDataValidatorSetUpdates>,
    pub eventdatastring: AscPtr<AscEventDataString>,
    pub blocksyncstatus: AscPtr<AscEventDataBlockSyncStatus>,
    pub statesyncstatus: AscPtr<AscEventDataStateSyncStatus>,
}

impl AscIndexId for AscEventList {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintEventList;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEventDataNewBlock {
    pub block: AscPtr<AscBlock>,
    pub block_id: AscPtr<AscBlockID>,
    pub result_begin_block: AscPtr<AscResponseBeginBlock>,
    pub result_end_block: AscPtr<AscResponseEndBlock>,
}

impl AscIndexId for AscEventDataNewBlock {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintEventDataNewBlock;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscResponseBeginBlock {
    pub events: AscPtr<AscEventArray>,
}

impl AscIndexId for AscResponseBeginBlock {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintResponseBeginBlock;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscResponseEndBlock {
    pub validator_updates: AscPtr<AscValidatorArray>,
    pub consensus_param_updates: AscPtr<AscConsensusParams>,
    pub events: AscPtr<AscEventArray>,
}

impl AscIndexId for AscResponseEndBlock {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintResponseEndBlock;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscConsensusParams {
    pub block: AscPtr<AscBlock>,
    pub evidence: AscPtr<AscEvidence>,
    pub validator: AscPtr<AscValidator>,
    pub version: AscPtr<AscVersion>,
}

impl AscIndexId for AscConsensusParams {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintConsensusParams;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscVersion {
    pub app_version: AscPtr<AscBigInt>,
}

impl AscIndexId for AscVersion {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintVersion;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscBlock {
    pub header: AscPtr<AscHeader>,
    pub data: AscPtr<AscData>,
    pub evidence: AscPtr<AscEvidenceList>,
    pub last_commit: AscPtr<AscCommit>,
}

impl AscIndexId for AscBlock {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintBlock;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscCommit {
    pub height: AscPtr<AscBigInt>,
    pub round: i32,
    pub block_id: AscPtr<AscBlockID>,
    pub signatures: AscPtr<AscCommitSigArray>,
}

impl AscIndexId for AscCommit {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintCommit;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscCommitSig {
    pub block_id_flag: AscPtr<AscBlockIDFlagEnum>,
    pub validator_address: AscPtr<AscAddress>,
    pub timestamp: AscPtr<AscTimestamp>,
    pub signature: AscPtr<Uint8Array>,
}

impl AscIndexId for AscCommitSig {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintCommitSig;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEventDataNewBlockHeader {
    pub header: AscPtr<AscHeader>,
    pub num_txs: i64,
    pub result_begin_block: AscPtr<AscResponseBeginBlock>,
    pub result_end_block: AscPtr<AscResponseEndBlock>,
}

impl AscIndexId for AscEventDataNewBlockHeader {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintEventDataNewBlockHeader;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscHeader {
    pub version: AscPtr<AscConsensus>,
    pub chain_id: AscPtr<AscString>,
    pub height:  AscPtr<AscBigInt>,
    pub time: AscPtr<AscTimestamp>,
    pub last_block_id: AscPtr<AscBlockID>,
    pub last_commit_hash: AscPtr<AscHash>,
    pub data_hash: AscPtr<AscHash>,
    pub validators_hash: AscPtr<AscHash>,
    pub next_validators_hash: AscPtr<AscHash>,
    pub consensus_hash: AscPtr<AscHash>,
    pub app_hash: AscPtr<AscHash>,
    pub last_results_hash: AscPtr<AscHash>,
    pub evidence_hash: AscPtr<AscHash>,
    pub proposer_address: AscPtr<AscAddress>,
}

impl AscIndexId for AscHeader {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintHeader;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscConsensus {
    pub block:  AscPtr<AscBigInt>,
    pub app: AscPtr<AscBigInt>,
}

impl AscIndexId for AscConsensus {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintConsensus;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscBlockID {
    pub hash: AscPtr<AscHash>,
    pub part_set_header: AscPtr<AscPartSetHeader>,
}

impl AscIndexId for AscBlockID {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintBlockID;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscPartSetHeader {
    pub total: u32,
    pub hash: AscPtr<AscHash>,
}

impl AscIndexId for AscPartSetHeader {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintPartSetHeader;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscData {
    pub txs: AscPtr<AscBytesArray>,
}

impl AscIndexId for AscData {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintData;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEvidence {
    pub duplicate_vote_evidence: AscPtr<AscDuplicateVoteEvidence>,
    pub light_client_attack_evidence: AscPtr<AscLightClientAttackEvidence>,
}

impl AscIndexId for AscEvidence {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintEvidence;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscDuplicateVoteEvidence {
    pub vote_a: AscPtr<AscEventDataVote>,
    pub vote_b: AscPtr<AscEventDataVote>,
    pub total_voting_power: i64,
    pub validator_power: i64,
    pub timestamp: AscPtr<AscTimestamp>,
}

impl AscIndexId for AscDuplicateVoteEvidence {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintDuplicateVoteEvidence;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEventDataTx {
    pub tx_result: AscPtr<AscTxResult>,
}

impl AscIndexId for AscEventDataTx {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintEventDataTx;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEventDataVote {
    pub eventvotetype: AscPtr<AscSignedMsgTypeEnum>,
    pub height: AscPtr<AscBigInt>,
    pub round: i32,
    pub block_id: AscPtr<AscBlockID>,
    pub timestamp: AscPtr<AscTimestamp>,
    pub validator_address: AscPtr<AscAddress>,
    pub validator_index: i32,
    pub signature: AscPtr<Uint8Array>,
}

impl AscIndexId for AscEventDataVote {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintEventDataVote;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscLightClientAttackEvidence {
    pub conflicting_block: AscPtr<AscLightBlock>,
    pub common_height: i64,
    pub byzantine_validators: AscPtr<AscValidatorArray>,
    pub total_voting_power: i64,
    pub timestamp: AscPtr<AscTimestamp>,
}

impl AscIndexId for AscLightClientAttackEvidence {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintLightClientAttackEvidence;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscLightBlock {
    pub signed_header: AscPtr<AscSignedHeader>,
    pub validator_set: AscPtr<AscValidatorSet>,
}

impl AscIndexId for AscLightBlock {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintLightBlock;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscValidatorSet {
    pub validators: AscPtr<AscValidatorArray>,
    pub proposer: AscPtr<AscValidator>,
    pub total_voting_power: i64,
}

impl AscIndexId for AscValidatorSet {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintValidatorSet;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscSignedHeader {
    pub header: AscPtr<AscHeader>,
    pub commit: AscPtr<AscCommit>,
}

impl AscIndexId for AscSignedHeader {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintSignedHeader;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEvidenceList {
    pub evidence: AscPtr<AscEvidenceArray>,
}

impl AscIndexId for AscEvidenceList {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintEvidenceList;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscValidator {
    pub address: AscPtr<Uint8Array>,
    pub pub_key: AscPtr<AscPublicKey>,
    pub voting_power: i64,
    pub proposer_priority: i64,
}

impl AscIndexId for AscValidator {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintValidator;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscPublicKey {
    pub ed25519: AscPtr<Uint8Array>,
    pub secp256k1: AscPtr<Uint8Array>,
    pub sr25519: AscPtr<Uint8Array>,
}

impl AscIndexId for AscPublicKey {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintPublicKey;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscTxResult {
    pub height: AscPtr<AscBigInt>,
    pub index: u32,
    pub tx: AscPtr<Uint8Array>,
    pub result: AscPtr<AscResponseDeliverTx>,
}

impl AscIndexId for AscTxResult {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintTxResult;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscResponseDeliverTx {
    pub code: u32,
    pub data: AscPtr<Uint8Array>,
    pub log: AscPtr<AscString>,
    pub info: AscPtr<AscString>,
    pub gas_wanted: AscPtr<AscBigInt>,
    pub gas_used: AscPtr<AscBigInt>,
    pub events: AscPtr<AscEventArray>,
    pub codespace: AscPtr<AscString>,
}

impl AscIndexId for AscResponseDeliverTx {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintResponseDeliverTx;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEvent {
    pub eventtype: AscPtr<AscString>,
    pub attributes: AscPtr<AscEventAttributeArray>,
}

impl AscIndexId for AscEvent {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintEvent;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEventAttribute {
    pub key: AscPtr<AscString>,
    pub value: AscPtr<AscString>,
    //pub index: bool,
}

impl AscIndexId for AscEventAttribute {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintEventAttribute;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEventDataRoundState {
    pub height: AscPtr<AscBigInt>,
    pub round: i32,
    pub step: AscPtr<AscString>,
}

impl AscIndexId for AscEventDataRoundState {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintEventDataRoundState;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEventDataNewRound {
    pub height: AscPtr<AscBigInt>,
    pub round: i32,
    pub step: AscPtr<AscString>,
    pub proposer: AscPtr<AscValidatorInfo>,
}

impl AscIndexId for AscEventDataNewRound {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintEventDataNewRound;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscValidatorInfo {
    pub address: AscPtr<AscAddress>,
    pub index: i32,
}

impl AscIndexId for AscValidatorInfo {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintValidatorInfo;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscAddress {
    pub address: AscPtr<Uint8Array>,
}

impl AscIndexId for AscAddress {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintAddress;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEventDataCompleteProposal {
    pub height: AscPtr<AscBigInt>,
    pub round: i32,
    pub step: AscPtr<AscString>,
    pub block_id: AscPtr<AscBlockID>,
}

impl AscIndexId for AscEventDataCompleteProposal {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintEventDataCompleteProposal;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEventDataValidatorSetUpdates {
    pub validator_updates: AscPtr<AscValidatorArray>,
}

impl AscIndexId for AscEventDataValidatorSetUpdates {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintEventDataValidatorSetUpdates;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEventDataString {
    pub eventdatastring: AscPtr<AscString>,
}

impl AscIndexId for AscEventDataString {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintEventDataString;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEventDataBlockSyncStatus {
    pub complete: bool,
    pub height: AscPtr<AscBigInt>,
}

impl AscIndexId for AscEventDataBlockSyncStatus {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintEventDataBlockSyncStatus;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEventDataStateSyncStatus {
    pub complete: bool,
    pub height: AscPtr<AscBigInt>,
}

impl AscIndexId for AscEventDataStateSyncStatus {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintEventDataStateSyncStatus;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscTimestamp {
    pub seconds:  AscPtr<AscBigInt>,
    pub nanos:  i32,
}

impl AscIndexId for AscTimestamp {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintTimestamp;
}


#[repr(C)]
#[derive(AscType)]
pub(crate) struct Ascfig {
}

impl AscIndexId for Ascfig {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::Tendermintfig;
}


