use graph::runtime::{
    AscIndexId, AscPtr, AscType, AscValue, DeterministicHostError, IndexForAscTypeId,
};
use graph::semver::Version;
use graph_runtime_derive::AscType;
use graph_runtime_wasm::asc_abi::class::{Array, AscEnum, AscString, Uint8Array};

pub(crate) type AscBytes = Uint8Array;
pub(crate) type AscHash = Uint8Array;
pub(crate) type AscGas = i64;

pub struct AscEventTxArray(pub(crate) Array<AscPtr<AscEventTx>>);

impl AscType for AscEventTxArray {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(
        asc_obj: &[u8],
        api_version: &Version,
    ) -> Result<Self, DeterministicHostError> {
        Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
    }
}

impl AscIndexId for AscEventTxArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintArrayEventTx;
}

pub struct AscCommitSigArray(pub(crate) Array<AscPtr<AscCommitSig>>);

impl AscType for AscCommitSigArray {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(
        asc_obj: &[u8],
        api_version: &Version,
    ) -> Result<Self, DeterministicHostError> {
        Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
    }
}

impl AscIndexId for AscCommitSigArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintArrayCommitSig;
}

pub struct AscBytesArray(pub(crate) Array<AscPtr<AscBytes>>);

impl AscType for AscBytesArray {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(
        asc_obj: &[u8],
        api_version: &Version,
    ) -> Result<Self, DeterministicHostError> {
        Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
    }
}

impl AscIndexId for AscBytesArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintArrayBytes;
}

pub struct AscEventAttributeArray(pub(crate) Array<AscPtr<AscEventAttribute>>);

impl AscType for AscEventAttributeArray {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(
        asc_obj: &[u8],
        api_version: &Version,
    ) -> Result<Self, DeterministicHostError> {
        Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
    }
}

impl AscIndexId for AscEventAttributeArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintArrayEventAttribute;
}

pub struct AscValidatorArray(pub(crate) Array<AscPtr<AscValidator>>);

impl AscType for AscValidatorArray {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(
        asc_obj: &[u8],
        api_version: &Version,
    ) -> Result<Self, DeterministicHostError> {
        Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
    }
}

impl AscIndexId for AscValidatorArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintArrayValidator;
}

pub struct AscEvidenceArray(pub(crate) Array<AscPtr<AscEvidence>>);

impl AscType for AscEvidenceArray {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(
        asc_obj: &[u8],
        api_version: &Version,
    ) -> Result<Self, DeterministicHostError> {
        Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
    }
}

impl AscIndexId for AscEvidenceArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintArrayEvidence;
}

pub struct AscEventArray(pub(crate) Array<AscPtr<AscEvent>>);

impl AscType for AscEventArray {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(
        asc_obj: &[u8],
        api_version: &Version,
    ) -> Result<Self, DeterministicHostError> {
        Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
    }
}

impl AscIndexId for AscEventArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintArrayEvent;
}

pub struct AscValidatorUpdateArray(pub(crate) Array<AscPtr<AscValidatorUpdate>>);

impl AscType for AscValidatorUpdateArray {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(
        asc_obj: &[u8],
        api_version: &Version,
    ) -> Result<Self, DeterministicHostError> {
        Ok(Self(Array::from_asc_bytes(asc_obj, api_version)?))
    }
}

impl AscIndexId for AscValidatorUpdateArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintArrayValidatorUpdate;
}

pub struct AscBlockIDFlagEnum(pub(crate) AscEnum<AscBlockIDFlag>);

impl AscType for AscBlockIDFlagEnum {
    fn to_asc_bytes(&self) -> Result<Vec<u8>, DeterministicHostError> {
        self.0.to_asc_bytes()
    }

    fn from_asc_bytes(
        asc_obj: &[u8],
        api_version: &Version,
    ) -> Result<Self, DeterministicHostError> {
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

    fn from_asc_bytes(
        asc_obj: &[u8],
        api_version: &Version,
    ) -> Result<Self, DeterministicHostError> {
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
pub(crate) struct AscEventData {
    pub event: AscPtr<AscEvent>,
    pub block: AscPtr<AscEventBlock>,
}

impl AscIndexId for AscEventData {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintEventData;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEventList {
    pub new_block: AscPtr<AscEventBlock>,
    pub transaction: AscPtr<AscEventTxArray>,
    pub validator_set_updates: AscPtr<AscEventValidatorSetUpdates>,
}

impl AscIndexId for AscEventList {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintEventList;
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
pub(crate) struct AscBlockID {
    pub hash: AscPtr<AscHash>,
    pub part_set_header: AscPtr<AscPartSetHeader>,
}

impl AscIndexId for AscBlockID {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintBlockID;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscBlockParams {
    pub max_bytes: i64,
    pub max_gas: AscGas,
}

impl AscIndexId for AscBlockParams {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintBlockParams;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscCommit {
    pub height: i64,
    pub round: i32,
    pub block_id: AscPtr<AscBlockID>,
    pub signatures: AscPtr<AscCommitSigArray>,
    pub _padding: u32,
}

impl AscIndexId for AscCommit {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintCommit;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscCommitSig {
    pub block_id_flag: u32,
    pub validator_address: AscPtr<AscHash>,
    pub timestamp: AscPtr<AscTimestamp>,
    pub signature: AscPtr<AscBytes>,
}

impl AscIndexId for AscCommitSig {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintCommitSig;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscConsensus {
    pub block: u64,
    pub app: u64,
}

impl AscIndexId for AscConsensus {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintConsensus;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscConsensusParams {
    pub block: AscPtr<AscBlockParams>,
    pub evidence: AscPtr<AscEvidenceParams>,
    pub validator: AscPtr<AscValidatorParams>,
    pub version: AscPtr<AscVersionParams>,
}

impl AscIndexId for AscConsensusParams {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintConsensusParams;
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
pub(crate) struct AscDuration {
    pub seconds: i64,
    pub nanos: i32,
    pub _padding: u32,
}

impl AscIndexId for AscDuration {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintDuration;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscDuplicateVoteEvidence {
    pub vote_a: AscPtr<AscEventVote>,
    pub vote_b: AscPtr<AscEventVote>,
    pub total_voting_power: i64,
    pub validator_power: i64,
    pub timestamp: AscPtr<AscTimestamp>,
}

impl AscIndexId for AscDuplicateVoteEvidence {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintDuplicateVoteEvidence;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEvent {
    pub event_type: AscPtr<AscString>,
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
    pub index: bool,
    pub _padding: u8,
    pub _padding2: u16,
}

impl AscIndexId for AscEventAttribute {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintEventAttribute;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEventBlock {
    pub block: AscPtr<AscBlock>,
    pub block_id: AscPtr<AscBlockID>,
    pub result_begin_block: AscPtr<AscResponseBeginBlock>,
    pub result_end_block: AscPtr<AscResponseEndBlock>,
}

impl AscIndexId for AscEventBlock {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintEventBlock;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEventTx {
    pub tx_result: AscPtr<AscTxResult>,
}

impl AscIndexId for AscEventTx {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintEventTx;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEventValidatorSetUpdates {
    pub validator_updates: AscPtr<AscValidatorArray>,
}

impl AscIndexId for AscEventValidatorSetUpdates {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId =
        IndexForAscTypeId::TendermintEventValidatorSetUpdates;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEventVote {
    pub event_vote_type: u32,
    pub height: u64,
    pub round: i32,
    pub block_id: AscPtr<AscBlockID>,
    pub timestamp: AscPtr<AscTimestamp>,
    pub validator_address: AscPtr<AscHash>,
    pub validator_index: i32,
    pub signature: AscPtr<AscBytes>,
}

impl AscIndexId for AscEventVote {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintEventVote;
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
pub(crate) struct AscEvidenceList {
    pub evidence: AscPtr<AscEvidenceArray>,
}

impl AscIndexId for AscEvidenceList {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintEvidenceList;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEvidenceParams {
    pub max_age_num_blocks: i64,
    pub max_age_duration: AscPtr<AscDuration>,
    pub max_bytes: i64,
}

impl AscIndexId for AscEvidenceParams {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintEvidenceParams;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscHeader {
    pub version: AscPtr<AscConsensus>,
    pub chain_id: AscPtr<AscString>,
    pub height: u64,
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
    pub proposer_address: AscPtr<AscHash>,
    pub _padding: u32,
}

impl AscIndexId for AscHeader {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintHeader;
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
pub(crate) struct AscLightClientAttackEvidence {
    pub conflicting_block: AscPtr<AscLightBlock>,
    pub common_height: i64,
    pub byzantine_validators: AscPtr<AscValidatorArray>,
    pub total_voting_power: i64,
    pub timestamp: AscPtr<AscTimestamp>,
}

impl AscIndexId for AscLightClientAttackEvidence {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId =
        IndexForAscTypeId::TendermintLightClientAttackEvidence;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscPublicKey {
    pub ed25519: AscPtr<AscBytes>,
    pub secp256k1: AscPtr<AscBytes>,
}

impl AscIndexId for AscPublicKey {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintPublicKey;
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
pub(crate) struct AscResponseBeginBlock {
    pub events: AscPtr<AscEventArray>,
}

impl AscIndexId for AscResponseBeginBlock {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintResponseBeginBlock;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscResponseEndBlock {
    pub validator_updates: AscPtr<AscValidatorUpdateArray>,
    pub consensus_param_updates: AscPtr<AscConsensusParams>,
    pub events: AscPtr<AscEventArray>,
}

impl AscIndexId for AscResponseEndBlock {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintResponseEndBlock;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscResponseDeliverTx {
    pub code: u32,
    pub data: AscPtr<AscBytes>,
    pub log: AscPtr<AscString>,
    pub info: AscPtr<AscString>,
    pub gas_wanted: AscGas,
    pub gas_used: AscGas,
    pub events: AscPtr<AscEventArray>,
    pub codespace: AscPtr<AscString>,
}

impl AscIndexId for AscResponseDeliverTx {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintResponseDeliverTx;
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
pub(crate) struct AscTimestamp {
    pub seconds: i64,
    pub nanos: i32,
    pub _padding: u32,
}

impl AscIndexId for AscTimestamp {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintTimestamp;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscTxResult {
    pub height: u64,
    pub index: u32,
    pub tx: AscPtr<AscBytes>,
    pub result: AscPtr<AscResponseDeliverTx>,
    pub _padding: u32,
}

impl AscIndexId for AscTxResult {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintTxResult;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscValidator {
    pub address: AscPtr<AscHash>,
    pub pub_key: AscPtr<AscPublicKey>,
    pub voting_power: i64,
    pub proposer_priority: i64,
}

impl AscIndexId for AscValidator {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintValidator;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscValidatorParams {
    pub pub_key_types: AscPtr<Array<AscPtr<AscString>>>,
}

impl AscIndexId for AscValidatorParams {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintValidatorParams;
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
pub(crate) struct AscValidatorUpdate {
    pub address: AscPtr<Uint8Array>,
    pub pub_key: AscPtr<AscPublicKey>,
    pub power: i64,
}

impl AscIndexId for AscValidatorUpdate {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintValidatorUpdate;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscVersionParams {
    pub app_version: u64,
}

impl AscIndexId for AscVersionParams {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::TendermintVersionParams;
}
