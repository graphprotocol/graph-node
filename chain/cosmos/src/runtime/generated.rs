use graph::runtime::{
    AscIndexId, AscPtr, AscType, AscValue, DeterministicHostError, IndexForAscTypeId,
};
use graph::semver::Version;
use graph_runtime_derive::AscType;
use graph_runtime_wasm::asc_abi::class::{Array, AscEnum, AscString, Uint8Array};

pub struct AscTxResultArray(pub(crate) Array<AscPtr<AscTxResult>>);

impl AscType for AscTxResultArray {
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

impl AscIndexId for AscTxResultArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosArrayTxResult;
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
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosArrayValidator;
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
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosArrayEvidence;
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
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosArrayCommitSig;
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
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosArrayEvent;
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
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosArrayEventAttribute;
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
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosArrayValidatorUpdate;
}

pub struct AscBytesArray(pub(crate) Array<AscPtr<Uint8Array>>);

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
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosArrayBytes;
}

pub struct AscAnyArray(pub(crate) Array<AscPtr<AscAny>>);

impl AscType for AscAnyArray {
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

impl AscIndexId for AscAnyArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosArrayAny;
}

pub struct AscSignerInfoArray(pub(crate) Array<AscPtr<AscSignerInfo>>);

impl AscType for AscSignerInfoArray {
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

impl AscIndexId for AscSignerInfoArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosArraySignerInfo;
}

pub struct AscModeInfoArray(pub(crate) Array<AscPtr<AscModeInfo>>);

impl AscType for AscModeInfoArray {
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

impl AscIndexId for AscModeInfoArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosArrayModeInfo;
}

pub struct AscCoinArray(pub(crate) Array<AscPtr<AscCoin>>);

impl AscType for AscCoinArray {
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

impl AscIndexId for AscCoinArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosArrayCoin;
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
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosSignedMsgTypeEnum;
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
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosBlockIDFlagEnum;
}

pub struct AscSignModeEnum(pub(crate) AscEnum<AscSignMode>);

impl AscType for AscSignModeEnum {
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

impl AscIndexId for AscSignModeEnum {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosSignModeEnum;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscBlock {
    pub header: AscPtr<AscHeader>,
    pub evidence: AscPtr<AscEvidenceList>,
    pub last_commit: AscPtr<AscCommit>,
    pub result_begin_block: AscPtr<AscResponseBeginBlock>,
    pub result_end_block: AscPtr<AscResponseEndBlock>,
    pub transactions: AscPtr<AscTxResultArray>,
    pub validator_updates: AscPtr<AscValidatorArray>,
}

impl AscIndexId for AscBlock {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosBlock;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscHeaderOnlyBlock {
    pub header: AscPtr<AscHeader>,
}

impl AscIndexId for AscHeaderOnlyBlock {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosHeaderOnlyBlock;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEventData {
    pub event: AscPtr<AscEvent>,
    pub block: AscPtr<AscHeaderOnlyBlock>,
}

impl AscIndexId for AscEventData {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosEventData;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscTransactionData {
    pub tx: AscPtr<AscTxResult>,
    pub block: AscPtr<AscHeaderOnlyBlock>,
}

impl AscIndexId for AscTransactionData {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosTransactionData;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscHeader {
    pub version: AscPtr<AscConsensus>,
    pub chain_id: AscPtr<AscString>,
    pub height: u64,
    pub time: AscPtr<AscTimestamp>,
    pub last_block_id: AscPtr<AscBlockID>,
    pub last_commit_hash: AscPtr<Uint8Array>,
    pub data_hash: AscPtr<Uint8Array>,
    pub validators_hash: AscPtr<Uint8Array>,
    pub next_validators_hash: AscPtr<Uint8Array>,
    pub consensus_hash: AscPtr<Uint8Array>,
    pub app_hash: AscPtr<Uint8Array>,
    pub last_results_hash: AscPtr<Uint8Array>,
    pub evidence_hash: AscPtr<Uint8Array>,
    pub proposer_address: AscPtr<Uint8Array>,
    pub hash: AscPtr<Uint8Array>,
}

impl AscIndexId for AscHeader {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosHeader;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscConsensus {
    pub block: u64,
    pub app: u64,
}

impl AscIndexId for AscConsensus {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosConsensus;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscTimestamp {
    pub seconds: i64,
    pub nanos: i32,
    pub _padding: u32,
}

impl AscIndexId for AscTimestamp {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosTimestamp;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscBlockID {
    pub hash: AscPtr<Uint8Array>,
    pub part_set_header: AscPtr<AscPartSetHeader>,
}

impl AscIndexId for AscBlockID {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosBlockID;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscPartSetHeader {
    pub total: u32,
    pub hash: AscPtr<Uint8Array>,
}

impl AscIndexId for AscPartSetHeader {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosPartSetHeader;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEvidenceList {
    pub evidence: AscPtr<AscEvidenceArray>,
}

impl AscIndexId for AscEvidenceList {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosEvidenceList;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEvidence {
    pub duplicate_vote_evidence: AscPtr<AscDuplicateVoteEvidence>,
    pub light_client_attack_evidence: AscPtr<AscLightClientAttackEvidence>,
}

impl AscIndexId for AscEvidence {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosEvidence;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscDuplicateVoteEvidence {
    pub vote_a: AscPtr<AscEventVote>,
    pub vote_b: AscPtr<AscEventVote>,
    pub total_voting_power: i64,
    pub validator_power: i64,
    pub timestamp: AscPtr<AscTimestamp>,
    pub _padding: u32,
}

impl AscIndexId for AscDuplicateVoteEvidence {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosDuplicateVoteEvidence;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEventVote {
    pub event_vote_type: i32,
    pub _padding: u32,
    pub height: u64,
    pub round: i32,
    pub block_id: AscPtr<AscBlockID>,
    pub timestamp: AscPtr<AscTimestamp>,
    pub validator_address: AscPtr<Uint8Array>,
    pub validator_index: i32,
    pub signature: AscPtr<Uint8Array>,
}

impl AscIndexId for AscEventVote {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosEventVote;
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

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscLightClientAttackEvidence {
    pub conflicting_block: AscPtr<AscLightBlock>,
    pub _padding: u32,
    pub common_height: i64,
    pub total_voting_power: i64,
    pub byzantine_validators: AscPtr<AscValidatorArray>,
    pub timestamp: AscPtr<AscTimestamp>,
}

impl AscIndexId for AscLightClientAttackEvidence {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosLightClientAttackEvidence;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscLightBlock {
    pub signed_header: AscPtr<AscSignedHeader>,
    pub validator_set: AscPtr<AscValidatorSet>,
}

impl AscIndexId for AscLightBlock {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosLightBlock;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscSignedHeader {
    pub header: AscPtr<AscHeader>,
    pub commit: AscPtr<AscCommit>,
}

impl AscIndexId for AscSignedHeader {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosSignedHeader;
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
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosCommit;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscCommitSig {
    pub block_id_flag: i32,
    pub validator_address: AscPtr<Uint8Array>,
    pub timestamp: AscPtr<AscTimestamp>,
    pub signature: AscPtr<Uint8Array>,
}

impl AscIndexId for AscCommitSig {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosCommitSig;
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
pub(crate) struct AscValidatorSet {
    pub validators: AscPtr<AscValidatorArray>,
    pub proposer: AscPtr<AscValidator>,
    pub total_voting_power: i64,
}

impl AscIndexId for AscValidatorSet {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosValidatorSet;
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
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosValidator;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscPublicKey {
    pub ed25519: AscPtr<Uint8Array>,
    pub secp256k1: AscPtr<Uint8Array>,
}

impl AscIndexId for AscPublicKey {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosPublicKey;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscResponseBeginBlock {
    pub events: AscPtr<AscEventArray>,
}

impl AscIndexId for AscResponseBeginBlock {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosResponseBeginBlock;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEvent {
    pub event_type: AscPtr<AscString>,
    pub attributes: AscPtr<AscEventAttributeArray>,
}

impl AscIndexId for AscEvent {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosEvent;
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
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosEventAttribute;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscResponseEndBlock {
    pub validator_updates: AscPtr<AscValidatorUpdateArray>,
    pub consensus_param_updates: AscPtr<AscConsensusParams>,
    pub events: AscPtr<AscEventArray>,
}

impl AscIndexId for AscResponseEndBlock {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosResponseEndBlock;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscValidatorUpdate {
    pub address: AscPtr<Uint8Array>,
    pub pub_key: AscPtr<AscPublicKey>,
    pub power: i64,
}

impl AscIndexId for AscValidatorUpdate {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosValidatorUpdate;
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
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosConsensusParams;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscBlockParams {
    pub max_bytes: i64,
    pub max_gas: i64,
}

impl AscIndexId for AscBlockParams {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosBlockParams;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscEvidenceParams {
    pub max_age_num_blocks: i64,
    pub max_age_duration: AscPtr<AscDuration>,
    pub _padding: u32,
    pub max_bytes: i64,
}

impl AscIndexId for AscEvidenceParams {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosEvidenceParams;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscDuration {
    pub seconds: i64,
    pub nanos: i32,
    pub _padding: u32,
}

impl AscIndexId for AscDuration {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosDuration;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscValidatorParams {
    pub pub_key_types: AscPtr<Array<AscPtr<AscString>>>,
}

impl AscIndexId for AscValidatorParams {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosValidatorParams;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscVersionParams {
    pub app_version: u64,
}

impl AscIndexId for AscVersionParams {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosVersionParams;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscTxResult {
    pub height: u64,
    pub index: u32,
    pub tx: AscPtr<AscTx>,
    pub result: AscPtr<AscResponseDeliverTx>,
    pub hash: AscPtr<Uint8Array>,
}

impl AscIndexId for AscTxResult {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosTxResult;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscTx {
    pub body: AscPtr<AscTxBody>,
    pub auth_info: AscPtr<AscAuthInfo>,
    pub signatures: AscPtr<AscBytesArray>,
}

impl AscIndexId for AscTx {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosTx;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscTxBody {
    pub messages: AscPtr<AscAnyArray>,
    pub memo: AscPtr<AscString>,
    pub timeout_height: u64,
    pub extension_options: AscPtr<AscAnyArray>,
    pub non_critical_extension_options: AscPtr<AscAnyArray>,
}

impl AscIndexId for AscTxBody {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosTxBody;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscAny {
    pub type_url: AscPtr<AscString>,
    pub value: AscPtr<Uint8Array>,
}

impl AscIndexId for AscAny {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosAny;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscAuthInfo {
    pub signer_infos: AscPtr<AscSignerInfoArray>,
    pub fee: AscPtr<AscFee>,
    pub tip: AscPtr<AscTip>,
}

impl AscIndexId for AscAuthInfo {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosAuthInfo;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscSignerInfo {
    pub public_key: AscPtr<AscAny>,
    pub mode_info: AscPtr<AscModeInfo>,
    pub sequence: u64,
}

impl AscIndexId for AscSignerInfo {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosSignerInfo;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscModeInfo {
    pub single: AscPtr<AscModeInfoSingle>,
    pub multi: AscPtr<AscModeInfoMulti>,
}

impl AscIndexId for AscModeInfo {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosModeInfo;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscModeInfoSingle {
    pub mode: i32,
}

impl AscIndexId for AscModeInfoSingle {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosModeInfoSingle;
}

#[repr(u32)]
#[derive(AscType, Copy, Clone)]
pub(crate) enum AscSignMode {
    SignModeUnspecified,
    SignModeDirect,
    SignModeTextual,
    SignModeLegacyAminoJson,
}

impl AscValue for AscSignMode {}

impl Default for AscSignMode {
    fn default() -> Self {
        Self::SignModeUnspecified
    }
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscModeInfoMulti {
    pub bitarray: AscPtr<AscCompactBitArray>,
    pub mode_infos: AscPtr<AscModeInfoArray>,
}

impl AscIndexId for AscModeInfoMulti {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosModeInfoMulti;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscCompactBitArray {
    pub extra_bits_stored: u32,
    pub elems: AscPtr<Uint8Array>,
}

impl AscIndexId for AscCompactBitArray {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosCompactBitArray;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscFee {
    pub amount: AscPtr<AscCoinArray>,
    pub _padding: u32,
    pub gas_limit: u64,
    pub payer: AscPtr<AscString>,
    pub granter: AscPtr<AscString>,
}

impl AscIndexId for AscFee {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosFee;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscCoin {
    pub denom: AscPtr<AscString>,
    pub amount: AscPtr<AscString>,
}

impl AscIndexId for AscCoin {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosCoin;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscTip {
    pub amount: AscPtr<AscCoinArray>,
    pub tipper: AscPtr<AscString>,
}

impl AscIndexId for AscTip {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosTip;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscResponseDeliverTx {
    pub code: u32,
    pub data: AscPtr<Uint8Array>,
    pub log: AscPtr<AscString>,
    pub info: AscPtr<AscString>,
    pub gas_wanted: i64,
    pub gas_used: i64,
    pub events: AscPtr<AscEventArray>,
    pub codespace: AscPtr<AscString>,
}

impl AscIndexId for AscResponseDeliverTx {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosResponseDeliverTx;
}

#[repr(C)]
#[derive(AscType)]
pub(crate) struct AscValidatorSetUpdates {
    pub validator_updates: AscPtr<AscValidatorArray>,
}

impl AscIndexId for AscValidatorSetUpdates {
    const INDEX_ASC_TYPE_ID: IndexForAscTypeId = IndexForAscTypeId::CosmosValidatorSetUpdates;
}
