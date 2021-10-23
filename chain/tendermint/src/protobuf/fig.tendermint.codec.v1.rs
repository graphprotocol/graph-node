#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventList {
    #[prost(message, optional, tag = "1")]
    pub newblock: ::core::option::Option<EventDataNewBlock>,
    #[prost(message, repeated, tag = "2")]
    pub transaction: ::prost::alloc::vec::Vec<EventDataTx>,
    #[prost(message, optional, tag = "3")]
    pub vote: ::core::option::Option<EventDataVote>,
    #[prost(message, optional, tag = "4")]
    pub roundstate: ::core::option::Option<EventDataRoundState>,
    #[prost(message, optional, tag = "5")]
    pub newround: ::core::option::Option<EventDataNewRound>,
    #[prost(message, optional, tag = "6")]
    pub completeproposal: ::core::option::Option<EventDataCompleteProposal>,
    #[prost(message, optional, tag = "7")]
    pub validatorsetupdates: ::core::option::Option<EventDataValidatorSetUpdates>,
    #[prost(message, optional, tag = "8")]
    pub eventdatastring: ::core::option::Option<EventDataString>,
    #[prost(message, optional, tag = "9")]
    pub blocksyncstatus: ::core::option::Option<EventDataBlockSyncStatus>,
    #[prost(message, optional, tag = "10")]
    pub statesyncstatus: ::core::option::Option<EventDataStateSyncStatus>,
}
/// EventDataNewBlock
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventDataNewBlock {
    #[prost(message, optional, tag = "1")]
    pub block: ::core::option::Option<Block>,
    /// not present in v0.34.13
    #[prost(message, optional, tag = "2")]
    pub block_id: ::core::option::Option<BlockId>,
    #[prost(message, optional, tag = "3")]
    pub result_begin_block: ::core::option::Option<ResponseBeginBlock>,
    #[prost(message, optional, tag = "4")]
    pub result_end_block: ::core::option::Option<ResponseEndBlock>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResponseBeginBlock {
    #[prost(message, repeated, tag = "1")]
    pub events: ::prost::alloc::vec::Vec<Event>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResponseEndBlock {
    #[prost(message, repeated, tag = "1")]
    pub validator_updates: ::prost::alloc::vec::Vec<Validator>,
    #[prost(message, optional, tag = "2")]
    pub consensus_param_updates: ::core::option::Option<ConsensusParams>,
    #[prost(message, repeated, tag = "3")]
    pub events: ::prost::alloc::vec::Vec<Event>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConsensusParams {
    #[prost(message, optional, tag = "1")]
    pub block: ::core::option::Option<Block>,
    #[prost(message, optional, tag = "2")]
    pub evidence: ::core::option::Option<Evidence>,
    #[prost(message, optional, tag = "3")]
    pub validator: ::core::option::Option<Validator>,
    #[prost(message, optional, tag = "4")]
    pub version: ::core::option::Option<Version>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Version {
    #[prost(uint64, tag = "1")]
    pub app_version: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Block {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<Header>,
    #[prost(message, optional, tag = "2")]
    pub data: ::core::option::Option<Data>,
    #[prost(message, optional, tag = "3")]
    pub evidence: ::core::option::Option<EvidenceList>,
    #[prost(message, optional, tag = "4")]
    pub last_commit: ::core::option::Option<Commit>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Commit {
    /// must be > 0
    #[prost(uint64, tag = "1")]
    pub height: u64,
    /// must be > 0
    #[prost(int32, tag = "2")]
    pub round: i32,
    #[prost(message, optional, tag = "3")]
    pub block_id: ::core::option::Option<BlockId>,
    /// must be > 0
    #[prost(message, repeated, tag = "4")]
    pub signatures: ::prost::alloc::vec::Vec<CommitSig>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommitSig {
    /// must match those in the enum above
    #[prost(enumeration = "BlockIdFlag", tag = "1")]
    pub block_id_flag: i32,
    /// hash 256 20 bytes - this is the first 20 characters of a 32-byte -
    #[prost(message, optional, tag = "2")]
    pub validator_address: ::core::option::Option<Address>,
    /// key - SHA256(pubkey)[:20]
    #[prost(message, optional, tag = "3")]
    pub timestamp: ::core::option::Option<Timestamp>,
    /// length should be > 0 and < 64
    #[prost(bytes = "vec", tag = "4")]
    pub signature: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventDataNewBlockHeader {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<Header>,
    #[prost(int64, tag = "2")]
    pub num_txs: i64,
    #[prost(message, optional, tag = "3")]
    pub result_begin_block: ::core::option::Option<ResponseBeginBlock>,
    #[prost(message, optional, tag = "4")]
    pub result_end_block: ::core::option::Option<ResponseEndBlock>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Header {
    #[prost(message, optional, tag = "1")]
    pub version: ::core::option::Option<Consensus>,
    #[prost(string, tag = "2")]
    pub chain_id: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub height: u64,
    #[prost(message, optional, tag = "4")]
    pub time: ::core::option::Option<Timestamp>,
    #[prost(message, optional, tag = "5")]
    pub last_block_id: ::core::option::Option<BlockId>,
    /// hash 256 32 bytes
    #[prost(bytes = "vec", tag = "6")]
    pub last_commit_hash: ::prost::alloc::vec::Vec<u8>,
    /// hash 256 32 bytes
    #[prost(bytes = "vec", tag = "7")]
    pub data_hash: ::prost::alloc::vec::Vec<u8>,
    /// hash 256 32 bytes
    #[prost(bytes = "vec", tag = "8")]
    pub validators_hash: ::prost::alloc::vec::Vec<u8>,
    /// hash 256 32 bytes
    #[prost(bytes = "vec", tag = "9")]
    pub next_validators_hash: ::prost::alloc::vec::Vec<u8>,
    /// hash 256 32 bytes
    #[prost(bytes = "vec", tag = "10")]
    pub consensus_hash: ::prost::alloc::vec::Vec<u8>,
    /// determined by application, not a fixed length
    #[prost(bytes = "vec", tag = "11")]
    pub app_hash: ::prost::alloc::vec::Vec<u8>,
    /// hash 256 32 bytes - first block will be results of an empty hash
    #[prost(bytes = "vec", tag = "12")]
    pub last_results_hash: ::prost::alloc::vec::Vec<u8>,
    /// hash sha256 32 bytes
    #[prost(bytes = "vec", tag = "13")]
    pub evidence_hash: ::prost::alloc::vec::Vec<u8>,
    /// hash 256 20 bytes - this is the first 20 characters
    #[prost(message, optional, tag = "14")]
    pub proposer_address: ::core::option::Option<Address>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Consensus {
    #[prost(uint64, tag = "1")]
    pub block: u64,
    #[prost(uint64, tag = "2")]
    pub app: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockId {
    #[prost(bytes = "vec", tag = "1")]
    pub hash: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, optional, tag = "2")]
    pub part_set_header: ::core::option::Option<PartSetHeader>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PartSetHeader {
    #[prost(uint32, tag = "1")]
    pub total: u32,
    /// hash 256 32 bytes
    #[prost(bytes = "vec", tag = "2")]
    pub hash: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Data {
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub txs: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Evidence {
    #[prost(oneof = "evidence::Sum", tags = "1, 2")]
    pub sum: ::core::option::Option<evidence::Sum>,
}
/// Nested message and enum types in `Evidence`.
pub mod evidence {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Sum {
        #[prost(message, tag = "1")]
        DuplicateVoteEvidence(super::DuplicateVoteEvidence),
        #[prost(message, tag = "2")]
        LightClientAttackEvidence(super::LightClientAttackEvidence),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DuplicateVoteEvidence {
    #[prost(message, optional, tag = "1")]
    pub vote_a: ::core::option::Option<EventDataVote>,
    #[prost(message, optional, tag = "2")]
    pub vote_b: ::core::option::Option<EventDataVote>,
    #[prost(int64, tag = "3")]
    pub total_voting_power: i64,
    #[prost(int64, tag = "4")]
    pub validator_power: i64,
    #[prost(message, optional, tag = "5")]
    pub timestamp: ::core::option::Option<Timestamp>,
}
/// EventDataTx
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventDataTx {
    #[prost(message, optional, tag = "1")]
    pub tx_result: ::core::option::Option<TxResult>,
}
/// EventDataVote
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventDataVote {
    /// should be present in the enum at the top of this file
    #[prost(enumeration = "SignedMsgType", tag = "1")]
    pub eventvotetype: i32,
    /// must be > 0
    #[prost(uint64, tag = "2")]
    pub height: u64,
    /// must be > 0
    #[prost(int32, tag = "3")]
    pub round: i32,
    #[prost(message, optional, tag = "4")]
    pub block_id: ::core::option::Option<BlockId>,
    #[prost(message, optional, tag = "5")]
    pub timestamp: ::core::option::Option<Timestamp>,
    /// hash with a length of 20
    #[prost(message, optional, tag = "6")]
    pub validator_address: ::core::option::Option<Address>,
    #[prost(int32, tag = "7")]
    pub validator_index: i32,
    /// hash length should be >0 and <64
    #[prost(bytes = "vec", tag = "8")]
    pub signature: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LightClientAttackEvidence {
    #[prost(message, optional, tag = "1")]
    pub conflicting_block: ::core::option::Option<LightBlock>,
    #[prost(int64, tag = "2")]
    pub common_height: i64,
    #[prost(message, repeated, tag = "3")]
    pub byzantine_validators: ::prost::alloc::vec::Vec<Validator>,
    #[prost(int64, tag = "4")]
    pub total_voting_power: i64,
    #[prost(message, optional, tag = "5")]
    pub timestamp: ::core::option::Option<Timestamp>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LightBlock {
    /// cannot be nil
    #[prost(message, optional, tag = "1")]
    pub signed_header: ::core::option::Option<SignedHeader>,
    /// cannot be nil
    #[prost(message, optional, tag = "2")]
    pub validator_set: ::core::option::Option<ValidatorSet>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValidatorSet {
    /// cannot be empty of nil
    #[prost(message, repeated, tag = "1")]
    pub validators: ::prost::alloc::vec::Vec<Validator>,
    /// cannot be nil
    #[prost(message, optional, tag = "2")]
    pub proposer: ::core::option::Option<Validator>,
    #[prost(int64, tag = "3")]
    pub total_voting_power: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SignedHeader {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<Header>,
    #[prost(message, optional, tag = "2")]
    pub commit: ::core::option::Option<Commit>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EvidenceList {
    #[prost(message, repeated, tag = "1")]
    pub evidence: ::prost::alloc::vec::Vec<Evidence>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Validator {
    #[prost(bytes = "vec", tag = "1")]
    pub address: ::prost::alloc::vec::Vec<u8>,
    /// must be >0
    #[prost(message, optional, tag = "2")]
    pub pub_key: ::core::option::Option<PublicKey>,
    #[prost(int64, tag = "3")]
    pub voting_power: i64,
    #[prost(int64, tag = "4")]
    pub proposer_priority: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PublicKey {
    #[prost(oneof = "public_key::Sum", tags = "1, 2, 3")]
    pub sum: ::core::option::Option<public_key::Sum>,
}
/// Nested message and enum types in `PublicKey`.
pub mod public_key {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Sum {
        #[prost(bytes, tag = "1")]
        Ed25519(::prost::alloc::vec::Vec<u8>),
        #[prost(bytes, tag = "2")]
        Secp256k1(::prost::alloc::vec::Vec<u8>),
        #[prost(bytes, tag = "3")]
        Sr25519(::prost::alloc::vec::Vec<u8>),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TxResult {
    #[prost(uint64, tag = "1")]
    pub height: u64,
    #[prost(uint32, tag = "2")]
    pub index: u32,
    #[prost(bytes = "vec", tag = "3")]
    pub tx: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, optional, tag = "4")]
    pub result: ::core::option::Option<ResponseDeliverTx>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResponseDeliverTx {
    #[prost(uint32, tag = "1")]
    pub code: u32,
    #[prost(bytes = "vec", tag = "2")]
    pub data: ::prost::alloc::vec::Vec<u8>,
    #[prost(string, tag = "3")]
    pub log: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub info: ::prost::alloc::string::String,
    #[prost(int64, tag = "5")]
    pub gas_wanted: i64,
    #[prost(int64, tag = "6")]
    pub gas_used: i64,
    #[prost(message, repeated, tag = "7")]
    pub events: ::prost::alloc::vec::Vec<Event>,
    #[prost(string, tag = "8")]
    pub codespace: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Event {
    #[prost(string, tag = "1")]
    pub eventtype: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub attributes: ::prost::alloc::vec::Vec<EventAttribute>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventAttribute {
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub value: ::prost::alloc::string::String,
    #[prost(bool, tag = "3")]
    pub index: bool,
}
/// EventDataRoundState
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventDataRoundState {
    #[prost(uint64, tag = "1")]
    pub height: u64,
    #[prost(int32, tag = "2")]
    pub round: i32,
    #[prost(string, tag = "3")]
    pub step: ::prost::alloc::string::String,
}
/// EventDataNewRound
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventDataNewRound {
    #[prost(uint64, tag = "1")]
    pub height: u64,
    #[prost(int32, tag = "2")]
    pub round: i32,
    #[prost(string, tag = "3")]
    pub step: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "4")]
    pub proposer: ::core::option::Option<ValidatorInfo>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValidatorInfo {
    #[prost(message, optional, tag = "1")]
    pub address: ::core::option::Option<Address>,
    #[prost(int32, tag = "2")]
    pub index: i32,
}
/// Unsure about this piece, needs confirming
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Address {
    #[prost(bytes = "vec", tag = "1")]
    pub address: ::prost::alloc::vec::Vec<u8>,
}
/// EventDataCompleteProposal
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventDataCompleteProposal {
    #[prost(uint64, tag = "1")]
    pub height: u64,
    #[prost(int32, tag = "2")]
    pub round: i32,
    #[prost(string, tag = "3")]
    pub step: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "4")]
    pub block_id: ::core::option::Option<BlockId>,
}
/// EventDataValidatorSetUpdates
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventDataValidatorSetUpdates {
    #[prost(message, repeated, tag = "1")]
    pub validator_updates: ::prost::alloc::vec::Vec<Validator>,
}
/// EventDataString is just a string and not a struct, unsure how we make a
/// message for this one
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventDataString {
    #[prost(string, tag = "1")]
    pub eventdatastring: ::prost::alloc::string::String,
}
/// EventDataBlockSyncStatus
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventDataBlockSyncStatus {
    #[prost(bool, tag = "1")]
    pub complete: bool,
    #[prost(uint64, tag = "2")]
    pub height: u64,
}
/// EventDataStateSyncStatus
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventDataStateSyncStatus {
    #[prost(bool, tag = "1")]
    pub complete: bool,
    #[prost(uint64, tag = "2")]
    pub height: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Timestamp {
    #[prost(int64, tag = "1")]
    pub seconds: i64,
    #[prost(int32, tag = "2")]
    pub nanos: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Fig {}
/// used in Vote
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SignedMsgType {
    Unknown = 0,
    /// Votes
    Prevote = 1,
    Precommit = 2,
    /// Proposals
    Proposal = 3,
}
/// BlockIdFlag indicates which BlockID the signature is for
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum BlockIdFlag {
    Unknown = 0,
    Absent = 1,
    Commit = 2,
    Nil = 3,
}
