#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventList {
    #[prost(message, optional, tag = "1")]
    pub new_block: ::core::option::Option<EventBlock>,
    #[prost(message, repeated, tag = "2")]
    pub transaction: ::prost::alloc::vec::Vec<EventTx>,
    #[prost(message, optional, tag = "3")]
    pub validator_set_updates: ::core::option::Option<EventValidatorSetUpdates>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventData {
    #[prost(message, optional, tag = "1")]
    pub event: ::core::option::Option<Event>,
    #[prost(message, optional, tag = "2")]
    pub block: ::core::option::Option<EventBlock>,
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
pub struct BlockId {
    #[prost(bytes = "vec", tag = "1")]
    pub hash: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, optional, tag = "2")]
    pub part_set_header: ::core::option::Option<PartSetHeader>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockParams {
    /// Note: must be greater than 0
    #[prost(int64, tag = "1")]
    pub max_bytes: i64,
    /// Note: must be greater or equal to -1
    #[prost(int64, tag = "2")]
    pub max_gas: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Commit {
    /// must be > 0
    #[prost(int64, tag = "1")]
    pub height: i64,
    /// must be > 0
    #[prost(int32, tag = "2")]
    pub round: i32,
    #[prost(message, optional, tag = "3")]
    pub block_id: ::core::option::Option<BlockId>,
    #[prost(message, repeated, tag = "4")]
    pub signatures: ::prost::alloc::vec::Vec<CommitSig>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommitSig {
    /// must match those in the enum above
    #[prost(enumeration = "BlockIdFlag", tag = "1")]
    pub block_id_flag: i32,
    /// hash 256 20 bytes - this is the first 20 characters of a 32-byte key - SHA256(pubkey)[:20]
    #[prost(bytes = "vec", tag = "2")]
    pub validator_address: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, optional, tag = "3")]
    pub timestamp: ::core::option::Option<Timestamp>,
    /// length should be > 0 and < 64
    #[prost(bytes = "vec", tag = "4")]
    pub signature: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Consensus {
    #[prost(uint64, tag = "1")]
    pub block: u64,
    #[prost(uint64, tag = "2")]
    pub app: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConsensusParams {
    #[prost(message, optional, tag = "1")]
    pub block: ::core::option::Option<BlockParams>,
    #[prost(message, optional, tag = "2")]
    pub evidence: ::core::option::Option<EvidenceParams>,
    #[prost(message, optional, tag = "3")]
    pub validator: ::core::option::Option<ValidatorParams>,
    #[prost(message, optional, tag = "4")]
    pub version: ::core::option::Option<VersionParams>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Data {
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub txs: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Duration {
    #[prost(int64, tag = "1")]
    pub seconds: i64,
    #[prost(int32, tag = "2")]
    pub nanos: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DuplicateVoteEvidence {
    #[prost(message, optional, tag = "1")]
    pub vote_a: ::core::option::Option<EventVote>,
    #[prost(message, optional, tag = "2")]
    pub vote_b: ::core::option::Option<EventVote>,
    #[prost(int64, tag = "3")]
    pub total_voting_power: i64,
    #[prost(int64, tag = "4")]
    pub validator_power: i64,
    #[prost(message, optional, tag = "5")]
    pub timestamp: ::core::option::Option<Timestamp>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Event {
    #[prost(string, tag = "1")]
    pub event_type: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub attributes: ::prost::alloc::vec::Vec<EventAttribute>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventAttribute {
    #[prost(string, tag = "1")]
    pub key: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub value: ::prost::alloc::string::String,
    /// nondeterministic
    #[prost(bool, tag = "3")]
    pub index: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventBlock {
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
pub struct EventTx {
    #[prost(message, optional, tag = "1")]
    pub tx_result: ::core::option::Option<TxResult>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventValidatorSetUpdates {
    #[prost(message, repeated, tag = "1")]
    pub validator_updates: ::prost::alloc::vec::Vec<Validator>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventVote {
    /// should be present in the enum at the top of this file
    #[prost(enumeration = "SignedMsgType", tag = "1")]
    pub event_vote_type: i32,
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
    #[prost(bytes = "vec", tag = "6")]
    pub validator_address: ::prost::alloc::vec::Vec<u8>,
    #[prost(int32, tag = "7")]
    pub validator_index: i32,
    /// hash length should be >0 and <64
    #[prost(bytes = "vec", tag = "8")]
    pub signature: ::prost::alloc::vec::Vec<u8>,
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
pub struct EvidenceList {
    #[prost(message, repeated, tag = "1")]
    pub evidence: ::prost::alloc::vec::Vec<Evidence>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EvidenceParams {
    #[prost(int64, tag = "1")]
    pub max_age_num_blocks: i64,
    #[prost(message, optional, tag = "2")]
    pub max_age_duration: ::core::option::Option<Duration>,
    /// Default is 1048576 or 1MB
    #[prost(int64, tag = "3")]
    pub max_bytes: i64,
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
    /// hash 256 20 bytes - this is the first 20 characters of a 32-byte key - SHA256(pubkey)[:20]
    #[prost(bytes = "vec", tag = "14")]
    pub proposer_address: ::prost::alloc::vec::Vec<u8>,
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
pub struct PublicKey {
    #[prost(oneof = "public_key::Sum", tags = "1, 2")]
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
    }
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
pub struct ResponseBeginBlock {
    #[prost(message, repeated, tag = "1")]
    pub events: ::prost::alloc::vec::Vec<Event>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResponseEndBlock {
    #[prost(message, repeated, tag = "1")]
    pub validator_updates: ::prost::alloc::vec::Vec<ValidatorUpdate>,
    #[prost(message, optional, tag = "2")]
    pub consensus_param_updates: ::core::option::Option<ConsensusParams>,
    #[prost(message, repeated, tag = "3")]
    pub events: ::prost::alloc::vec::Vec<Event>,
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
pub struct SignedHeader {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<Header>,
    #[prost(message, optional, tag = "2")]
    pub commit: ::core::option::Option<Commit>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Timestamp {
    #[prost(int64, tag = "1")]
    pub seconds: i64,
    #[prost(int32, tag = "2")]
    pub nanos: i32,
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
pub struct Validator {
    #[prost(bytes = "vec", tag = "1")]
    pub address: ::prost::alloc::vec::Vec<u8>,
    /// must be > 0
    #[prost(message, optional, tag = "2")]
    pub pub_key: ::core::option::Option<PublicKey>,
    #[prost(int64, tag = "3")]
    pub voting_power: i64,
    #[prost(int64, tag = "4")]
    pub proposer_priority: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValidatorParams {
    #[prost(string, repeated, tag = "1")]
    pub pub_key_types: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValidatorSet {
    /// cannot be empty or nil
    #[prost(message, repeated, tag = "1")]
    pub validators: ::prost::alloc::vec::Vec<Validator>,
    /// cannot be nil
    #[prost(message, optional, tag = "2")]
    pub proposer: ::core::option::Option<Validator>,
    #[prost(int64, tag = "3")]
    pub total_voting_power: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValidatorUpdate {
    #[prost(bytes = "vec", tag = "1")]
    pub address: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, optional, tag = "2")]
    pub pub_key: ::core::option::Option<PublicKey>,
    #[prost(int64, tag = "3")]
    pub power: i64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct VersionParams {
    #[prost(uint64, tag = "1")]
    pub app_version: u64,
}
/// used in Vote
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SignedMsgType {
    Unknown = 0,
    /// Votes
    Prevote = 1,
    Precommit = 2,
    /// Proposals
    Proposal = 32,
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
