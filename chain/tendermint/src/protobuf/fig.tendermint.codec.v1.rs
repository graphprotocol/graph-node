//------------------------------------------------------------------------
// imported from cosmos/crypto/multisig/v1beta1/multisig.proto
//

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MultiSignature {
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub signatures: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CompactBitArray {
    #[prost(uint32, tag = "1")]
    pub extra_bits_stored: u32,
    #[prost(bytes = "vec", tag = "2")]
    pub elems: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SignatureDescriptors {
    /// signatures are the signature descriptors
    #[prost(message, repeated, tag = "1")]
    pub signatures: ::prost::alloc::vec::Vec<SignatureDescriptor>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Single {
    /// mode is the signing mode of the single signer
    #[prost(enumeration = "SignMode", tag = "1")]
    pub mode: i32,
    /// signature is the raw signature bytes
    #[prost(bytes = "vec", tag = "2")]
    pub signature: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Multi {
    /// bitarray specifies which keys within the multisig are signing
    #[prost(message, optional, tag = "1")]
    pub bitarray: ::core::option::Option<CompactBitArray>,
    /// signatures is the signatures of the multi-signature
    #[prost(message, repeated, tag = "2")]
    pub signatures: ::prost::alloc::vec::Vec<Data>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Data {
    /// sum is the oneof that specifies whether this represents single or multi-signature data
    #[prost(oneof = "data::Sum", tags = "1, 2")]
    pub sum: ::core::option::Option<data::Sum>,
}
/// Nested message and enum types in `Data`.
pub mod data {
    /// sum is the oneof that specifies whether this represents single or multi-signature data
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Sum {
        /// single represents a single signer
        #[prost(message, tag = "1")]
        Single(super::Single),
        /// multi represents a multisig signer
        #[prost(message, tag = "2")]
        Multi(super::Multi),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SignatureDescriptor {
    /// public_key is the public key of the signer
    #[prost(message, optional, tag = "1")]
    pub public_key: ::core::option::Option<::prost_types::Any>,
    #[prost(message, optional, tag = "2")]
    pub data: ::core::option::Option<Data>,
    /// sequence is the sequence of the account, which describes the
    /// number of committed transactions signed by a given address. It is used to prevent
    /// replay attacks.
    #[prost(uint64, tag = "3")]
    pub sequence: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InterfaceDescriptor {
    /// name is the name of the interface. It should be a short-name (without
    /// a period) such that the fully qualified name of the interface will be
    /// package.name, ex. for the package a.b and interface named C, the
    /// fully-qualified name will be a.b.C.
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    /// description is a human-readable description of the interface and its
    /// purpose.
    #[prost(string, tag = "2")]
    pub description: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ScalarDescriptor {
    /// name is the name of the scalar. It should be a short-name (without
    /// a period) such that the fully qualified name of the scalar will be
    /// package.name, ex. for the package a.b and scalar named C, the
    /// fully-qualified name will be a.b.C.
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    /// description is a human-readable description of the scalar and its
    /// encoding format. For instance a big integer or decimal scalar should
    /// specify precisely the expected encoding format.
    #[prost(string, tag = "2")]
    pub description: ::prost::alloc::string::String,
    /// field_type is the type of field with which this scalar can be used.
    /// Scalars can be used with one and only one type of field so that
    /// encoding standards and simple and clear. Currently only string and
    /// bytes fields are supported for scalars.
    #[prost(enumeration = "ScalarType", repeated, tag = "3")]
    pub field_type: ::prost::alloc::vec::Vec<i32>,
}
//------------------------------------------------------------------------
// imported from cosmos/base/v1beta1/coin.proto
//

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Coin {
    #[prost(string, tag = "1")]
    pub denom: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub amount: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DecCoin {
    #[prost(string, tag = "1")]
    pub denom: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub amount: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IntProto {
    #[prost(string, tag = "1")]
    pub int: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DecProto {
    #[prost(string, tag = "1")]
    pub dec: ::prost::alloc::string::String,
}
//------------------------------------------------------------------------
// imported from cosmos/tx/v1beta1/tx.proto
//

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Tx {
    /// body is the processable content of the transaction
    #[prost(message, optional, tag = "1")]
    pub body: ::core::option::Option<TxBody>,
    /// auth_info is the authorization related content of the transaction,
    /// specifically signers, signer modes and fee
    #[prost(message, optional, tag = "2")]
    pub auth_info: ::core::option::Option<AuthInfo>,
    /// signatures is a list of signatures that matches the length and order of
    /// AuthInfo's signer_infos to allow connecting signature meta information like
    /// public key and signing mode by position.
    #[prost(bytes = "vec", repeated, tag = "3")]
    pub signatures: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TxRaw {
    /// body_bytes is a protobuf serialization of a TxBody that matches the
    /// representation in SignDoc.
    #[prost(bytes = "vec", tag = "1")]
    pub body_bytes: ::prost::alloc::vec::Vec<u8>,
    /// auth_info_bytes is a protobuf serialization of an AuthInfo that matches the
    /// representation in SignDoc.
    #[prost(bytes = "vec", tag = "2")]
    pub auth_info_bytes: ::prost::alloc::vec::Vec<u8>,
    /// signatures is a list of signatures that matches the length and order of
    /// AuthInfo's signer_infos to allow connecting signature meta information like
    /// public key and signing mode by position.
    #[prost(bytes = "vec", repeated, tag = "3")]
    pub signatures: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SignDoc {
    /// body_bytes is protobuf serialization of a TxBody that matches the
    /// representation in TxRaw.
    #[prost(bytes = "vec", tag = "1")]
    pub body_bytes: ::prost::alloc::vec::Vec<u8>,
    /// auth_info_bytes is a protobuf serialization of an AuthInfo that matches the
    /// representation in TxRaw.
    #[prost(bytes = "vec", tag = "2")]
    pub auth_info_bytes: ::prost::alloc::vec::Vec<u8>,
    /// chain_id is the unique identifier of the chain this transaction targets.
    /// It prevents signed transactions from being used on another chain by an
    /// attacker
    #[prost(string, tag = "3")]
    pub chain_id: ::prost::alloc::string::String,
    /// account_number is the account number of the account in state
    #[prost(uint64, tag = "4")]
    pub account_number: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SignDocDirectAux {
    /// body_bytes is protobuf serialization of a TxBody that matches the
    /// representation in TxRaw.
    #[prost(bytes = "vec", tag = "1")]
    pub body_bytes: ::prost::alloc::vec::Vec<u8>,
    /// public_key is the public key of the signing account.
    #[prost(message, optional, tag = "2")]
    pub public_key: ::core::option::Option<::prost_types::Any>,
    /// chain_id is the identifier of the chain this transaction targets.
    /// It prevents signed transactions from being used on another chain by an
    /// attacker.
    #[prost(string, tag = "3")]
    pub chain_id: ::prost::alloc::string::String,
    /// account_number is the account number of the account in state.
    #[prost(uint64, tag = "4")]
    pub account_number: u64,
    /// sequence is the sequence number of the signing account.
    #[prost(uint64, tag = "5")]
    pub sequence: u64,
    /// Tip is the optional tip used for meta-transactions. It should be left
    /// empty if the signer is not the tipper for this transaction.
    #[prost(message, optional, tag = "6")]
    pub tip: ::core::option::Option<Tip>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TxBody {
    /// messages is a list of messages to be executed. The required signers of
    /// those messages define the number and order of elements in AuthInfo's
    /// signer_infos and Tx's signatures. Each required signer address is added to
    /// the list only the first time it occurs.
    /// By convention, the first required signer (usually from the first message)
    /// is referred to as the primary signer and pays the fee for the whole
    /// transaction.
    #[prost(message, repeated, tag = "1")]
    pub messages: ::prost::alloc::vec::Vec<::prost_types::Any>,
    /// memo is any arbitrary note/comment to be added to the transaction.
    /// WARNING: in clients, any publicly exposed text should not be called memo,
    /// but should be called `note` instead (see https://github.com/cosmos/cosmos-sdk/issues/9122).
    #[prost(string, tag = "2")]
    pub memo: ::prost::alloc::string::String,
    /// timeout is the block height after which this transaction will not
    /// be processed by the chain
    #[prost(uint64, tag = "3")]
    pub timeout_height: u64,
    /// extension_options are arbitrary options that can be added by chains
    /// when the default options are not sufficient. If any of these are present
    /// and can't be handled, the transaction will be rejected
    #[prost(message, repeated, tag = "1023")]
    pub extension_options: ::prost::alloc::vec::Vec<::prost_types::Any>,
    /// extension_options are arbitrary options that can be added by chains
    /// when the default options are not sufficient. If any of these are present
    /// and can't be handled, they will be ignored
    #[prost(message, repeated, tag = "2047")]
    pub non_critical_extension_options: ::prost::alloc::vec::Vec<::prost_types::Any>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthInfo {
    /// signer_infos defines the signing modes for the required signers. The number
    /// and order of elements must match the required signers from TxBody's
    /// messages. The first element is the primary signer and the one which pays
    /// the fee.
    #[prost(message, repeated, tag = "1")]
    pub signer_infos: ::prost::alloc::vec::Vec<SignerInfo>,
    /// Fee is the fee and gas limit for the transaction. The first signer is the
    /// primary signer and the one which pays the fee. The fee can be calculated
    /// based on the cost of evaluating the body and doing signature verification
    /// of the signers. This can be estimated via simulation.
    #[prost(message, optional, tag = "2")]
    pub fee: ::core::option::Option<Fee>,
    /// Tip is the optional tip used for meta-transactions.
    ///
    /// Since: cosmos-sdk 0.46
    #[prost(message, optional, tag = "3")]
    pub tip: ::core::option::Option<Tip>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SignerInfo {
    /// public_key is the public key of the signer. It is optional for accounts
    /// that already exist in state. If unset, the verifier can use the required \
    /// signer address for this position and lookup the public key.
    #[prost(message, optional, tag = "1")]
    pub public_key: ::core::option::Option<::prost_types::Any>,
    /// mode_info describes the signing mode of the signer and is a nested
    /// structure to support nested multisig pubkey's
    #[prost(message, optional, tag = "2")]
    pub mode_info: ::core::option::Option<ModeInfo>,
    /// sequence is the sequence of the account, which describes the
    /// number of committed transactions signed by a given address. It is used to
    /// prevent replay attacks.
    #[prost(uint64, tag = "3")]
    pub sequence: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ModeInfo {
    /// sum is the oneof that specifies whether this represents a single or nested
    /// multisig signer
    #[prost(oneof = "mode_info::Sum", tags = "1, 2")]
    pub sum: ::core::option::Option<mode_info::Sum>,
}
/// Nested message and enum types in `ModeInfo`.
pub mod mode_info {
    /// Single is the mode info for a single signer. It is structured as a message
    /// to allow for additional fields such as locale for SIGN_MODE_TEXTUAL in the
    /// future
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Single {
        /// mode is the signing mode of the single signer
        #[prost(enumeration = "super::SignMode", tag = "1")]
        pub mode: i32,
    }
    /// Multi is the mode info for a multisig public key
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Multi {
        /// bitarray specifies which keys within the multisig are signing
        #[prost(message, optional, tag = "1")]
        pub bitarray: ::core::option::Option<super::CompactBitArray>,
        /// mode_infos is the corresponding modes of the signers of the multisig
        /// which could include nested multisig public keys
        #[prost(message, repeated, tag = "2")]
        pub mode_infos: ::prost::alloc::vec::Vec<super::ModeInfo>,
    }
    /// sum is the oneof that specifies whether this represents a single or nested
    /// multisig signer
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Sum {
        /// single represents a single signer
        #[prost(message, tag = "1")]
        Single(Single),
        /// multi represents a nested multisig signer
        #[prost(message, tag = "2")]
        Multi(Multi),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Fee {
    /// amount is the amount of coins to be paid as a fee
    #[prost(message, repeated, tag = "1")]
    pub amount: ::prost::alloc::vec::Vec<Coin>,
    /// gas_limit is the maximum gas that can be used in transaction processing
    /// before an out of gas error occurs
    #[prost(uint64, tag = "2")]
    pub gas_limit: u64,
    /// if unset, the first signer is responsible for paying the fees. If set, the specified account must pay the fees.
    /// the payer must be a tx signer (and thus have signed this field in AuthInfo).
    /// setting this field does *not* change the ordering of required signers for the transaction.
    #[prost(string, tag = "3")]
    pub payer: ::prost::alloc::string::String,
    /// if set, the fee payer (either the first signer or the value of the payer field) requests that a fee grant be used
    /// to pay fees instead of the fee payer's own balance. If an appropriate fee grant does not exist or the chain does
    /// not support fee grants, this will fail
    #[prost(string, tag = "4")]
    pub granter: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Tip {
    /// amount is the amount of the tip
    #[prost(message, repeated, tag = "1")]
    pub amount: ::prost::alloc::vec::Vec<Coin>,
    /// tipper is the address of the account paying for the tip
    #[prost(string, tag = "2")]
    pub tipper: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuxSignerData {
    /// address is the bech32-encoded address of the auxiliary signer. If using
    /// AuxSignerData across different chains, the bech32 prefix of the target
    /// chain (where the final transaction is broadcasted) should be used.
    #[prost(string, tag = "1")]
    pub address: ::prost::alloc::string::String,
    /// sign_doc is the SIGN_MOD_DIRECT_AUX sign doc that the auxiliary signer
    /// signs. Note: we use the same sign doc even if we're signing with
    /// LEGACY_AMINO_JSON.
    #[prost(message, optional, tag = "2")]
    pub sign_doc: ::core::option::Option<SignDocDirectAux>,
    /// mode is the signing mode of the single signer
    #[prost(enumeration = "SignMode", tag = "3")]
    pub mode: i32,
    /// sig is the signature of the sign doc.
    #[prost(bytes = "vec", tag = "4")]
    pub sig: ::prost::alloc::vec::Vec<u8>,
}
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
pub struct TransactionData {
    #[prost(message, optional, tag = "1")]
    pub tx: ::core::option::Option<TxResult>,
    #[prost(message, optional, tag = "2")]
    pub block: ::core::option::Option<EventBlock>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Block {
    #[prost(message, optional, tag = "1")]
    pub header: ::core::option::Option<Header>,
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
    /// hash 256 20 bytes - this is the first 20 characters of a 32-byte key -
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
    #[prost(message, optional, tag = "3")]
    pub tx: ::core::option::Option<Tx>,
    #[prost(message, optional, tag = "4")]
    pub result: ::core::option::Option<ResponseDeliverTx>,
    #[prost(bytes = "vec", tag = "5")]
    pub hash: ::prost::alloc::vec::Vec<u8>,
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
//------------------------------------------------------------------------
// imported from cosmos/tx/signing/v1beta1/signing.proto
//

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SignMode {
    /// SIGN_MODE_UNSPECIFIED specifies an unknown signing mode and will be
    /// rejected
    Unspecified = 0,
    /// SIGN_MODE_DIRECT specifies a signing mode which uses SignDoc and is
    /// verified with raw bytes from Tx
    Direct = 1,
    /// SIGN_MODE_TEXTUAL is a future signing mode that will verify some
    /// human-readable textual representation on top of the binary representation
    /// from SIGN_MODE_DIRECT
    Textual = 2,
    /// SIGN_MODE_LEGACY_AMINO_JSON is a backwards compatibility mode which uses
    /// Amino JSON and will be removed in the future
    LegacyAminoJson = 127,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ScalarType {
    Unspecified = 0,
    String = 1,
    Bytes = 2,
}
//------------------------------------------------------------------------
// imported from fig/tendermint/codec/v1/codec.proto
//

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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum BlockIdFlag {
    Unknown = 0,
    Absent = 1,
    Commit = 2,
    Nil = 3,
}
