#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockWrapper {
    /// repeated IndexerShard shards = 2;
    /// repeated StateChangeWithCause state_changes = 3;
    #[prost(message, optional, tag = "1")]
    pub block: ::core::option::Option<Block>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StateChangeWithCause {
    #[prost(message, optional, tag = "1")]
    pub cause: ::core::option::Option<StateChangeCause>,
    #[prost(message, optional, tag = "2")]
    pub value: ::core::option::Option<StateChangeValue>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StateChangeCause {
    #[prost(oneof = "state_change_cause::Cause", tags = "1")]
    pub cause: ::core::option::Option<state_change_cause::Cause>,
}
/// Nested message and enum types in `StateChangeCause`.
pub mod state_change_cause {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Cause {
        ///    NotWritableToDisk not_writable_to_disk = 1;
        ///InitialState initial_state = 2;
        ///TransactionProcessing  transaction_processing = 3;
        ///ActionReceiptProcessingStarted action_receipt_processing_started = 4;
        ///ActionReceiptGasReward action_receipt_gas_reward = 5;
        ///ReceiptProcessing receipt_processing = 6;
        ///PostponedReceipt postponed_receipt = 7;
        ///UpdatedDelayedReceipts updated_delayed_receipts = 8;
        ///ValidatorAccountsUpdate validator_accounts_update = 9;
        ///Migration migration = 10;
        #[prost(message, tag = "1")]
        TransactionProcessing(super::TransactionProcessing),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NotWritableToDisk {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InitialState {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransactionProcessing {
    #[prost(message, optional, tag = "1")]
    pub tx_hash: ::core::option::Option<CryptoHash>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActionReceiptProcessingStarted {
    #[prost(message, optional, tag = "1")]
    pub receipt_hash: ::core::option::Option<CryptoHash>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActionReceiptGasReward {
    #[prost(message, optional, tag = "1")]
    pub tx_hash: ::core::option::Option<CryptoHash>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReceiptProcessing {
    #[prost(message, optional, tag = "1")]
    pub tx_hash: ::core::option::Option<CryptoHash>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PostponedReceipt {
    #[prost(message, optional, tag = "1")]
    pub tx_hash: ::core::option::Option<CryptoHash>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UpdatedDelayedReceipts {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValidatorAccountsUpdate {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Migration {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StateChangeValue {
    #[prost(oneof = "state_change_value::Value", tags = "1")]
    pub value: ::core::option::Option<state_change_value::Value>,
}
/// Nested message and enum types in `StateChangeValue`.
pub mod state_change_value {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(message, tag = "1")]
        AccountUpdate(super::DataUpdate),
    }
}
///
///message AccountUpdate {string account_id = 1; Account account = 2;}
///message AccountDeletion {string account_id = 1;}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AccessKeyUpdate {
    #[prost(string, tag = "1")]
    pub account_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub public_key: ::core::option::Option<PublicKey>,
    #[prost(message, optional, tag = "3")]
    pub access_key: ::core::option::Option<AccessKey>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AccessKeyDeletion {
    #[prost(string, tag = "1")]
    pub account_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub public_key: ::core::option::Option<PublicKey>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DataUpdate {
    #[prost(string, tag = "1")]
    pub account_id: ::prost::alloc::string::String,
    #[prost(bytes = "vec", tag = "2")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub value: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DataDeletion {
    #[prost(string, tag = "1")]
    pub account_id: ::prost::alloc::string::String,
    #[prost(bytes = "vec", tag = "2")]
    pub key: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ContractCodeUpdate {
    #[prost(string, tag = "1")]
    pub account_id: ::prost::alloc::string::String,
    #[prost(bytes = "vec", tag = "2")]
    pub code: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ContractCodeDeletion {
    #[prost(string, tag = "1")]
    pub account_id: ::prost::alloc::string::String,
}
//
//message Account {
//BigInt amount = 1;
//BigInt locked = 2;
//CryptoHash code_hash = 3;
//uint64 storage_usage = 4;
//}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Block {
    #[prost(string, tag = "1")]
    pub author: ::prost::alloc::string::String,
    /// repeated ChunkHeader chunks = 3;
    #[prost(message, optional, tag = "2")]
    pub header: ::core::option::Option<BlockHeader>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockHeader {
    #[prost(uint64, tag = "1")]
    pub height: u64,
    #[prost(uint64, tag = "2")]
    pub prev_height: u64,
    #[prost(message, optional, tag = "3")]
    pub hash: ::core::option::Option<CryptoHash>,
    ///
    ///CryptoHash epoch_id = 3;
    ///CryptoHash next_epoch_id = 4;
    ///CryptoHash hash = 5;
    ///CryptoHash prev_hash = 6;
    ///CryptoHash prev_state_root = 7;
    ///CryptoHash chunk_receipts_root = 8;
    ///CryptoHash chunk_headers_root = 9;
    ///CryptoHash chunk_tx_root = 10;
    ///CryptoHash outcome_root = 11;
    ///uint64 chunks_included = 12;
    ///CryptoHash challenges_root = 13;
    ///uint64 timestamp = 14;
    ///uint64 timestamp_nanosec = 15;
    ///CryptoHash random_value = 16;
    ///repeated ValidatorStake validator_proposals = 17;
    ///repeated bool chunk_mask = 18;
    ///BigInt gas_price = 19;
    ///uint64 block_ordinal = 20;
    ///BigInt validator_reward = 21;
    ///BigInt total_supply = 22;
    ///repeated SlashedValidator challenges_result = 23;
    ///CryptoHash last_final_block = 24;
    ///CryptoHash last_ds_final_block = 25;
    ///CryptoHash next_bp_hash = 26;
    ///CryptoHash block_merkle_root = 27;
    ///bytes epoch_sync_data_hash = 28;
    ///repeated Signature approvals = 29;
    ///Signature signature = 30;
    ///uint32 latest_protocol_version = 31;
    #[prost(message, optional, tag = "4")]
    pub prev_hash: ::core::option::Option<CryptoHash>,
}
//
//message BlockHeader {
//uint64 height = 1;
//uint64 prev_height = 2;
//CryptoHash epoch_id = 3;
//CryptoHash next_epoch_id = 4;
//CryptoHash hash = 5;
//CryptoHash prev_hash = 6;
//CryptoHash prev_state_root = 7;
//CryptoHash chunk_receipts_root = 8;
//CryptoHash chunk_headers_root = 9;
//CryptoHash chunk_tx_root = 10;
//CryptoHash outcome_root = 11;
//uint64 chunks_included = 12;
//CryptoHash challenges_root = 13;
//uint64 timestamp = 14;
//uint64 timestamp_nanosec = 15;
//CryptoHash random_value = 16;
//repeated ValidatorStake validator_proposals = 17;
//repeated bool chunk_mask = 18;
//BigInt gas_price = 19;
//uint64 block_ordinal = 20;
//BigInt validator_reward = 21;
//BigInt total_supply = 22;
//repeated SlashedValidator challenges_result = 23;
//CryptoHash last_final_block = 24;
//CryptoHash last_ds_final_block = 25;
//CryptoHash next_bp_hash = 26;
//CryptoHash block_merkle_root = 27;
//bytes epoch_sync_data_hash = 28;
//repeated Signature approvals = 29;
//Signature signature = 30;
//uint32 latest_protocol_version = 31;
//}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BigInt {
    #[prost(bytes = "vec", tag = "1")]
    pub bytes: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CryptoHash {
    #[prost(bytes = "vec", tag = "1")]
    pub bytes: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Signature {
    #[prost(enumeration = "SignatureType", tag = "1")]
    pub r#type: i32,
    #[prost(bytes = "vec", tag = "2")]
    pub bytes: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PublicKey {
    #[prost(bytes = "vec", tag = "1")]
    pub bytes: ::prost::alloc::vec::Vec<u8>,
}
//
//message ChunkHeader {
//bytes chunk_hash = 1;
//bytes prev_block_hash = 2;
//bytes outcome_root = 3;
//bytes prev_state_root = 4;
//bytes encoded_merkle_root = 5;
//uint64 encoded_length = 6;
//uint64 height_created = 7;
//uint64 height_included = 8;
//uint64 shard_id = 9;
//uint64 gas_used = 10;
//uint64 gas_limit = 11;
//BigInt validator_reward = 12;
//BigInt balance_burnt = 13;
//bytes outgoing_receipts_root = 14;
//bytes tx_root = 15;
//repeated ValidatorStake validator_proposals = 16;
//Signature signature = 17;
//}

//
//message IndexerShard {
//uint64 shard_id = 1;
//IndexerChunk chunk = 2;
//repeated IndexerExecutionOutcomeWithReceipt receipt_execution_outcomes = 3;
//}
//
//message IndexerExecutionOutcomeWithReceipt {
//ExecutionOutcomeWithIdView execution_outcome = 1;
//Receipt receipt = 2;
//}

//
//message IndexerChunk {
//string author = 1;
//ChunkHeader header = 2;
//repeated IndexerTransactionWithOutcome transactions = 3;
//repeated Receipt receipts = 4;
//}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexerTransactionWithOutcome {
    #[prost(message, optional, tag = "1")]
    pub transaction: ::core::option::Option<SignedTransaction>,
    #[prost(message, optional, tag = "2")]
    pub outcome: ::core::option::Option<IndexerExecutionOutcomeWithOptionalReceipt>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SignedTransaction {
    #[prost(string, tag = "1")]
    pub signer_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub public_key: ::core::option::Option<PublicKey>,
    #[prost(uint64, tag = "3")]
    pub nonce: u64,
    #[prost(string, tag = "4")]
    pub receiver_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "5")]
    pub actions: ::prost::alloc::vec::Vec<Action>,
    #[prost(message, optional, tag = "6")]
    pub signature: ::core::option::Option<Signature>,
    #[prost(message, optional, tag = "7")]
    pub hash: ::core::option::Option<CryptoHash>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexerExecutionOutcomeWithOptionalReceipt {
    #[prost(message, optional, tag = "1")]
    pub execution_outcome: ::core::option::Option<ExecutionOutcomeWithIdView>,
    #[prost(message, optional, tag = "2")]
    pub receipt: ::core::option::Option<Receipt>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Receipt {
    #[prost(string, tag = "1")]
    pub predecessor_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub receiver_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub receipt_id: ::core::option::Option<CryptoHash>,
    #[prost(oneof = "receipt::Receipt", tags = "10, 11")]
    pub receipt: ::core::option::Option<receipt::Receipt>,
}
/// Nested message and enum types in `Receipt`.
pub mod receipt {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Receipt {
        #[prost(message, tag = "10")]
        Action(super::ReceiptAction),
        #[prost(message, tag = "11")]
        Data(super::ReceiptData),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReceiptData {
    #[prost(message, optional, tag = "1")]
    pub data_id: ::core::option::Option<CryptoHash>,
    #[prost(bytes = "vec", tag = "2")]
    pub data: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReceiptAction {
    #[prost(string, tag = "1")]
    pub signer_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub signer_public_key: ::core::option::Option<PublicKey>,
    #[prost(message, optional, tag = "3")]
    pub gas_price: ::core::option::Option<BigInt>,
    #[prost(message, repeated, tag = "4")]
    pub output_data_receivers: ::prost::alloc::vec::Vec<DataReceiver>,
    #[prost(message, repeated, tag = "5")]
    pub input_data_ids: ::prost::alloc::vec::Vec<CryptoHash>,
    #[prost(message, repeated, tag = "6")]
    pub actions: ::prost::alloc::vec::Vec<Action>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DataReceiver {
    #[prost(message, optional, tag = "1")]
    pub data_id: ::core::option::Option<CryptoHash>,
    #[prost(string, tag = "2")]
    pub receiver_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutionOutcomeWithIdView {
    #[prost(message, optional, tag = "1")]
    pub proof: ::core::option::Option<MerklePath>,
    #[prost(message, optional, tag = "2")]
    pub block_hash: ::core::option::Option<CryptoHash>,
    #[prost(message, optional, tag = "3")]
    pub id: ::core::option::Option<CryptoHash>,
    #[prost(message, optional, tag = "4")]
    pub outcome: ::core::option::Option<ExecutionOutcome>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutionOutcome {
    #[prost(string, repeated, tag = "1")]
    pub logs: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(message, repeated, tag = "2")]
    pub receipt_ids: ::prost::alloc::vec::Vec<CryptoHash>,
    #[prost(uint64, tag = "3")]
    pub gas_burnt: u64,
    #[prost(message, optional, tag = "4")]
    pub tokens_burnt: ::core::option::Option<BigInt>,
    #[prost(string, tag = "5")]
    pub executor_id: ::prost::alloc::string::String,
    #[prost(oneof = "execution_outcome::Status", tags = "20, 21, 22, 23")]
    pub status: ::core::option::Option<execution_outcome::Status>,
}
/// Nested message and enum types in `ExecutionOutcome`.
pub mod execution_outcome {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Status {
        #[prost(message, tag = "20")]
        Unknown(super::UnknownExecutionStatus),
        #[prost(message, tag = "21")]
        Failure(super::FailureExecutionStatus),
        #[prost(message, tag = "22")]
        SuccessValue(super::SuccessValueExecutionStatus),
        #[prost(message, tag = "23")]
        SuccessReceiptId(super::SuccessReceiptIdExecutionStatus),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SuccessValueExecutionStatus {
    #[prost(string, tag = "1")]
    pub value: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SuccessReceiptIdExecutionStatus {
    #[prost(message, optional, tag = "1")]
    pub id: ::core::option::Option<CryptoHash>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnknownExecutionStatus {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FailureExecutionStatus {
    #[prost(oneof = "failure_execution_status::Failure", tags = "1, 2")]
    pub failure: ::core::option::Option<failure_execution_status::Failure>,
}
/// Nested message and enum types in `FailureExecutionStatus`.
pub mod failure_execution_status {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Failure {
        #[prost(message, tag = "1")]
        ActionError(super::ActionError),
        #[prost(enumeration = "super::InvalidTxError", tag = "2")]
        InvalidTxError(i32),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActionError {
    #[prost(uint64, tag = "1")]
    pub index: u64,
    #[prost(
        oneof = "action_error::Kind",
        tags = "21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36"
    )]
    pub kind: ::core::option::Option<action_error::Kind>,
}
/// Nested message and enum types in `ActionError`.
pub mod action_error {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Kind {
        #[prost(message, tag = "21")]
        AccountAlreadyExist(super::AccountAlreadyExistsErrorKind),
        #[prost(message, tag = "22")]
        AccountDoesNotExist(super::AccountDoesNotExistErrorKind),
        #[prost(message, tag = "23")]
        CreateAccountOnlyByRegistrar(super::CreateAccountOnlyByRegistrarErrorKind),
        #[prost(message, tag = "24")]
        CreateAccountNotAllowed(super::CreateAccountNotAllowedErrorKind),
        #[prost(message, tag = "25")]
        ActorNoPermission(super::ActorNoPermissionErrorKind),
        #[prost(message, tag = "26")]
        DeleteKeyDoesNotExist(super::DeleteKeyDoesNotExistErrorKind),
        #[prost(message, tag = "27")]
        AddKeyAlreadyExists(super::AddKeyAlreadyExistsErrorKind),
        #[prost(message, tag = "28")]
        DeleteAccountStaking(super::DeleteAccountStakingErrorKind),
        #[prost(message, tag = "29")]
        LackBalanceForState(super::LackBalanceForStateErrorKind),
        #[prost(message, tag = "30")]
        TriesToUnstake(super::TriesToUnstakeErrorKind),
        #[prost(message, tag = "31")]
        TriesToStake(super::TriesToStakeErrorKind),
        #[prost(message, tag = "32")]
        InsufficientStake(super::InsufficientStakeErrorKind),
        ///todo: uncompleted
        #[prost(message, tag = "33")]
        FunctionCall(super::FunctionCallErrorKind),
        ///todo: uncompleted
        #[prost(message, tag = "34")]
        NewReceiptValidation(super::NewReceiptValidationErrorKind),
        #[prost(message, tag = "35")]
        OnlyImplicitAccountCreationAllowed(super::OnlyImplicitAccountCreationAllowedErrorKind),
        #[prost(message, tag = "36")]
        DeleteAccountWithLargeState(super::DeleteAccountWithLargeStateErrorKind),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AccountAlreadyExistsErrorKind {
    #[prost(string, tag = "1")]
    pub account_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AccountDoesNotExistErrorKind {
    #[prost(string, tag = "1")]
    pub account_id: ::prost::alloc::string::String,
}
//// A top-level account ID can only be created by registrar.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateAccountOnlyByRegistrarErrorKind {
    #[prost(string, tag = "1")]
    pub account_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub registrar_account_id: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub predecessor_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateAccountNotAllowedErrorKind {
    #[prost(string, tag = "1")]
    pub account_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub predecessor_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActorNoPermissionErrorKind {
    #[prost(string, tag = "1")]
    pub account_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub actor_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteKeyDoesNotExistErrorKind {
    #[prost(string, tag = "1")]
    pub account_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub public_key: ::core::option::Option<PublicKey>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddKeyAlreadyExistsErrorKind {
    #[prost(string, tag = "1")]
    pub account_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub public_key: ::core::option::Option<PublicKey>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteAccountStakingErrorKind {
    #[prost(string, tag = "1")]
    pub account_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LackBalanceForStateErrorKind {
    #[prost(string, tag = "1")]
    pub account_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub balance: ::core::option::Option<BigInt>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TriesToUnstakeErrorKind {
    #[prost(string, tag = "1")]
    pub account_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TriesToStakeErrorKind {
    #[prost(string, tag = "1")]
    pub account_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub stake: ::core::option::Option<BigInt>,
    #[prost(message, optional, tag = "3")]
    pub locked: ::core::option::Option<BigInt>,
    #[prost(message, optional, tag = "4")]
    pub balance: ::core::option::Option<BigInt>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InsufficientStakeErrorKind {
    #[prost(string, tag = "1")]
    pub account_id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub stake: ::core::option::Option<BigInt>,
    #[prost(message, optional, tag = "3")]
    pub minimum_stake: ::core::option::Option<BigInt>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FunctionCallErrorKind {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NewReceiptValidationErrorKind {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OnlyImplicitAccountCreationAllowedErrorKind {
    #[prost(string, tag = "1")]
    pub account_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteAccountWithLargeStateErrorKind {
    #[prost(string, tag = "1")]
    pub account_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MerklePath {
    #[prost(message, repeated, tag = "1")]
    pub path: ::prost::alloc::vec::Vec<MerklePathItem>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MerklePathItem {
    #[prost(message, optional, tag = "1")]
    pub hash: ::core::option::Option<CryptoHash>,
    #[prost(enumeration = "Direction", tag = "2")]
    pub direction: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Action {
    #[prost(oneof = "action::Action", tags = "1, 2, 3, 4, 5, 6, 7, 8")]
    pub action: ::core::option::Option<action::Action>,
}
/// Nested message and enum types in `Action`.
pub mod action {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Action {
        #[prost(message, tag = "1")]
        CreateAccount(super::CreateAccountAction),
        #[prost(message, tag = "2")]
        DeployContract(super::DeployContractAction),
        #[prost(message, tag = "3")]
        FunctionCall(super::FunctionCallAction),
        #[prost(message, tag = "4")]
        Transfer(super::TransferAction),
        #[prost(message, tag = "5")]
        Stake(super::StakeAction),
        #[prost(message, tag = "6")]
        AddKey(super::AddKeyAction),
        #[prost(message, tag = "7")]
        DeleteKey(super::DeleteKeyAction),
        #[prost(message, tag = "8")]
        DeleteAccount(super::DeleteAccountAction),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateAccountAction {}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeployContractAction {
    #[prost(string, tag = "1")]
    pub code: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FunctionCallAction {
    #[prost(string, tag = "1")]
    pub method_name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub args: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub gas: u64,
    #[prost(message, optional, tag = "4")]
    pub deposit: ::core::option::Option<BigInt>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransferAction {
    #[prost(message, optional, tag = "1")]
    pub deposit: ::core::option::Option<BigInt>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StakeAction {
    #[prost(message, optional, tag = "1")]
    pub stake: ::core::option::Option<BigInt>,
    #[prost(message, optional, tag = "2")]
    pub public_key: ::core::option::Option<PublicKey>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddKeyAction {
    #[prost(message, optional, tag = "1")]
    pub public_key: ::core::option::Option<PublicKey>,
    #[prost(message, optional, tag = "2")]
    pub access_key: ::core::option::Option<AccessKey>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteKeyAction {
    #[prost(message, optional, tag = "1")]
    pub public_key: ::core::option::Option<PublicKey>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DeleteAccountAction {
    #[prost(string, tag = "1")]
    pub beneficiary_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AccessKey {
    #[prost(uint64, tag = "1")]
    pub nonce: u64,
    #[prost(message, optional, tag = "2")]
    pub permission: ::core::option::Option<AccessKeyPermission>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AccessKeyPermission {
    #[prost(oneof = "access_key_permission::Permission", tags = "1, 2")]
    pub permission: ::core::option::Option<access_key_permission::Permission>,
}
/// Nested message and enum types in `AccessKeyPermission`.
pub mod access_key_permission {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Permission {
        #[prost(message, tag = "1")]
        FunctionCall(super::FunctionCallPermission),
        #[prost(message, tag = "2")]
        FullAccess(super::FullAccessPermission),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FunctionCallPermission {
    #[prost(message, optional, tag = "1")]
    pub allowance: ::core::option::Option<BigInt>,
    #[prost(string, tag = "2")]
    pub receiver_id: ::prost::alloc::string::String,
    #[prost(string, repeated, tag = "3")]
    pub method_names: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FullAccessPermission {}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SignatureType {
    Ed25519 = 0,
    Secp256k1 = 1,
}
///todo: half baked
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum InvalidTxError {
    InvalidAccessKeyError = 0,
    InvalidSignerId = 1,
    SignerDoesNotExist = 2,
    InvalidNonce = 3,
    NonceTooLarge = 4,
    InvalidReceiverId = 5,
    InvalidSignature = 6,
    NotEnoughBalance = 7,
    LackBalanceForState = 8,
    CostOverflow = 9,
    InvalidChain = 10,
    Expired = 11,
    ActionsValidation = 12,
    TransactionSizeExceeded = 13,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Direction {
    Left = 0,
    Right = 1,
}
