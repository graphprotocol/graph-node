#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Block {
    /// corresponds to the Slot id (or hash)
    #[prost(bytes = "vec", tag = "1")]
    pub id: ::prost::alloc::vec::Vec<u8>,
    /// corresponds to the Slot number for this block
    #[prost(uint64, tag = "2")]
    pub number: u64,
    #[prost(uint32, tag = "3")]
    pub version: u32,
    /// corresponds to the previous_blockhash, might skip some slots, so beware
    #[prost(bytes = "vec", tag = "4")]
    pub previous_id: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag = "5")]
    pub previous_block: u64,
    #[prost(uint64, tag = "6")]
    pub genesis_unix_timestamp: u64,
    #[prost(uint64, tag = "7")]
    pub clock_unix_timestamp: u64,
    #[prost(uint64, tag = "8")]
    pub root_num: u64,
    #[prost(bytes = "vec", tag = "9")]
    pub last_entry_hash: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, repeated, tag = "10")]
    pub transactions: ::prost::alloc::vec::Vec<Transaction>,
    #[prost(uint32, tag = "11")]
    pub transaction_count: u32,
    #[prost(bool, tag = "12")]
    pub has_split_account_changes: bool,
    #[prost(string, tag = "13")]
    pub account_changes_file_ref: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Batch {
    #[prost(message, repeated, tag = "1")]
    pub transactions: ::prost::alloc::vec::Vec<Transaction>,
}
/// Bundled in separate files, referenced by `account_changes_file_ref`
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AccountChangesBundle {
    /// Maps to the index of the `repeated` field for Block::transactions
    #[prost(message, repeated, tag = "1")]
    pub transactions: ::prost::alloc::vec::Vec<AccountChangesPerTrxIndex>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AccountChangesPerTrxIndex {
    #[prost(bytes = "vec", tag = "1")]
    pub trx_id: ::prost::alloc::vec::Vec<u8>,
    /// Maps to the index within the `repeated` field of the proto for
    /// Transaction::instructions
    #[prost(message, repeated, tag = "2")]
    pub instructions: ::prost::alloc::vec::Vec<AccountChangesPerInstruction>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AccountChangesPerInstruction {
    /// Data to be put in Instruction::account_changes
    #[prost(message, repeated, tag = "1")]
    pub changes: ::prost::alloc::vec::Vec<AccountChange>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Transaction {
    /// The transaction ID corresponds to the _first_
    /// signature. Additional signatures are in `additional_signatures`.
    #[prost(bytes = "vec", tag = "1")]
    pub id: ::prost::alloc::vec::Vec<u8>,
    /// Index from within a single Slot, deterministically ordered to the
    /// best of our ability using the transaction ID as a sort key for
    /// the batch of transactions executed in parallel.
    #[prost(uint64, tag = "2")]
    pub index: u64,
    #[prost(bytes = "vec", repeated, tag = "3")]
    pub additional_signatures: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
    #[prost(message, optional, tag = "4")]
    pub header: ::core::option::Option<MessageHeader>,
    /// From the original Message object
    #[prost(bytes = "vec", repeated, tag = "5")]
    pub account_keys: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
    /// From the original Message object
    #[prost(bytes = "vec", tag = "6")]
    pub recent_blockhash: ::prost::alloc::vec::Vec<u8>,
    /// What follows Once executed these can be set:
    #[prost(string, repeated, tag = "7")]
    pub log_messages: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Instructions, containing both top-level and nested transactions
    #[prost(message, repeated, tag = "8")]
    pub instructions: ::prost::alloc::vec::Vec<Instruction>,
    #[prost(bool, tag = "9")]
    pub failed: bool,
    #[prost(message, optional, tag = "10")]
    pub error: ::core::option::Option<TransactionError>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MessageHeader {
    #[prost(uint32, tag = "1")]
    pub num_required_signatures: u32,
    #[prost(uint32, tag = "2")]
    pub num_readonly_signed_accounts: u32,
    #[prost(uint32, tag = "3")]
    pub num_readonly_unsigned_accounts: u32,
}
//*
//- instr1 (id=1, parent=0)
//- instr2 (id=2, parent=0) (pubkey1 is writable)
//- instr3 (id=3, parent=2) (pubkey1 is writable)
//- instr4 (id=4, parent=3) (pubkey1 is writable)
//- instr5 (id=5, parent=4) (pubkey1 is writable, mutates pubkey1)
//collect delta of pubkey1
//collect delta of pubkey1 ONLY IF CHANGED AGAIN, from last time we took a snapshot of it.
//collect delta of pubkey1
//- instr6 (id=6, parent=0)

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Instruction {
    #[prost(bytes = "vec", tag = "3")]
    pub program_id: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", repeated, tag = "4")]
    pub account_keys: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
    #[prost(bytes = "vec", tag = "5")]
    pub data: ::prost::alloc::vec::Vec<u8>,
    // What follows is execution trace data, could be empty for un-executed transactions.
    #[prost(uint32, tag = "6")]
    pub ordinal: u32,
    #[prost(uint32, tag = "7")]
    pub parent_ordinal: u32,
    #[prost(uint32, tag = "8")]
    pub depth: u32,
    #[prost(message, repeated, tag = "9")]
    pub balance_changes: ::prost::alloc::vec::Vec<BalanceChange>,
    #[prost(message, repeated, tag = "10")]
    pub account_changes: ::prost::alloc::vec::Vec<AccountChange>,
    #[prost(bool, tag = "15")]
    pub failed: bool,
    #[prost(message, optional, tag = "16")]
    pub error: ::core::option::Option<InstructionError>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BalanceChange {
    #[prost(bytes = "vec", tag = "1")]
    pub pubkey: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub prev_lamports: u64,
    #[prost(uint64, tag = "3")]
    pub new_lamports: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AccountChange {
    #[prost(bytes = "vec", tag = "1")]
    pub pubkey: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub prev_data: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub new_data: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag = "4")]
    pub new_data_length: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransactionError {
    #[prost(string, tag = "2")]
    pub error: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransactionInstructionError {
    #[prost(string, tag = "2")]
    pub error: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InstructionError {
    #[prost(string, tag = "2")]
    pub error: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InstructionErrorCustom {
    #[prost(string, tag = "2")]
    pub error: ::prost::alloc::string::String,
}
