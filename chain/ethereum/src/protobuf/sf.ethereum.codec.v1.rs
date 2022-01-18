#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Block {
    #[prost(int32, tag = "1")]
    pub ver: i32,
    #[prost(bytes = "vec", tag = "2")]
    pub hash: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag = "3")]
    pub number: u64,
    #[prost(uint64, tag = "4")]
    pub size: u64,
    #[prost(message, optional, tag = "5")]
    pub header: ::core::option::Option<BlockHeader>,
    #[prost(message, repeated, tag = "6")]
    pub uncles: ::prost::alloc::vec::Vec<BlockHeader>,
    #[prost(message, repeated, tag = "10")]
    pub transaction_traces: ::prost::alloc::vec::Vec<TransactionTrace>,
    #[prost(message, repeated, tag = "11")]
    pub balance_changes: ::prost::alloc::vec::Vec<BalanceChange>,
    #[prost(message, repeated, tag = "20")]
    pub code_changes: ::prost::alloc::vec::Vec<CodeChange>,
    /// Wheter or not a filtering process was run on this block. The filtering process populated the
    /// `transaction_traces` according to the `filtering_include_filter_expr` and
    /// `filtering_exclude_filter_expr` CEL expressions. A transaction will be present in the `transaction_traces`
    /// array if it matched the `filtering_include_filter_expr` and did *NOT* match the `filtering_exclude_filter_expr`.
    ///
    /// Moreover, each matching call that brought the transaction to be in `transaction_traces`
    /// array will have a `filtering_matched` flag set on it to broadcast the fact that this call
    /// match the inclusion/exclusion list.
    ///
    /// This flag controls all `transaction_traces` elements on the Block structure and on
    /// substructures if present.
    #[prost(bool, tag = "40")]
    pub filtering_applied: bool,
    /// The CEL filter expression used to include transaction in `transaction_traces` array, works
    /// in combination with `filtering_exclude_filter_expr` value.
    #[prost(string, tag = "41")]
    pub filtering_include_filter_expr: ::prost::alloc::string::String,
    /// The CEL filter expression used to exclude transaction in `transaction_traces` array, works
    /// in combination with `filtering_include_filter_expr` value.
    #[prost(string, tag = "42")]
    pub filtering_exclude_filter_expr: ::prost::alloc::string::String,
}
/// HeaderOnlyBlock is a standard [Block] structure where all other fields are
/// removed so that hydrating that object from a [Block] bytes payload will
/// drastically reduced allocated memory required to hold the full block.
///
/// This can be used to unpack a [Block] when only the [BlockHeader] information
/// is required and greatly reduced required memory.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HeaderOnlyBlock {
    #[prost(message, optional, tag = "5")]
    pub header: ::core::option::Option<BlockHeader>,
}
/// BlockWithRefs is a lightweight block, with traces and transactions
/// purged from the `block` within, and only.  It is used in transports
/// to pass block data around.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockWithRefs {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "2")]
    pub block: ::core::option::Option<Block>,
    #[prost(message, optional, tag = "3")]
    pub transaction_trace_refs: ::core::option::Option<TransactionRefs>,
    #[prost(bool, tag = "4")]
    pub irreversible: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransactionRefs {
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub hashes: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct UnclesHeaders {
    #[prost(message, repeated, tag = "1")]
    pub uncles: ::prost::alloc::vec::Vec<BlockHeader>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockRef {
    #[prost(bytes = "vec", tag = "1")]
    pub hash: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub number: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockHeader {
    /// geth: ParentHash + parentHash, parity: parentHash
    #[prost(bytes = "vec", tag = "1")]
    pub parent_hash: ::prost::alloc::vec::Vec<u8>,
    /// geth: sha3Uncles, but sha3 is badly worded, so we prefer `uncle_hash`, parity: uncleHash
    #[prost(bytes = "vec", tag = "2")]
    pub uncle_hash: ::prost::alloc::vec::Vec<u8>,
    /// geth: Coinbase + miner, parity: coinbase
    #[prost(bytes = "vec", tag = "3")]
    pub coinbase: ::prost::alloc::vec::Vec<u8>,
    /// geth: Root + json=stateRoot, parity: stateRoot
    #[prost(bytes = "vec", tag = "4")]
    pub state_root: ::prost::alloc::vec::Vec<u8>,
    /// geth: TxHash + transactionsRoot, parity: transactionsTrie
    #[prost(bytes = "vec", tag = "5")]
    pub transactions_root: ::prost::alloc::vec::Vec<u8>,
    /// geth: ReceiptHash + receiptRoot, parity: receiptTrie
    #[prost(bytes = "vec", tag = "6")]
    pub receipt_root: ::prost::alloc::vec::Vec<u8>,
    /// internally called `Bloom`, parity uses `bloom`, geth's json uses `logsBloom`
    #[prost(bytes = "vec", tag = "7")]
    pub logs_bloom: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, optional, tag = "8")]
    pub difficulty: ::core::option::Option<BigInt>,
    #[prost(uint64, tag = "9")]
    pub number: u64,
    #[prost(uint64, tag = "10")]
    pub gas_limit: u64,
    #[prost(uint64, tag = "11")]
    pub gas_used: u64,
    #[prost(message, optional, tag = "12")]
    pub timestamp: ::core::option::Option<::prost_types::Timestamp>,
    /// geth: Extra []byte + extraData, parity: "0x"-prefixed extraData
    #[prost(bytes = "vec", tag = "13")]
    pub extra_data: ::prost::alloc::vec::Vec<u8>,
    /// geth: MixDigest + mixHash, parity: mixHash
    #[prost(bytes = "vec", tag = "14")]
    pub mix_hash: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag = "15")]
    pub nonce: u64,
    #[prost(bytes = "vec", tag = "16")]
    pub hash: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BigInt {
    #[prost(bytes = "vec", tag = "1")]
    pub bytes: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransactionState {
    #[prost(enumeration = "transaction_state::State", tag = "1")]
    pub previous_state: i32,
    #[prost(enumeration = "transaction_state::State", tag = "2")]
    pub current_state: i32,
    #[prost(enumeration = "transaction_state::Transition", tag = "10")]
    pub transition: i32,
    #[prost(bytes = "vec", tag = "11")]
    pub hash: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, optional, tag = "3")]
    pub trx: ::core::option::Option<Transaction>,
    #[prost(message, optional, tag = "4")]
    pub block_header: ::core::option::Option<BlockHeader>,
    #[prost(message, optional, tag = "5")]
    pub transaction_traces: ::core::option::Option<TransactionTrace>,
    #[prost(uint64, tag = "6")]
    pub confirmation: u64,
    #[prost(message, optional, tag = "7")]
    pub head_block_header: ::core::option::Option<BlockHeader>,
    #[prost(bytes = "vec", tag = "8")]
    pub replaced_by_hash: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, optional, tag = "12")]
    pub pending_first_seen: ::core::option::Option<::prost_types::Timestamp>,
    #[prost(message, optional, tag = "13")]
    pub pending_last_seen: ::core::option::Option<::prost_types::Timestamp>,
}
/// Nested message and enum types in `TransactionState`.
pub mod transaction_state {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Transition {
        TransInit = 0,
        TransPooled = 1,
        TransMined = 2,
        TransForked = 3,
        TransConfirmed = 4,
        TransReplaced = 5,
        /// makes speculative traces available on a PENDING transaction. May not be emitted if the transaction is seen a block before
        TransSpeculativelyExecuted = 6,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum State {
        Unknown = 0,
        Pending = 1,
        InBlock = 2,
        Replaced = 3,
    }
}
/// A Transaction not yet in block
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Transaction {
    /// consensus
    #[prost(bytes = "vec", tag = "1")]
    pub to: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub nonce: u64,
    #[prost(message, optional, tag = "3")]
    pub gas_price: ::core::option::Option<BigInt>,
    #[prost(uint64, tag = "4")]
    pub gas_limit: u64,
    /// amount of ETH transfered, in addition to used_gas * gas_price, sometimes referred to as `Amount`
    #[prost(message, optional, tag = "5")]
    pub value: ::core::option::Option<BigInt>,
    #[prost(bytes = "vec", tag = "6")]
    pub input: ::prost::alloc::vec::Vec<u8>,
    /// signature values
    #[prost(bytes = "vec", tag = "7")]
    pub v: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "8")]
    pub r: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "9")]
    pub s: ::prost::alloc::vec::Vec<u8>,
    /// meta
    #[prost(bytes = "vec", tag = "21")]
    pub hash: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "22")]
    pub from: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransactionTrace {
    /// consensus
    #[prost(bytes = "vec", tag = "1")]
    pub to: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub nonce: u64,
    #[prost(message, optional, tag = "3")]
    pub gas_price: ::core::option::Option<BigInt>,
    #[prost(uint64, tag = "4")]
    pub gas_limit: u64,
    /// amount of ETH transfered, in addition to used_gas * gas_price, sometimes referred to as `Amount`
    #[prost(message, optional, tag = "5")]
    pub value: ::core::option::Option<BigInt>,
    #[prost(bytes = "vec", tag = "6")]
    pub input: ::prost::alloc::vec::Vec<u8>,
    /// signature values
    #[prost(bytes = "vec", tag = "7")]
    pub v: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "8")]
    pub r: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "9")]
    pub s: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag = "10")]
    pub gas_used: u64,
    /// meta
    #[prost(uint32, tag = "20")]
    pub index: u32,
    #[prost(bytes = "vec", tag = "21")]
    pub hash: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "22")]
    pub from: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "23")]
    pub return_data: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "24")]
    pub public_key: ::prost::alloc::vec::Vec<u8>,
    #[prost(enumeration = "TransactionTraceStatus", tag = "30")]
    pub status: i32,
    #[prost(message, optional, tag = "31")]
    pub receipt: ::core::option::Option<TransactionReceipt>,
    #[prost(message, repeated, tag = "32")]
    pub calls: ::prost::alloc::vec::Vec<Call>,
}
/// TransactionTraceWithBlockRef
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransactionTraceWithBlockRef {
    #[prost(message, optional, tag = "1")]
    pub trace: ::core::option::Option<TransactionTrace>,
    #[prost(message, optional, tag = "2")]
    pub block_ref: ::core::option::Option<BlockRef>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransactionReceipt {
    /// consensus
    ///
    /// this was an intermediate state_root hash,
    /// computed in-between transactions to make
    /// SURE you could build a proof and point to
    /// state in the middle of a block; geth:
    /// PostState + root + PostStateOrStatus,
    /// parity: status_code, root... this piles
    /// hardforks, see (read the EIPs first):
    /// https://github.com/eoscanada/go-ethereum-private/blob/deep-mind/core/types/receipt.go#L147
    /// and
    /// https://github.com/eoscanada/go-ethereum-private/blob/deep-mind/core/types/receipt.go#L50-L86
    /// and
    /// https://github.com/ethereum/EIPs/blob/master/EIPS/eip-658.md
    /// and the notion of Outcome in parity, which
    /// segregates the two concepts, which are
    /// stored in the same field
    ///
    ///status_code can be computed based on such a
    ///hack of the `state_root` field, following
    ///EIP-658. This is optional before the
    ///BYZANTINIUM hardfork.
    #[prost(bytes = "vec", tag = "1")]
    pub state_root: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub cumulative_gas_used: u64,
    #[prost(bytes = "vec", tag = "3")]
    pub logs_bloom: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, repeated, tag = "4")]
    pub logs: ::prost::alloc::vec::Vec<Log>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Log {
    /// consensus
    #[prost(bytes = "vec", tag = "1")]
    pub address: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", repeated, tag = "2")]
    pub topics: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
    #[prost(bytes = "vec", tag = "3")]
    pub data: ::prost::alloc::vec::Vec<u8>,
    /// supplement
    ///
    /// position inside a trx
    #[prost(uint32, tag = "4")]
    pub index: u32,
    /// position inside a block
    #[prost(uint32, tag = "6")]
    pub block_index: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Call {
    #[prost(uint32, tag = "1")]
    pub index: u32,
    #[prost(uint32, tag = "2")]
    pub parent_index: u32,
    #[prost(uint32, tag = "3")]
    pub depth: u32,
    #[prost(enumeration = "CallType", tag = "4")]
    pub call_type: i32,
    #[prost(bytes = "vec", tag = "5")]
    pub caller: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "6")]
    pub address: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, optional, tag = "7")]
    pub value: ::core::option::Option<BigInt>,
    #[prost(uint64, tag = "8")]
    pub gas_limit: u64,
    #[prost(uint64, tag = "9")]
    pub gas_consumed: u64,
    #[prost(bytes = "vec", tag = "13")]
    pub return_data: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "14")]
    pub input: ::prost::alloc::vec::Vec<u8>,
    #[prost(bool, tag = "15")]
    pub executed_code: bool,
    #[prost(bool, tag = "16")]
    pub suicide: bool,
    /// hex representation of the hash -> preimage
    #[prost(map = "string, string", tag = "20")]
    pub keccak_preimages:
        ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
    #[prost(message, repeated, tag = "21")]
    pub storage_changes: ::prost::alloc::vec::Vec<StorageChange>,
    #[prost(message, repeated, tag = "22")]
    pub balance_changes: ::prost::alloc::vec::Vec<BalanceChange>,
    #[prost(message, repeated, tag = "24")]
    pub nonce_changes: ::prost::alloc::vec::Vec<NonceChange>,
    #[prost(message, repeated, tag = "25")]
    pub logs: ::prost::alloc::vec::Vec<Log>,
    #[prost(message, repeated, tag = "26")]
    pub code_changes: ::prost::alloc::vec::Vec<CodeChange>,
    #[prost(bytes = "vec", repeated, tag = "27")]
    pub created_accounts: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
    #[prost(message, repeated, tag = "28")]
    pub gas_changes: ::prost::alloc::vec::Vec<GasChange>,
    #[prost(message, repeated, tag = "29")]
    pub gas_events: ::prost::alloc::vec::Vec<GasEvent>,
    /// In Ethereum, a call can be either:
    /// - Successfull, execution passes without any problem encountered
    /// - Failed, execution failed, and remaining gas should be consumed
    /// - Reverted, execution failed, but only gas consumed so far is billed, remaining gas is refunded
    ///
    /// When a call is either `failed` or `reverted`, the `status_failed` field
    /// below is set to `true`. If the status is `reverted`, then both `status_failed`
    /// and `status_reverted` are going to be set to `true`.
    #[prost(bool, tag = "10")]
    pub status_failed: bool,
    #[prost(bool, tag = "12")]
    pub status_reverted: bool,
    /// Populated when a call either failed or reverted, so when `status_failed == true`,
    /// see above for details about those flags.
    #[prost(string, tag = "11")]
    pub failure_reason: ::prost::alloc::string::String,
    /// This field represents wheter or not the state changes performed
    /// by this call were correctly recorded by the blockchain.
    ///
    /// On Ethereum, a transaction can record state changes even if some
    /// of its inner nested calls failed. This is problematic however since
    /// a call will invalidate all its state changes as well as all state
    /// changes performed by its child call. This means that even if a call
    /// has a status of `SUCCESS`, the chain might have reverted all the state
    /// changes it performed.
    ///
    /// ```text
    ///   Trx 1
    ///    Call #1 <Failed>
    ///      Call #2 <Execution Success>
    ///      Call #3 <Execution Success>
    ///      |--- Failure here
    ///    Call #4
    /// ```
    ///
    /// In the transaction above, while Call #2 and Call #3 would have the
    /// status `EXECUTED`
    #[prost(bool, tag = "30")]
    pub state_reverted: bool,
    #[prost(message, repeated, tag = "50")]
    pub erc20_balance_changes: ::prost::alloc::vec::Vec<Erc20BalanceChange>,
    #[prost(message, repeated, tag = "51")]
    pub erc20_transfer_events: ::prost::alloc::vec::Vec<Erc20TransferEvent>,
    /// Whether this call trace was a successful match, present only when filtering was applied on block. This
    /// will be `true` if the Block `filtering_applied` is `true`, if the include CEL filter matched and
    /// if the exclude CEL filter did NOT match.
    #[prost(bool, tag = "60")]
    pub filtering_matched: bool,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Erc20BalanceChange {
    #[prost(bytes = "vec", tag = "1")]
    pub holder_address: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, optional, tag = "2")]
    pub old_balance: ::core::option::Option<BigInt>,
    #[prost(message, optional, tag = "3")]
    pub new_balance: ::core::option::Option<BigInt>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Erc20TransferEvent {
    #[prost(bytes = "vec", tag = "1")]
    pub from: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub to: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, optional, tag = "3")]
    pub amount: ::core::option::Option<BigInt>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StorageChange {
    #[prost(bytes = "vec", tag = "1")]
    pub address: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub key: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub old_value: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    pub new_value: ::prost::alloc::vec::Vec<u8>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BalanceChange {
    #[prost(bytes = "vec", tag = "1")]
    pub address: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, optional, tag = "2")]
    pub old_value: ::core::option::Option<BigInt>,
    #[prost(message, optional, tag = "3")]
    pub new_value: ::core::option::Option<BigInt>,
    #[prost(enumeration = "balance_change::Reason", tag = "4")]
    pub reason: i32,
}
/// Nested message and enum types in `BalanceChange`.
pub mod balance_change {
    /// Obtain all balanche change reasons under deep mind repository:
    ///
    /// ```shell
    /// ack -ho 'BalanceChangeReason\(".*"\)' | grep -Eo '".*"' | sort | uniq
    /// ```
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Reason {
        Unknown = 0,
        RewardMineUncle = 1,
        RewardMineBlock = 2,
        DaoRefundContract = 3,
        DaoAdjustBalance = 4,
        Transfer = 5,
        GenesisBalance = 6,
        GasBuy = 7,
        RewardTransactionFee = 8,
        RewardFeeReset = 14,
        GasRefund = 9,
        TouchAccount = 10,
        SuicideRefund = 11,
        SuicideWithdraw = 13,
        CallBalanceOverride = 12,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NonceChange {
    #[prost(bytes = "vec", tag = "1")]
    pub address: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub old_value: u64,
    #[prost(uint64, tag = "3")]
    pub new_value: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CodeChange {
    #[prost(bytes = "vec", tag = "1")]
    pub address: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub old_hash: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub old_code: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    pub new_hash: ::prost::alloc::vec::Vec<u8>,
    #[prost(bytes = "vec", tag = "5")]
    pub new_code: ::prost::alloc::vec::Vec<u8>,
}
/// The gas change model represents the reason why some gas cost has occurred.
/// The gas is computed per actual op codes. Doing them completely might prove
/// overwhelming in most cases.
///
/// Hence, we only index some of them, those that are costy like all the calls
/// one, log events, return data, etc.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GasChange {
    #[prost(uint64, tag = "1")]
    pub old_value: u64,
    #[prost(uint64, tag = "2")]
    pub new_value: u64,
    #[prost(enumeration = "gas_change::Reason", tag = "3")]
    pub reason: i32,
}
/// Nested message and enum types in `GasChange`.
pub mod gas_change {
    /// Obtain all gas change reasons under deep mind repository:
    ///
    /// ```shell
    /// ack -ho 'GasChangeReason\(".*"\)' | grep -Eo '".*"' | sort | uniq
    /// ```
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Reason {
        Unknown = 0,
        Call = 1,
        CallCode = 2,
        CallDataCopy = 3,
        CodeCopy = 4,
        CodeStorage = 5,
        ContractCreation = 6,
        ContractCreation2 = 7,
        DelegateCall = 8,
        EventLog = 9,
        ExtCodeCopy = 10,
        FailedExecution = 11,
        IntrinsicGas = 12,
        PrecompiledContract = 13,
        RefundAfterExecution = 14,
        Return = 15,
        ReturnDataCopy = 16,
        Revert = 17,
        SelfDestruct = 18,
        StaticCall = 19,
        /// Added in Berlin fork (Geth 1.10+)
        StateColdAccess = 20,
    }
}
/// Gas events are emitted to faciliate gas tracking avoid the execution
/// call stack that happens while processing a transaction on the chain.
///
/// We currently have events for tracing of gas amount before and after
/// each child call.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GasEvent {
    #[prost(enumeration = "gas_event::Id", tag = "1")]
    pub id: i32,
    #[prost(uint64, tag = "2")]
    pub gas: u64,
    #[prost(uint64, tag = "3")]
    pub linked_call_index: u64,
}
/// Nested message and enum types in `GasEvent`.
pub mod gas_event {
    /// Obtain all gas change reasons under deep mind repository:
    ///
    /// ```shell
    /// ack -ho 'GasEventID\(".*"\)' | grep -Eo '".*"' | sort | uniq
    /// ```
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Id {
        Unknown = 0,
        AfterCall = 1,
        BeforeCall = 2,
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TransactionTraceStatus {
    Unknown = 0,
    Succeeded = 1,
    Failed = 2,
    Reverted = 3,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum CallType {
    Unspecified = 0,
    /// direct? what's the name for `Call` alone?
    Call = 1,
    Callcode = 2,
    Delegate = 3,
    Static = 4,
    /// create2 ? any other form of calls?
    Create = 5,
}
