/// Stream block headers only. The `transactions` field is always empty.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockHeaderOnly {}
/// Stream every single block, but each block will only contain transactions that match with `event_filters`.
/// A TransactionEventFilter message with an empty `event_filters` is invalid. Do not send any filter instead
/// if you wish to receive full blocks.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TransactionEventFilter {
    #[prost(message, repeated, tag = "1")]
    pub event_filters: ::prost::alloc::vec::Vec<ContractEventFilter>,
}
/// Only include transactions which emit at least one event that *BOTH*
/// * is emitted by `contract_address`
/// * matches with at least one topic in `topics`
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ContractEventFilter {
    #[prost(bytes = "vec", tag = "1")]
    pub contract_address: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, repeated, tag = "2")]
    pub topics: ::prost::alloc::vec::Vec<TopicWithRanges>,
}
/// Matches events whose `keys\[0\]` equals `topic`, *AND* in any of the `block_ranges`.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TopicWithRanges {
    #[prost(bytes = "vec", tag = "1")]
    pub topic: ::prost::alloc::vec::Vec<u8>,
    #[prost(message, repeated, tag = "2")]
    pub block_ranges: ::prost::alloc::vec::Vec<BlockRange>,
}
/// A range of blocks. `start_block` is inclusive, and `end_block` is exclusive. When `end_block` is `0`, it means
/// that any block height >= `start_block` is matched.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BlockRange {
    #[prost(uint64, tag = "1")]
    pub start_block: u64,
    #[prost(uint64, tag = "2")]
    pub end_block: u64,
}
