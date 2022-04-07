/// MultiLogFilter concatenates the results of each LogFilter (inclusive OR)
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MultiLogFilter {
    #[prost(message, repeated, tag = "1")]
    pub log_filters: ::prost::alloc::vec::Vec<LogFilter>,
}
/// LogFilter will match calls where *BOTH*
/// * the contract address that emits the log is one in the provided addresses -- OR addresses list is empty --
/// * the event signature (topic.0) is one of the provided event_signatures -- OR event_signatures is empty --
///
/// a LogFilter with both empty addresses and event_signatures lists is invalid and will fail.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LogFilter {
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub addresses: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
    /// corresponds to the keccak of the event signature which is stores in topic.0
    #[prost(bytes = "vec", repeated, tag = "2")]
    pub event_signatures: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
}
/// MultiCallToFilter concatenates the results of each CallToFilter (inclusive OR)
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MultiCallToFilter {
    #[prost(message, repeated, tag = "1")]
    pub call_filters: ::prost::alloc::vec::Vec<CallToFilter>,
}
/// CallToFilter will match calls where *BOTH*
/// * the contract address (TO) is one in the provided addresses -- OR addresses list is empty --
/// * the method signature (in 4-bytes format) is one of the provided signatures -- OR signatures is empty --
///
/// a CallToFilter with both empty addresses and signatures lists is invalid and will fail.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CallToFilter {
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub addresses: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
    #[prost(bytes = "vec", repeated, tag = "2")]
    pub signatures: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LightBlock {}
