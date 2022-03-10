#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MultiLogFilter {
    #[prost(message, repeated, tag = "1")]
    pub basic_log_filters: ::prost::alloc::vec::Vec<BasicLogFilter>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BasicLogFilter {
    #[prost(bytes = "vec", repeated, tag = "1")]
    pub addresses: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
    /// corresponds to the keccak of the event signature which is stores in topic.0
    #[prost(bytes = "vec", repeated, tag = "2")]
    pub event_signatures: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LightBlock {}
