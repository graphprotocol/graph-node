#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventFilter {
    #[prost(string, repeated, tag = "1")]
    pub event_types: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
