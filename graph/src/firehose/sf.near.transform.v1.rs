#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BasicReceiptFilter {
    #[prost(string, repeated, tag="1")]
    pub accounts: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
