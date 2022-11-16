#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EntityChanges {
    #[prost(message, repeated, tag="5")]
    pub entity_changes: ::prost::alloc::vec::Vec<EntityChange>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EntityChange {
    #[prost(string, tag="1")]
    pub entity: ::prost::alloc::string::String,
    #[prost(string, tag="2")]
    pub id: ::prost::alloc::string::String,
    #[prost(uint64, tag="3")]
    pub ordinal: u64,
    #[prost(enumeration="entity_change::Operation", tag="4")]
    pub operation: i32,
    #[prost(message, repeated, tag="5")]
    pub fields: ::prost::alloc::vec::Vec<Field>,
}
/// Nested message and enum types in `EntityChange`.
pub mod entity_change {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Operation {
        /// Protobuf default should not be used, this is used so that the consume can ensure that the value was actually specified
        Unset = 0,
        Create = 1,
        Update = 2,
        Delete = 3,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Value {
    #[prost(oneof="value::Typed", tags="1, 2, 3, 4, 5, 6, 10")]
    pub typed: ::core::option::Option<value::Typed>,
}
/// Nested message and enum types in `Value`.
pub mod value {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Typed {
        #[prost(int32, tag="1")]
        Int32(i32),
        #[prost(string, tag="2")]
        Bigdecimal(::prost::alloc::string::String),
        #[prost(string, tag="3")]
        Bigint(::prost::alloc::string::String),
        #[prost(string, tag="4")]
        String(::prost::alloc::string::String),
        #[prost(bytes, tag="5")]
        Bytes(::prost::alloc::vec::Vec<u8>),
        #[prost(bool, tag="6")]
        Bool(bool),
        //reserved 7 to 9;  // For future types

        #[prost(message, tag="10")]
        Array(super::Array),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Array {
    #[prost(message, repeated, tag="1")]
    pub value: ::prost::alloc::vec::Vec<Value>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Field {
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    #[prost(message, optional, tag="3")]
    pub new_value: ::core::option::Option<Value>,
    #[prost(message, optional, tag="5")]
    pub old_value: ::core::option::Option<Value>,
}
