#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EntitiesChanges {
    #[prost(bytes="vec", tag="1")]
    pub block_id: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag="2")]
    pub block_number: u64,
    #[prost(bytes="vec", tag="3")]
    pub prev_block_id: ::prost::alloc::vec::Vec<u8>,
    #[prost(uint64, tag="4")]
    pub prev_block_number: u64,
    #[prost(message, repeated, tag="5")]
    pub entity_changes: ::prost::alloc::vec::Vec<EntityChange>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EntityChange {
    #[prost(string, tag="1")]
    pub entity: ::prost::alloc::string::String,
    #[prost(bytes="vec", tag="2")]
    pub id: ::prost::alloc::vec::Vec<u8>,
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
pub struct Field {
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
    #[prost(enumeration="field::Type", tag="2")]
    pub value_type: i32,
    #[prost(bytes="vec", tag="3")]
    pub new_value: ::prost::alloc::vec::Vec<u8>,
    #[prost(bool, tag="4")]
    pub new_value_null: bool,
    #[prost(bytes="vec", tag="5")]
    pub old_value: ::prost::alloc::vec::Vec<u8>,
    #[prost(bool, tag="6")]
    pub old_value_null: bool,
}
/// Nested message and enum types in `Field`.
pub mod field {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Type {
        /// Protobuf default should not be used, this is used so that the consume can ensure that the value was actually specified
        Unset = 0,
        Bigdecimal = 1,
        Bigint = 2,
        /// int32
        Int = 3,
        Bytes = 4,
        String = 5,
    }
}
