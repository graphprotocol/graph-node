mod serialization;

/// Utilities for validating GraphQL schemas.
pub mod traversal;

/// Types to represent built in scalar values in GraphQL documents
pub mod scalar;

/// Utilities for working with GraphQL values.
mod values;

/// Serializable wrapper around a GraphQL value.
pub use self::serialization::SerializableValue;

pub use self::values::{
    // Trait for converting from GraphQL values into other types.
    TryFromValue,

    // Trait for plucking typed values from a GraphQL list.
    ValueList,

    // Trait for plucking typed values out of a GraphQL value maps.
    ValueMap,
};
