mod serialization;

/// Utilities for validating GraphQL schemas.
pub(crate) mod validation;

/// Utilities for working with GraphQL values.
mod values;

/// Serializable wrapper around a GraphQL value.
pub use self::serialization::SerializableValue;

pub use self::values::{
    // Trait for converting from GraphQL values into other types.
    TryFromValue,

    // Trait for plucking typed values out of a GraphQL value maps.
    ValueMap,
};
