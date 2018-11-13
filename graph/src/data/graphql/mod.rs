mod serialization;

/// Utilities for validating GraphQL schemas.
pub(crate) mod validation;

/// Serializable wrapper around a GraphQL value.
pub use self::serialization::SerializableValue;
