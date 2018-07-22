use graphql_parser::query::Value;
use std::collections::BTreeMap;
use std::iter::FromIterator;

/// Utilties for coercing GraphQL values based on GraphQL types.
pub mod coercion;

/// Utilities for serializing GraphQL values with Serde.
pub mod serialization;

pub use self::coercion::MaybeCoercible;
pub use self::serialization::SerializableValue;

/// Creates a `graphql_parser::query::Value::Object` from key/value pairs.
pub fn object_value(data: Vec<(&str, Value)>) -> Value {
    Value::Object(BTreeMap::from_iter(
        data.into_iter().map(|(k, v)| (k.to_string(), v)),
    ))
}
