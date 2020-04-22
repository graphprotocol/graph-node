use graphql_parser::query::{Number, Value};
use std::collections::BTreeMap;
use std::iter::FromIterator;

/// Utilities for coercing GraphQL values based on GraphQL types.
pub mod coercion;

pub use self::coercion::MaybeCoercible;

/// Creates a `graphql_parser::query::Value::Object` from key/value pairs.
/// If you don't need to determine which keys are included dynamically at runtime
/// consider using the `object! {}` macro instead.
pub fn object_value(data: Vec<(&str, Value)>) -> Value {
    Value::Object(BTreeMap::from_iter(
        data.into_iter().map(|(k, v)| (k.to_string(), v)),
    ))
}

pub trait IntoValue {
    fn into_value(self) -> Value;
}

impl IntoValue for Value {
    #[inline]
    fn into_value(self) -> Value {
        self
    }
}

impl IntoValue for &'_ str {
    #[inline]
    fn into_value(self) -> Value {
        self.to_owned().into_value()
    }
}

impl<T: IntoValue> IntoValue for Option<T> {
    #[inline]
    fn into_value(self) -> Value {
        match self {
            Some(v) => v.into_value(),
            None => Value::Null,
        }
    }
}

macro_rules! impl_into_values {
    ($(($T:ty, $V:ident)),*) => {
        $(
            impl IntoValue for $T {
                #[inline]
                fn into_value(self) -> Value {
                    Value::$V(self)
                }
            }
        )+
    };
}

impl_into_values![
    (String, String),
    (f64, Float),
    (bool, Boolean),
    (Vec<Value>, List),
    (Number, Int)
];

/// Creates a `graphql_parser::query::Value::Object` from key/value pairs.
#[macro_export]
macro_rules! object {
    ($($name:ident: $value:expr,)*) => {
        {
            let mut result = ::std::collections::BTreeMap::new();
            $(
                let value = $crate::prelude::IntoValue::into_value($value);
                result.insert(stringify!($name).to_string(), value);
            )*
            ::graphql_parser::query::Value::Object(result)
        }
    };
    ($($name:ident: $value:expr),*) => {
        object! {$($name: $value,)*}
    };
}
