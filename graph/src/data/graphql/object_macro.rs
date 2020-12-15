use crate::prelude::q::{self, Number};
use std::collections::BTreeMap;
use std::iter::FromIterator;

/// Creates a `graphql_parser::query::Value::Object` from key/value pairs.
/// If you don't need to determine which keys are included dynamically at runtime
/// consider using the `object! {}` macro instead.
pub fn object_value(data: Vec<(&str, q::Value)>) -> q::Value {
    q::Value::Object(BTreeMap::from_iter(
        data.into_iter().map(|(k, v)| (k.to_string(), v)),
    ))
}

pub trait IntoValue {
    fn into_value(self) -> q::Value;
}

impl IntoValue for q::Value {
    #[inline]
    fn into_value(self) -> q::Value {
        self
    }
}

impl IntoValue for &'_ str {
    #[inline]
    fn into_value(self) -> q::Value {
        self.to_owned().into_value()
    }
}

impl IntoValue for i32 {
    #[inline]
    fn into_value(self) -> q::Value {
        q::Value::Int(q::Number::from(self))
    }
}

impl IntoValue for u64 {
    #[inline]
    fn into_value(self) -> q::Value {
        q::Value::String(self.to_string())
    }
}

impl<T: IntoValue> IntoValue for Option<T> {
    #[inline]
    fn into_value(self) -> q::Value {
        match self {
            Some(v) => v.into_value(),
            None => q::Value::Null,
        }
    }
}

impl<T: IntoValue> IntoValue for Vec<T> {
    #[inline]
    fn into_value(self) -> q::Value {
        q::Value::List(self.into_iter().map(|e| e.into_value()).collect::<Vec<_>>())
    }
}

macro_rules! impl_into_values {
    ($(($T:ty, $V:ident)),*) => {
        $(
            impl IntoValue for $T {
                #[inline]
                fn into_value(self) -> q::Value {
                    q::Value::$V(self)
                }
            }
        )+
    };
}

impl_into_values![
    (String, String),
    (f64, Float),
    (bool, Boolean),
    (Number, Int)
];

/// Creates a `graphql_parser::query::Value::Object` from key/value pairs.
#[macro_export]
macro_rules! object {
    ($($name:ident: $value:expr,)*) => {
        {
            let mut result = ::std::collections::BTreeMap::new();
            $(
                let value = $crate::data::graphql::object_macro::IntoValue::into_value($value);
                result.insert(stringify!($name).to_string(), value);
            )*
            ::graphql_parser::query::Value::Object(result)
        }
    };
    ($($name:ident: $value:expr),*) => {
        object! {$($name: $value,)*}
    };
}
