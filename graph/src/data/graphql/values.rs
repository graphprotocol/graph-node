use graphql_parser::query::Value;
use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;

use crate::prelude::{BigInt, Entity, Error};
use web3::types::{H160, H256};

pub trait TryFromValue: Sized {
    fn try_from_value(value: &Value<'static, String>) -> Result<Self, Error>;
}

impl TryFromValue for Value<'static, String> {
    fn try_from_value(value: &Value<'static, String>) -> Result<Self, Error> {
        Ok(value.clone())
    }
}

impl TryFromValue for bool {
    fn try_from_value(value: &Value<'static, String>) -> Result<Self, Error> {
        match value {
            Value::Boolean(b) => Ok(*b),
            _ => Err(anyhow::anyhow!(
                "Cannot parse value into a boolean: {:?}",
                value
            )),
        }
    }
}

impl TryFromValue for String {
    fn try_from_value(value: &Value<'static, String>) -> Result<Self, Error> {
        match value {
            Value::String(s) => Ok(s.clone()),
            Value::Enum(s) => Ok(s.clone()),
            _ => Err(anyhow::anyhow!(
                "Cannot parse value into a string: {:?}",
                value
            )),
        }
    }
}

impl TryFromValue for u64 {
    fn try_from_value(value: &Value<'static, String>) -> Result<Self, Error> {
        match value {
            Value::Int(n) => n
                .as_i64()
                .map(|n| n as u64)
                .ok_or_else(|| anyhow::anyhow!("Cannot parse value into an integer/u64: {:?}", n)),

            // `BigInt`s are represented as `String`s.
            Value::String(s) => u64::from_str(s).map_err(Into::into),
            _ => Err(anyhow::anyhow!(
                "Cannot parse value into an integer/u64: {:?}",
                value
            )),
        }
    }
}
impl TryFromValue for H160 {
    fn try_from_value(value: &Value<'static, String>) -> Result<Self, Error> {
        match value {
            Value::String(s) => {
                // `H160::from_str` takes a hex string with no leading `0x`.
                let string = s.trim_start_matches("0x");
                H160::from_str(string).map_err(|e| {
                    anyhow::anyhow!("Cannot parse Address/H160 value from string `{}`: {}", s, e)
                })
            }
            _ => Err(anyhow::anyhow!(
                "Cannot parse value into an Address/H160: {:?}",
                value
            )),
        }
    }
}

impl TryFromValue for H256 {
    fn try_from_value(value: &Value<'static, String>) -> Result<Self, Error> {
        match value {
            Value::String(s) => {
                // `H256::from_str` takes a hex string with no leading `0x`.
                let string = s.trim_start_matches("0x");
                H256::from_str(string).map_err(|e| {
                    anyhow::anyhow!("Cannot parse H256 value from string `{}`: {}", s, e)
                })
            }
            _ => Err(anyhow::anyhow!(
                "Cannot parse value into an H256: {:?}",
                value
            )),
        }
    }
}

impl TryFromValue for BigInt {
    fn try_from_value(value: &Value<'static, String>) -> Result<Self, Error> {
        match value {
            Value::String(s) => BigInt::from_str(s).map_err(|e| {
                anyhow::anyhow!("Cannot parse BigInt value from string `{}`: {}", s, e)
            }),
            _ => Err(anyhow::anyhow!(
                "Cannot parse value into an BigInt: {:?}",
                value
            )),
        }
    }
}

impl<T> TryFromValue for Vec<T>
where
    T: TryFromValue,
{
    fn try_from_value(value: &Value<'static, String>) -> Result<Self, Error> {
        match value {
            Value::List(values) => values.into_iter().try_fold(vec![], |mut values, value| {
                values.push(T::try_from_value(value)?);
                Ok(values)
            }),
            _ => Err(anyhow::anyhow!(
                "Cannot parse value into a vector: {:?}",
                value
            )),
        }
    }
}

/// Assumes the entity is stored as a JSON string.
impl TryFromValue for Entity {
    fn try_from_value(value: &Value<'static, String>) -> Result<Self, Error> {
        match value {
            Value::String(s) => serde_json::from_str(s).map_err(Into::into),
            _ => Err(anyhow::anyhow!(
                "Cannot parse entity, value is not a string: {:?}",
                value
            )),
        }
    }
}

pub trait ValueMap {
    fn get_required<T: TryFromValue>(&self, key: &str) -> Result<T, Error>;
    fn get_optional<T: TryFromValue>(&self, key: &str) -> Result<Option<T>, Error>;
}

impl ValueMap for Value<'static, String> {
    fn get_required<T: TryFromValue>(&self, key: &str) -> Result<T, Error> {
        match self {
            Value::Object(map) => map.get_required(key),
            _ => Err(anyhow::anyhow!("value is not a map: {:?}", self)),
        }
    }

    fn get_optional<T>(&self, key: &str) -> Result<Option<T>, Error>
    where
        T: TryFromValue,
    {
        match self {
            Value::Object(map) => map.get_optional(key),
            _ => Err(anyhow::anyhow!("value is not a map: {:?}", self)),
        }
    }
}

impl ValueMap for &BTreeMap<String, Value<'static, String>> {
    fn get_required<T>(&self, key: &str) -> Result<T, Error>
    where
        T: TryFromValue,
    {
        self.get(key)
            .ok_or_else(|| anyhow::anyhow!("Required field `{}` not set", key))
            .and_then(|value| T::try_from_value(value).map_err(|e| e.into()))
    }

    fn get_optional<T>(&self, key: &str) -> Result<Option<T>, Error>
    where
        T: TryFromValue,
    {
        self.get(key).map_or(Ok(None), |value| match value {
            Value::Null => Ok(None),
            _ => T::try_from_value(value)
                .map(|value| Some(value))
                .map_err(|e| e.into()),
        })
    }
}

impl ValueMap for &HashMap<&String, Value<'static, String>> {
    fn get_required<T>(&self, key: &str) -> Result<T, Error>
    where
        T: TryFromValue,
    {
        self.get(&String::from(key))
            .ok_or_else(|| anyhow::anyhow!("Required field `{}` not set", key))
            .and_then(|value| T::try_from_value(value).map_err(|e| e.into()))
    }

    fn get_optional<T>(&self, key: &str) -> Result<Option<T>, Error>
    where
        T: TryFromValue,
    {
        self.get(&String::from(key))
            .map_or(Ok(None), |value| match value {
                Value::Null => Ok(None),
                _ => T::try_from_value(value)
                    .map(|value| Some(value))
                    .map_err(|e| e.into()),
            })
    }
}

pub trait ValueList {
    fn get_values<T>(&self) -> Result<Vec<T>, Error>
    where
        T: TryFromValue;
}

impl ValueList for Value<'static, String> {
    fn get_values<T>(&self) -> Result<Vec<T>, Error>
    where
        T: TryFromValue,
    {
        match self {
            Value::List(values) => values.get_values(),
            _ => Err(anyhow::anyhow!("value is not a list: {:?}", self)),
        }
    }
}

impl ValueList for Vec<Value<'static, String>> {
    fn get_values<T>(&self) -> Result<Vec<T>, Error>
    where
        T: TryFromValue,
    {
        self.iter().try_fold(vec![], |mut acc, value| {
            acc.push(T::try_from_value(value)?);
            Ok(acc)
        })
    }
}
