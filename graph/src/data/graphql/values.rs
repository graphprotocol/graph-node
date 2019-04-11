use failure::Error;
use graphql_parser::query::Value;
use std::collections::BTreeMap;
use std::str::FromStr;

use crate::web3::types::H160;

pub trait TryFromValue: Sized {
    fn try_from_value(value: &Value) -> Result<Self, Error>;
}

impl TryFromValue for String {
    fn try_from_value(value: &Value) -> Result<Self, Error> {
        match value {
            Value::String(s) => Ok(s.clone()),
            Value::Enum(s) => Ok(s.clone()),
            _ => Err(format_err!("Cannot parse value into a string: {:?}", value)),
        }
    }
}

impl TryFromValue for H160 {
    fn try_from_value(value: &Value) -> Result<Self, Error> {
        match value {
            Value::String(s) => {
                // `H160::from_str` takes a hex string with no leading `0x`.
                let string = s.trim_start_matches("0x");
                H160::from_str(string).map_err(|e| {
                    format_err!("Cannot parse Address/H160 value from string `{}`: {}", s, e)
                })
            }
            _ => Err(format_err!(
                "Cannot parse value into an Address/H160: {:?}",
                value
            )),
        }
    }
}

impl<T> TryFromValue for Vec<T>
where
    T: TryFromValue,
{
    fn try_from_value(value: &Value) -> Result<Self, Error> {
        match value {
            Value::List(values) => values.into_iter().try_fold(vec![], |mut values, value| {
                values.push(T::try_from_value(value)?);
                Ok(values)
            }),
            _ => Err(format_err!("Cannot parse value into a vector: {:?}", value)),
        }
    }
}

pub trait ValueMap {
    fn get_required<T>(&self, key: &str) -> Result<T, Error>
    where
        T: TryFromValue;
    fn get_optional<T>(&self, key: &str) -> Result<Option<T>, Error>
    where
        T: TryFromValue;
}

impl ValueMap for &BTreeMap<String, Value> {
    fn get_required<T>(&self, key: &str) -> Result<T, Error>
    where
        T: TryFromValue,
    {
        self.get(key)
            .ok_or_else(|| format_err!("Required field `{}` not set", key))
            .and_then(|value| T::try_from_value(value).map_err(|e| e.into()))
    }

    fn get_optional<T>(&self, key: &str) -> Result<Option<T>, Error>
    where
        T: TryFromValue,
    {
        self.get(key).map_or(Ok(None), |value| {
            T::try_from_value(value)
                .map(|value| Some(value))
                .map_err(|e| e.into())
        })
    }
}
