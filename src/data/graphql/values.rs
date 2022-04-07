use anyhow::{anyhow, Error};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::str::FromStr;

use crate::data::value::Object;
use crate::prelude::{r, BigInt, Entity};
use web3::types::{H160, H256};

pub trait TryFromValue: Sized {
    fn try_from_value(value: &r::Value) -> Result<Self, Error>;
}

impl TryFrom<r::Value> for bool {
    type Error = Error;

    fn try_from(value: r::Value) -> Result<Self, Error> {
        match value {
            r::Value::Boolean(b) => Ok(b),
            _ => Err(anyhow!("Cannot parse value into a boolean: {:?}", value)),
        }
    }
}

impl TryFrom<r::Value> for String {
    type Error = Error;

    fn try_from(value: r::Value) -> Result<Self, Error> {
        match value {
            r::Value::String(s) => Ok(s.clone()),
            r::Value::Enum(s) => Ok(s.clone()),
            _ => Err(anyhow!("Cannot parse value into a string: {:?}", value)),
        }
    }
}

impl TryFrom<r::Value> for u64 {
    type Error = Error;

    fn try_from(value: r::Value) -> Result<Self, Error> {
        match value {
            r::Value::Int(n) => {
                if n >= 0 {
                    Ok(n as u64)
                } else {
                    Err(anyhow!("Cannot parse value into an integer/u64: {:?}", n))
                }
            }
            // `BigInt`s are represented as `String`s.
            r::Value::String(s) => u64::from_str(&s).map_err(Into::into),
            _ => Err(anyhow!(
                "Cannot parse value into an integer/u64: {:?}",
                value
            )),
        }
    }
}
