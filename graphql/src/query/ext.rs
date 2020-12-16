//! Extension traits for graphql_parser::query structs

use graphql_parser::Pos;

use std::collections::{BTreeMap, HashMap};
use std::convert::TryFrom;

use anyhow::anyhow;
use graph::data::graphql::TryFromValue;
use graph::data::query::QueryExecutionError;
use graph::prelude::{q, web3::types::H256, BlockNumber, Error};

pub trait ValueExt: Sized {
    fn as_object(&self) -> &BTreeMap<String, q::Value>;
    fn as_string(&self) -> &str;

    /// If `self` is a variable reference, look it up in `vars` and return
    /// that. Otherwise, just return `self`.
    ///
    /// If `self` is a variable reference, but has no entry in `vars` return
    /// an error
    fn lookup<'a>(
        &'a self,
        vars: &'a HashMap<String, Self>,
        pos: Pos,
    ) -> Result<&'a Self, QueryExecutionError>;
}

impl ValueExt for q::Value {
    fn as_object(&self) -> &BTreeMap<String, q::Value> {
        match self {
            q::Value::Object(object) => object,
            _ => panic!("expected a Value::Object"),
        }
    }

    fn as_string(&self) -> &str {
        match self {
            q::Value::String(string) => string,
            _ => panic!("expected a Value::String"),
        }
    }

    fn lookup<'a>(
        &'a self,
        vars: &'a HashMap<String, q::Value>,
        pos: Pos,
    ) -> Result<&'a q::Value, QueryExecutionError> {
        match self {
            q::Value::Variable(name) => vars
                .get(name)
                .ok_or_else(|| QueryExecutionError::MissingVariableError(pos, name.to_owned())),
            _ => Ok(self),
        }
    }
}

#[derive(PartialEq, Eq, Hash, Debug)]
pub enum BlockConstraint {
    Hash(H256),
    Number(BlockNumber),
    Latest,
}

impl Default for BlockConstraint {
    fn default() -> Self {
        BlockConstraint::Latest
    }
}

impl TryFromValue for BlockConstraint {
    /// `value` should be the output of input object coercion.
    fn try_from_value(value: &q::Value) -> Result<Self, Error> {
        let map = match value {
            q::Value::Object(map) => map,
            q::Value::Null => return Ok(Self::default()),
            _ => return Err(anyhow!("invalid `BlockConstraint`")),
        };

        if let Some(hash) = map.get("hash") {
            Ok(BlockConstraint::Hash(TryFromValue::try_from_value(hash)?))
        } else if let Some(number_value) = map.get("number") {
            let number: u64 = TryFromValue::try_from_value(number_value)?;
            Ok(BlockConstraint::Number(TryFrom::try_from(number)?))
        } else {
            Err(anyhow!("invalid `BlockConstraint`"))
        }
    }
}
