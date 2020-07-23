//! Extension traits for graphql_parser::query structs

use graphql_parser::query as q;

use std::collections::BTreeMap;
use std::convert::TryFrom;

use graph::data::graphql::TryFromValue;
use graph::data::query::QueryExecutionError;
use graph::prelude::web3::types::H256;
use graph::prelude::BlockNumber;

pub trait ValueExt {
    fn as_object(&self) -> &BTreeMap<q::Name, q::Value>;
    fn as_string(&self) -> &str;
}

impl ValueExt for q::Value {
    fn as_object(&self) -> &BTreeMap<q::Name, q::Value> {
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

pub trait FieldExt {
    fn block_constraint<'a>(&self) -> Result<BlockConstraint, QueryExecutionError>;
}

impl FieldExt for q::Field {
    fn block_constraint<'a>(&self) -> Result<BlockConstraint, QueryExecutionError> {
        fn invalid_argument(arg: &str, field: &q::Field, value: &q::Value) -> QueryExecutionError {
            QueryExecutionError::InvalidArgumentError(
                field.position.clone(),
                arg.to_owned(),
                value.clone(),
            )
        }

        let value =
            self.arguments.iter().find_map(
                |(name, value)| {
                    if name == "block" {
                        Some(value)
                    } else {
                        None
                    }
                },
            );
        if let Some(value) = value {
            if let q::Value::Object(map) = value {
                if map.len() != 1 {
                    return Err(invalid_argument("block", self, value));
                }
                if let Some(hash) = map.get("hash") {
                    TryFromValue::try_from_value(hash)
                        .map_err(|_| invalid_argument("block.hash", self, value))
                        .map(|hash| BlockConstraint::Hash(hash))
                } else if let Some(number_value) = map.get("number") {
                    TryFromValue::try_from_value(number_value)
                        .map_err(|_| invalid_argument("block.number", self, number_value))
                        .and_then(|number: u64| {
                            TryFrom::try_from(number)
                                .map_err(|_| invalid_argument("block.number", self, number_value))
                        })
                        .map(|number| BlockConstraint::Number(number))
                } else {
                    Err(invalid_argument("block", self, value))
                }
            } else {
                Err(invalid_argument("block", self, value))
            }
        } else {
            Ok(BlockConstraint::Latest)
        }
    }
}
