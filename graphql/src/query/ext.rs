//! Extension traits for graphql_parser::query structs

use graphql_parser::query as q;

use std::collections::BTreeMap;
use std::convert::TryFrom;

use graph::data::graphql::TryFromValue;
use graph::data::query::QueryExecutionError;
use graph::data::subgraph::SubgraphDeploymentId;
use graph::prelude::web3::types::H256;
use graph::prelude::BlockNumber;

use crate::execution::ObjectOrInterface;
use crate::store::parse_subgraph_id;

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

pub enum BlockLocator {
    Hash(H256),
    Number(BlockNumber),
}

pub struct BlockConstraint {
    pub subgraph: SubgraphDeploymentId,
    pub block: BlockLocator,
}

pub trait FieldExt {
    fn block_constraint<'a>(
        &self,
        object_type: impl Into<ObjectOrInterface<'a>>,
    ) -> Result<Option<BlockConstraint>, QueryExecutionError>;
}

impl FieldExt for q::Field {
    fn block_constraint<'a>(
        &self,
        object_type: impl Into<ObjectOrInterface<'a>>,
    ) -> Result<Option<BlockConstraint>, QueryExecutionError> {
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
                if map.len() != 1 || !(map.contains_key("hash") || map.contains_key("number")) {
                    return Err(invalid_argument("block", self, value));
                }
                let subgraph = parse_subgraph_id(object_type)?;
                if let Some(hash) = map.get("hash") {
                    TryFromValue::try_from_value(hash)
                        .map_err(|_| invalid_argument("block.hash", self, value))
                        .map(|hash| {
                            Some(BlockConstraint {
                                subgraph,
                                block: BlockLocator::Hash(hash),
                            })
                        })
                } else if let Some(number_value) = map.get("number") {
                    TryFromValue::try_from_value(number_value)
                        .map_err(|_| invalid_argument("block.number", self, number_value))
                        .and_then(|number: u64| {
                            TryFrom::try_from(number)
                                .map_err(|_| invalid_argument("block.number", self, number_value))
                        })
                        .map(|number| {
                            Some(BlockConstraint {
                                subgraph,
                                block: BlockLocator::Number(number),
                            })
                        })
                } else {
                    unreachable!("We already checked that there is a hash or number entry")
                }
            } else {
                Err(invalid_argument("block", self, value))
            }
        } else {
            Ok(None)
        }
    }
}
