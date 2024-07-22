use async_graphql::{InputValueError, InputValueResult};
use async_graphql::{Scalar, ScalarType, Value};

#[derive(Clone, Debug)]
pub struct BlockNumber(pub i32);

#[Scalar]
impl ScalarType for BlockNumber {
    fn parse(value: Value) -> InputValueResult<Self> {
        let Value::String(value) = value else {
            return Err(InputValueError::expected_type(value));
        };

        Ok(value.parse().map(BlockNumber)?)
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

impl From<graph::components::store::BlockNumber> for BlockNumber {
    fn from(block_number: graph::prelude::BlockNumber) -> Self {
        Self(block_number)
    }
}
