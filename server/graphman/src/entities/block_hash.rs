use async_graphql::InputValueError;
use async_graphql::InputValueResult;
use async_graphql::Scalar;
use async_graphql::ScalarType;
use async_graphql::Value;

/// Represents a block hash in hex form.
#[derive(Clone, Debug)]
pub struct BlockHash(pub String);

/// Represents a block hash in hex form.
#[Scalar]
impl ScalarType for BlockHash {
    fn parse(value: Value) -> InputValueResult<Self> {
        let Value::String(value) = value else {
            return Err(InputValueError::expected_type(value));
        };

        Ok(BlockHash(value))
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.clone())
    }
}

impl From<graph::blockchain::BlockHash> for BlockHash {
    fn from(block_hash: graph::blockchain::BlockHash) -> Self {
        Self(block_hash.hash_hex())
    }
}
