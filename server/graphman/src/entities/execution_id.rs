use async_graphql::InputValueError;
use async_graphql::InputValueResult;
use async_graphql::Scalar;
use async_graphql::ScalarType;
use async_graphql::Value;

#[derive(Clone, Debug)]
pub struct ExecutionId(pub i64);

#[Scalar]
impl ScalarType for ExecutionId {
    fn parse(value: Value) -> InputValueResult<Self> {
        let Value::String(value) = value else {
            return Err(InputValueError::expected_type(value));
        };

        Ok(value.parse().map(ExecutionId)?)
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

impl From<graphman_store::ExecutionId> for ExecutionId {
    fn from(id: graphman_store::ExecutionId) -> Self {
        Self(id.0)
    }
}

impl From<ExecutionId> for graphman_store::ExecutionId {
    fn from(id: ExecutionId) -> Self {
        Self(id.0)
    }
}
