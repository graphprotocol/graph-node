use graphql_parser::query as q;
use serde::ser::*;

use super::error::{QueryError, QueryExecutionError};
use data::graphql::SerializableValue;

fn serialize_data<S>(data: &Option<q::Value>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    SerializableValue(data.as_ref().unwrap_or(&q::Value::Null)).serialize(serializer)
}

/// The result of running a query, if successful.
#[derive(Debug, Serialize)]
pub struct QueryResult {
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_data"
    )]
    pub data: Option<q::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub errors: Option<Vec<QueryError>>,
}

impl QueryResult {
    pub fn new(data: Option<q::Value>) -> Self {
        QueryResult { data, errors: None }
    }
}

impl From<QueryExecutionError> for QueryResult {
    fn from(e: QueryExecutionError) -> Self {
        let mut result = Self::new(None);
        result.errors = Some(vec![QueryError::from(e)]);
        result
    }
}

impl From<Vec<QueryExecutionError>> for QueryResult {
    fn from(e: Vec<QueryExecutionError>) -> Self {
        let mut result = Self::new(None);
        result.errors = Some(e.into_iter().map(|error| QueryError::from(error)).collect());
        result
    }
}
