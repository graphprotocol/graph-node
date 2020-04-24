use super::error::{QueryError, QueryExecutionError};
use crate::data::graphql::SerializableValue;
use graphql_parser::query as q;
use serde::ser::*;
use serde::Serialize;

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
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_data"
    )]
    pub extensions: Option<q::Value>,
}

impl QueryResult {
    pub fn new(data: Option<q::Value>) -> Self {
        QueryResult {
            data,
            errors: None,
            extensions: None,
        }
    }
    pub fn with_extensions(mut self, extensions: q::Value) -> Self {
        self.extensions = Some(extensions);
        self
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
        QueryResult {
            data: None,
            errors: Some(e.into_iter().map(QueryError::from).collect()),
            extensions: None,
        }
    }
}

impl From<Result<q::Value, Vec<QueryExecutionError>>> for QueryResult {
    fn from(result: Result<q::Value, Vec<QueryExecutionError>>) -> Self {
        match result {
            Ok(v) => QueryResult::new(Some(v)),
            Err(errors) => QueryResult::from(errors),
        }
    }
}
