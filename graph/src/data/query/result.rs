use super::error::{QueryError, QueryExecutionError};
use crate::data::graphql::SerializableValue;
use graphql_parser::query as q;
use serde::ser::*;
use serde::Serialize;
use std::collections::BTreeMap;

fn serialize_data<S>(data: &Option<q::Value>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    SerializableValue(data.as_ref().unwrap_or(&q::Value::Null)).serialize(serializer)
}

/// The result of running a query, if successful.
#[derive(Debug, Clone, Serialize)]
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
    /// A result with an empty object as the data.
    pub fn empty() -> Self {
        QueryResult {
            data: Some(q::Value::Object(BTreeMap::new())),
            errors: None,
            extensions: None,
        }
    }

    pub fn new(data: Option<q::Value>) -> Self {
        QueryResult {
            data,
            errors: None,
            extensions: None,
        }
    }

    pub fn with_extensions(mut self, extensions: BTreeMap<q::Name, q::Value>) -> Self {
        self.extensions = Some(q::Value::Object(extensions));
        self
    }

    pub fn has_errors(&self) -> bool {
        return self.errors.is_some();
    }

    pub fn append(&mut self, mut other: QueryResult) {
        // Currently we don't used extensions, the desired behaviour for merging them is tbd.
        assert!(self.extensions.is_none());
        assert!(other.extensions.is_none());

        match (&mut self.data, &mut other.data) {
            (Some(q::Value::Object(ours)), Some(q::Value::Object(other))) => ours.append(other),

            // Subgraph queries always return objects.
            (Some(_), Some(_)) => unreachable!(),

            // Only one side has data, use that.
            _ => self.data = self.data.take().or(other.data),
        }

        match (&mut self.errors, &mut other.errors) {
            (Some(ours), Some(other)) => ours.append(other),

            // Only one side has errors, use that.
            _ => self.errors = self.errors.take().or(other.errors),
        }
    }

    pub fn as_http_response<T: From<String>>(&self) -> http::Response<T> {
        let status_code = http::StatusCode::OK;
        let json =
            serde_json::to_string(&self).expect("Failed to serialize GraphQL response to JSON");
        http::Response::builder()
            .status(status_code)
            .header("Access-Control-Allow-Origin", "*")
            .header("Access-Control-Allow-Headers", "Content-Type, User-Agent")
            .header("Access-Control-Allow-Methods", "GET, OPTIONS, POST")
            .header("Content-Type", "application/json")
            .body(T::from(json))
            .unwrap()
    }
}

impl From<QueryExecutionError> for QueryResult {
    fn from(e: QueryExecutionError) -> Self {
        let mut result = Self::new(None);
        result.errors = Some(vec![QueryError::from(e)]);
        result
    }
}

impl From<QueryError> for QueryResult {
    fn from(e: QueryError) -> Self {
        QueryResult {
            data: None,
            errors: Some(vec![e]),
            extensions: None,
        }
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

impl From<BTreeMap<String, q::Value>> for QueryResult {
    fn from(val: BTreeMap<String, q::Value>) -> Self {
        QueryResult::new(Some(q::Value::Object(val)))
    }
}

impl<V: Into<QueryResult>, E: Into<QueryResult>> From<Result<V, E>> for QueryResult {
    fn from(result: Result<V, E>) -> Self {
        match result {
            Ok(v) => v.into(),
            Err(e) => e.into(),
        }
    }
}
