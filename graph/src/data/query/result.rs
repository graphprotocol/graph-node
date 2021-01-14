use super::error::{QueryError, QueryExecutionError};
use crate::{
    data::graphql::SerializableValue,
    prelude::{q, CacheWeight, SubgraphDeploymentId},
};
use serde::ser::*;
use serde::Serialize;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::sync::Arc;

fn serialize_data<S>(data: &Option<Data>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_map(None)?;

    // Unwrap: data is only serialized if it is `Some`.
    for (k, v) in data.as_ref().unwrap() {
        ser.serialize_entry(k, &SerializableValue(v))?;
    }
    ser.end()
}

fn serialize_value_map<'a, S>(
    data: impl Iterator<Item = &'a Data>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_map(None)?;
    for map in data {
        for (k, v) in map {
            ser.serialize_entry(k, &SerializableValue(v))?;
        }
    }
    ser.end()
}

pub type Data = BTreeMap<String, q::Value>;

#[derive(Debug)]
/// A collection of query results that is serialized as a single result.
pub struct QueryResults {
    results: Vec<Arc<QueryResult>>,
}

impl QueryResults {
    pub fn empty() -> Self {
        QueryResults {
            results: Vec::new(),
        }
    }

    pub fn first(&self) -> Option<&Arc<QueryResult>> {
        self.results.first()
    }
}

impl Serialize for QueryResults {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut len = 0;
        let has_data = self.results.iter().any(|r| r.has_data());
        if has_data {
            len += 1;
        }
        let has_errors = self.results.iter().any(|r| r.has_errors());
        if has_errors {
            len += 1;
        }

        let mut state = serializer.serialize_struct("QueryResults", len)?;

        // Serialize data.
        if has_data {
            struct SerData<'a>(&'a QueryResults);

            impl Serialize for SerData<'_> {
                fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                    serialize_value_map(
                        self.0.results.iter().filter_map(|r| r.data.as_ref()),
                        serializer,
                    )
                }
            }

            state.serialize_field("data", &SerData(self))?;
        }

        // Serialize errors.
        if has_errors {
            struct SerError<'a>(&'a QueryResults);

            impl Serialize for SerError<'_> {
                fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                    let mut seq = serializer.serialize_seq(None)?;
                    for err in self.0.results.iter().map(|r| &r.errors).flatten() {
                        seq.serialize_element(err)?;
                    }
                    seq.end()
                }
            }

            state.serialize_field("errors", &SerError(self))?;
        }

        state.end()
    }
}

impl From<Data> for QueryResults {
    fn from(x: Data) -> Self {
        QueryResults {
            results: vec![Arc::new(x.into())],
        }
    }
}

impl From<QueryResult> for QueryResults {
    fn from(x: QueryResult) -> Self {
        QueryResults {
            results: vec![Arc::new(x)],
        }
    }
}

impl From<Arc<QueryResult>> for QueryResults {
    fn from(x: Arc<QueryResult>) -> Self {
        QueryResults { results: vec![x] }
    }
}

impl From<QueryExecutionError> for QueryResults {
    fn from(x: QueryExecutionError) -> Self {
        QueryResults {
            results: vec![Arc::new(x.into())],
        }
    }
}

impl From<Vec<QueryExecutionError>> for QueryResults {
    fn from(x: Vec<QueryExecutionError>) -> Self {
        QueryResults {
            results: vec![Arc::new(x.into())],
        }
    }
}

impl QueryResults {
    pub fn append(&mut self, other: Arc<QueryResult>) {
        self.results.push(other);
    }

    pub fn as_http_response<T: From<String>>(&self) -> http::Response<T> {
        let status_code = http::StatusCode::OK;
        let json =
            serde_json::to_string(self).expect("Failed to serialize GraphQL response to JSON");
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

/// The result of running a query, if successful.
#[derive(Debug, Serialize)]
pub struct QueryResult {
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_data"
    )]
    data: Option<Data>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    errors: Vec<QueryError>,
    #[serde(skip_serializing)]
    pub deployment: Option<SubgraphDeploymentId>,
}

impl QueryResult {
    pub fn new(data: Data) -> Self {
        QueryResult {
            data: Some(data),
            errors: Vec::new(),
            deployment: None,
        }
    }

    /// This is really `clone`, but we do not want to implement `Clone`;
    /// this is only meant for test purposes and should not be used in production
    /// code since cloning query results can be very expensive
    #[cfg(debug_assertions)]
    pub fn duplicate(&self) -> Self {
        Self {
            data: self.data.clone(),
            errors: self.errors.clone(),
            deployment: self.deployment.clone(),
        }
    }

    pub fn has_errors(&self) -> bool {
        return !self.errors.is_empty();
    }

    pub fn has_data(&self) -> bool {
        self.data.is_some()
    }

    pub fn to_result(self) -> Result<Option<q::Value>, Vec<QueryError>> {
        if self.has_errors() {
            Err(self.errors)
        } else {
            Ok(self.data.map(|v| q::Value::Object(v)))
        }
    }

    pub fn take_data(&mut self) -> Option<Data> {
        self.data.take()
    }

    pub fn set_data(&mut self, data: Option<Data>) {
        self.data = data
    }

    pub fn errors_mut(&mut self) -> &mut Vec<QueryError> {
        &mut self.errors
    }
}

impl From<QueryExecutionError> for QueryResult {
    fn from(e: QueryExecutionError) -> Self {
        QueryResult {
            data: None,
            errors: vec![e.into()],
            deployment: None,
        }
    }
}

impl From<QueryError> for QueryResult {
    fn from(e: QueryError) -> Self {
        QueryResult {
            data: None,
            errors: vec![e],
            deployment: None,
        }
    }
}

impl From<Vec<QueryExecutionError>> for QueryResult {
    fn from(e: Vec<QueryExecutionError>) -> Self {
        QueryResult {
            data: None,
            errors: e.into_iter().map(QueryError::from).collect(),
            deployment: None,
        }
    }
}

impl From<Data> for QueryResult {
    fn from(val: Data) -> Self {
        QueryResult::new(val)
    }
}

impl TryFrom<q::Value> for QueryResult {
    type Error = &'static str;

    fn try_from(value: q::Value) -> Result<Self, Self::Error> {
        match value {
            q::Value::Object(map) => Ok(QueryResult::from(map)),
            _ => Err("only objects can be turned into a QueryResult"),
        }
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

impl CacheWeight for QueryResult {
    fn indirect_weight(&self) -> usize {
        self.data.indirect_weight() + self.errors.indirect_weight()
    }
}

// Check that when we serialize a `QueryResult` with multiple entries
// in `data` it appears as if we serialized one big map
#[test]
fn multiple_data_items() {
    use serde_json::json;

    fn make_obj(key: &str, value: &str) -> Arc<QueryResult> {
        let mut map = BTreeMap::new();
        map.insert(key.to_owned(), q::Value::String(value.to_owned()));
        Arc::new(map.into())
    }

    let obj1 = make_obj("key1", "value1");
    let obj2 = make_obj("key2", "value2");

    let mut res = QueryResults::empty();
    res.append(obj1);
    res.append(obj2);

    let expected =
        serde_json::to_string(&json!({"data":{"key1": "value1", "key2": "value2"}})).unwrap();
    let actual = serde_json::to_string(&res).unwrap();
    assert_eq!(expected, actual)
}
