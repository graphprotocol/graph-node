use super::error::{QueryError, QueryExecutionError};
use crate::{data::graphql::SerializableValue, prelude::CacheWeight};
use graphql_parser::query as q;
use serde::ser::*;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;

fn serialize_data<S>(data: &Option<q::Value>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    SerializableValue(data.as_ref().unwrap_or(&q::Value::Null)).serialize(serializer)
}

fn serialize_datas<S>(data: &Vec<Arc<q::Value>>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if data.is_empty() {
        SerializableValue(&q::Value::Null).serialize(serializer)
    } else {
        let mut ser = serializer.serialize_map(None)?;
        for value in data {
            match value.as_ref() {
                q::Value::Object(map) => {
                    for (k, v) in map {
                        ser.serialize_entry(k, &SerializableValue(v))?;
                    }
                }
                _ => unreachable!("all data entries in a QueryResult are maps"),
            }
        }
        ser.end()
    }
}

/// The result of running a query, if successful.
#[derive(Debug, Clone, Serialize)]
pub struct QueryResult {
    #[serde(
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "serialize_datas"
    )]
    data: Vec<Arc<q::Value>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    errors: Vec<QueryError>,
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
            data: Vec::new(),
            errors: Vec::new(),
            extensions: None,
        }
    }

    pub fn new(data: Vec<Arc<q::Value>>) -> Self {
        QueryResult {
            data,
            errors: Vec::new(),
            extensions: None,
        }
    }

    pub fn with_extensions(mut self, extensions: BTreeMap<q::Name, q::Value>) -> Self {
        self.extensions = Some(q::Value::Object(extensions));
        self
    }

    pub fn has_errors(&self) -> bool {
        return self.errors.len() > 0;
    }

    pub fn append(&mut self, other: QueryResult) {
        // Currently we don't used extensions, the desired behaviour for merging them is tbd.
        assert!(self.extensions.is_none());
        assert!(other.extensions.is_none());

        self.data.extend(other.data);
        self.errors.extend(other.errors);
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

    pub fn take_data(mut self) -> Option<q::Value> {
        fn take_or_clone(value: Arc<q::Value>) -> q::Value {
            Arc::try_unwrap(value).unwrap_or_else(|value| value.as_ref().clone())
        }

        if self.data.is_empty() {
            None
        } else {
            let value = self.data.pop().map(take_or_clone).unwrap();
            let res = self.data.into_iter().fold(value, |mut acc, value| {
                let mut value = take_or_clone(value);
                match (&mut acc, &mut value) {
                    (q::Value::Object(ours), q::Value::Object(other)) => {
                        ours.append(other);
                    }
                    // Subgraph queries always return objects, so both
                    // acc and value must be objects
                    (_, _) => unreachable!(),
                }
                acc
            });
            Some(res)
        }
    }

    pub fn has_data(&self) -> bool {
        !self.data.is_empty()
    }

    /// Return either the data or the errors for this `QueryResult`. If there
    /// are errors, the data just gets dropped.
    pub fn to_result(self) -> Result<Option<q::Value>, Vec<QueryError>> {
        if self.has_errors() {
            Err(self.errors)
        } else {
            Ok(self.take_data())
        }
    }
}

impl From<QueryExecutionError> for QueryResult {
    fn from(e: QueryExecutionError) -> Self {
        let mut result = Self::new(Vec::new());
        result.errors = vec![QueryError::from(e)];
        result
    }
}

impl From<QueryError> for QueryResult {
    fn from(e: QueryError) -> Self {
        QueryResult {
            data: Vec::new(),
            errors: vec![e],
            extensions: None,
        }
    }
}

impl From<Vec<QueryExecutionError>> for QueryResult {
    fn from(e: Vec<QueryExecutionError>) -> Self {
        QueryResult {
            data: Vec::new(),
            errors: e.into_iter().map(QueryError::from).collect(),
            extensions: None,
        }
    }
}

impl From<BTreeMap<String, q::Value>> for QueryResult {
    fn from(val: BTreeMap<String, q::Value>) -> Self {
        QueryResult::from(q::Value::Object(val))
    }
}

impl From<q::Value> for QueryResult {
    fn from(val: q::Value) -> Self {
        QueryResult::new(vec![Arc::new(val)])
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
        self.data.indirect_weight()
            + self.errors.indirect_weight()
            + self.extensions.indirect_weight()
    }
}

// Check that when we serialize a `QueryResult` with multiple entries
// in `data` it appears as if we serialized one big map
#[test]
fn multiple_data_items() {
    use serde_json::json;

    fn make_obj(key: &str, value: &str) -> Arc<q::Value> {
        let mut map = BTreeMap::new();
        map.insert(key.to_owned(), q::Value::String(value.to_owned()));
        Arc::new(q::Value::Object(map))
    }

    let obj1 = make_obj("key1", "value1");
    let obj2 = make_obj("key2", "value2");

    let res = QueryResult::new(vec![obj1, obj2]);

    let expected =
        serde_json::to_string(&json!({"data":{"key1": "value1", "key2": "value2"}})).unwrap();
    let actual = serde_json::to_string(&res).unwrap();
    assert_eq!(expected, actual)
}
