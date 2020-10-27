use super::error::{QueryError, QueryExecutionError};
use crate::{data::graphql::SerializableValue, prelude::CacheWeight};
use graphql_parser::query as q;
use serde::ser::*;
use serde::Serialize;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::sync::Arc;

fn serialize_data<S>(data: &Option<q::Value>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    SerializableValue(data.as_ref().unwrap_or(&q::Value::Null)).serialize(serializer)
}

fn serialize_value_map<S>(data: &Vec<Arc<Data>>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    // We only serialize `data` if it is not empty
    assert!(!data.is_empty());
    let mut ser = serializer.serialize_map(None)?;
    for map in data {
        for (k, v) in map.as_ref() {
            ser.serialize_entry(k, &SerializableValue(v))?;
        }
    }
    ser.end()
}

pub type Data = BTreeMap<String, q::Value>;

/// The result of running a query, if successful.
#[derive(Debug, Clone, Serialize)]
pub struct QueryResult {
    #[serde(
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "serialize_value_map"
    )]
    data: Vec<Arc<Data>>,
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

    pub fn new(data: Vec<Arc<Data>>) -> Self {
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
        return !self.errors.is_empty();
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

    /// Combine all the data into one `q::Value`. This method might clone
    /// all of the data in this result
    fn take_data(self) -> Option<q::Value> {
        fn take_or_clone(value: Arc<Data>) -> Data {
            Arc::try_unwrap(value).unwrap_or_else(|value| value.as_ref().clone())
        }

        if self.data.is_empty() {
            None
        } else {
            let res = self.data.into_iter().fold(Data::new(), |mut acc, value| {
                let mut value = take_or_clone(value);
                acc.append(&mut value);
                acc
            });
            Some(q::Value::Object(res))
        }
    }

    pub fn has_data(&self) -> bool {
        !self.data.is_empty()
    }

    /// Return either the data or the errors for this `QueryResult`. The data
    /// will be cloned for any of the entries in `self.data` that have a
    /// reference count greater than 1. If there are errors, the data is ignored.
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

impl From<Data> for QueryResult {
    fn from(val: Data) -> Self {
        QueryResult::from(Arc::new(val))
    }
}

impl From<Arc<Data>> for QueryResult {
    fn from(val: Arc<Data>) -> Self {
        QueryResult::new(vec![val])
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
        // self.data is  a Vev<Arc<Data>>. For cached `QueryResult`, the
        // `Arc` is not shared with anything else, and we calculate the
        // indirect weight as if we had a `Vec<Data>`
        let data_weight = self
            .data
            .iter()
            .map(Arc::as_ref)
            .map(CacheWeight::indirect_weight)
            .sum::<usize>()
            + self.data.capacity() * std::mem::size_of::<Arc<Data>>();

        data_weight + self.errors.indirect_weight() + self.extensions.indirect_weight()
    }
}

// Check that when we serialize a `QueryResult` with multiple entries
// in `data` it appears as if we serialized one big map
#[test]
fn multiple_data_items() {
    use serde_json::json;

    fn make_obj(key: &str, value: &str) -> Arc<Data> {
        let mut map = BTreeMap::new();
        map.insert(key.to_owned(), q::Value::String(value.to_owned()));
        Arc::new(map)
    }

    let obj1 = make_obj("key1", "value1");
    let obj2 = make_obj("key2", "value2");

    let res = QueryResult::new(vec![obj1, obj2]);

    let expected =
        serde_json::to_string(&json!({"data":{"key1": "value1", "key2": "value2"}})).unwrap();
    let actual = serde_json::to_string(&res).unwrap();
    assert_eq!(expected, actual)
}
