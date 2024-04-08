use super::error::{QueryError, QueryExecutionError};
use super::trace::{HttpTrace, TRACE_NONE};
use crate::cheap_clone::CheapClone;
use crate::components::server::query::ServerResponse;
use crate::data::value::Object;
use crate::derive::CacheWeight;
use crate::prelude::{r, CacheWeight, DeploymentHash};
use http_body_util::Full;
use hyper::header::{
    ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
    CONTENT_TYPE,
};
use hyper::Response;
use serde::ser::*;
use serde::Serialize;
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::Instant;

use super::{CacheStatus, Trace};

fn serialize_data<S>(data: &Option<Data>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut ser = serializer.serialize_map(None)?;

    // Unwrap: data is only serialized if it is `Some`.
    for (k, v) in data.as_ref().unwrap() {
        ser.serialize_entry(k, v)?;
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
            ser.serialize_entry(k, v)?;
        }
    }
    ser.end()
}

pub type Data = Object;

#[derive(Debug)]
/// A collection of query results that is serialized as a single result.
pub struct QueryResults {
    results: Vec<Arc<QueryResult>>,
    pub trace: Trace,
}

impl QueryResults {
    pub fn empty(trace: Trace) -> Self {
        QueryResults {
            results: Vec::new(),
            trace,
        }
    }

    pub fn first(&self) -> Option<&Arc<QueryResult>> {
        self.results.first()
    }

    pub fn has_errors(&self) -> bool {
        self.results.iter().any(|result| result.has_errors())
    }

    pub fn not_found(&self) -> bool {
        self.results.iter().any(|result| result.not_found())
    }

    pub fn deployment_hash(&self) -> Option<&DeploymentHash> {
        self.results
            .iter()
            .filter_map(|result| result.deployment.as_ref())
            .next()
    }

    pub fn errors(&self) -> Vec<QueryError> {
        self.results.iter().flat_map(|r| r.errors.clone()).collect()
    }

    pub fn is_attestable(&self) -> bool {
        self.results.iter().all(|r| r.is_attestable())
    }
}

impl Serialize for QueryResults {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let start = Instant::now();
        let mut len = 0;
        let has_data = self.results.iter().any(|r| r.has_data());
        if has_data {
            len += 1;
        }
        let has_errors = self.results.iter().any(|r| r.has_errors());
        if has_errors {
            len += 1;
        }
        len += 1;
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
                    for err in self.0.results.iter().flat_map(|r| &r.errors) {
                        seq.serialize_element(err)?;
                    }
                    seq.end()
                }
            }

            state.serialize_field("errors", &SerError(self))?;
        }

        if !self.trace.is_none() {
            let http = HttpTrace::new(start.elapsed(), self.results.weight());
            state.serialize_field("trace", &self.trace)?;
            state.serialize_field("http", &http)?;
        }
        state.end()
    }
}

impl From<Data> for QueryResults {
    fn from(x: Data) -> Self {
        QueryResults {
            results: vec![Arc::new(x.into())],
            trace: Trace::None,
        }
    }
}

impl From<QueryResult> for QueryResults {
    fn from(x: QueryResult) -> Self {
        QueryResults {
            results: vec![Arc::new(x)],
            trace: Trace::None,
        }
    }
}

impl From<Arc<QueryResult>> for QueryResults {
    fn from(x: Arc<QueryResult>) -> Self {
        QueryResults {
            results: vec![x],
            trace: Trace::None,
        }
    }
}

impl From<QueryExecutionError> for QueryResults {
    fn from(x: QueryExecutionError) -> Self {
        QueryResults {
            results: vec![Arc::new(x.into())],
            trace: Trace::None,
        }
    }
}

impl From<Vec<QueryExecutionError>> for QueryResults {
    fn from(x: Vec<QueryExecutionError>) -> Self {
        QueryResults {
            results: vec![Arc::new(x.into())],
            trace: Trace::None,
        }
    }
}

impl QueryResults {
    pub fn append(&mut self, other: Arc<QueryResult>, cache_status: CacheStatus) {
        let trace = other.trace.cheap_clone();
        self.trace.append(trace, cache_status);
        self.results.push(other);
    }

    pub fn as_http_response(&self) -> ServerResponse {
        let json = serde_json::to_string(&self).unwrap();
        let attestable = self.results.iter().all(|r| r.is_attestable());
        Response::builder()
            .status(200)
            .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
            .header(CONTENT_TYPE, "application/json")
            .header(ACCESS_CONTROL_ALLOW_HEADERS, "Content-Type, User-Agent")
            .header(ACCESS_CONTROL_ALLOW_METHODS, "GET, OPTIONS, POST")
            .header(CONTENT_TYPE, "application/json")
            .header("Graph-Attestable", attestable.to_string())
            .body(Full::from(json))
            .unwrap()
    }
}

/// The result of running a query, if successful.
#[derive(Debug, CacheWeight, Default, Serialize)]
pub struct QueryResult {
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_data"
    )]
    data: Option<Data>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    errors: Vec<QueryError>,
    #[serde(skip_serializing)]
    pub deployment: Option<DeploymentHash>,
    #[serde(skip_serializing)]
    pub trace: Arc<Trace>,
}

impl QueryResult {
    pub fn new(data: Data) -> Self {
        QueryResult {
            data: Some(data),
            errors: Vec::new(),
            deployment: None,
            trace: TRACE_NONE.cheap_clone(),
        }
    }

    /// This is really `clone`, but we do not want to implement `Clone`;
    /// this is only meant for test purposes and should not be used in production
    /// code since cloning query results can be very expensive
    pub fn duplicate(&self) -> Self {
        Self {
            data: self.data.clone(),
            errors: self.errors.clone(),
            deployment: self.deployment.clone(),
            trace: TRACE_NONE.cheap_clone(),
        }
    }

    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    pub fn not_found(&self) -> bool {
        self.errors.iter().any(|e| {
            matches!(
                e,
                QueryError::ExecutionError(QueryExecutionError::DeploymentNotFound(_))
            )
        })
    }

    pub fn has_data(&self) -> bool {
        self.data.is_some()
    }

    pub fn is_attestable(&self) -> bool {
        self.errors.iter().all(|err| err.is_attestable())
    }

    pub fn to_result(self) -> Result<Option<r::Value>, Vec<QueryError>> {
        if self.has_errors() {
            Err(self.errors)
        } else {
            Ok(self.data.map(r::Value::Object))
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

    pub fn data(&self) -> Option<&Data> {
        self.data.as_ref()
    }
}

impl From<QueryExecutionError> for QueryResult {
    fn from(e: QueryExecutionError) -> Self {
        QueryResult {
            data: None,
            errors: vec![e.into()],
            deployment: None,
            trace: TRACE_NONE.cheap_clone(),
        }
    }
}

impl From<QueryError> for QueryResult {
    fn from(e: QueryError) -> Self {
        QueryResult {
            data: None,
            errors: vec![e],
            deployment: None,
            trace: TRACE_NONE.cheap_clone(),
        }
    }
}

impl From<Vec<QueryExecutionError>> for QueryResult {
    fn from(e: Vec<QueryExecutionError>) -> Self {
        QueryResult {
            data: None,
            errors: e.into_iter().map(QueryError::from).collect(),
            deployment: None,
            trace: TRACE_NONE.cheap_clone(),
        }
    }
}

impl From<Object> for QueryResult {
    fn from(val: Object) -> Self {
        QueryResult::new(val)
    }
}

impl From<(Object, Trace)> for QueryResult {
    fn from((val, trace): (Object, Trace)) -> Self {
        let mut res = QueryResult::new(val);
        res.trace = Arc::new(trace);
        res
    }
}

impl TryFrom<r::Value> for QueryResult {
    type Error = &'static str;

    fn try_from(value: r::Value) -> Result<Self, Self::Error> {
        match value {
            r::Value::Object(map) => Ok(QueryResult::from(map)),
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

// Check that when we serialize a `QueryResult` with multiple entries
// in `data` it appears as if we serialized one big map
#[test]
fn multiple_data_items() {
    use serde_json::json;

    fn make_obj(key: &str, value: &str) -> Arc<QueryResult> {
        let obj = Object::from_iter([(
            crate::data::value::Word::from(key),
            r::Value::String(value.to_owned()),
        )]);
        Arc::new(obj.into())
    }

    let obj1 = make_obj("key1", "value1");
    let obj2 = make_obj("key2", "value2");

    let trace = Trace::None;
    let mut res = QueryResults::empty(trace);
    res.append(obj1, CacheStatus::default());
    res.append(obj2, CacheStatus::default());

    let expected =
        serde_json::to_string(&json!({"data":{"key1": "value1", "key2": "value2"}})).unwrap();
    let actual = serde_json::to_string(&res).unwrap();
    assert_eq!(expected, actual)
}
