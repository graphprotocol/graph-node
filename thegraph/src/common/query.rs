use futures::sync::oneshot;
use futures::sync::mpsc::Sender;
use graphql_parser::query;
use serde::ser::*;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::string::FromUtf8Error;

/// A GraphQL query as submitted by a client, either directly or through a subscription.
#[derive(Debug)]
pub struct Query {
    pub document: query::Document,
    pub result_sender: oneshot::Sender<QueryResult>,
}

/// The result of running a query, if successful.
#[derive(Debug, Serialize)]
pub struct QueryResult {
    pub data: Option<HashMap<String, String>>,
    #[serde(skip_serializing)]
    pub errors: Option<Vec<QueryError>>,
}

impl QueryResult {
    pub fn new(data: Option<HashMap<String, String>>) -> Self {
        QueryResult { data, errors: None }
    }
}

/// Error caused while running a [Query](struct.Query.html).
#[derive(Debug)]
pub enum QueryError {
    EncodingError(FromUtf8Error),
    ParseError(query::ParseError),
}

impl From<FromUtf8Error> for QueryError {
    fn from(e: FromUtf8Error) -> Self {
        QueryError::EncodingError(e)
    }
}

impl From<query::ParseError> for QueryError {
    fn from(e: query::ParseError) -> Self {
        QueryError::ParseError(e)
    }
}

impl Error for QueryError {
    fn description(&self) -> &str {
        "Query error"
    }

    fn cause(&self) -> Option<&Error> {
        match self {
            &QueryError::EncodingError(ref e) => Some(e),
            _ => None,
        }
    }
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &QueryError::EncodingError(ref e) => write!(f, "{}", e),
            &QueryError::ParseError(ref e) => write!(f, "{}", e),
        }
    }
}

impl Serialize for QueryError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(1))?;
        let mut msg = format!("{}", self);

        // Serialize parse errors with their location (line, column) to make it easier
        // for users to find where the errors are; this is likely to change as the
        // graphql_parser team makes improvements to their error reporting
        if let &QueryError::ParseError(_) = self {
            // Split the inner message into (first line, rest)
            let inner_msg = msg.replace("query parse error:", "");
            let inner_msg = inner_msg.trim();
            let parts: Vec<&str> = inner_msg.splitn(2, "\n").collect();

            // Find the colon in the first line and split there
            let colon_pos = parts[0].rfind(":").unwrap();
            let (a, b) = parts[0].split_at(colon_pos);

            // Find the line and column numbers and convert them to u32
            let line: u32 = a.matches(char::is_numeric)
                .collect::<String>()
                .parse()
                .unwrap();
            let column: u32 = b.matches(char::is_numeric)
                .collect::<String>()
                .parse()
                .unwrap();

            // Generate the list of locations
            let mut location = HashMap::new();
            location.insert("line", line);
            location.insert("column", column);
            let locations = vec![location];
            map.serialize_entry("locations", &locations)?;

            // Only use the remainder after the location as the error message
            msg = parts[1].to_string();
        }

        map.serialize_entry("message", msg.as_str())?;
        map.end()
    }
}

/// Common trait for query runners that run queries against a [Store](../store/trait.Store.html).
pub trait QueryRunner {
    // Sender to which others can write queries that need to be run.
    fn query_sink(&mut self) -> Sender<Query>;
}
