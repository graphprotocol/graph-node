use graphql_parser;

use graph::components::server::query::GraphQLServerError;
use graph::prelude::*;

use hyper::body::Bytes;
use serde_json;

/// Future for a query parsed from an HTTP request.
pub struct IndexNodeRequest {
    body: Bytes,
}

impl IndexNodeRequest {
    /// Creates a new IndexNodeRequest future based on an HTTP request and a result sender.
    pub fn new(body: Bytes) -> Self {
        IndexNodeRequest { body }
    }
}

impl Future for IndexNodeRequest {
    type Item = Query;
    type Error = GraphQLServerError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // Parse request body as JSON
        let json: serde_json::Value = serde_json::from_slice(&self.body)
            .map_err(|e| GraphQLServerError::ClientError(format!("{}", e)))?;

        // Ensure the JSON data is an object
        let obj = json.as_object().ok_or_else(|| {
            GraphQLServerError::ClientError(String::from("IndexNodeRequest data is not an object"))
        })?;

        // Ensure the JSON data has a "query" field
        let query_value = obj.get("query").ok_or_else(|| {
            GraphQLServerError::ClientError(String::from(
                "The \"query\" field is missing in request data",
            ))
        })?;

        // Ensure the "query" field is a string
        let query_string = query_value.as_str().ok_or_else(|| {
            GraphQLServerError::ClientError(String::from("The\"query\" field is not a string"))
        })?;

        // Parse the "query" field of the JSON body
        let document = graphql_parser::parse_query(query_string)
            .map_err(|e| GraphQLServerError::from(QueryError::ParseError(Arc::new(e.into()))))?
            .into_static();

        // Parse the "variables" field of the JSON body, if present
        let variables = match obj.get("variables") {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(variables @ serde_json::Value::Object(_)) => {
                serde_json::from_value(variables.clone())
                    .map_err(|e| GraphQLServerError::ClientError(e.to_string()))
                    .map(Some)
            }
            _ => Err(GraphQLServerError::ClientError(
                "Invalid query variables provided".to_string(),
            )),
        }?;

        Ok(Async::Ready(Query::new(document, variables)))
    }
}

#[cfg(test)]
mod tests {
    use graphql_parser;
    use hyper;
    use std::collections::{BTreeMap, HashMap};

    use graph::prelude::*;

    use super::IndexNodeRequest;

    #[test]
    fn rejects_invalid_json() {
        let request = IndexNodeRequest::new(hyper::body::Bytes::from("!@#)%"));
        request.wait().expect_err("Should reject invalid JSON");
    }

    #[test]
    fn rejects_json_without_query_field() {
        let request = IndexNodeRequest::new(hyper::body::Bytes::from("{}"));
        request
            .wait()
            .expect_err("Should reject JSON without query field");
    }

    #[test]
    fn rejects_json_with_non_string_query_field() {
        let request = IndexNodeRequest::new(hyper::body::Bytes::from("{\"query\": 5}"));
        request
            .wait()
            .expect_err("Should reject JSON with a non-string query field");
    }

    #[test]
    fn rejects_broken_queries() {
        let request = IndexNodeRequest::new(hyper::body::Bytes::from("{\"query\": \"foo\"}"));
        request.wait().expect_err("Should reject broken queries");
    }

    #[test]
    fn accepts_valid_queries() {
        let request = IndexNodeRequest::new(hyper::body::Bytes::from(
            "{\"query\": \"{ user { name } }\"}",
        ));
        let query = request.wait().expect("Should accept valid queries");
        assert_eq!(
            query.document,
            graphql_parser::parse_query("{ user { name } }")
                .unwrap()
                .into_static()
        );
    }

    #[test]
    fn accepts_null_variables() {
        let request = IndexNodeRequest::new(hyper::body::Bytes::from(
            "\
                 {\
                 \"query\": \"{ user { name } }\", \
                 \"variables\": null \
                 }",
        ));
        let query = request.wait().expect("Should accept null variables");

        let expected_query = graphql_parser::parse_query("{ user { name } }")
            .unwrap()
            .into_static();
        assert_eq!(query.document, expected_query);
        assert_eq!(query.variables, None);
    }

    #[test]
    fn rejects_non_map_variables() {
        let request = IndexNodeRequest::new(hyper::body::Bytes::from(
            "\
                 {\
                 \"query\": \"{ user { name } }\", \
                 \"variables\": 5 \
                 }",
        ));
        request.wait().expect_err("Should reject non-map variables");
    }

    #[test]
    fn parses_variables() {
        let request = IndexNodeRequest::new(hyper::body::Bytes::from(
            "\
                 {\
                 \"query\": \"{ user { name } }\", \
                 \"variables\": { \
                 \"string\": \"s\", \"map\": {\"k\": \"v\"}, \"int\": 5 \
                 } \
                 }",
        ));
        let query = request.wait().expect("Should accept valid queries");

        let expected_query = graphql_parser::parse_query("{ user { name } }")
            .unwrap()
            .into_static();
        let expected_variables = QueryVariables::new(HashMap::from_iter(
            vec![
                (String::from("string"), q::Value::String(String::from("s"))),
                (
                    String::from("map"),
                    q::Value::Object(BTreeMap::from_iter(
                        vec![(String::from("k"), q::Value::String(String::from("v")))].into_iter(),
                    )),
                ),
                (String::from("int"), q::Value::Int(q::Number::from(5))),
            ]
            .into_iter(),
        ));

        assert_eq!(query.document, expected_query);
        assert_eq!(query.variables, Some(expected_variables));
    }
}
