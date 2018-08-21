use graphql_parser::query as q;
use std::cmp::PartialEq;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

use super::QueryResult;
use data::schema::Schema;

#[derive(Deserialize)]
#[serde(untagged, remote = "q::Value")]
enum GraphQLValue {
    String(String),
}

/// Variable value for a GraphQL query.
#[derive(Debug, Deserialize)]
pub struct QueryVariableValue(#[serde(with = "GraphQLValue")] q::Value);

impl Deref for QueryVariableValue {
    type Target = q::Value;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for QueryVariableValue {
    fn deref_mut(&mut self) -> &mut q::Value {
        &mut self.0
    }
}

impl PartialEq for QueryVariableValue {
    fn eq(&self, other: &QueryVariableValue) -> bool {
        self.0 == other.0
    }
}

impl<'a> From<&'a str> for QueryVariableValue {
    fn from(s: &'a str) -> Self {
        QueryVariableValue(q::Value::String(s.to_string()))
    }
}

/// Variable values for a GraphQL query.
#[derive(Debug, Deserialize)]
pub struct QueryVariables(HashMap<String, QueryVariableValue>);

impl QueryVariables {
    pub fn new() -> Self {
        QueryVariables(HashMap::new())
    }
}

impl Deref for QueryVariables {
    type Target = HashMap<String, QueryVariableValue>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for QueryVariables {
    fn deref_mut(&mut self) -> &mut HashMap<String, QueryVariableValue> {
        &mut self.0
    }
}

impl PartialEq for QueryVariables {
    fn eq(&self, other: &QueryVariables) -> bool {
        self.0 == other.0
    }
}

/// A GraphQL query as submitted by a client, either directly or through a subscription.
#[derive(Debug)]
pub struct Query {
    pub schema: Schema,
    pub document: q::Document,
    pub variables: Option<QueryVariables>,
}
