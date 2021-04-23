use serde::de::Deserializer;
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crate::{
    data::graphql::shape_hash::shape_hash,
    prelude::{q, DeploymentHash, SubgraphName},
};

fn deserialize_number<'de, D>(deserializer: D) -> Result<q::Number, D::Error>
where
    D: Deserializer<'de>,
{
    let i: i32 = Deserialize::deserialize(deserializer)?;
    Ok(q::Number::from(i))
}

fn deserialize_list<'de, D>(deserializer: D) -> Result<Vec<q::Value>, D::Error>
where
    D: Deserializer<'de>,
{
    let values: Vec<DeserializableGraphQlValue> = Deserialize::deserialize(deserializer)?;
    Ok(values.into_iter().map(|v| v.0).collect())
}

fn deserialize_object<'de, D>(deserializer: D) -> Result<BTreeMap<String, q::Value>, D::Error>
where
    D: Deserializer<'de>,
{
    let pairs: BTreeMap<String, DeserializableGraphQlValue> =
        Deserialize::deserialize(deserializer)?;
    Ok(pairs.into_iter().map(|(k, v)| (k, v.0)).collect())
}

#[derive(Deserialize)]
#[serde(untagged, remote = "q::Value")]
enum GraphQLValue {
    #[serde(deserialize_with = "deserialize_number")]
    Int(q::Number),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
    Enum(String),
    #[serde(deserialize_with = "deserialize_list")]
    List(Vec<q::Value>),
    #[serde(deserialize_with = "deserialize_object")]
    Object(BTreeMap<String, q::Value>),
}

/// Variable value for a GraphQL query.
#[derive(Clone, Debug, Deserialize)]
pub struct DeserializableGraphQlValue(#[serde(with = "GraphQLValue")] q::Value);

fn deserialize_variables<'de, D>(deserializer: D) -> Result<HashMap<String, q::Value>, D::Error>
where
    D: Deserializer<'de>,
{
    let pairs: BTreeMap<String, DeserializableGraphQlValue> =
        Deserialize::deserialize(deserializer)?;
    Ok(pairs.into_iter().map(|(k, v)| (k, v.0)).collect())
}

/// Variable values for a GraphQL query.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
pub struct QueryVariables(
    #[serde(deserialize_with = "deserialize_variables")] HashMap<String, q::Value>,
);

impl QueryVariables {
    pub fn new(variables: HashMap<String, q::Value>) -> Self {
        QueryVariables(variables)
    }
}

impl Deref for QueryVariables {
    type Target = HashMap<String, q::Value>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for QueryVariables {
    fn deref_mut(&mut self) -> &mut HashMap<String, q::Value> {
        &mut self.0
    }
}

impl serde::ser::Serialize for QueryVariables {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        use crate::data::graphql::SerializableValue;
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (k, v) in &self.0 {
            map.serialize_entry(k, &SerializableValue(v))?;
        }
        map.end()
    }
}

#[derive(Clone, Debug)]
pub enum QueryTarget {
    Name(SubgraphName),
    Deployment(DeploymentHash),
}

impl From<DeploymentHash> for QueryTarget {
    fn from(id: DeploymentHash) -> Self {
        Self::Deployment(id)
    }
}

impl From<SubgraphName> for QueryTarget {
    fn from(name: SubgraphName) -> Self {
        QueryTarget::Name(name)
    }
}

/// A GraphQL query as submitted by a client, either directly or through a subscription.
#[derive(Clone, Debug)]
pub struct Query {
    pub document: q::Document,
    pub variables: Option<QueryVariables>,
    pub shape_hash: u64,
    pub query_text: Arc<String>,
    pub variables_text: Arc<String>,
    _force_use_of_new: (),
}

impl Query {
    pub fn new(document: q::Document, variables: Option<QueryVariables>) -> Self {
        let shape_hash = shape_hash(&document);

        let (query_text, variables_text) = if *crate::log::LOG_GQL_TIMING {
            (
                document
                    .format(&graphql_parser::Style::default().indent(0))
                    .replace('\n', " "),
                serde_json::to_string(&variables).unwrap_or_default(),
            )
        } else {
            ("(gql logging turned off)".to_owned(), "".to_owned())
        };

        Query {
            document,
            variables,
            shape_hash,
            query_text: Arc::new(query_text),
            variables_text: Arc::new(variables_text),
            _force_use_of_new: (),
        }
    }
}
