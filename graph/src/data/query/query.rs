use graphql_parser::query as q;
use serde::de::Deserializer;
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crate::data::schema::Schema;

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

/// A GraphQL query as submitted by a client, either directly or through a subscription.
#[derive(Clone, Debug)]
pub struct Query {
    pub schema: Arc<Schema>,
    pub document: q::Document,
    pub variables: Option<QueryVariables>,
}
