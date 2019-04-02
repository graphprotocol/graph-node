use ethabi::Address;
use failure::Error;
use serde::de;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::iter::FromIterator;
use std::ops::{Deref, DerefMut};
use std::str::FromStr;

use crate::data::subgraph::SubgraphDeploymentId;
use crate::prelude::QueryExecutionError;
use graphql_parser::query;
use graphql_parser::schema;

/// Custom scalars in GraphQL.
pub mod scalar;

/// A pair of subgraph ID and entity type name.
pub type SubgraphEntityPair = (SubgraphDeploymentId, String);

/// Information about a subgraph version entity used to reconcile subgraph deployment assignments.
#[derive(Clone, Debug)]
pub struct SubgraphVersionSummary {
    pub id: String,
    pub subgraph_id: String,
    pub deployment_id: SubgraphDeploymentId,
    pub pending: bool,
    pub current: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeId(String);

impl NodeId {
    pub fn new(s: impl Into<String>) -> Result<Self, ()> {
        let s = s.into();

        // Enforce length limit
        if s.len() > 31 {
            return Err(());
        }

        // Check that the ID contains only allowed characters.
        // Note: these restrictions are relied upon to prevent SQL injection
        if !s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(());
        }

        Ok(NodeId(s))
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<'de> de::Deserialize<'de> for NodeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s: String = de::Deserialize::deserialize(deserializer)?;
        NodeId::new(s.clone())
            .map_err(|()| de::Error::invalid_value(de::Unexpected::Str(&s), &"valid node ID"))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "type")]
pub enum AssignmentEvent {
    Add {
        subgraph_id: SubgraphDeploymentId,
        node_id: NodeId,
    },
    Remove {
        subgraph_id: SubgraphDeploymentId,
        node_id: NodeId,
    },
}

impl AssignmentEvent {
    pub fn node_id(&self) -> &NodeId {
        match self {
            AssignmentEvent::Add { node_id, .. } => node_id,
            AssignmentEvent::Remove { node_id, .. } => node_id,
        }
    }
}

/// An entity attribute name is represented as a string.
pub type Attribute = String;

pub const ID: &str = "ID";
pub const BYTES_SCALAR: &str = "Bytes";
pub const BIG_INT_SCALAR: &str = "BigInt";
pub const BIG_DECIMAL_SCALAR: &str = "BigDecimal";

#[derive(Clone, Debug, PartialEq)]
pub enum ValueType {
    Boolean,
    BigInt,
    Bytes,
    BigDecimal,
    ID,
    Int,
    String,
    List,
}

impl FromStr for ValueType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Boolean" => Ok(ValueType::Boolean),
            "BigInt" => Ok(ValueType::BigInt),
            "Bytes" => Ok(ValueType::Bytes),
            "BigDecimal" => Ok(ValueType::BigDecimal),
            "ID" => Ok(ValueType::ID),
            "Int" => Ok(ValueType::Int),
            "String" => Ok(ValueType::String),
            "List" => Ok(ValueType::List),
            s => Err(format_err!("Type not available in this context: {}", s)),
        }
    }
}

/// An attribute value is represented as an enum with variants for all supported value types.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(tag = "type", content = "data")]
pub enum Value {
    String(String),
    Int(i32),
    BigDecimal(scalar::BigDecimal),
    Bool(bool),
    List(Vec<Value>),
    Null,
    Bytes(scalar::Bytes),
    BigInt(scalar::BigInt),
}

impl Value {
    pub fn from_query_value(
        value: &query::Value,
        ty: &schema::Type,
    ) -> Result<Value, QueryExecutionError> {
        use self::schema::Type::{ListType, NamedType, NonNullType};

        Ok(match (value, ty) {
            // When dealing with non-null types, use the inner type to convert the value
            (value, NonNullType(t)) => Value::from_query_value(value, t)?,

            (query::Value::List(values), ListType(ty)) => Value::List(
                values
                    .iter()
                    .map(|value| Self::from_query_value(value, ty))
                    .collect::<Result<Vec<_>, _>>()?,
            ),

            (query::Value::List(values), NamedType(n)) => Value::List(
                values
                    .iter()
                    .map(|value| Self::from_query_value(value, &NamedType(n.to_string())))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            (query::Value::Enum(e), NamedType(_)) => Value::String(e.clone()),
            (query::Value::String(s), NamedType(n)) => {
                // Check if `ty` is a custom scalar type, otherwise assume it's
                // just a string.
                match n.as_str() {
                    BYTES_SCALAR => Value::Bytes(scalar::Bytes::from_str(s)?),
                    BIG_INT_SCALAR => Value::BigInt(scalar::BigInt::from_str(s)?),
                    BIG_DECIMAL_SCALAR => Value::BigDecimal(scalar::BigDecimal::from_str(s)?),
                    _ => Value::String(s.clone()),
                }
            }
            (query::Value::Int(i), _) => Value::Int(
                i.to_owned()
                    .as_i64()
                    .ok_or_else(|| QueryExecutionError::NamedTypeError("Int".to_string()))?
                    as i32,
            ),
            (query::Value::Boolean(b), _) => Value::Bool(b.to_owned()),
            (query::Value::Null, _) => Value::Null,
            _ => {
                return Err(QueryExecutionError::AttributeTypeError(
                    value.to_string(),
                    ty.to_string(),
                ));
            }
        })
    }

    pub fn as_string(self) -> Option<String> {
        if let Value::String(s) = self {
            Some(s)
        } else {
            None
        }
    }

    pub fn as_int(self) -> Option<i32> {
        if let Value::Int(i) = self {
            Some(i)
        } else {
            None
        }
    }

    pub fn as_big_decimal(self) -> Option<scalar::BigDecimal> {
        if let Value::BigDecimal(d) = self {
            Some(d)
        } else {
            None
        }
    }

    pub fn as_bool(self) -> Option<bool> {
        if let Value::Bool(b) = self {
            Some(b)
        } else {
            None
        }
    }

    pub fn as_list(self) -> Option<Vec<Value>> {
        if let Value::List(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn as_bytes(self) -> Option<scalar::Bytes> {
        if let Value::Bytes(b) = self {
            Some(b)
        } else {
            None
        }
    }

    pub fn as_bigint(self) -> Option<scalar::BigInt> {
        if let Value::BigInt(b) = self {
            Some(b)
        } else {
            None
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Value::String(s) => s.to_string(),
                Value::Int(i) => i.to_string(),
                Value::BigDecimal(d) => d.to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Null => "null".to_string(),
                Value::List(ref values) => values
                    .into_iter()
                    .map(|value| format!("{}", value))
                    .collect(),
                Value::Bytes(ref bytes) => bytes.to_string(),
                Value::BigInt(ref number) => number.to_string(),
            }
        )
    }
}

impl From<Value> for query::Value {
    fn from(value: Value) -> Self {
        match value {
            Value::String(s) => query::Value::String(s.to_string()),
            Value::Int(i) => query::Value::Int(query::Number::from(i)),
            Value::BigDecimal(d) => query::Value::String(d.to_string()),
            Value::Bool(b) => query::Value::Boolean(b),
            Value::Null => query::Value::Null,
            Value::List(values) => {
                query::Value::List(values.into_iter().map(|value| value.into()).collect())
            }
            Value::Bytes(bytes) => query::Value::String(bytes.to_string()),
            Value::BigInt(number) => query::Value::String(number.to_string()),
        }
    }
}

impl<'a> From<&'a str> for Value {
    fn from(value: &'a str) -> Value {
        Value::String(value.to_owned())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Value {
        Value::String(value)
    }
}

impl<'a> From<&'a String> for Value {
    fn from(value: &'a String) -> Value {
        Value::String(value.clone())
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Value {
        Value::Bool(value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Value {
        Value::Int(value)
    }
}

impl From<scalar::BigDecimal> for Value {
    fn from(value: scalar::BigDecimal) -> Value {
        Value::BigDecimal(value)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Value {
        Value::BigInt(value.into())
    }
}

impl From<Vec<Value>> for Value {
    fn from(list: Vec<Value>) -> Value {
        Value::List(list)
    }
}

impl From<Address> for Value {
    fn from(address: Address) -> Value {
        Value::Bytes(scalar::Bytes::from(address.as_ref()))
    }
}

impl<T> From<Option<T>> for Value
where
    Value: From<T>,
{
    fn from(x: Option<T>) -> Value {
        match x {
            Some(x) => x.into(),
            None => Value::Null,
        }
    }
}

/// An entity is represented as a map of attribute names to values.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct Entity(HashMap<Attribute, Value>);
impl Entity {
    /// Creates a new entity with no attributes set.
    pub fn new() -> Self {
        Default::default()
    }

    /// Try to get this entity's ID
    pub fn id(&self) -> Result<String, Error> {
        match self.get("id") {
            None => Err(format_err!("Entity is missing an `id` attribute")),
            Some(Value::String(s)) => Ok(s.to_owned()),
            _ => Err(format_err!("Entity has non-string `id` attribute")),
        }
    }

    /// Convenience method to save having to `.into()` the arguments.
    pub fn set(&mut self, name: impl Into<Attribute>, value: impl Into<Value>) -> Option<Value> {
        self.insert(name.into(), value.into())
    }

    /// Merges an entity update `update` into this entity.
    ///
    /// If a key exists in both entities, the value from `update` is chosen.
    /// If a key only exists on one entity, the value from that entity is chosen.
    /// If a key is set to `Value::Null` in `update`, the key/value pair is removed.
    pub fn merge(&mut self, update: Entity) {
        for (key, value) in update.0.into_iter() {
            match value {
                Value::Null => self.remove(&key),
                _ => self.insert(key, value),
            };
        }
    }
}

impl Deref for Entity {
    type Target = HashMap<Attribute, Value>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Entity {
    fn deref_mut(&mut self) -> &mut HashMap<Attribute, Value> {
        &mut self.0
    }
}

impl Into<query::Value> for Entity {
    fn into(self) -> query::Value {
        let mut fields = BTreeMap::new();
        for (attr, value) in self.iter() {
            fields.insert(attr.to_string(), value.clone().into());
        }
        query::Value::Object(fields)
    }
}

impl From<HashMap<Attribute, Value>> for Entity {
    fn from(m: HashMap<Attribute, Value>) -> Entity {
        Entity(m)
    }
}

impl<'a> From<Vec<(&'a str, Value)>> for Entity {
    fn from(entries: Vec<(&'a str, Value)>) -> Entity {
        Entity::from(HashMap::from_iter(
            entries.into_iter().map(|(k, v)| (String::from(k), v)),
        ))
    }
}

#[test]
fn value_bytes() {
    let graphql_value = query::Value::String("0x8f494c66afc1d3f8ac1b45df21f02a46".to_owned());
    let ty = query::Type::NamedType(BYTES_SCALAR.to_owned());
    let from_query = Value::from_query_value(&graphql_value, &ty).unwrap();
    assert_eq!(
        from_query,
        Value::Bytes(scalar::Bytes::from(
            &[143, 73, 76, 102, 175, 193, 211, 248, 172, 27, 69, 223, 33, 240, 42, 70][..]
        ))
    );
    assert_eq!(query::Value::from(from_query), graphql_value);
}

#[test]
fn value_bigint() {
    let big_num = "340282366920938463463374607431768211456";
    let graphql_value = query::Value::String(big_num.to_owned());
    let ty = query::Type::NamedType(BIG_INT_SCALAR.to_owned());
    let from_query = Value::from_query_value(&graphql_value, &ty).unwrap();
    assert_eq!(
        from_query,
        Value::BigInt(FromStr::from_str(big_num).unwrap())
    );
    assert_eq!(query::Value::from(from_query), graphql_value);
}
