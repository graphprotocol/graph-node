use prelude::QueryExecutionError;
use graphql_parser::query;
use graphql_parser::schema;

use std::collections::{BTreeMap, HashMap};
use std::iter::FromIterator;
use std::ops::{Deref, DerefMut};
use std::str::FromStr;

/// Custom scalars in GraphQL.
pub mod scalar;

/// An entity attribute name is represented as a string.
pub type Attribute = String;

pub const ID: &str = "ID";
pub const BYTES_SCALAR: &str = "Bytes";
pub const BIG_INT_SCALAR: &str = "BigInt";

/// An attribute value is represented as an enum with variants for all supported value types.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(untagged)]
pub enum Value {
    String(String),
    Int(i32),
    Float(f32),
    Bool(bool),
    List(Vec<Value>),
    Null,
    Bytes(scalar::Bytes),
    BigInt(scalar::BigInt),
}

impl Value {
    pub fn from_query_value(value: &query::Value, ty: &schema::Type) -> Result<Value, QueryExecutionError>{
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
            (query::Value::Enum(e), NamedType(n)) => {
                // Check if `ty` is a custom scalar type, otherwise assume it's
                // just a string.
                match n.as_str() {
                    BYTES_SCALAR => {
                        Value::Bytes(scalar::Bytes::from_str(e)?)
                    }
                    BIG_INT_SCALAR => {
                        Value::BigInt(scalar::BigInt::from_str(e)?)
                    }
                    _ => Value::String(e.clone()),
                }
            }
            (query::Value::String(s), NamedType(n)) => {
                // Check if `ty` is a custom scalar type, otherwise assume it's
                // just a string.
                match n.as_str() {
                    BYTES_SCALAR => {
                        Value::Bytes(scalar::Bytes::from_str(s)?)
                    }
                    BIG_INT_SCALAR => {
                        Value::BigInt(scalar::BigInt::from_str(s)?)
                    }
                    _ => Value::String(s.clone()),
                }
            }
            (query::Value::Int(i), _) => Value::Int(
                i.to_owned()
                    .as_i64()
                    .ok_or(QueryExecutionError::NamedTypeError("Int".to_string()))?
                    as i32,
            ),
            (query::Value::Float(f), _) => Value::Float(f.to_owned() as f32),
            (query::Value::Boolean(b), _) => Value::Bool(b.to_owned()),
            (query::Value::Null, _) => Value::Null,
            _ => Value::Null,
        })
    }
}

impl From<Value> for query::Value {
    fn from(value: Value) -> Self {
        match value {
            Value::String(s) => query::Value::String(s.to_string()),
            Value::Int(i) => query::Value::Int(query::Number::from(i)),
            Value::Float(f) => query::Value::Float(f.into()),
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

/// An entity is represented as a map of attribute names to values.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Entity(HashMap<Attribute, Value>);
impl Entity {
    /// Creates a new entity with no attributes set.
    pub fn new() -> Self {
        Entity(HashMap::new())
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
