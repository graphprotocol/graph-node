use graphql_parser::query;
use graphql_parser::schema;
use hex;
use std::collections::{BTreeMap, HashMap};
use std::iter::FromIterator;
use std::ops::{Deref, DerefMut};

/// An entity attribute name is represented as a string.
pub type Attribute = String;

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
    /// In GraphQL, a hex string prefixed by `0x`.
    Bytes(Box<[u8]>),
}

impl Value {
    pub fn from_query_value(value: &query::Value, ty: &schema::Type) -> Value {
        use self::schema::Type::{ListType, NamedType};

        match (value, ty) {
            (query::Value::String(s), NamedType(n)) if n == "Bytes" => Value::Bytes(
                hex::decode(s.trim_left_matches("0x"))
                    .expect("Value is not a hex string")
                    .into(),
            ),
            (query::Value::String(s), _) => Value::String(s.clone()),
            (query::Value::Int(i), _) => Value::Int(i.to_owned()
                .as_i64()
                .expect("Unable to parse graphql_parser::query::Number into i64")
                as i32),
            (query::Value::Float(f), _) => Value::Float(f.to_owned() as f32),
            (query::Value::Boolean(b), _) => Value::Bool(b.to_owned()),
            (query::Value::List(values), ListType(ty)) => Value::List(
                values
                    .iter()
                    .map(|value| Self::from_query_value(value, ty))
                    .collect(),
            ),
            (query::Value::Null, _) => Value::Null,
            _ => unimplemented!(),
        }
    }
}

impl Into<query::Value> for Value {
    fn into(self) -> query::Value {
        match self {
            Value::String(s) => query::Value::String(s.to_string()),
            Value::Int(i) => query::Value::Int(query::Number::from(i)),
            Value::Float(f) => query::Value::Float(f.into()),
            Value::Bool(b) => query::Value::Boolean(b),
            Value::Null => query::Value::Null,
            Value::List(values) => {
                query::Value::List(values.into_iter().map(|value| value.into()).collect())
            }
            Value::Bytes(bytes) => query::Value::String(format!("0x{}", hex::encode(bytes))),
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
