use graphql_parser::query;
use std::collections::{BTreeMap, HashMap};
use std::ops::{Deref, DerefMut};

/// An entity attribute name is represented as a string.
pub type Attribute = String;

/// An attribute value is represented as an enum with variants for all supported value types.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub enum Value {
    String(String),
    Int(i32),
    Float(f32),
    Bool(bool),
}

impl Into<query::Value> for Value {
    fn into(self) -> query::Value {
        match self {
            Value::String(s) => query::Value::String(s.to_string()),
            Value::Float(f) => query::Value::Float(f.into()),
            Value::Bool(b) => query::Value::Boolean(b),
            _ => unimplemented!(),
        }
    }
}

impl From<query::Value> for Value {
    fn from(value: query::Value) -> Value {
        match value {
            query::Value::String(s) => Value::String(s),
            query::Value::Int(i) => Value::Int(i.as_i64()
                .expect("Unable to parse graphql_parser::query::Number into i64")
                as i32),
            query::Value::Float(f) => Value::Float(f as f32),
            query::Value::Boolean(b) => Value::Bool(b),
            _ => unimplemented!(),
        }
    }
}

impl<'a> From<&'a query::Value> for Value {
    fn from(value: &'a query::Value) -> Value {
        match value {
            query::Value::String(s) => Value::String(s.to_owned()),
            query::Value::Int(i) => Value::Int(i.to_owned()
                .as_i64()
                .expect("Unable to parse graphql_parser::query::Number into i64")
                as i32),
            query::Value::Float(f) => Value::Float(f.to_owned() as f32),
            query::Value::Boolean(b) => Value::Bool(b.to_owned()),
            _ => unimplemented!(),
        }
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
