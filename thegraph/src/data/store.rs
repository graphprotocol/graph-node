use graphql_parser::query;
use std::collections::{BTreeMap, HashMap};
use std::ops::{Deref, DerefMut};

/// An entity attribute name is represented as a string.
pub type Attribute = String;

/// An attribute value is represented as an enum with variants for all supported value types.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    String(String),
}

impl Into<query::Value> for Value {
    fn into(self) -> query::Value {
        match self {
            Value::String(s) => query::Value::String(s.to_string()),
        }
    }
}

impl From<query::Value> for Value {
    fn from(value: query::Value) -> Value {
        match value {
            query::Value::String(s) => Value::String(s),
            _ => unimplemented!(),
        }
    }
}

impl<'a> From<&'a query::Value> for Value {
    fn from(value: &'a query::Value) -> Value {
        match value {
            query::Value::String(s) => Value::String(s.to_owned()),
            _ => unimplemented!(),
        }
    }
}

/// An entity is represented as a map of attribute names to values.
#[derive(Clone, Debug, PartialEq)]
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
