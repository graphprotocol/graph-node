use crate::prelude::{q, s, CacheWeight};
use serde::ser::{SerializeMap, SerializeSeq, Serializer};
use serde::Serialize;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::iter::FromIterator;

const TOMBSTONE_KEY: &str = "*dead*";

#[derive(Clone, Debug, PartialEq)]
struct Entry {
    key: String,
    value: Value,
}

#[derive(Clone, PartialEq, Default)]
pub struct Object(Vec<Entry>);

impl Object {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0
            .iter()
            .find(|entry| entry.key == key)
            .map(|entry| &entry.value)
    }

    pub fn remove(&mut self, key: &str) -> Option<Value> {
        self.0
            .iter_mut()
            .find(|entry| entry.key == key)
            .map(|entry| {
                entry.key = TOMBSTONE_KEY.to_string();
                std::mem::replace(&mut entry.value, Value::Null)
            })
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Value)> {
        ObjectIter::new(self)
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    pub fn extend(&mut self, other: Object) {
        self.0.extend(other.0)
    }

    pub fn insert(&mut self, key: String, value: Value) -> Option<Value> {
        match self.0.iter_mut().find(|entry| entry.key == key) {
            Some(entry) => Some(std::mem::replace(&mut entry.value, value)),
            None => {
                self.0.push(Entry { key, value });
                None
            }
        }
    }
}

impl FromIterator<(String, Value)> for Object {
    fn from_iter<T: IntoIterator<Item = (String, Value)>>(iter: T) -> Self {
        let mut items: Vec<_> = Vec::new();
        for (key, value) in iter {
            items.push(Entry { key, value })
        }
        Object(items)
    }
}

pub struct ObjectOwningIter {
    iter: std::vec::IntoIter<Entry>,
}

impl Iterator for ObjectOwningIter {
    type Item = (String, Value);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(entry) = self.iter.next() {
            if &entry.key != TOMBSTONE_KEY {
                return Some((entry.key, entry.value));
            }
        }
        None
    }
}

impl IntoIterator for Object {
    type Item = (String, Value);

    type IntoIter = ObjectOwningIter;

    fn into_iter(self) -> Self::IntoIter {
        ObjectOwningIter {
            iter: self.0.into_iter(),
        }
    }
}

pub struct ObjectIter<'a> {
    iter: std::slice::Iter<'a, Entry>,
}

impl<'a> ObjectIter<'a> {
    fn new(object: &'a Object) -> Self {
        Self {
            iter: object.0.as_slice().iter(),
        }
    }
}
impl<'a> Iterator for ObjectIter<'a> {
    type Item = (&'a String, &'a Value);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(entry) = self.iter.next() {
            if entry.key != TOMBSTONE_KEY {
                return Some((&entry.key, &entry.value));
            }
        }
        None
    }
}

impl<'a> IntoIterator for &'a Object {
    type Item = <ObjectIter<'a> as Iterator>::Item;

    type IntoIter = ObjectIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        ObjectIter::new(self)
    }
}

impl CacheWeight for Entry {
    fn indirect_weight(&self) -> usize {
        self.key.indirect_weight() + self.value.indirect_weight()
    }
}

impl CacheWeight for Object {
    fn indirect_weight(&self) -> usize {
        self.0.indirect_weight()
    }
}

impl std::fmt::Debug for Object {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    Null,
    Enum(String),
    List(Vec<Value>),
    Object(Object),
}

impl Value {
    pub fn object(map: BTreeMap<String, Value>) -> Self {
        let items = map
            .into_iter()
            .map(|(key, value)| Entry { key, value })
            .collect();
        Value::Object(Object(items))
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn coerce_enum(self, using_type: &s::EnumType) -> Result<Value, Value> {
        match self {
            Value::Null => Ok(Value::Null),
            Value::String(name) | Value::Enum(name)
                if using_type.values.iter().any(|value| value.name == name) =>
            {
                Ok(Value::Enum(name))
            }
            _ => Err(self),
        }
    }

    pub fn coerce_scalar(self, using_type: &s::ScalarType) -> Result<Value, Value> {
        match (using_type.name.as_str(), self) {
            (_, Value::Null) => Ok(Value::Null),
            ("Boolean", Value::Boolean(b)) => Ok(Value::Boolean(b)),
            ("BigDecimal", Value::Float(f)) => Ok(Value::String(f.to_string())),
            ("BigDecimal", Value::Int(i)) => Ok(Value::String(i.to_string())),
            ("BigDecimal", Value::String(s)) => Ok(Value::String(s)),
            ("Int", Value::Int(num)) => {
                if i32::min_value() as i64 <= num && num <= i32::max_value() as i64 {
                    Ok(Value::Int(num))
                } else {
                    Err(Value::Int(num))
                }
            }
            ("String", Value::String(s)) => Ok(Value::String(s)),
            ("ID", Value::String(s)) => Ok(Value::String(s)),
            ("ID", Value::Int(n)) => Ok(Value::String(n.to_string())),
            ("Bytes", Value::String(s)) => Ok(Value::String(s)),
            ("BigInt", Value::String(s)) => Ok(Value::String(s)),
            ("BigInt", Value::Int(n)) => Ok(Value::String(n.to_string())),
            ("JSONObject", Value::Object(obj)) => Ok(Value::Object(obj)),
            ("Date", Value::String(obj)) => Ok(Value::String(obj)),
            (_, v) => Err(v),
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Value::Int(ref num) => write!(f, "{}", num),
            Value::Float(val) => write!(f, "{}", val),
            Value::String(ref val) => write!(f, "\"{}\"", val.replace('"', "\\\"")),
            Value::Boolean(true) => write!(f, "true"),
            Value::Boolean(false) => write!(f, "false"),
            Value::Null => write!(f, "null"),
            Value::Enum(ref name) => write!(f, "{}", name),
            Value::List(ref items) => {
                write!(f, "[")?;
                if !items.is_empty() {
                    write!(f, "{}", items[0])?;
                    for item in &items[1..] {
                        write!(f, ", {}", item)?;
                    }
                }
                write!(f, "]")
            }
            Value::Object(ref items) => {
                write!(f, "{{")?;
                let mut first = true;
                for (name, value) in items.iter() {
                    if first {
                        first = false;
                    } else {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {}", name, value)?;
                }
                write!(f, "}}")
            }
        }
    }
}

impl CacheWeight for Value {
    fn indirect_weight(&self) -> usize {
        match self {
            Value::Boolean(_) | Value::Int(_) | Value::Null | Value::Float(_) => 0,
            Value::Enum(s) | Value::String(s) => s.indirect_weight(),
            Value::List(l) => l.indirect_weight(),
            Value::Object(o) => o.indirect_weight(),
        }
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Value::Boolean(v) => serializer.serialize_bool(*v),
            Value::Enum(v) => serializer.serialize_str(v),
            Value::Float(v) => serializer.serialize_f64(*v),
            Value::Int(v) => serializer.serialize_i64(*v),
            Value::List(l) => {
                let mut seq = serializer.serialize_seq(Some(l.len()))?;
                for v in l {
                    seq.serialize_element(v)?;
                }
                seq.end()
            }
            Value::Null => serializer.serialize_none(),
            Value::String(s) => serializer.serialize_str(s),
            Value::Object(o) => {
                let mut map = serializer.serialize_map(Some(o.len()))?;
                for (k, v) in o {
                    map.serialize_entry(k, v)?;
                }
                map.end()
            }
        }
    }
}

impl TryFrom<q::Value> for Value {
    type Error = q::Value;

    fn try_from(value: q::Value) -> Result<Self, Self::Error> {
        match value {
            q::Value::Variable(_) => Err(value),
            q::Value::Int(ref num) => match num.as_i64() {
                Some(i) => Ok(Value::Int(i)),
                None => Err(value),
            },
            q::Value::Float(f) => Ok(Value::Float(f)),
            q::Value::String(s) => Ok(Value::String(s)),
            q::Value::Boolean(b) => Ok(Value::Boolean(b)),
            q::Value::Null => Ok(Value::Null),
            q::Value::Enum(s) => Ok(Value::Enum(s)),
            q::Value::List(vals) => {
                let vals: Vec<_> = vals
                    .into_iter()
                    .map(Value::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Value::List(vals))
            }
            q::Value::Object(map) => {
                let mut rmap = BTreeMap::new();
                for (key, value) in map.into_iter() {
                    let value = Value::try_from(value)?;
                    rmap.insert(key, value);
                }
                Ok(Value::object(rmap))
            }
        }
    }
}

impl From<serde_json::Value> for Value {
    fn from(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Boolean(b),
            serde_json::Value::Number(n) => match n.as_i64() {
                Some(i) => Value::Int(i),
                None => Value::Float(n.as_f64().unwrap()),
            },
            serde_json::Value::String(s) => Value::String(s),
            serde_json::Value::Array(vals) => {
                let vals: Vec<_> = vals.into_iter().map(Value::from).collect::<Vec<_>>();
                Value::List(vals)
            }
            serde_json::Value::Object(map) => {
                let mut rmap = Object::new();
                for (key, value) in map.into_iter() {
                    let value = Value::from(value);
                    rmap.insert(key, value);
                }
                Value::Object(rmap)
            }
        }
    }
}

impl From<Value> for q::Value {
    fn from(value: Value) -> Self {
        match value {
            Value::Int(i) => q::Value::Int((i as i32).into()),
            Value::Float(f) => q::Value::Float(f),
            Value::String(s) => q::Value::String(s),
            Value::Boolean(b) => q::Value::Boolean(b),
            Value::Null => q::Value::Null,
            Value::Enum(s) => q::Value::Enum(s),
            Value::List(vals) => {
                let vals: Vec<q::Value> = vals.into_iter().map(q::Value::from).collect();
                q::Value::List(vals)
            }
            Value::Object(map) => {
                let mut rmap = BTreeMap::new();
                for (key, value) in map.into_iter() {
                    let value = q::Value::from(value);
                    rmap.insert(key, value);
                }
                q::Value::Object(rmap)
            }
        }
    }
}
