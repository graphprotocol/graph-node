use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

/// An entity attribute name is represented as a string.
pub type Attribute = String;

/// An attribute value is represented as an enum with variants for all supported value types.
#[derive(Clone, Debug)]
pub enum Value {
    String(String),
}

/// An entity is represented as a map of attribute names to values.
#[derive(Clone, Debug)]
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
