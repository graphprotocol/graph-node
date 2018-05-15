use std::collections::HashMap;

/// An entity attribute name is represented as a string.
pub type Attribute = String;

/// An attribute value is represented as an enum with variants for all supported value types.
pub enum Value {
    String(String),
}

/// An entity is represented as a map of attribute names to values.
pub type Entity = HashMap<Attribute, Value>;
