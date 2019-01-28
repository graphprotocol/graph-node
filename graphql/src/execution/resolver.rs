use graphql_parser::{query as q, schema as s};
use std::collections::HashMap;

use graph::prelude::{QueryExecutionError, StoreEventStreamBox};
use prelude::*;

#[derive(Copy, Clone)]
pub enum ObjectOrInterface<'a> {
    Object(&'a s::ObjectType),
    Interface(&'a s::InterfaceType),
}

impl<'a> From<&'a s::ObjectType> for ObjectOrInterface<'a> {
    fn from(object: &'a s::ObjectType) -> Self {
        ObjectOrInterface::Object(object)
    }
}

impl<'a> From<&'a s::InterfaceType> for ObjectOrInterface<'a> {
    fn from(interface: &'a s::InterfaceType) -> Self {
        ObjectOrInterface::Interface(interface)
    }
}

impl<'a> ObjectOrInterface<'a> {
    pub fn name(self) -> &'a str {
        match self {
            ObjectOrInterface::Object(object) => &object.name,
            ObjectOrInterface::Interface(interface) => &interface.name,
        }
    }

    pub fn directives(self) -> &'a Vec<s::Directive> {
        match self {
            ObjectOrInterface::Object(object) => &object.directives,
            ObjectOrInterface::Interface(interface) => &interface.directives,
        }
    }

    pub fn fields(self) -> &'a Vec<s::Field> {
        match self {
            ObjectOrInterface::Object(object) => &object.fields,
            ObjectOrInterface::Interface(interface) => &interface.fields,
        }
    }
}

/// A GraphQL resolver that can resolve entities, enum values, scalar types and interfaces/unions.
pub trait Resolver: Clone + Send + Sync {
    /// Resolves entities referenced by a parent object.
    fn resolve_objects<'a>(
        &self,
        parent: &Option<q::Value>,
        field: &q::Name,
        field_definition: &s::Field,
        object_type: impl Into<ObjectOrInterface<'a>>,
        arguments: &HashMap<&q::Name, q::Value>,
    ) -> Result<q::Value, QueryExecutionError>;

    /// Resolves an entity referenced by a parent object.
    fn resolve_object<'a>(
        &self,
        parent: &Option<q::Value>,
        field: &q::Name,
        object_type: impl Into<ObjectOrInterface<'a>>,
        arguments: &HashMap<&q::Name, q::Value>,
    ) -> Result<q::Value, QueryExecutionError>;

    /// Resolves an enum value for a given enum type.
    fn resolve_enum_value(&self, enum_type: &s::EnumType, value: Option<&q::Value>) -> q::Value {
        value
            .and_then(|value| value.coerce(enum_type))
            .unwrap_or(q::Value::Null)
    }

    /// Resolves a scalar value for a given scalar type.
    fn resolve_scalar_value(
        &self,
        scalar_type: &s::ScalarType,
        value: Option<&q::Value>,
    ) -> q::Value {
        value
            .and_then(|value| value.coerce(scalar_type))
            .unwrap_or(q::Value::Null)
    }

    /// Resolves a list of enum values for a given enum type.
    fn resolve_enum_values(&self, enum_type: &s::EnumType, value: Option<&q::Value>) -> q::Value {
        value
            .and_then(|value| match value {
                q::Value::List(values) => Some(values),
                _ => None,
            })
            .and_then(|values| {
                let coerced_values: Vec<q::Value> = values
                    .iter()
                    .filter_map(|value| value.coerce(enum_type))
                    .collect();

                if coerced_values.len() == values.len() {
                    Some(q::Value::List(coerced_values))
                } else {
                    None
                }
            })
            .unwrap_or(q::Value::Null)
    }

    /// Resolves a list of scalar values for a given list type.
    fn resolve_scalar_values(
        &self,
        scalar_type: &s::ScalarType,
        value: Option<&q::Value>,
    ) -> q::Value {
        value
            .and_then(|value| match value {
                q::Value::List(values) => Some(values),
                _ => None,
            })
            .and_then(|values| {
                let coerced_values: Vec<q::Value> = values
                    .iter()
                    .filter_map(|value| value.coerce(scalar_type))
                    .collect();

                if coerced_values.len() == values.len() {
                    Some(q::Value::List(coerced_values))
                } else {
                    None
                }
            })
            .unwrap_or(q::Value::Null)
    }

    // Resolves an abstract type into the specific type of an object.
    fn resolve_abstract_type<'a>(
        &self,
        _schema: &'a s::Document,
        _abstract_type: &s::TypeDefinition,
        _object_value: &q::Value,
    ) -> Option<&'a s::ObjectType> {
        None
    }

    // Resolves a change stream for a given field.
    fn resolve_field_stream<'a, 'b>(
        &self,
        _schema: &'a s::Document,
        _object_type: &'a s::ObjectType,
        _field: &'b q::Field,
    ) -> Result<StoreEventStreamBox, QueryExecutionError> {
        Err(QueryExecutionError::NotSupported(String::from(
            "Resolving field streams is not supported by this resolver",
        )))
    }
}
