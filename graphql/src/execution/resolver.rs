use graphql_parser::{query as q, schema as s};
use std::collections::{BTreeMap, HashMap};

use graph::prelude::{QueryExecutionError, StoreEventStreamBox};
use prelude::*;
use schema::ast::get_named_type;

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
    fn resolve_objects(
        &self,
        parent: &Option<q::Value>,
        field: &q::Name,
        field_definition: &s::Field,
        object_type: ObjectOrInterface<'_>,
        arguments: &HashMap<&q::Name, q::Value>,
        types_for_interface: &BTreeMap<Name, Vec<ObjectType>>,
    ) -> Result<q::Value, QueryExecutionError>;

    /// Resolves an entity referenced by a parent object.
    fn resolve_object(
        &self,
        parent: &Option<q::Value>,
        field: &q::Name,
        object_type: ObjectOrInterface<'_>,
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
        schema: &'a s::Document,
        _abstract_type: &s::TypeDefinition,
        object_value: &q::Value,
    ) -> Option<&'a s::ObjectType> {
        let concrete_type_name = match object_value {
            // All objects contain `__typename`
            q::Value::Object(data) => match &data["__typename"] {
                q::Value::String(name) => name.clone(),
                _ => unreachable!("__typename must be a string"),
            },
            _ => unreachable!("abstract type value must be an object"),
        };

        // A name returned in a `__typename` must exist in the schema.
        match get_named_type(schema, &concrete_type_name).unwrap() {
            s::TypeDefinition::Object(object) => Some(object),
            _ => unreachable!("only objects may implement interfaces"),
        }
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
