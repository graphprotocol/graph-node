use graphql_parser::{query as q, schema as s};
use std::collections::HashMap;

use graph::prelude::{EntityChangeStream, QueryExecutionError};
use prelude::*;

/// A GraphQL resolver that can resolve entities, enum values, scalar types and interfaces/unions.
pub trait Resolver: Clone + Send + Sync {
    /// Resolves entities referenced by a parent object.
    fn resolve_objects(
        &self,
        parent: &Option<q::Value>,
        field: &q::Name,
        field_definition: &s::Field,
        object_type: &s::ObjectType,
        arguments: &HashMap<&q::Name, q::Value>,
    ) -> q::Value;

    /// Resolves an entity referenced by a parent object.
    fn resolve_object(
        &self,
        parent: &Option<q::Value>,
        field: &q::Name,
        field_definition: &s::Field,
        object_type: &s::ObjectType,
        arguments: &HashMap<&q::Name, q::Value>,
    ) -> q::Value;

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
    ) -> Result<EntityChangeStream, QueryExecutionError> {
        Err(QueryExecutionError::NotSupported(String::from(
            "Resolving field streams is not supported by this resolver",
        )))
    }
}
