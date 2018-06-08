use graphql_parser::{query as q, schema as s};
use std::collections::HashMap;

/// A GraphQL resolver that can resolve entities, enum values, scalar types and interfaces/unions.
pub trait Resolver: Clone {
    /// Resolves entities referenced by a parent object.
    fn resolve_entities(
        &self,
        parent: &Option<q::Value>,
        entity: &q::Name,
        arguments: &HashMap<&q::Name, q::Value>,
    ) -> q::Value;

    /// Resolves an entity referenced by a parent object.
    fn resolve_entity(
        &self,
        parent: &Option<q::Value>,
        field: &q::Name,
        entity: &q::Name,
        arguments: &HashMap<&q::Name, q::Value>,
    ) -> q::Value;

    /// Resolves an enum value for a given enum type.
    fn resolve_enum_value(&self, enum_type: &s::EnumType, value: Option<&q::Value>) -> q::Value;

    /// Resolves a scalar value for a given scalar type.
    fn resolve_scalar_value(
        &self,
        scalar_type: &s::ScalarType,
        value: Option<&q::Value>,
    ) -> q::Value;

    /// Resolves a list of enum values for a given enum type.
    fn resolve_enum_values(&self, enum_type: &s::EnumType, value: Option<&q::Value>) -> q::Value;

    /// Resolves a list of scalar values for a given list type.
    fn resolve_scalar_values(
        &self,
        scalar_type: &s::ScalarType,
        value: Option<&q::Value>,
    ) -> q::Value;

    // Resolves an abstract type into the specific type of an object.
    fn resolve_abstract_type<'a>(
        &self,
        schema: &'a s::Document,
        abstract_type: &s::TypeDefinition,
        object_value: &q::Value,
    ) -> Option<&'a s::ObjectType>;
}
