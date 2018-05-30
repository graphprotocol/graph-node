use graphql_parser::{query as q, schema as s};
use std::collections::HashMap;

use super::Resolver;

#[derive(Clone)]
pub struct MockResolver;

impl Resolver for MockResolver {
    fn resolve_entities(
        &self,
        _parent: &Option<q::Value>,
        _entity: &q::Name,
        _arguments: &HashMap<&q::Name, q::Value>,
    ) -> q::Value {
        q::Value::Null
    }

    fn resolve_entity(
        &self,
        _parent: &Option<q::Value>,
        _entity: &q::Name,
        _arguments: &HashMap<&q::Name, q::Value>,
    ) -> q::Value {
        q::Value::Null
    }

    fn resolve_enum_value(&self, _enum_type: &s::EnumType, _value: Option<&q::Value>) -> q::Value {
        q::Value::Null
    }

    fn resolve_scalar_value(
        &self,
        _scalar_type: &s::ScalarType,
        _value: Option<&q::Value>,
    ) -> q::Value {
        q::Value::Null
    }

    fn resolve_enum_values(&self, _enum_type: &s::EnumType, _value: Option<&q::Value>) -> q::Value {
        q::Value::Null
    }

    fn resolve_scalar_values(
        &self,
        _scalar_type: &s::ScalarType,
        _value: Option<&q::Value>,
    ) -> q::Value {
        q::Value::Null
    }

    fn resolve_abstract_type<'a>(
        &self,
        _schema: &'a s::Document,
        _abstract_type: &s::TypeDefinition,
        _object_value: &q::Value,
    ) -> Option<&'a s::ObjectType> {
        None
    }
}
