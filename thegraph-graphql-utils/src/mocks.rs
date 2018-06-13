use graphql_parser::{query as q, schema as s};
use std::collections::HashMap;

use super::Resolver;

#[derive(Clone)]
pub struct MockResolver;

impl Resolver for MockResolver {
    fn resolve_entities(
        &self,
        _parent: &Option<q::Value>,
        _field: &q::Name,
        _entity: &q::Name,
        _arguments: &HashMap<&q::Name, q::Value>,
    ) -> q::Value {
        q::Value::Null
    }

    fn resolve_entity(
        &self,
        _parent: &Option<q::Value>,
        _field: &q::Name,
        _entity: &q::Name,
        _arguments: &HashMap<&q::Name, q::Value>,
    ) -> q::Value {
        q::Value::Null
    }
}
