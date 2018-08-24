use graph::prelude::*;
use graphql_parser::{query as q, schema as s};
use schema::ast as sast;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

/// Builds a StoreQuery from GraphQL arguments.
pub fn build_query(entity: &s::ObjectType, arguments: &HashMap<&q::Name, q::Value>) -> StoreQuery {
    StoreQuery {
        subgraph: parse_subgraph_id(entity)
            .expect(format!("Failed to get subgraph ID from type: {}", entity.name).as_str()),
        entity: entity.name.to_owned(),
        range: build_range(arguments),
        filter: build_filter(entity, arguments),
        order_by: build_order_by(arguments),
        order_direction: build_order_direction(arguments),
    }
}

/// Parses GraphQL arguments into a StoreRange, if present.
fn build_range(arguments: &HashMap<&q::Name, q::Value>) -> Option<StoreRange> {
    let first = arguments
        .get(&"first".to_string())
        .and_then(|value| match value {
            q::Value::Int(n) => n.as_i64(),
            _ => None,
        })
        .and_then(|n| if n > 0 { Some(n as usize) } else { None });

    let skip = arguments
        .get(&"skip".to_string())
        .and_then(|value| match value {
            q::Value::Int(n) => n.as_i64(),
            _ => None,
        })
        .and_then(|n| if n >= 0 { Some(n as usize) } else { None });

    match (first, skip) {
        (None, None) => None,
        (Some(first), None) => Some(StoreRange { first, skip: 0 }),
        (Some(first), Some(skip)) => Some(StoreRange { first, skip }),
        (None, Some(skip)) => Some(StoreRange { first: 100, skip }),
    }
}

/// Parses GraphQL arguments into a StoreFilter, if present.
fn build_filter(
    entity: &s::ObjectType,
    arguments: &HashMap<&q::Name, q::Value>,
) -> Option<StoreFilter> {
    arguments
        .get(&"where".to_string())
        .and_then(|value| match value {
            q::Value::Object(object) => Some(object),
            _ => None,
        })
        .map(|object| build_filter_from_object(entity, object))
}

/// Parses a GraphQL input object into a StoreFilter, if present.
fn build_filter_from_object(
    entity: &s::ObjectType,
    object: &BTreeMap<q::Name, q::Value>,
) -> StoreFilter {
    StoreFilter::And(
        object
            .iter()
            .map(|(key, value)| {
                use self::sast::FilterOp::*;

                let (attribute, op) = sast::parse_field_as_filter(key);

                let field = sast::get_field_type(entity, &attribute)
                    .expect("attribute does not belong to entity");
                let ty = &field.field_type;
                let store_value = Value::from_query_value(value, &ty);

                match op {
                    Not => StoreFilter::Not(attribute, store_value),
                    GreaterThan => StoreFilter::GreaterThan(attribute, store_value),
                    LessThan => StoreFilter::LessThan(attribute, store_value),
                    GreaterOrEqual => StoreFilter::GreaterOrEqual(attribute, store_value),
                    LessOrEqual => StoreFilter::LessOrEqual(attribute, store_value),
                    In => StoreFilter::In(attribute, list_values(store_value)),
                    NotIn => StoreFilter::NotIn(attribute, list_values(store_value)),
                    Contains => StoreFilter::Contains(attribute, store_value),
                    NotContains => StoreFilter::NotContains(attribute, store_value),
                    StartsWith => StoreFilter::StartsWith(attribute, store_value),
                    NotStartsWith => StoreFilter::NotStartsWith(attribute, store_value),
                    EndsWith => StoreFilter::EndsWith(attribute, store_value),
                    NotEndsWith => StoreFilter::NotEndsWith(attribute, store_value),
                    Equal => StoreFilter::Equal(attribute, store_value),
                }
            })
            .collect::<Vec<StoreFilter>>(),
    )
}

/// Parses a list of GraphQL values into a vector of entity attribute values.
fn list_values(value: Value) -> Vec<Value> {
    match value {
        Value::List(values) => values,
        _ => panic!("value is not a list"),
    }
}

/// Parses GraphQL arguments into an attribute name to order by, if present.
fn build_order_by(arguments: &HashMap<&q::Name, q::Value>) -> Option<String> {
    arguments
        .get(&"orderBy".to_string())
        .and_then(|value| match value {
            q::Value::Enum(name) => Some(name.to_owned()),
            _ => None,
        })
}

/// Parses GraphQL arguments into a StoreOrder, if present.
fn build_order_direction(arguments: &HashMap<&q::Name, q::Value>) -> Option<StoreOrder> {
    arguments
        .get(&"orderDirection".to_string())
        .and_then(|value| match value {
            q::Value::Enum(name) if name == "asc" => Some(StoreOrder::Ascending),
            q::Value::Enum(name) if name == "desc" => Some(StoreOrder::Descending),
            _ => None,
        })
}

/// Parses the subgraph ID from the ObjectType directives.
pub fn parse_subgraph_id(entity: &s::ObjectType) -> Option<String> {
    entity
        .clone()
        .directives
        .into_iter()
        .find(|directive| directive.name == "subgraphId".to_string())
        .and_then(|directive| {
            directive
                .arguments
                .into_iter()
                .find(|(name, _)| name == &"id".to_string())
        })
        .and_then(|(_, value)| match value {
            s::Value::String(id) => Some(id),
            _ => None,
        })
}

#[cfg(test)]
mod tests {
    use graphql_parser::{
        query as q, schema as s,
        schema::{Directive, Field, ObjectType, Type},
        Pos,
    };
    use std::collections::{BTreeMap, HashMap};
    use std::iter::FromIterator;

    use graph::prelude::*;

    use super::build_query;

    fn default_object() -> ObjectType {
        let subgraph_id_argument = (
            s::Name::from("id"),
            s::Value::String("QmZ5dsusHwD1PEbx6L4dLCWkDsk1BLhrx9mPsGyPvTxPCM".to_string()),
        );
        let subgraph_id_directive = Directive {
            name: "subgraphId".to_string(),
            position: Pos::default(),
            arguments: vec![subgraph_id_argument],
        };
        ObjectType {
            position: Default::default(),
            description: None,
            name: String::new(),
            implements_interfaces: vec![],
            directives: vec![subgraph_id_directive],
            fields: vec![],
        }
    }

    fn object(name: &str) -> ObjectType {
        ObjectType {
            name: name.to_owned(),
            ..default_object()
        }
    }

    fn field(name: &str, field_type: Type) -> Field {
        Field {
            position: Default::default(),
            description: None,
            name: name.to_owned(),
            arguments: vec![],
            field_type,
            directives: vec![],
        }
    }

    #[test]
    fn build_query_uses_the_entity_name() {
        assert_eq!(
            build_query(&object("Entity1"), &HashMap::new()).entity,
            "Entity1".to_string()
        );
        assert_eq!(
            build_query(&object("Entity2"), &HashMap::new()).entity,
            "Entity2".to_string()
        );
    }

    #[test]
    fn build_query_yields_no_order_if_order_arguments_are_missing() {
        assert_eq!(
            build_query(&default_object(), &HashMap::new()).order_by,
            None,
        );
        assert_eq!(
            build_query(&default_object(), &HashMap::new()).order_direction,
            None,
        );
    }

    #[test]
    fn build_query_parses_order_by_from_enum_values_correctly() {
        assert_eq!(
            build_query(
                &default_object(),
                &HashMap::from_iter(
                    vec![(&"orderBy".to_string(), q::Value::Enum("name".to_string()))].into_iter(),
                )
            ).order_by,
            Some("name".to_string())
        );
        assert_eq!(
            build_query(
                &default_object(),
                &HashMap::from_iter(
                    vec![(&"orderBy".to_string(), q::Value::Enum("email".to_string()))].into_iter()
                )
            ).order_by,
            Some("email".to_string())
        );
    }

    #[test]
    fn build_query_ignores_order_by_from_non_enum_values() {
        assert_eq!(
            build_query(
                &default_object(),
                &HashMap::from_iter(
                    vec![(&"orderBy".to_string(), q::Value::String("name".to_string()))]
                        .into_iter()
                ),
            ).order_by,
            None,
        );
        assert_eq!(
            build_query(
                &default_object(),
                &HashMap::from_iter(
                    vec![(
                        &"orderBy".to_string(),
                        q::Value::String("email".to_string()),
                    )].into_iter(),
                )
            ).order_by,
            None,
        );
    }

    #[test]
    fn build_query_parses_order_direction_from_enum_values_correctly() {
        assert_eq!(
            build_query(
                &default_object(),
                &HashMap::from_iter(
                    vec![(
                        &"orderDirection".to_string(),
                        q::Value::Enum("asc".to_string()),
                    )].into_iter(),
                )
            ).order_direction,
            Some(StoreOrder::Ascending)
        );
        assert_eq!(
            build_query(
                &default_object(),
                &HashMap::from_iter(
                    vec![(
                        &"orderDirection".to_string(),
                        q::Value::Enum("desc".to_string()),
                    )].into_iter()
                )
            ).order_direction,
            Some(StoreOrder::Descending)
        );
        assert_eq!(
            build_query(
                &default_object(),
                &HashMap::from_iter(
                    vec![(
                        &"orderDirection".to_string(),
                        q::Value::Enum("ascending...".to_string()),
                    )].into_iter()
                )
            ).order_direction,
            None,
        );
    }

    #[test]
    fn build_query_ignores_order_direction_from_non_enum_values() {
        assert_eq!(
            build_query(
                &default_object(),
                &HashMap::from_iter(
                    vec![(
                        &"orderDirection".to_string(),
                        q::Value::String("asc".to_string()),
                    )].into_iter()
                ),
            ).order_direction,
            None,
        );
        assert_eq!(
            build_query(
                &default_object(),
                &HashMap::from_iter(
                    vec![(
                        &"orderDirection".to_string(),
                        q::Value::String("desc".to_string()),
                    )].into_iter(),
                )
            ).order_direction,
            None,
        );
    }

    #[test]
    fn build_query_yields_no_range_if_none_is_present() {
        assert_eq!(build_query(&default_object(), &HashMap::new()).range, None,);
    }

    #[test]
    fn build_query_yields_default_first_if_only_skip_is_present() {
        assert_eq!(
            build_query(
                &default_object(),
                &HashMap::from_iter(
                    vec![(&"skip".to_string(), q::Value::Int(q::Number::from(50)))].into_iter()
                )
            ).range,
            Some(StoreRange {
                first: 100,
                skip: 50,
            }),
        );
    }

    #[test]
    fn build_query_yields_default_skip_if_only_first_is_present() {
        assert_eq!(
            build_query(
                &default_object(),
                &HashMap::from_iter(
                    vec![(&"first".to_string(), q::Value::Int(q::Number::from(70)))].into_iter()
                )
            ).range,
            Some(StoreRange { first: 70, skip: 0 }),
        );
    }

    #[test]
    fn build_query_yields_filters() {
        assert_eq!(
            build_query(
                &ObjectType {
                    fields: vec![field("name", Type::NamedType("string".to_owned()))],
                    ..default_object()
                },
                &HashMap::from_iter(
                    vec![(
                        &"where".to_string(),
                        q::Value::Object(BTreeMap::from_iter(vec![(
                            "name_ends_with".to_string(),
                            q::Value::String("ello".to_string()),
                        )])),
                    )].into_iter(),
                )
            ).filter,
            Some(StoreFilter::And(vec![StoreFilter::EndsWith(
                "name".to_string(),
                Value::String("ello".to_string()),
            )]))
        )
    }
}
