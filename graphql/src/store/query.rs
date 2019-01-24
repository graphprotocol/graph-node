use graph::prelude::*;
use graphql_parser::{query as q, schema as s};
use schema::ast as sast;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::mem::discriminant;

/// Builds a EntityQuery from GraphQL arguments.
pub fn build_query(
    entity: &s::ObjectType,
    arguments: &HashMap<&q::Name, q::Value>,
) -> Result<EntityQuery, QueryExecutionError> {
    Ok(EntityQuery {
        subgraph_id: parse_subgraph_id(entity)?,
        entity_type: entity.name.to_owned(),
        range: build_range(arguments)?,
        filter: build_filter(entity, arguments)?,
        order_by: build_order_by(entity, arguments)?,
        order_direction: build_order_direction(arguments)?,
    })
}

/// Parses GraphQL arguments into a EntityRange, if present.
fn build_range(
    arguments: &HashMap<&q::Name, q::Value>,
) -> Result<Option<EntityRange>, QueryExecutionError> {
    let first = arguments
        .get(&"first".to_string())
        .map_or(Ok(None), |value| {
            if let q::Value::Int(n) = value {
                match n.as_i64() {
                    Some(n) if n > 0 && n <= 100 => Ok(Some(n)),
                    _ => Err("first".to_string()),
                }
            } else {
                Err("first".to_string())
            }
        })
        .map(|n| match n {
            Some(n) => Some(n as usize),
            _ => None,
        });

    let skip = arguments
        .get(&"skip".to_string())
        .map_or(Ok(None), |value| {
            if let q::Value::Int(n) = value {
                match n.as_i64() {
                    Some(n) if n > 0 => Ok(Some(n)),
                    _ => Err("skip".to_string()),
                }
            } else {
                Err("skip".to_string())
            }
        })
        .map(|n| match n {
            Some(n) => Some(n as usize),
            _ => None,
        });

    if first.is_err() || skip.is_err() {
        let errors: Vec<String> = vec![first.clone(), skip.clone()]
            .into_iter()
            .filter(|r| r.is_err())
            .map(|e| e.unwrap_err())
            .collect();
        return Err(QueryExecutionError::RangeArgumentsError(errors));
    }

    Ok(match (first.unwrap(), skip.unwrap()) {
        (None, None) => Some(EntityRange {
            first: 100,
            skip: 0,
        }),
        (Some(first), None) => Some(EntityRange { first, skip: 0 }),
        (Some(first), Some(skip)) => Some(EntityRange { first, skip }),
        (None, Some(skip)) => Some(EntityRange { first: 100, skip }),
    })
}

/// Parses GraphQL arguments into a EntityFilter, if present.
fn build_filter(
    entity: &s::ObjectType,
    arguments: &HashMap<&q::Name, q::Value>,
) -> Result<Option<EntityFilter>, QueryExecutionError> {
    match arguments.get(&"where".to_string()) {
        Some(value) => match value {
            q::Value::Object(object) => Ok(object),
            _ => return Err(QueryExecutionError::InvalidFilterError),
        },
        None => return Ok(None),
    }
    .and_then(|object| build_filter_from_object(entity, &object))
}

/// Parses a GraphQL input object into a EntityFilter, if present.
fn build_filter_from_object(
    entity: &s::ObjectType,
    object: &BTreeMap<q::Name, q::Value>,
) -> Result<Option<EntityFilter>, QueryExecutionError> {
    Ok(Some(EntityFilter::And({
        object
            .iter()
            .map(|(key, value)| {
                use self::sast::FilterOp::*;

                let (field_name, op) = sast::parse_field_as_filter(key);

                let field = sast::get_field_type(entity, &field_name).ok_or_else(|| {
                    QueryExecutionError::EntityFieldError(entity.name.clone(), field_name.clone())
                })?;

                let ty = &field.field_type;
                let store_value = Value::from_query_value(value, &ty)?;

                Ok(match op {
                    Not => EntityFilter::Not(field_name, store_value),
                    GreaterThan => EntityFilter::GreaterThan(field_name, store_value),
                    LessThan => EntityFilter::LessThan(field_name, store_value),
                    GreaterOrEqual => EntityFilter::GreaterOrEqual(field_name, store_value),
                    LessOrEqual => EntityFilter::LessOrEqual(field_name, store_value),
                    In => EntityFilter::In(field_name, list_values(store_value, "_in")?),
                    NotIn => EntityFilter::NotIn(field_name, list_values(store_value, "_not_in")?),
                    Contains => EntityFilter::Contains(field_name, store_value),
                    NotContains => EntityFilter::NotContains(field_name, store_value),
                    StartsWith => EntityFilter::StartsWith(field_name, store_value),
                    NotStartsWith => EntityFilter::NotStartsWith(field_name, store_value),
                    EndsWith => EntityFilter::EndsWith(field_name, store_value),
                    NotEndsWith => EntityFilter::NotEndsWith(field_name, store_value),
                    Equal => EntityFilter::Equal(field_name, store_value),
                })
            })
            .collect::<Result<Vec<EntityFilter>, QueryExecutionError>>()?
    })))
}

/// Parses a list of GraphQL values into a vector of entity field values.
fn list_values(value: Value, filter_type: &str) -> Result<Vec<Value>, QueryExecutionError> {
    match value {
        Value::List(ref values) if !values.is_empty() => {
            // Check that all values in list are of the same type
            let root_discriminant = discriminant(&values[0]);
            values
                .into_iter()
                .map(|value| {
                    let current_discriminant = discriminant(value);
                    if root_discriminant == current_discriminant {
                        Ok(value.clone())
                    } else {
                        Err(QueryExecutionError::ListTypesError(
                            filter_type.to_string(),
                            vec![values[0].to_string(), value.to_string()],
                        ))
                    }
                })
                .collect::<Result<Vec<_>, _>>()
        }
        Value::List(ref values) if values.is_empty() => Ok(vec![]),
        _ => Err(QueryExecutionError::ListFilterError(
            filter_type.to_string(),
        )),
    }
}

/// Parses GraphQL arguments into an field name to order by, if present.
fn build_order_by(
    entity: &s::ObjectType,
    arguments: &HashMap<&q::Name, q::Value>,
) -> Result<Option<(String, ValueType)>, QueryExecutionError> {
    arguments
        .get(&"orderBy".to_string())
        .map_or(Ok(None), |value| match value {
            q::Value::Enum(name) => {
                let field = sast::get_field_type(entity, &name).ok_or_else(|| {
                    QueryExecutionError::EntityFieldError(entity.name.clone(), name.clone())
                })?;
                sast::get_field_value_type(&field.field_type)
                    .map(|value_type| Some((name.to_owned(), value_type)))
                    .map_err(|_| {
                        QueryExecutionError::OrderByNotSupportedError(
                            entity.name.clone(),
                            name.clone(),
                        )
                    })
            }
            _ => Ok(None),
        })
}

/// Parses GraphQL arguments into a EntityOrder, if present.
fn build_order_direction(
    arguments: &HashMap<&q::Name, q::Value>,
) -> Result<Option<EntityOrder>, QueryExecutionError> {
    Ok(arguments
        .get(&"orderDirection".to_string())
        .and_then(|value| match value {
            q::Value::Enum(name) if name == "asc" => Some(EntityOrder::Ascending),
            q::Value::Enum(name) if name == "desc" => Some(EntityOrder::Descending),
            _ => None,
        }))
}

/// Parses the subgraph ID from the ObjectType directives.
pub fn parse_subgraph_id(
    entity: &s::ObjectType,
) -> Result<SubgraphDeploymentId, QueryExecutionError> {
    let entity_name = entity.name.clone();
    entity
        .directives
        .iter()
        .find(|directive| directive.name == "subgraphId")
        .and_then(|directive| {
            directive
                .arguments
                .iter()
                .find(|(name, _)| name == &"id".to_string())
        })
        .and_then(|(_, value)| match value {
            s::Value::String(id) => Some(id.clone()),
            _ => None,
        })
        .ok_or(())
        .and_then(|id| SubgraphDeploymentId::new(id))
        .map_err(|()| QueryExecutionError::SubgraphDeploymentIdError(entity_name))
}

/// Recursively collects entities involved in a query field as `(subgraph ID, name)` tuples.
pub fn collect_entities_from_query_field(
    schema: &s::Document,
    object_type: &s::ObjectType,
    field: &q::Field,
) -> Vec<(SubgraphDeploymentId, String)> {
    // Output entities
    let mut entities = HashSet::new();

    // List of objects/fields to visit next
    let mut queue = VecDeque::new();
    queue.push_back((object_type, field));

    while let Some((object_type, field)) = queue.pop_front() {
        // Check if the field exists on the object type
        if let Some(field_type) = sast::get_field_type(object_type, &field.name) {
            // Check if the field type corresponds to a type definition (in a valid schema,
            // this should always be the case)
            if let Some(type_definition) =
                sast::get_type_definition_from_field_type(schema, field_type)
            {
                // If the field's type definition is an object type, extract that type
                if let s::TypeDefinition::Object(object_type) = type_definition {
                    // Only collect whether the field's type has an @entity directive
                    if sast::get_object_type_directive(object_type, String::from("entity"))
                        .is_some()
                    {
                        // Obtain the subgraph ID from the object type
                        if let Ok(subgraph_id) = parse_subgraph_id(&object_type) {
                            // Add the (subgraph_id, entity_name) tuple to the result set
                            entities.insert((subgraph_id, object_type.name.to_owned()));
                        }
                    }

                    // If the query field has a non-empty selection set, this means we
                    // need to recursively process it
                    for selection in field.selection_set.items.iter() {
                        if let q::Selection::Field(sub_field) = selection {
                            queue.push_back((&object_type, sub_field))
                        }
                    }
                }
            }
        }
    }

    entities.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use graphql_parser::{
        query as q, schema as s,
        schema::{Directive, Field, InputValue, ObjectType, Type, Value as SchemaValue},
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
        let name_input_value = InputValue {
            position: Pos::default(),
            description: Some("name input".to_string()),
            name: "name".to_string(),
            value_type: Type::NamedType("String".to_string()),
            default_value: Some(SchemaValue::String("name".to_string())),
            directives: vec![],
        };
        let name_field = Field {
            position: Pos::default(),
            description: Some("name field".to_string()),
            name: "name".to_string(),
            arguments: vec![name_input_value.clone()],
            field_type: Type::NamedType("String".to_string()),
            directives: vec![],
        };
        let email_field = Field {
            position: Pos::default(),
            description: Some("email field".to_string()),
            name: "email".to_string(),
            arguments: vec![name_input_value],
            field_type: Type::NamedType("String".to_string()),
            directives: vec![],
        };

        ObjectType {
            position: Default::default(),
            description: None,
            name: String::new(),
            implements_interfaces: vec![],
            directives: vec![subgraph_id_directive],
            fields: vec![name_field, email_field],
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
            build_query(&object("Entity1"), &HashMap::new())
                .unwrap()
                .entity_type,
            "Entity1".to_string()
        );
        assert_eq!(
            build_query(&object("Entity2"), &HashMap::new())
                .unwrap()
                .entity_type,
            "Entity2".to_string()
        );
    }

    #[test]
    fn build_query_yields_no_order_if_order_arguments_are_missing() {
        assert_eq!(
            build_query(&default_object(), &HashMap::new())
                .unwrap()
                .order_by,
            None,
        );
        assert_eq!(
            build_query(&default_object(), &HashMap::new())
                .unwrap()
                .order_direction,
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
            )
            .unwrap()
            .order_by,
            Some(("name".to_string(), ValueType::String))
        );
        assert_eq!(
            build_query(
                &default_object(),
                &HashMap::from_iter(
                    vec![(&"orderBy".to_string(), q::Value::Enum("email".to_string()))].into_iter()
                )
            )
            .unwrap()
            .order_by,
            Some(("email".to_string(), ValueType::String))
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
            )
            .unwrap()
            .order_by,
            None,
        );
        assert_eq!(
            build_query(
                &default_object(),
                &HashMap::from_iter(
                    vec![(
                        &"orderBy".to_string(),
                        q::Value::String("email".to_string()),
                    )]
                    .into_iter(),
                )
            )
            .unwrap()
            .order_by,
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
                    )]
                    .into_iter(),
                )
            )
            .unwrap()
            .order_direction,
            Some(EntityOrder::Ascending)
        );
        assert_eq!(
            build_query(
                &default_object(),
                &HashMap::from_iter(
                    vec![(
                        &"orderDirection".to_string(),
                        q::Value::Enum("desc".to_string()),
                    )]
                    .into_iter()
                )
            )
            .unwrap()
            .order_direction,
            Some(EntityOrder::Descending)
        );
        assert_eq!(
            build_query(
                &default_object(),
                &HashMap::from_iter(
                    vec![(
                        &"orderDirection".to_string(),
                        q::Value::Enum("ascending...".to_string()),
                    )]
                    .into_iter()
                )
            )
            .unwrap()
            .order_direction,
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
                    )]
                    .into_iter()
                ),
            )
            .unwrap()
            .order_direction,
            None,
        );
        assert_eq!(
            build_query(
                &default_object(),
                &HashMap::from_iter(
                    vec![(
                        &"orderDirection".to_string(),
                        q::Value::String("desc".to_string()),
                    )]
                    .into_iter(),
                )
            )
            .unwrap()
            .order_direction,
            None,
        );
    }

    #[test]
    fn build_query_yields_no_range_if_none_is_present() {
        assert_eq!(
            build_query(&default_object(), &HashMap::new())
                .unwrap()
                .range,
            None,
        );
    }

    #[test]
    fn build_query_yields_default_first_if_only_skip_is_present() {
        assert_eq!(
            build_query(
                &default_object(),
                &HashMap::from_iter(
                    vec![(&"skip".to_string(), q::Value::Int(q::Number::from(50)))].into_iter()
                )
            )
            .unwrap()
            .range,
            Some(EntityRange {
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
            )
            .unwrap()
            .range,
            Some(EntityRange { first: 70, skip: 0 }),
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
                    )]
                    .into_iter(),
                )
            )
            .unwrap()
            .filter,
            Some(EntityFilter::And(vec![EntityFilter::EndsWith(
                "name".to_string(),
                Value::String("ello".to_string()),
            )]))
        )
    }
}
