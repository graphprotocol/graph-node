use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::mem::discriminant;

use graph::data::value::Object;
use graph::data::value::Value as DataValue;
use graph::prelude::s::Type;
use graph::prelude::*;
use graph::{components::store::EntityType, data::graphql::ObjectOrInterface};

use crate::execution::ast as a;
use crate::schema::ast as sast;

use super::prefetch::SelectedAttributes;

#[derive(Debug)]
enum OrderDirection {
    Ascending,
    Descending,
}

/// Builds a EntityQuery from GraphQL arguments.
///
/// Panics if `entity` is not present in `schema`.
pub(crate) fn build_query<'a>(
    entity: impl Into<ObjectOrInterface<'a>>,
    block: BlockNumber,
    field: &a::Field,
    types_for_interface: &'a BTreeMap<EntityType, Vec<s::ObjectType>>,
    max_first: u32,
    max_skip: u32,
    mut column_names: SelectedAttributes,
    schema: &ApiSchema,
) -> Result<EntityQuery, QueryExecutionError> {
    let entity = entity.into();
    let entity_types = EntityCollection::All(match &entity {
        ObjectOrInterface::Object(object) => {
            let selected_columns = column_names.get(object);
            vec![((*object).into(), selected_columns)]
        }
        ObjectOrInterface::Interface(interface) => types_for_interface
            [&EntityType::from(*interface)]
            .iter()
            .map(|o| {
                let selected_columns = column_names.get(o);
                (o.into(), selected_columns)
            })
            .collect(),
    });
    let mut query = EntityQuery::new(parse_subgraph_id(entity)?, block, entity_types)
        .range(build_range(field, max_first, max_skip)?);
    if let Some(filter) = build_filter(entity, field, schema)? {
        query = query.filter(filter);
    }
    let order = match (
        build_order_by(entity, field)?,
        build_order_direction(field)?,
    ) {
        (Some((attr, value_type)), OrderDirection::Ascending) => {
            EntityOrder::Ascending(attr, value_type)
        }
        (Some((attr, value_type)), OrderDirection::Descending) => {
            EntityOrder::Descending(attr, value_type)
        }
        (None, _) => EntityOrder::Default,
    };
    query = query.order(order);
    Ok(query)
}

/// Parses GraphQL arguments into a EntityRange, if present.
fn build_range(
    field: &a::Field,
    max_first: u32,
    max_skip: u32,
) -> Result<EntityRange, QueryExecutionError> {
    let first = match field.argument_value("first") {
        Some(r::Value::Int(n)) => {
            let n = *n;
            if n > 0 && n <= (max_first as i64) {
                n as u32
            } else {
                return Err(QueryExecutionError::RangeArgumentsError(
                    "first", max_first, n,
                ));
            }
        }
        Some(r::Value::Null) | None => 100,
        _ => unreachable!("first is an Int with a default value"),
    };

    let skip = match field.argument_value("skip") {
        Some(r::Value::Int(n)) => {
            let n = *n;
            if n >= 0 && n <= (max_skip as i64) {
                n as u32
            } else {
                return Err(QueryExecutionError::RangeArgumentsError(
                    "skip", max_skip, n,
                ));
            }
        }
        Some(r::Value::Null) | None => 0,
        _ => unreachable!("skip is an Int with a default value"),
    };

    Ok(EntityRange {
        first: Some(first),
        skip,
    })
}

/// Parses GraphQL arguments into an EntityFilter, if present.
fn build_filter(
    entity: ObjectOrInterface,
    field: &a::Field,
    schema: &ApiSchema,
) -> Result<Option<EntityFilter>, QueryExecutionError> {
    match field.argument_value("where") {
        Some(r::Value::Object(object)) => match build_filter_from_object(entity, object, schema) {
            Ok(filter) => Ok(Some(filter)),
            Err(e) => Err(e),
        },
        Some(r::Value::Null) => Ok(None),
        None => match field.argument_value("text") {
            Some(r::Value::Object(filter)) => build_fulltext_filter_from_object(filter),
            None => Ok(None),
            _ => Err(QueryExecutionError::InvalidFilterError),
        },
        _ => Err(QueryExecutionError::InvalidFilterError),
    }
}

fn resolve_type_name(ty: &Type) -> &String {
    match ty {
        Type::ListType(inner) => resolve_type_name(inner),
        Type::NonNullType(inner) => resolve_type_name(inner),
        Type::NamedType(name) => name,
    }
}

/// Parses a GraphQL input object into an EntityFilter, if present.
fn build_filter_from_object(
    entity: ObjectOrInterface,
    object: &Object,
    schema: &ApiSchema,
) -> Result<EntityFilter, QueryExecutionError> {
    Ok(EntityFilter::And({
        object
            .iter()
            .map(|(key, value)| {
                use self::sast::FilterOp::*;
                let (field_name, op) = sast::parse_field_as_filter(key);

                let field = sast::get_field(entity, &field_name).ok_or_else(|| {
                    QueryExecutionError::EntityFieldError(
                        entity.name().to_owned(),
                        field_name.clone(),
                    )
                })?;

                let ty = &field.field_type;

                Ok(match op {
                    Child => match value {
                        DataValue::Object(obj) => {
                            build_child_filter_from_object(entity, field_name, obj, schema)?
                        }
                        _ => {
                            return Err(QueryExecutionError::AttributeTypeError(
                                value.to_string(),
                                ty.to_string(),
                            ))
                        }
                    },
                    _ => {
                        let store_value = Value::from_query_value(value, ty)?;

                        match op {
                            Not => EntityFilter::Not(field_name, store_value),
                            GreaterThan => EntityFilter::GreaterThan(field_name, store_value),
                            LessThan => EntityFilter::LessThan(field_name, store_value),
                            GreaterOrEqual => EntityFilter::GreaterOrEqual(field_name, store_value),
                            LessOrEqual => EntityFilter::LessOrEqual(field_name, store_value),
                            In => EntityFilter::In(field_name, list_values(store_value, "_in")?),
                            NotIn => EntityFilter::NotIn(
                                field_name,
                                list_values(store_value, "_not_in")?,
                            ),
                            Contains => EntityFilter::Contains(field_name, store_value),
                            NotContains => EntityFilter::NotContains(field_name, store_value),
                            StartsWith => EntityFilter::StartsWith(field_name, store_value),
                            NotStartsWith => EntityFilter::NotStartsWith(field_name, store_value),
                            EndsWith => EntityFilter::EndsWith(field_name, store_value),
                            NotEndsWith => EntityFilter::NotEndsWith(field_name, store_value),
                            Equal => EntityFilter::Equal(field_name, store_value),
                            _ => unreachable!(),
                        }
                    }
                })
            })
            .collect::<Result<Vec<EntityFilter>, QueryExecutionError>>()?
    }))
}

fn build_child_filter_from_object(
    entity: ObjectOrInterface,
    field_name: String,
    object: &Object,
    schema: &ApiSchema,
) -> Result<EntityFilter, QueryExecutionError> {
    let field = entity
        .field(&field_name)
        .ok_or(QueryExecutionError::InvalidFilterError)?;
    let type_name = resolve_type_name(&field.field_type);
    let child_entity = schema
        .object_or_interface(type_name.as_str())
        .ok_or(QueryExecutionError::InvalidFilterError)?;
    let filter = build_filter_from_object(child_entity, object, schema)?;

    Ok(EntityFilter::Child(
        field_name,
        EntityType::new(type_name.to_string()),
        Box::new(filter),
    ))
}

fn build_fulltext_filter_from_object(
    object: &Object,
) -> Result<Option<EntityFilter>, QueryExecutionError> {
    object.iter().next().map_or(
        Err(QueryExecutionError::FulltextQueryRequiresFilter),
        |(key, value)| {
            if let r::Value::String(s) = value {
                Ok(Some(EntityFilter::Equal(
                    key.clone(),
                    Value::String(s.clone()),
                )))
            } else {
                Err(QueryExecutionError::FulltextQueryRequiresFilter)
            }
        },
    )
}

/// Parses a list of GraphQL values into a vector of entity field values.
fn list_values(value: Value, filter_type: &str) -> Result<Vec<Value>, QueryExecutionError> {
    match value {
        Value::List(ref values) if !values.is_empty() => {
            // Check that all values in list are of the same type
            let root_discriminant = discriminant(&values[0]);
            values
                .iter()
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
    entity: ObjectOrInterface,
    field: &a::Field,
) -> Result<Option<(String, ValueType)>, QueryExecutionError> {
    match field.argument_value("orderBy") {
        Some(r::Value::Enum(name)) => {
            let field = sast::get_field(entity, name).ok_or_else(|| {
                QueryExecutionError::EntityFieldError(entity.name().to_owned(), name.clone())
            })?;
            sast::get_field_value_type(&field.field_type)
                .map(|value_type| Some((name.to_owned(), value_type)))
                .map_err(|_| {
                    QueryExecutionError::OrderByNotSupportedError(
                        entity.name().to_owned(),
                        name.clone(),
                    )
                })
        }
        _ => match field.argument_value("text") {
            Some(r::Value::Object(filter)) => build_fulltext_order_by_from_object(filter),
            None => Ok(None),
            _ => Err(QueryExecutionError::InvalidFilterError),
        },
    }
}

fn build_fulltext_order_by_from_object(
    object: &Object,
) -> Result<Option<(String, ValueType)>, QueryExecutionError> {
    object.iter().next().map_or(
        Err(QueryExecutionError::FulltextQueryRequiresFilter),
        |(key, value)| {
            if let r::Value::String(_) = value {
                Ok(Some((key.clone(), ValueType::String)))
            } else {
                Err(QueryExecutionError::FulltextQueryRequiresFilter)
            }
        },
    )
}

/// Parses GraphQL arguments into a EntityOrder, if present.
fn build_order_direction(field: &a::Field) -> Result<OrderDirection, QueryExecutionError> {
    Ok(field
        .argument_value("orderDirection")
        .map(|value| match value {
            r::Value::Enum(name) if name == "asc" => OrderDirection::Ascending,
            r::Value::Enum(name) if name == "desc" => OrderDirection::Descending,
            _ => OrderDirection::Ascending,
        })
        .unwrap_or(OrderDirection::Ascending))
}

/// Parses the subgraph ID from the ObjectType directives.
pub fn parse_subgraph_id<'a>(
    entity: impl Into<ObjectOrInterface<'a>>,
) -> Result<DeploymentHash, QueryExecutionError> {
    let entity = entity.into();
    let entity_name = entity.name();
    entity
        .directives()
        .iter()
        .find(|directive| directive.name == "subgraphId")
        .and_then(|directive| directive.arguments.iter().find(|(name, _)| name == "id"))
        .and_then(|(_, value)| match value {
            s::Value::String(id) => Some(id),
            _ => None,
        })
        .ok_or(())
        .and_then(|id| DeploymentHash::new(id).map_err(|_| ()))
        .map_err(|_| QueryExecutionError::SubgraphDeploymentIdError(entity_name.to_owned()))
}

/// Recursively collects entities involved in a query field as `(subgraph ID, name)` tuples.
pub(crate) fn collect_entities_from_query_field(
    schema: &ApiSchema,
    object_type: sast::ObjectType,
    field: &a::Field,
) -> BTreeSet<SubscriptionFilter> {
    // Output entities
    let mut entities = HashSet::new();

    // List of objects/fields to visit next
    let mut queue = VecDeque::new();
    queue.push_back((object_type, field));

    while let Some((object_type, field)) = queue.pop_front() {
        // Check if the field exists on the object type
        if let Some(field_type) = sast::get_field(&object_type, &field.name) {
            // Check if the field type corresponds to a type definition (in a valid schema,
            // this should always be the case)
            if let Some(type_definition) = schema.get_type_definition_from_field(field_type) {
                // If the field's type definition is an object type, extract that type
                if let s::TypeDefinition::Object(object_type) = type_definition {
                    // Only collect whether the field's type has an @entity directive
                    if sast::get_object_type_directive(object_type, String::from("entity"))
                        .is_some()
                    {
                        // Obtain the subgraph ID from the object type
                        if let Ok(subgraph_id) = parse_subgraph_id(object_type) {
                            // Add the (subgraph_id, entity_name) tuple to the result set
                            entities.insert((subgraph_id, object_type.name.to_owned()));
                        }
                    }

                    // If the query field has a non-empty selection set, this means we
                    // need to recursively process it
                    let object_type = schema.object_type(object_type).into();
                    for sub_field in field.selection_set.fields_for(&object_type) {
                        queue.push_back((object_type.cheap_clone(), sub_field))
                    }
                }
            }
        }
    }

    entities
        .into_iter()
        .map(|(id, entity_type)| SubscriptionFilter::Entities(id, EntityType::new(entity_type)))
        .collect()
}

#[cfg(test)]
mod tests {
    use graph::{
        components::store::EntityType,
        data::value::Object,
        prelude::{
            r, ApiSchema, AttributeNames, DeploymentHash, EntityCollection, EntityFilter,
            EntityRange, Schema, Value, ValueType, BLOCK_NUMBER_MAX,
        },
        prelude::{
            s::{self, Directive, Field, InputValue, ObjectType, Type, Value as SchemaValue},
            EntityOrder,
        },
    };
    use graphql_parser::Pos;
    use std::{collections::BTreeMap, iter::FromIterator, sync::Arc};

    use super::{a, build_query};

    fn default_object() -> ObjectType {
        let subgraph_id_argument = (
            String::from("id"),
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

    fn default_field() -> a::Field {
        let arguments = vec![
            ("first".to_string(), r::Value::Int(100.into())),
            ("skip".to_string(), r::Value::Int(0.into())),
        ];
        let obj_type = Arc::new(object("SomeType")).into();
        a::Field {
            position: Default::default(),
            alias: None,
            name: "aField".to_string(),
            arguments,
            directives: vec![],
            selection_set: a::SelectionSet::new(vec![obj_type]),
        }
    }

    fn default_field_with(arg_name: &str, arg_value: r::Value) -> a::Field {
        let mut field = default_field();
        field.arguments.push((arg_name.to_string(), arg_value));
        field
    }

    fn default_field_with_vec(args: Vec<(&str, r::Value)>) -> a::Field {
        let mut field = default_field();
        for (name, value) in args {
            field.arguments.push((name.to_string(), value));
        }
        field
    }

    fn build_schema(raw_schema: &str) -> ApiSchema {
        let document = graphql_parser::parse_schema(raw_schema)
            .expect("Failed to parse raw schema")
            .into_static();

        ApiSchema::from_api_schema(Schema::new(DeploymentHash::new("id").unwrap(), document))
            .expect("Failed to build schema")
    }

    fn build_default_schema() -> ApiSchema {
        build_schema(
            r#"
                type Query {
                    aField(first: Int, skip: Int): [SomeType]
                }

                type SomeType @entity {
                    id: ID!
                    name: String!
                }
            "#,
        )
    }

    #[test]
    fn build_query_uses_the_entity_name() {
        let schema = build_default_schema();
        assert_eq!(
            build_query(
                &object("Entity1"),
                BLOCK_NUMBER_MAX,
                &default_field(),
                &BTreeMap::new(),
                std::u32::MAX,
                std::u32::MAX,
                Default::default(),
                &schema
            )
            .unwrap()
            .collection,
            EntityCollection::All(vec![(EntityType::from("Entity1"), AttributeNames::All)])
        );
        assert_eq!(
            build_query(
                &object("Entity2"),
                BLOCK_NUMBER_MAX,
                &default_field(),
                &BTreeMap::new(),
                std::u32::MAX,
                std::u32::MAX,
                Default::default(),
                &schema,
            )
            .unwrap()
            .collection,
            EntityCollection::All(vec![(EntityType::from("Entity2"), AttributeNames::All)])
        );
    }

    #[test]
    fn build_query_yields_no_order_if_order_arguments_are_missing() {
        let schema = build_default_schema();
        assert_eq!(
            build_query(
                &default_object(),
                BLOCK_NUMBER_MAX,
                &default_field(),
                &BTreeMap::new(),
                std::u32::MAX,
                std::u32::MAX,
                Default::default(),
                &schema,
            )
            .unwrap()
            .order,
            EntityOrder::Default,
        );
    }

    #[test]
    fn build_query_parses_order_by_from_enum_values_correctly() {
        let schema = build_default_schema();
        let field = default_field_with("orderBy", r::Value::Enum("name".to_string()));
        assert_eq!(
            build_query(
                &default_object(),
                BLOCK_NUMBER_MAX,
                &field,
                &BTreeMap::new(),
                std::u32::MAX,
                std::u32::MAX,
                Default::default(),
                &schema,
            )
            .unwrap()
            .order,
            EntityOrder::Ascending("name".to_string(), ValueType::String)
        );

        let field = default_field_with("orderBy", r::Value::Enum("email".to_string()));
        assert_eq!(
            build_query(
                &default_object(),
                BLOCK_NUMBER_MAX,
                &field,
                &BTreeMap::new(),
                std::u32::MAX,
                std::u32::MAX,
                Default::default(),
                &schema,
            )
            .unwrap()
            .order,
            EntityOrder::Ascending("email".to_string(), ValueType::String)
        );
    }

    #[test]
    fn build_query_ignores_order_by_from_non_enum_values() {
        let schema = build_default_schema();
        let field = default_field_with("orderBy", r::Value::String("name".to_string()));
        assert_eq!(
            build_query(
                &default_object(),
                BLOCK_NUMBER_MAX,
                &field,
                &BTreeMap::new(),
                std::u32::MAX,
                std::u32::MAX,
                Default::default(),
                &schema
            )
            .unwrap()
            .order,
            EntityOrder::Default
        );

        let field = default_field_with("orderBy", r::Value::String("email".to_string()));
        assert_eq!(
            build_query(
                &default_object(),
                BLOCK_NUMBER_MAX,
                &field,
                &BTreeMap::new(),
                std::u32::MAX,
                std::u32::MAX,
                Default::default(),
                &schema,
            )
            .unwrap()
            .order,
            EntityOrder::Default
        );
    }

    #[test]
    fn build_query_parses_order_direction_from_enum_values_correctly() {
        let schema = build_default_schema();
        let field = default_field_with_vec(vec![
            ("orderBy", r::Value::Enum("name".to_string())),
            ("orderDirection", r::Value::Enum("asc".to_string())),
        ]);
        assert_eq!(
            build_query(
                &default_object(),
                BLOCK_NUMBER_MAX,
                &field,
                &BTreeMap::new(),
                std::u32::MAX,
                std::u32::MAX,
                Default::default(),
                &schema,
            )
            .unwrap()
            .order,
            EntityOrder::Ascending("name".to_string(), ValueType::String)
        );

        let field = default_field_with_vec(vec![
            ("orderBy", r::Value::Enum("name".to_string())),
            ("orderDirection", r::Value::Enum("desc".to_string())),
        ]);
        assert_eq!(
            build_query(
                &default_object(),
                BLOCK_NUMBER_MAX,
                &field,
                &BTreeMap::new(),
                std::u32::MAX,
                std::u32::MAX,
                Default::default(),
                &schema,
            )
            .unwrap()
            .order,
            EntityOrder::Descending("name".to_string(), ValueType::String)
        );

        let field = default_field_with_vec(vec![
            ("orderBy", r::Value::Enum("name".to_string())),
            (
                "orderDirection",
                r::Value::Enum("descending...".to_string()),
            ),
        ]);
        assert_eq!(
            build_query(
                &default_object(),
                BLOCK_NUMBER_MAX,
                &field,
                &BTreeMap::new(),
                std::u32::MAX,
                std::u32::MAX,
                Default::default(),
                &schema,
            )
            .unwrap()
            .order,
            EntityOrder::Ascending("name".to_string(), ValueType::String)
        );

        // No orderBy -> EntityOrder::Default
        let field = default_field_with(
            "orderDirection",
            r::Value::Enum("descending...".to_string()),
        );
        assert_eq!(
            build_query(
                &default_object(),
                BLOCK_NUMBER_MAX,
                &field,
                &BTreeMap::new(),
                std::u32::MAX,
                std::u32::MAX,
                Default::default(),
                &schema
            )
            .unwrap()
            .order,
            EntityOrder::Default
        );
    }

    #[test]
    fn build_query_yields_default_range_if_none_is_present() {
        let schema = build_default_schema();
        assert_eq!(
            build_query(
                &default_object(),
                BLOCK_NUMBER_MAX,
                &default_field(),
                &BTreeMap::new(),
                std::u32::MAX,
                std::u32::MAX,
                Default::default(),
                &schema
            )
            .unwrap()
            .range,
            EntityRange::first(100)
        );
    }

    #[test]
    fn build_query_yields_default_first_if_only_skip_is_present() {
        let schema = build_default_schema();
        let mut field = default_field();
        field.arguments = vec![("skip".to_string(), r::Value::Int(50))];

        assert_eq!(
            build_query(
                &default_object(),
                BLOCK_NUMBER_MAX,
                &field,
                &BTreeMap::new(),
                std::u32::MAX,
                std::u32::MAX,
                Default::default(),
                &schema
            )
            .unwrap()
            .range,
            EntityRange {
                first: Some(100),
                skip: 50,
            },
        );
    }

    #[test]
    fn build_query_yields_filters() {
        let schema = build_default_schema();
        let query_field = default_field_with(
            "where",
            r::Value::Object(Object::from_iter(vec![(
                "name_ends_with".to_string(),
                r::Value::String("ello".to_string()),
            )])),
        );
        assert_eq!(
            build_query(
                &ObjectType {
                    fields: vec![field("name", Type::NamedType("string".to_owned()))],
                    ..default_object()
                },
                BLOCK_NUMBER_MAX,
                &query_field,
                &BTreeMap::new(),
                std::u32::MAX,
                std::u32::MAX,
                Default::default(),
                &schema
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
