use std::str::FromStr;

use graphql_parser::Pos;
use inflector::Inflector;
use lazy_static::lazy_static;

use crate::schema::ast;

use graph::data::{
    graphql::ext::{DirectiveExt, DocumentExt, ValueExt},
    schema::{META_FIELD_NAME, META_FIELD_TYPE, SCHEMA_TYPE_NAME},
};
use graph::prelude::s::{Value, *};
use graph::prelude::*;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum APISchemaError {
    #[error("type {0} already exists in the input schema")]
    TypeExists(String),
    #[error("Type {0} not found")]
    TypeNotFound(String),
    #[error("Fulltext search is not yet deterministic")]
    FulltextSearchNonDeterministic,
}

const BLOCK_HEIGHT: &str = "Block_height";

const ERROR_POLICY_TYPE: &str = "_SubgraphErrorPolicy_";

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum ErrorPolicy {
    Allow,
    Deny,
}

impl std::str::FromStr for ErrorPolicy {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<ErrorPolicy, anyhow::Error> {
        match s {
            "allow" => Ok(ErrorPolicy::Allow),
            "deny" => Ok(ErrorPolicy::Deny),
            _ => Err(anyhow::anyhow!("failed to parse `{}` as ErrorPolicy", s)),
        }
    }
}

impl TryFrom<&q::Value> for ErrorPolicy {
    type Error = anyhow::Error;

    /// `value` should be the output of input value coercion.
    fn try_from(value: &q::Value) -> Result<Self, Self::Error> {
        match value {
            q::Value::Enum(s) => ErrorPolicy::from_str(s),
            _ => Err(anyhow::anyhow!("invalid `ErrorPolicy`")),
        }
    }
}

impl TryFrom<&r::Value> for ErrorPolicy {
    type Error = anyhow::Error;

    /// `value` should be the output of input value coercion.
    fn try_from(value: &r::Value) -> Result<Self, Self::Error> {
        match value {
            r::Value::Enum(s) => ErrorPolicy::from_str(s),
            _ => Err(anyhow::anyhow!("invalid `ErrorPolicy`")),
        }
    }
}

/// Derives a full-fledged GraphQL API schema from an input schema.
///
/// The input schema should only have type/enum/interface/union definitions
/// and must not include a root Query type. This Query type is derived, with
/// all its fields and their input arguments, based on the existing types.
pub fn api_schema(input_schema: &Document) -> Result<Document, APISchemaError> {
    // Refactor: Take `input_schema` by value.
    let object_types = input_schema.get_object_type_definitions();
    let interface_types = input_schema.get_interface_type_definitions();

    // Refactor: Don't clone the schema.
    let mut schema = input_schema.clone();
    add_directives(&mut schema);
    add_builtin_scalar_types(&mut schema)?;
    add_order_direction_enum(&mut schema);
    add_block_height_type(&mut schema);
    add_meta_field_type(&mut schema);
    add_types_for_object_types(&mut schema, &object_types)?;
    add_types_for_interface_types(&mut schema, &interface_types)?;
    add_field_arguments(&mut schema, input_schema)?;
    add_query_type(&mut schema, &object_types, &interface_types)?;
    add_subscription_type(&mut schema, &object_types, &interface_types)?;

    // Remove the `_Schema_` type from the generated schema.
    schema.definitions.retain(|d| match d {
        Definition::TypeDefinition(def @ TypeDefinition::Object(_)) => match def {
            TypeDefinition::Object(t) if t.name.eq(SCHEMA_TYPE_NAME) => false,
            _ => true,
        },
        _ => true,
    });

    Ok(schema)
}

/// Adds built-in GraphQL scalar types (`Int`, `String` etc.) to the schema.
fn add_builtin_scalar_types(schema: &mut Document) -> Result<(), APISchemaError> {
    for name in [
        "Boolean",
        "ID",
        "Int",
        "BigDecimal",
        "String",
        "Bytes",
        "BigInt",
    ]
    .iter()
    {
        match schema.get_named_type(name) {
            None => {
                let typedef = TypeDefinition::Scalar(ScalarType {
                    position: Pos::default(),
                    description: None,
                    name: name.to_string(),
                    directives: vec![],
                });
                let def = Definition::TypeDefinition(typedef);
                schema.definitions.push(def);
            }
            Some(_) => return Err(APISchemaError::TypeExists(name.to_string())),
        }
    }
    Ok(())
}

/// Add directive definitions for our custom directives
fn add_directives(schema: &mut Document) {
    let entity = Definition::DirectiveDefinition(DirectiveDefinition {
        position: Pos::default(),
        description: None,
        name: "entity".to_owned(),
        arguments: vec![],
        locations: vec![DirectiveLocation::Object],
        repeatable: false,
    });

    let derived_from = Definition::DirectiveDefinition(DirectiveDefinition {
        position: Pos::default(),
        description: None,
        name: "derivedFrom".to_owned(),
        arguments: vec![InputValue {
            position: Pos::default(),
            description: None,
            name: "field".to_owned(),
            value_type: Type::NamedType("String".to_owned()),
            default_value: None,
            directives: vec![],
        }],
        locations: vec![DirectiveLocation::FieldDefinition],
        repeatable: false,
    });

    let subgraph_id = Definition::DirectiveDefinition(DirectiveDefinition {
        position: Pos::default(),
        description: None,
        name: "subgraphId".to_owned(),
        arguments: vec![InputValue {
            position: Pos::default(),
            description: None,
            name: "id".to_owned(),
            value_type: Type::NamedType("String".to_owned()),
            default_value: None,
            directives: vec![],
        }],
        locations: vec![DirectiveLocation::Object],
        repeatable: false,
    });

    schema.definitions.push(entity);
    schema.definitions.push(derived_from);
    schema.definitions.push(subgraph_id);
}

/// Adds a global `OrderDirection` type to the schema.
fn add_order_direction_enum(schema: &mut Document) {
    let typedef = TypeDefinition::Enum(EnumType {
        position: Pos::default(),
        description: None,
        name: "OrderDirection".to_string(),
        directives: vec![],
        values: ["asc", "desc"]
            .iter()
            .map(|name| EnumValue {
                position: Pos::default(),
                description: None,
                name: name.to_string(),
                directives: vec![],
            })
            .collect(),
    });
    let def = Definition::TypeDefinition(typedef);
    schema.definitions.push(def);
}

/// Adds a global `Block_height` type to the schema. The `block` argument
/// accepts values of this type
fn add_block_height_type(schema: &mut Document) {
    let typedef = TypeDefinition::InputObject(InputObjectType {
        position: Pos::default(),
        description: None,
        name: BLOCK_HEIGHT.to_string(),
        directives: vec![],
        fields: vec![
            InputValue {
                position: Pos::default(),
                description: None,
                name: "hash".to_owned(),
                value_type: Type::NamedType("Bytes".to_owned()),
                default_value: None,
                directives: vec![],
            },
            InputValue {
                position: Pos::default(),
                description: None,
                name: "number".to_owned(),
                value_type: Type::NamedType("Int".to_owned()),
                default_value: None,
                directives: vec![],
            },
            InputValue {
                position: Pos::default(),
                description: None,
                name: "number_gte".to_owned(),
                value_type: Type::NamedType("Int".to_owned()),
                default_value: None,
                directives: vec![],
            },
        ],
    });
    let def = Definition::TypeDefinition(typedef);
    schema.definitions.push(def);
}

/// Adds a global `_Meta_` type to the schema. The `_meta` field
/// accepts values of this type
fn add_meta_field_type(schema: &mut Document) {
    lazy_static! {
        static ref META_FIELD_SCHEMA: Document = {
            let schema = include_str!("meta.graphql");
            parse_schema(schema).expect("the schema `meta.graphql` is invalid")
        };
    }

    schema
        .definitions
        .extend(META_FIELD_SCHEMA.definitions.iter().cloned());
}

fn add_types_for_object_types(
    schema: &mut Document,
    object_types: &Vec<&ObjectType>,
) -> Result<(), APISchemaError> {
    for object_type in object_types {
        if !object_type.name.eq(SCHEMA_TYPE_NAME) {
            add_order_by_type(schema, &object_type.name, &object_type.fields)?;
            add_filter_type(schema, &object_type.name, &object_type.fields)?;
        }
    }
    Ok(())
}

/// Adds `*_orderBy` and `*_filter` enum types for the given interfaces to the schema.
fn add_types_for_interface_types(
    schema: &mut Document,
    interface_types: &[&InterfaceType],
) -> Result<(), APISchemaError> {
    for interface_type in interface_types {
        add_order_by_type(schema, &interface_type.name, &interface_type.fields)?;
        add_filter_type(schema, &interface_type.name, &interface_type.fields)?;
    }
    Ok(())
}

/// Adds a `<type_name>_orderBy` enum type for the given fields to the schema.
fn add_order_by_type(
    schema: &mut Document,
    type_name: &str,
    fields: &[Field],
) -> Result<(), APISchemaError> {
    let type_name = format!("{}_orderBy", type_name).to_string();

    match schema.get_named_type(&type_name) {
        None => {
            let typedef = TypeDefinition::Enum(EnumType {
                position: Pos::default(),
                description: None,
                name: type_name,
                directives: vec![],
                values: fields
                    .iter()
                    .map(|field| &field.name)
                    .map(|name| EnumValue {
                        position: Pos::default(),
                        description: None,
                        name: name.to_owned(),
                        directives: vec![],
                    })
                    .collect(),
            });
            let def = Definition::TypeDefinition(typedef);
            schema.definitions.push(def);
        }
        Some(_) => return Err(APISchemaError::TypeExists(type_name)),
    }
    Ok(())
}

/// Adds a `<type_name>_filter` enum type for the given fields to the schema.
fn add_filter_type(
    schema: &mut Document,
    type_name: &str,
    fields: &[Field],
) -> Result<(), APISchemaError> {
    let filter_type_name = format!("{}_filter", type_name).to_string();
    match schema.get_named_type(&filter_type_name) {
        None => {
            let typedef = TypeDefinition::InputObject(InputObjectType {
                position: Pos::default(),
                description: None,
                name: filter_type_name,
                directives: vec![],
                fields: field_input_values(schema, fields)?,
            });
            let def = Definition::TypeDefinition(typedef);
            schema.definitions.push(def);
        }
        Some(_) => return Err(APISchemaError::TypeExists(filter_type_name)),
    }

    Ok(())
}

/// Generates `*_filter` input values for the given set of fields.
fn field_input_values(
    schema: &Document,
    fields: &[Field],
) -> Result<Vec<InputValue>, APISchemaError> {
    let mut input_values = vec![];
    for field in fields {
        input_values.extend(field_filter_input_values(schema, field, &field.field_type)?);
    }
    Ok(input_values)
}

/// Generates `*_filter` input values for the given field.
fn field_filter_input_values(
    schema: &Document,
    field: &Field,
    field_type: &Type,
) -> Result<Vec<InputValue>, APISchemaError> {
    match field_type {
        Type::NamedType(ref name) => {
            let named_type = schema
                .get_named_type(name)
                .ok_or_else(|| APISchemaError::TypeNotFound(name.clone()))?;
            Ok(match named_type {
                TypeDefinition::Object(_) | TypeDefinition::Interface(_) => {
                    // Only add `where` filter fields for object and interface fields
                    // if they are not @derivedFrom
                    if ast::get_derived_from_directive(field).is_some() {
                        vec![]
                    } else {
                        // We allow filtering with `where: { other: "some-id" }` and
                        // `where: { others: ["some-id", "other-id"] }`. In both cases,
                        // we allow ID strings as the values to be passed to these
                        // filters.
                        let mut input_values = field_scalar_filter_input_values(
                            schema,
                            field,
                            &ScalarType::new(String::from("String")),
                        );

                        extend_with_child_filter_input_value(field, name, &mut input_values);

                        input_values
                    }
                }
                TypeDefinition::Scalar(ref t) => field_scalar_filter_input_values(schema, field, t),
                TypeDefinition::Enum(ref t) => field_enum_filter_input_values(schema, field, t),
                _ => vec![],
            })
        }
        Type::ListType(ref t) => {
            Ok(field_list_filter_input_values(schema, field, t).unwrap_or(vec![]))
        }
        Type::NonNullType(ref t) => field_filter_input_values(schema, field, t),
    }
}

/// Generates `*_filter` input values for the given scalar field.
fn field_scalar_filter_input_values(
    _schema: &Document,
    field: &Field,
    field_type: &ScalarType,
) -> Vec<InputValue> {
    match field_type.name.as_ref() {
        "BigInt" => vec!["", "not", "gt", "lt", "gte", "lte", "in", "not_in"],
        "Boolean" => vec!["", "not", "in", "not_in"],
        "Bytes" => vec!["", "not", "in", "not_in", "contains", "not_contains"],
        "BigDecimal" => vec!["", "not", "gt", "lt", "gte", "lte", "in", "not_in"],
        "ID" => vec!["", "not", "gt", "lt", "gte", "lte", "in", "not_in"],
        "Int" => vec!["", "not", "gt", "lt", "gte", "lte", "in", "not_in"],
        "String" => vec![
            "",
            "not",
            "gt",
            "lt",
            "gte",
            "lte",
            "in",
            "not_in",
            "contains",
            "not_contains",
            "starts_with",
            "not_starts_with",
            "ends_with",
            "not_ends_with",
        ],
        _ => vec!["", "not"],
    }
    .into_iter()
    .map(|filter_type| {
        let field_type = Type::NamedType(field_type.name.to_owned());
        let value_type = match filter_type {
            "in" | "not_in" => Type::ListType(Box::new(Type::NonNullType(Box::new(field_type)))),
            _ => field_type,
        };
        input_value(&field.name, filter_type, value_type)
    })
    .collect()
}

/// Appends a child filter to input values
fn extend_with_child_filter_input_value(
    field: &Field,
    field_type_name: &String,
    input_values: &mut Vec<InputValue>,
) {
    input_values.push(input_value(
        &format!("{}_", field.name),
        "",
        Type::NamedType(format!("{}_filter", field_type_name)),
    ));
}

/// Generates `*_filter` input values for the given enum field.
fn field_enum_filter_input_values(
    _schema: &Document,
    field: &Field,
    field_type: &EnumType,
) -> Vec<InputValue> {
    vec!["", "not", "in", "not_in"]
        .into_iter()
        .map(|filter_type| {
            let field_type = Type::NamedType(field_type.name.to_owned());
            let value_type = match filter_type {
                "in" | "not_in" => {
                    Type::ListType(Box::new(Type::NonNullType(Box::new(field_type))))
                }
                _ => field_type,
            };
            input_value(&field.name, filter_type, value_type)
        })
        .collect()
}

/// Generates `*_filter` input values for the given list field.
fn field_list_filter_input_values(
    schema: &Document,
    field: &Field,
    field_type: &Type,
) -> Option<Vec<InputValue>> {
    // Only add a filter field if the type of the field exists in the schema
    ast::get_type_definition_from_type(schema, field_type).and_then(|typedef| {
        // Decide what type of values can be passed to the filter. In the case
        // one-to-many or many-to-many object or interface fields that are not
        // derived, we allow ID strings to be passed on.
        let (input_field_type, parent_type_name) = match typedef {
            TypeDefinition::Object(parent) => {
                if ast::get_derived_from_directive(field).is_some() {
                    return None;
                } else {
                    (Type::NamedType("String".into()), Some(parent.name.clone()))
                }
            }
            TypeDefinition::Interface(parent) => {
                if ast::get_derived_from_directive(field).is_some() {
                    return None;
                } else {
                    (Type::NamedType("String".into()), Some(parent.name.clone()))
                }
            }
            TypeDefinition::Scalar(ref t) => (Type::NamedType(t.name.to_owned()), None),
            TypeDefinition::Enum(ref t) => (Type::NamedType(t.name.to_owned()), None),
            TypeDefinition::InputObject(_) | TypeDefinition::Union(_) => return None,
        };

        let mut input_values: Vec<InputValue> = vec!["", "not", "contains", "not_contains"]
            .into_iter()
            .map(|filter_type| {
                input_value(
                    &field.name,
                    filter_type,
                    Type::ListType(Box::new(Type::NonNullType(Box::new(
                        input_field_type.clone(),
                    )))),
                )
            })
            .collect();

        if let Some(parent) = parent_type_name {
            extend_with_child_filter_input_value(field, &parent, &mut input_values);
        }

        Some(input_values)
    })
}

/// Generates a `*_filter` input value for the given field name, suffix and value type.
fn input_value(name: &str, suffix: &'static str, value_type: Type) -> InputValue {
    InputValue {
        position: Pos::default(),
        description: None,
        name: if suffix.is_empty() {
            name.to_owned()
        } else {
            format!("{}_{}", name, suffix)
        },
        value_type,
        default_value: None,
        directives: vec![],
    }
}

/// Adds a root `Query` object type to the schema.
fn add_query_type(
    schema: &mut Document,
    object_types: &[&ObjectType],
    interface_types: &[&InterfaceType],
) -> Result<(), APISchemaError> {
    let type_name = String::from("Query");

    if schema.get_named_type(&type_name).is_some() {
        return Err(APISchemaError::TypeExists(type_name));
    }

    let mut fields = object_types
        .iter()
        .map(|t| t.name.as_str())
        .filter(|name| !name.eq(&SCHEMA_TYPE_NAME))
        .chain(interface_types.iter().map(|t| t.name.as_str()))
        .flat_map(|name| query_fields_for_type(name))
        .collect::<Vec<Field>>();
    let mut fulltext_fields = schema
        .get_fulltext_directives()
        .map_err(|_| APISchemaError::FulltextSearchNonDeterministic)?
        .iter()
        .filter_map(|fulltext| query_field_for_fulltext(fulltext))
        .collect();
    fields.append(&mut fulltext_fields);
    fields.push(meta_field());

    let typedef = TypeDefinition::Object(ObjectType {
        position: Pos::default(),
        description: None,
        name: type_name,
        implements_interfaces: vec![],
        directives: vec![],
        fields: fields,
    });
    let def = Definition::TypeDefinition(typedef);
    schema.definitions.push(def);
    Ok(())
}

fn query_field_for_fulltext(fulltext: &Directive) -> Option<Field> {
    let name = fulltext.argument("name").unwrap().as_str().unwrap().into();

    let includes = fulltext.argument("include").unwrap().as_list().unwrap();
    // Only one include is allowed per fulltext directive
    let include = includes.iter().next().unwrap();
    let included_entity = include.as_object().unwrap();
    let entity_name = included_entity.get("entity").unwrap().as_str().unwrap();

    let mut arguments = vec![
        // text: String
        InputValue {
            position: Pos::default(),
            description: None,
            name: String::from("text"),
            value_type: Type::NonNullType(Box::new(Type::NamedType(String::from("String")))),
            default_value: None,
            directives: vec![],
        },
        // first: Int
        InputValue {
            position: Pos::default(),
            description: None,
            name: String::from("first"),
            value_type: Type::NamedType(String::from("Int")),
            default_value: Some(Value::Int(100.into())),
            directives: vec![],
        },
        // skip: Int
        InputValue {
            position: Pos::default(),
            description: None,
            name: String::from("skip"),
            value_type: Type::NamedType(String::from("Int")),
            default_value: Some(Value::Int(0.into())),
            directives: vec![],
        },
        // block: BlockHeight
        block_argument(),
    ];

    arguments.push(subgraph_error_argument());

    Some(Field {
        position: Pos::default(),
        description: None,
        name,
        arguments: arguments,
        field_type: Type::NonNullType(Box::new(Type::ListType(Box::new(Type::NonNullType(
            Box::new(Type::NamedType(entity_name.into())),
        ))))), // included entity type name
        directives: vec![fulltext.clone()],
    })
}

/// Adds a root `Subscription` object type to the schema.
fn add_subscription_type(
    schema: &mut Document,
    object_types: &[&ObjectType],
    interface_types: &[&InterfaceType],
) -> Result<(), APISchemaError> {
    let type_name = String::from("Subscription");

    if schema.get_named_type(&type_name).is_some() {
        return Err(APISchemaError::TypeExists(type_name));
    }

    let mut fields: Vec<Field> = object_types
        .iter()
        .map(|t| &t.name)
        .filter(|name| !name.eq(&SCHEMA_TYPE_NAME))
        .chain(interface_types.iter().map(|t| &t.name))
        .flat_map(|name| query_fields_for_type(name))
        .collect();
    fields.push(meta_field());

    let typedef = TypeDefinition::Object(ObjectType {
        position: Pos::default(),
        description: None,
        name: type_name,
        implements_interfaces: vec![],
        directives: vec![],
        fields,
    });
    let def = Definition::TypeDefinition(typedef);
    schema.definitions.push(def);
    Ok(())
}

fn block_argument() -> InputValue {
    InputValue {
        position: Pos::default(),
        description: Some(
            "The block at which the query should be executed. \
             Can either be a `{ hash: Bytes }` value containing a block hash, \
             a `{ number: Int }` containing the block number, \
             or a `{ number_gte: Int }` containing the minimum block number. \
             In the case of `number_gte`, the query will be executed on the latest block only if \
             the subgraph has progressed to or past the minimum block number. \
             Defaults to the latest block when omitted."
                .to_owned(),
        ),
        name: "block".to_string(),
        value_type: Type::NamedType(BLOCK_HEIGHT.to_owned()),
        default_value: None,
        directives: vec![],
    }
}

fn subgraph_error_argument() -> InputValue {
    InputValue {
        position: Pos::default(),
        description: Some(
            "Set to `allow` to receive data even if the subgraph has skipped over errors while syncing."
                .to_owned(),
        ),
        name: "subgraphError".to_string(),
        value_type: Type::NonNullType(Box::new(Type::NamedType(ERROR_POLICY_TYPE.to_string()))),
        default_value: Some(Value::Enum("deny".to_string())),
        directives: vec![],
    }
}

/// Generates `Query` fields for the given type name (e.g. `users` and `user`).
fn query_fields_for_type(type_name: &str) -> Vec<Field> {
    let mut collection_arguments = collection_arguments_for_named_type(type_name);
    collection_arguments.push(block_argument());

    let mut by_id_arguments = vec![
        InputValue {
            position: Pos::default(),
            description: None,
            name: "id".to_string(),
            value_type: Type::NonNullType(Box::new(Type::NamedType("ID".to_string()))),
            default_value: None,
            directives: vec![],
        },
        block_argument(),
    ];

    collection_arguments.push(subgraph_error_argument());
    by_id_arguments.push(subgraph_error_argument());

    vec![
        Field {
            position: Pos::default(),
            description: None,
            name: type_name.to_camel_case(), // Name formatting must be updated in sync with `graph::data::schema::validate_fulltext_directive_name()`
            arguments: by_id_arguments,
            field_type: Type::NamedType(type_name.to_owned()),
            directives: vec![],
        },
        Field {
            position: Pos::default(),
            description: None,
            name: type_name.to_plural().to_camel_case(), // Name formatting must be updated in sync with `graph::data::schema::validate_fulltext_directive_name()`
            arguments: collection_arguments,
            field_type: Type::NonNullType(Box::new(Type::ListType(Box::new(Type::NonNullType(
                Box::new(Type::NamedType(type_name.to_owned())),
            ))))),
            directives: vec![],
        },
    ]
}

fn meta_field() -> Field {
    lazy_static! {
        static ref META_FIELD: Field = Field {
            position: Pos::default(),
            description: Some("Access to subgraph metadata".to_string()),
            name: META_FIELD_NAME.to_string(),
            arguments: vec![
                // block: BlockHeight
                InputValue {
                    position: Pos::default(),
                    description: None,
                    name: String::from("block"),
                    value_type: Type::NamedType(BLOCK_HEIGHT.to_string()),
                    default_value: None,
                    directives: vec![],
                },
            ],
            field_type: Type::NamedType(META_FIELD_TYPE.to_string()),
            directives: vec![],
        };
    }
    META_FIELD.clone()
}

/// Generates arguments for collection queries of a named type (e.g. User).
fn collection_arguments_for_named_type(type_name: &str) -> Vec<InputValue> {
    // `first` and `skip` should be non-nullable, but the Apollo graphql client
    // exhibts non-conforming behaviour by erroing if no value is provided for a
    // non-nullable field, regardless of the presence of a default.
    let mut skip = input_value(&"skip".to_string(), "", Type::NamedType("Int".to_string()));
    skip.default_value = Some(Value::Int(0.into()));

    let mut first = input_value(&"first".to_string(), "", Type::NamedType("Int".to_string()));
    first.default_value = Some(Value::Int(100.into()));

    let args = vec![
        skip,
        first,
        input_value(
            &"orderBy".to_string(),
            "",
            Type::NamedType(format!("{}_orderBy", type_name)),
        ),
        input_value(
            &"orderDirection".to_string(),
            "",
            Type::NamedType("OrderDirection".to_string()),
        ),
        input_value(
            &"where".to_string(),
            "",
            Type::NamedType(format!("{}_filter", type_name)),
        ),
    ];

    args
}

fn add_field_arguments(
    schema: &mut Document,
    input_schema: &Document,
) -> Result<(), APISchemaError> {
    // Refactor: Remove the `input_schema` argument and do a mutable iteration
    // over the definitions in `schema`. Also the duplication between this and
    // the loop for interfaces below.
    for input_object_type in input_schema.get_object_type_definitions() {
        for input_field in &input_object_type.fields {
            if let Some(input_reference_type) =
                ast::get_referenced_entity_type(input_schema, input_field)
            {
                if ast::is_list_or_non_null_list_field(input_field) {
                    // Get corresponding object type and field in the output schema
                    let object_type = ast::get_object_type_mut(schema, &input_object_type.name)
                        .expect("object type from input schema is missing in API schema");
                    let mut field = object_type
                        .fields
                        .iter_mut()
                        .find(|field| field.name == input_field.name)
                        .expect("field from input schema is missing in API schema");

                    match input_reference_type {
                        TypeDefinition::Object(ot) => {
                            field.arguments = collection_arguments_for_named_type(&ot.name);
                        }
                        TypeDefinition::Interface(it) => {
                            field.arguments = collection_arguments_for_named_type(&it.name);
                        }
                        _ => unreachable!(
                            "referenced entity types can only be object or interface types"
                        ),
                    }
                }
            }
        }
    }

    for input_interface_type in input_schema.get_interface_type_definitions() {
        for input_field in &input_interface_type.fields {
            if let Some(input_reference_type) =
                ast::get_referenced_entity_type(input_schema, &input_field)
            {
                if ast::is_list_or_non_null_list_field(&input_field) {
                    // Get corresponding interface type and field in the output schema
                    let interface_type =
                        ast::get_interface_type_mut(schema, &input_interface_type.name)
                            .expect("interface type from input schema is missing in API schema");
                    let mut field = interface_type
                        .fields
                        .iter_mut()
                        .find(|field| field.name == input_field.name)
                        .expect("field from input schema is missing in API schema");

                    match input_reference_type {
                        TypeDefinition::Object(ot) => {
                            field.arguments = collection_arguments_for_named_type(&ot.name);
                        }
                        TypeDefinition::Interface(it) => {
                            field.arguments = collection_arguments_for_named_type(&it.name);
                        }
                        _ => unreachable!(
                            "referenced entity types can only be object or interface types"
                        ),
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use graph::data::graphql::DocumentExt;
    use graphql_parser::schema::*;

    use super::api_schema;
    use crate::schema::ast;

    #[test]
    fn api_schema_contains_built_in_scalar_types() {
        let input_schema =
            parse_schema("type User { id: ID! }").expect("Failed to parse input schema");
        let schema = api_schema(&input_schema).expect("Failed to derive API schema");

        schema
            .get_named_type("Boolean")
            .expect("Boolean type is missing in API schema");
        schema
            .get_named_type("ID")
            .expect("ID type is missing in API schema");
        schema
            .get_named_type("Int")
            .expect("Int type is missing in API schema");
        schema
            .get_named_type("BigDecimal")
            .expect("BigDecimal type is missing in API schema");
        schema
            .get_named_type("String")
            .expect("String type is missing in API schema");
    }

    #[test]
    fn api_schema_contains_order_direction_enum() {
        let input_schema = parse_schema("type User { id: ID!, name: String! }")
            .expect("Failed to parse input schema");
        let schema = api_schema(&input_schema).expect("Failed to derived API schema");

        let order_direction = schema
            .get_named_type("OrderDirection")
            .expect("OrderDirection type is missing in derived API schema");
        let enum_type = match order_direction {
            TypeDefinition::Enum(t) => Some(t),
            _ => None,
        }
        .expect("OrderDirection type is not an enum");

        let values: Vec<&str> = enum_type
            .values
            .iter()
            .map(|value| value.name.as_str())
            .collect();
        assert_eq!(values, ["asc", "desc"]);
    }

    #[test]
    fn api_schema_contains_query_type() {
        let input_schema =
            parse_schema("type User { id: ID! }").expect("Failed to parse input schema");
        let schema = api_schema(&input_schema).expect("Failed to derive API schema");
        schema
            .get_named_type("Query")
            .expect("Root Query type is missing in API schema");
    }

    #[test]
    fn api_schema_contains_field_order_by_enum() {
        let input_schema = parse_schema("type User { id: ID!, name: String! }")
            .expect("Failed to parse input schema");
        let schema = api_schema(&input_schema).expect("Failed to derived API schema");

        let user_order_by = schema
            .get_named_type("User_orderBy")
            .expect("User_orderBy type is missing in derived API schema");

        let enum_type = match user_order_by {
            TypeDefinition::Enum(t) => Some(t),
            _ => None,
        }
        .expect("User_orderBy type is not an enum");

        let values: Vec<&str> = enum_type
            .values
            .iter()
            .map(|value| value.name.as_str())
            .collect();
        assert_eq!(values, ["id", "name"]);
    }

    #[test]
    fn api_schema_contains_object_type_filter_enum() {
        let input_schema = parse_schema(
            r#"
              enum FurType {
                  NONE
                  FLUFFY
                  BRISTLY
              }

              type Pet {
                  id: ID!
                  name: String!
                  mostHatedBy: [User!]!
                  mostLovedBy: [User!]!
              }

              type User {
                  id: ID!
                  name: String!
                  favoritePetNames: [String!]
                  pets: [Pet!]!
                  favoriteFurType: FurType!
                  favoritePet: Pet!
                  leastFavoritePet: Pet @derivedFrom(field: "mostHatedBy")
                  mostFavoritePets: [Pet!] @derivedFrom(field: "mostLovedBy")
              }
            "#,
        )
        .expect("Failed to parse input schema");
        let schema = api_schema(&input_schema).expect("Failed to derived API schema");

        let user_filter = schema
            .get_named_type("User_filter")
            .expect("User_filter type is missing in derived API schema");

        let filter_type = match user_filter {
            TypeDefinition::InputObject(t) => Some(t),
            _ => None,
        }
        .expect("User_filter type is not an input object");

        assert_eq!(
            filter_type
                .fields
                .iter()
                .map(|field| field.name.to_owned())
                .collect::<Vec<String>>(),
            [
                "id",
                "id_not",
                "id_gt",
                "id_lt",
                "id_gte",
                "id_lte",
                "id_in",
                "id_not_in",
                "name",
                "name_not",
                "name_gt",
                "name_lt",
                "name_gte",
                "name_lte",
                "name_in",
                "name_not_in",
                "name_contains",
                "name_not_contains",
                "name_starts_with",
                "name_not_starts_with",
                "name_ends_with",
                "name_not_ends_with",
                "favoritePetNames",
                "favoritePetNames_not",
                "favoritePetNames_contains",
                "favoritePetNames_not_contains",
                "pets",
                "pets_not",
                "pets_contains",
                "pets_not_contains",
                "pets_",
                "favoriteFurType",
                "favoriteFurType_not",
                "favoriteFurType_in",
                "favoriteFurType_not_in",
                "favoritePet",
                "favoritePet_not",
                "favoritePet_gt",
                "favoritePet_lt",
                "favoritePet_gte",
                "favoritePet_lte",
                "favoritePet_in",
                "favoritePet_not_in",
                "favoritePet_contains",
                "favoritePet_not_contains",
                "favoritePet_starts_with",
                "favoritePet_not_starts_with",
                "favoritePet_ends_with",
                "favoritePet_not_ends_with",
                "favoritePet_",
            ]
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>()
        );
    }

    #[test]
    fn api_schema_contains_object_fields_on_query_type() {
        let input_schema = parse_schema(
            "type User { id: ID!, name: String! } type UserProfile { id: ID!, title: String! }",
        )
        .expect("Failed to parse input schema");
        let schema = api_schema(&input_schema).expect("Failed to derive API schema");

        let query_type = schema
            .get_named_type("Query")
            .expect("Query type is missing in derived API schema");

        let user_singular_field = match query_type {
            TypeDefinition::Object(t) => ast::get_field(t, &"user".to_string()),
            _ => None,
        }
        .expect("\"user\" field is missing on Query type");

        assert_eq!(
            user_singular_field.field_type,
            Type::NamedType("User".to_string())
        );

        assert_eq!(
            user_singular_field
                .arguments
                .iter()
                .map(|input_value| input_value.name.to_owned())
                .collect::<Vec<String>>(),
            vec![
                "id".to_string(),
                "block".to_string(),
                "subgraphError".to_string()
            ],
        );

        let user_plural_field = match query_type {
            TypeDefinition::Object(t) => ast::get_field(t, &"users".to_string()),
            _ => None,
        }
        .expect("\"users\" field is missing on Query type");

        assert_eq!(
            user_plural_field.field_type,
            Type::NonNullType(Box::new(Type::ListType(Box::new(Type::NonNullType(
                Box::new(Type::NamedType("User".to_string()))
            )))))
        );

        assert_eq!(
            user_plural_field
                .arguments
                .iter()
                .map(|input_value| input_value.name.to_owned())
                .collect::<Vec<String>>(),
            [
                "skip",
                "first",
                "orderBy",
                "orderDirection",
                "where",
                "block",
                "subgraphError",
            ]
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>()
        );

        let user_profile_singular_field = match query_type {
            TypeDefinition::Object(t) => ast::get_field(t, &"userProfile".to_string()),
            _ => None,
        }
        .expect("\"userProfile\" field is missing on Query type");

        assert_eq!(
            user_profile_singular_field.field_type,
            Type::NamedType("UserProfile".to_string())
        );

        let user_profile_plural_field = match query_type {
            TypeDefinition::Object(t) => ast::get_field(t, &"userProfiles".to_string()),
            _ => None,
        }
        .expect("\"userProfiles\" field is missing on Query type");

        assert_eq!(
            user_profile_plural_field.field_type,
            Type::NonNullType(Box::new(Type::ListType(Box::new(Type::NonNullType(
                Box::new(Type::NamedType("UserProfile".to_string()))
            )))))
        );
    }

    #[test]
    fn api_schema_contains_interface_fields_on_query_type() {
        let input_schema = parse_schema(
            "
            interface Node { id: ID!, name: String! }
            type User implements Node { id: ID!, name: String!, email: String }
            ",
        )
        .expect("Failed to parse input schema");
        let schema = api_schema(&input_schema).expect("Failed to derived API schema");

        let query_type = schema
            .get_named_type("Query")
            .expect("Query type is missing in derived API schema");

        let singular_field = match query_type {
            TypeDefinition::Object(ref t) => ast::get_field(t, &"node".to_string()),
            _ => None,
        }
        .expect("\"node\" field is missing on Query type");

        assert_eq!(
            singular_field.field_type,
            Type::NamedType("Node".to_string())
        );

        assert_eq!(
            singular_field
                .arguments
                .iter()
                .map(|input_value| input_value.name.to_owned())
                .collect::<Vec<String>>(),
            vec![
                "id".to_string(),
                "block".to_string(),
                "subgraphError".to_string()
            ],
        );

        let plural_field = match query_type {
            TypeDefinition::Object(ref t) => ast::get_field(t, &"nodes".to_string()),
            _ => None,
        }
        .expect("\"nodes\" field is missing on Query type");

        assert_eq!(
            plural_field.field_type,
            Type::NonNullType(Box::new(Type::ListType(Box::new(Type::NonNullType(
                Box::new(Type::NamedType("Node".to_string()))
            )))))
        );

        assert_eq!(
            plural_field
                .arguments
                .iter()
                .map(|input_value| input_value.name.to_owned())
                .collect::<Vec<String>>(),
            [
                "skip",
                "first",
                "orderBy",
                "orderDirection",
                "where",
                "block",
                "subgraphError"
            ]
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>()
        );
    }

    #[test]
    fn api_schema_contains_fulltext_query_field_on_query_type() {
        const SCHEMA: &str = r#"
type _Schema_ @fulltext(
  name: "metadata"
  language: en
  algorithm: rank
  include: [
    {
      entity: "Gravatar",
      fields: [
        { name: "displayName"},
        { name: "imageUrl"},
      ]
    }
  ]
)
type Gravatar @entity {
  id: ID!
  owner: Bytes!
  displayName: String!
  imageUrl: String!
}
"#;
        let input_schema = parse_schema(SCHEMA).expect("Failed to parse input schema");
        let schema = api_schema(&input_schema).expect("Failed to derive API schema");

        let query_type = schema
            .get_named_type("Query")
            .expect("Query type is missing in derived API schema");

        let _metadata_field = match query_type {
            TypeDefinition::Object(t) => ast::get_field(t, &String::from("metadata")),
            _ => None,
        }
        .expect("\"metadata\" field is missing on Query type");
    }

    #[test]
    fn api_schema_contains_child_entity_filter_on_lists() {
        let input_schema = parse_schema(
            r#"
                type Note @entity {
                    id: ID!
                    text: String!
                    author: User! @derived(field: "id")
                }
              
                type User @entity {
                    id: ID!
                    dateOfBirth: String
                    country: String
                    notes: [Note!]!
                }
            "#,
        )
        .expect("Failed to parse input schema");
        let schema = api_schema(&input_schema).expect("Failed to derived API schema");

        println!("{}", schema);

        let user_filter = schema
            .get_named_type("User_filter")
            .expect("User_filter type is missing in derived API schema");

        let filter_type = match user_filter {
            TypeDefinition::InputObject(t) => Some(t),
            _ => None,
        }
        .expect("User_filter type is not an input object");

        let user_notes_filter_field = filter_type
            .fields
            .iter()
            .find_map(|field| {
                if field.name == "notes_" {
                    Some(field)
                } else {
                    None
                }
            })
            .expect("notes_ field is missing in the User_filter input object");

        assert_eq!(user_notes_filter_field.name, "notes_");
        assert_eq!(
            user_notes_filter_field.value_type,
            Type::NamedType(String::from("Note_filter"))
        );
    }
}
