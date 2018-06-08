use graphql_parser::schema::*;
use graphql_parser::Pos;
use inflector::Inflector;
use std::error::Error;
use std::fmt;
use std::iter::IntoIterator;

use ast::schema as ast;

#[derive(Debug)]
pub enum APISchemaError {
    TypeExists(String),
}

impl Error for APISchemaError {
    fn description(&self) -> &str {
        "API schema error"
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}

impl fmt::Display for APISchemaError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            APISchemaError::TypeExists(s) => {
                write!(f, "Type \"{}\" already exists in the input schema", s)
            }
        }
    }
}

/// Derives a full-fledged GraphQL API schema from an input schema.
///
/// The input schema should only have type/enum/interface/union definitions
/// and must not include a root Query type. This Query type is derived,
/// with all its fields and their input arguments, based on the existing
/// types.
pub fn api_schema(input_schema: &Document) -> Result<Document, APISchemaError> {
    let object_types = ast::get_object_type_definitions(input_schema);
    let interface_types = ast::get_interface_type_definitions(input_schema);

    let mut schema = input_schema.clone();
    add_builtin_scalar_types(&mut schema)?;
    add_order_direction_enum(&mut schema);
    add_types_for_object_types(&mut schema, &object_types)?;
    add_types_for_interface_types(&mut schema, &interface_types)?;
    add_query_type(&mut schema, &object_types, &interface_types)?;

    Ok(schema)
}

/// Adds built-in GraphQL scalar types (`Int`, `String` etc.) to the schema.
fn add_builtin_scalar_types(schema: &mut Document) -> Result<(), APISchemaError> {
    for name in ["Boolean", "ID", "Int", "Float", "String"].into_iter() {
        match ast::get_named_type(schema, &name.to_string()) {
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

/// Adds a global `OrderDirection` type to the schema.
fn add_order_direction_enum(schema: &mut Document) {
    let typedef = TypeDefinition::Enum(EnumType {
        position: Pos::default(),
        description: None,
        name: "OrderDirection".to_string(),
        directives: vec![],
        values: ["asc", "desc"]
            .into_iter()
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

/// Adds `*_orderBy` and `*_filter` enum types for the given object types to the schema.
fn add_types_for_object_types(
    schema: &mut Document,
    object_types: &Vec<&ObjectType>,
) -> Result<(), APISchemaError> {
    for object_type in object_types {
        add_order_by_type(schema, &object_type.name, &object_type.fields)?;
        add_filter_type(schema, &object_type.name, &object_type.fields)?;
    }
    Ok(())
}

/// Adds `*_orderBy` and `*_filter` enum types for the given interfaces to the schema.
fn add_types_for_interface_types(
    schema: &mut Document,
    interface_types: &Vec<&InterfaceType>,
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
    type_name: &Name,
    fields: &Vec<Field>,
) -> Result<(), APISchemaError> {
    let type_name = format!("{}_orderBy", type_name).to_string();

    match ast::get_named_type(schema, &type_name) {
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
    type_name: &Name,
    fields: &Vec<Field>,
) -> Result<(), APISchemaError> {
    let filter_type_name = format!("{}_filter", type_name).to_string();

    match ast::get_named_type(schema, &filter_type_name) {
        None => {
            let typedef = TypeDefinition::InputObject(InputObjectType {
                position: Pos::default(),
                description: None,
                name: filter_type_name,
                directives: vec![],
                fields: field_input_values(schema, fields),
            });
            let def = Definition::TypeDefinition(typedef);
            schema.definitions.push(def);
        }
        Some(_) => return Err(APISchemaError::TypeExists(filter_type_name)),
    }

    Ok(())
}

/// Generates `*_filter` input values for the given set of fields.
fn field_input_values(schema: &Document, fields: &Vec<Field>) -> Vec<InputValue> {
    fields
        .iter()
        .flat_map(|field| field_filter_input_values(schema, &field, &field.field_type))
        .collect()
}

/// Generates `*_filter` input values for the given field.
fn field_filter_input_values(
    schema: &Document,
    field: &Field,
    field_type: &Type,
) -> Vec<InputValue> {
    match field_type {
        Type::NamedType(ref name) => {
            let named_type = ast::get_named_type(schema, name)
                .expect(format!("Unable to resolve named type: {}", name).as_str());
            match named_type {
                TypeDefinition::Scalar(ref t) => field_scalar_filter_input_values(schema, field, t),
                TypeDefinition::Enum(ref t) => field_enum_filter_input_values(schema, field, t),
                _ => vec![],
            }
        }
        Type::ListType(ref t) => field_list_filter_input_values(schema, field, t),
        Type::NonNullType(ref t) => field_filter_input_values(schema, field, t),
    }
}

/// Generates `*_filter` input values for the given scalar field.
fn field_scalar_filter_input_values(
    _schema: &Document,
    field: &Field,
    field_type: &ScalarType,
) -> Vec<InputValue> {
    vec![
        Some(input_value(
            &field.name,
            "",
            Type::NamedType(field_type.name.to_owned()),
        )),
        Some(input_value(
            &field.name,
            "not",
            Type::NamedType(field_type.name.to_owned()),
        )),
    ].into_iter()
        .filter_map(|value_opt| value_opt)
        .collect()
}

/// Generates `*_filter` input values for the given enum field.
fn field_enum_filter_input_values(
    _schema: &Document,
    _field: &Field,
    _field_type: &EnumType,
) -> Vec<InputValue> {
    unimplemented!()
}

/// Generates `*_filter` input values for the given list field.
fn field_list_filter_input_values(
    _schema: &Document,
    _field: &Field,
    _field_type: &Type,
) -> Vec<InputValue> {
    unimplemented!()
}

/// Generates a `*_filter` input value for the given field name, suffix and value type.
fn input_value(name: &Name, suffix: &'static str, value_type: Type) -> InputValue {
    InputValue {
        position: Pos::default(),
        description: None,
        name: if suffix.is_empty() {
            name.to_owned()
        } else {
            format!("{}_{}", name, suffix)
        },
        value_type: value_type,
        default_value: None,
        directives: vec![],
    }
}

/// Adds a root `Query` object type to the schema.
fn add_query_type(
    schema: &mut Document,
    object_types: &Vec<&ObjectType>,
    interface_types: &Vec<&InterfaceType>,
) -> Result<(), APISchemaError> {
    if ast::get_named_type(schema, &"Query".to_string()).is_some() {
        return Err(APISchemaError::TypeExists("Query".to_owned()));
    }

    let typedef = TypeDefinition::Object(ObjectType {
        position: Pos::default(),
        description: None,
        name: "Query".to_string(),
        implements_interfaces: vec![],
        directives: vec![],
        fields: object_types
            .iter()
            .map(|t| &t.name)
            .chain(interface_types.iter().map(|t| &t.name))
            .flat_map(|name| query_fields_for_type(schema, name))
            .collect(),
    });
    let def = Definition::TypeDefinition(typedef);
    schema.definitions.push(def);
    Ok(())
}

/// Generates `Query` fields for the given type name (e.g. `users` and `user`).
fn query_fields_for_type(_schema: &Document, type_name: &Name) -> Vec<Field> {
    vec![
        Field {
            position: Pos::default(),
            description: None,
            name: type_name.to_lowercase(),
            arguments: vec![InputValue {
                position: Pos::default(),
                description: None,
                name: "id".to_string(),
                value_type: Type::NonNullType(Box::new(Type::NamedType("ID".to_string()))),
                default_value: None,
                directives: vec![],
            }],
            field_type: Type::NamedType(type_name.to_owned()),
            directives: vec![],
        },
        Field {
            position: Pos::default(),
            description: None,
            name: type_name.to_plural().to_lowercase(),
            arguments: vec![
                input_value(&"skip".to_string(), "", Type::NamedType("Int".to_string())),
                input_value(&"first".to_string(), "", Type::NamedType("Int".to_string())),
                input_value(&"last".to_string(), "", Type::NamedType("Int".to_string())),
                input_value(&"after".to_string(), "", Type::NamedType("ID".to_string())),
                input_value(&"before".to_string(), "", Type::NamedType("ID".to_string())),
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
            ],
            field_type: Type::NonNullType(Box::new(Type::ListType(Box::new(Type::NonNullType(
                Box::new(Type::NamedType(type_name.to_owned())),
            ))))),
            directives: vec![],
        },
    ]
}

#[cfg(test)]
mod tests {
    use graphql_parser::schema::*;

    use super::api_schema;
    use ast::schema as ast;

    #[test]
    fn api_schema_contains_built_in_scalar_types() {
        let input_schema =
            parse_schema("type User { id: ID! }").expect("Failed to parse input schema");
        let schema = api_schema(&input_schema).expect("Failed to derive API schema");

        ast::get_named_type(&schema, &"Boolean".to_string())
            .expect("Boolean type is missing in API schema");
        ast::get_named_type(&schema, &"ID".to_string()).expect("ID type is missing in API schema");
        ast::get_named_type(&schema, &"Int".to_string())
            .expect("Int type is missing in API schema");
        ast::get_named_type(&schema, &"Float".to_string())
            .expect("Float type is missing in API schema");
        ast::get_named_type(&schema, &"String".to_string())
            .expect("String type is missing in API schema");
    }

    #[test]
    fn api_schema_contains_order_direction_enum() {
        let input_schema = parse_schema("type User { id: ID!, name: String! }")
            .expect("Failed to parse input schema");
        let schema = api_schema(&input_schema).expect("Failed to derived API schema");

        let order_direction = ast::get_named_type(&schema, &"OrderDirection".to_string())
            .expect("OrderDirection type is missing in derived API schema");
        let enum_type = match order_direction {
            TypeDefinition::Enum(t) => Some(t),
            _ => None,
        }.expect("OrderDirection type is not an enum");

        let values: Vec<&Name> = enum_type.values.iter().map(|value| &value.name).collect();
        assert_eq!(values, [&"asc".to_string(), &"desc".to_string()]);
    }

    #[test]
    fn api_schema_contains_query_type() {
        let input_schema =
            parse_schema("type User { id: ID! }").expect("Failed to parse input schema");
        let schema = api_schema(&input_schema).expect("Failed to derive API schema");
        ast::get_named_type(&schema, &"Query".to_string())
            .expect("Root Query type is missing in API schema");
    }

    #[test]
    fn api_schema_contains_field_order_by_enum() {
        let input_schema = parse_schema("type User { id: ID!, name: String! }")
            .expect("Failed to parse input schema");
        let schema = api_schema(&input_schema).expect("Failed to derived API schema");

        let user_order_by = ast::get_named_type(&schema, &"User_orderBy".to_string())
            .expect("User_orderBy type is missing in derived API schema");

        let enum_type = match user_order_by {
            TypeDefinition::Enum(t) => Some(t),
            _ => None,
        }.expect("User_orderBy type is not an enum");

        let values: Vec<&Name> = enum_type.values.iter().map(|value| &value.name).collect();
        assert_eq!(values, [&"id".to_string(), &"name".to_string()]);
    }

    #[test]
    fn api_schema_contains_object_type_filter_enum() {
        let input_schema = parse_schema("type User { id: ID!, name: String! }")
            .expect("Failed to parse input schema");
        let schema = api_schema(&input_schema).expect("Failed to derived API schema");

        let user_filter = ast::get_named_type(&schema, &"User_filter".to_string())
            .expect("User_filter type is missing in derived API schema");

        let filter_type = match user_filter {
            TypeDefinition::InputObject(t) => Some(t),
            _ => None,
        }.expect("User_filter type is not an input object");

        assert_eq!(
            filter_type
                .fields
                .iter()
                .map(|field| field.name.to_owned())
                .collect::<Vec<String>>(),
            ["id", "id_not", "name", "name_not"]
                .iter()
                .map(|name| name.to_string())
                .collect::<Vec<String>>()
        );
    }

    #[test]
    fn api_schema_contains_object_fields_on_query_type() {
        let input_schema = parse_schema("type User { id: ID!, name: String! }")
            .expect("Failed to parse input schema");
        let schema = api_schema(&input_schema).expect("Failed to derived API schema");

        let query_type = ast::get_named_type(&schema, &"Query".to_string())
            .expect("Query type is missing in derived API schema");

        let singular_field = match query_type {
            TypeDefinition::Object(ref t) => ast::get_field_type(t, &"user".to_string()),
            _ => None,
        }.expect("\"user\" field is missing on Query type");

        assert_eq!(
            singular_field.field_type,
            Type::NamedType("User".to_string())
        );

        assert_eq!(
            singular_field
                .arguments
                .iter()
                .map(|input_value| input_value.name.to_owned())
                .collect::<Vec<String>>(),
            vec!["id".to_string()],
        );

        let plural_field = match query_type {
            TypeDefinition::Object(ref t) => ast::get_field_type(t, &"users".to_string()),
            _ => None,
        }.expect("\"users\" field is missing on Query type");

        assert_eq!(
            plural_field.field_type,
            Type::NonNullType(Box::new(Type::ListType(Box::new(Type::NonNullType(
                Box::new(Type::NamedType("User".to_string()))
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
                "last",
                "after",
                "before",
                "orderBy",
                "orderDirection",
                "where",
            ].into_iter()
                .map(|name| name.to_string())
                .collect::<Vec<String>>()
        );
    }

    #[test]
    fn api_schema_contains_interface_fields_on_query_type() {
        let input_schema = parse_schema(
            "
            interface Node { id: ID!, name: String! }
            type User implements Node { id: ID!, name: String!, email: String }
            ",
        ).expect("Failed to parse input schema");
        let schema = api_schema(&input_schema).expect("Failed to derived API schema");

        let query_type = ast::get_named_type(&schema, &"Query".to_string())
            .expect("Query type is missing in derived API schema");

        let singular_field = match query_type {
            TypeDefinition::Object(ref t) => ast::get_field_type(t, &"node".to_string()),
            _ => None,
        }.expect("\"node\" field is missing on Query type");

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
            vec!["id".to_string()],
        );

        let plural_field = match query_type {
            TypeDefinition::Object(ref t) => ast::get_field_type(t, &"nodes".to_string()),
            _ => None,
        }.expect("\"nodes\" field is missing on Query type");

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
                "last",
                "after",
                "before",
                "orderBy",
                "orderDirection",
                "where",
            ].into_iter()
                .map(|name| name.to_string())
                .collect::<Vec<String>>()
        );
    }
}
