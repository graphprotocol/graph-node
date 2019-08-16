use graphql_parser::{query as q, schema as s, Pos};
use std::collections::{BTreeMap, HashMap};

use graph::prelude::*;

use crate::prelude::*;
use crate::schema::ast as sast;

type TypeObjectsMap = BTreeMap<String, q::Value>;

fn object_field<'a>(object: &'a Option<q::Value>, field: &str) -> Option<&'a q::Value> {
    object
        .as_ref()
        .and_then(|object| match object {
            q::Value::Object(ref data) => Some(data),
            _ => None,
        })
        .and_then(|data| data.get(field))
}

fn schema_type_objects(schema: &Schema) -> TypeObjectsMap {
    sast::get_type_definitions(&schema.document).iter().fold(
        BTreeMap::new(),
        |mut type_objects, typedef| {
            let type_name = sast::get_type_name(typedef);
            if !type_objects.contains_key(type_name) {
                let type_object = type_definition_object(schema, &mut type_objects, typedef);
                type_objects.insert(type_name.to_owned(), type_object);
            }
            type_objects
        },
    )
}

fn type_object(schema: &Schema, type_objects: &mut TypeObjectsMap, t: &s::Type) -> q::Value {
    match t {
        // We store the name of the named type here to be able to resolve it dynamically later
        s::Type::NamedType(s) => q::Value::String(s.to_owned()),
        s::Type::ListType(ref inner) => list_type_object(schema, type_objects, inner),
        s::Type::NonNullType(ref inner) => non_null_type_object(schema, type_objects, inner),
    }
}

fn list_type_object(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    inner_type: &s::Type,
) -> q::Value {
    object_value(vec![
        ("kind", q::Value::Enum(String::from("LIST"))),
        ("ofType", type_object(schema, type_objects, inner_type)),
    ])
}

fn non_null_type_object(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    inner_type: &s::Type,
) -> q::Value {
    object_value(vec![
        ("kind", q::Value::Enum(String::from("NON_NULL"))),
        ("ofType", type_object(schema, type_objects, inner_type)),
    ])
}

fn type_definition_object(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    typedef: &s::TypeDefinition,
) -> q::Value {
    let type_name = sast::get_type_name(typedef);

    type_objects.get(type_name).cloned().unwrap_or_else(|| {
        let type_object = match typedef {
            s::TypeDefinition::Enum(enum_type) => enum_type_object(enum_type),
            s::TypeDefinition::InputObject(input_object_type) => {
                input_object_type_object(schema, type_objects, input_object_type)
            }
            s::TypeDefinition::Interface(interface_type) => {
                interface_type_object(schema, type_objects, interface_type)
            }
            s::TypeDefinition::Object(object_type) => {
                object_type_object(schema, type_objects, object_type)
            }
            s::TypeDefinition::Scalar(scalar_type) => scalar_type_object(scalar_type),
            s::TypeDefinition::Union(union_type) => union_type_object(schema, union_type),
        };

        type_objects.insert(type_name.to_owned(), type_object.clone());
        type_object
    })
}

fn enum_type_object(enum_type: &s::EnumType) -> q::Value {
    object_value(vec![
        ("kind", q::Value::Enum(String::from("ENUM"))),
        ("name", q::Value::String(enum_type.name.to_owned())),
        (
            "description",
            enum_type
                .description
                .as_ref()
                .map_or(q::Value::Null, |s| q::Value::String(s.to_owned())),
        ),
        ("enumValues", enum_values(enum_type)),
    ])
}

fn enum_values(enum_type: &s::EnumType) -> q::Value {
    q::Value::List(enum_type.values.iter().map(enum_value).collect())
}

fn enum_value(enum_value: &s::EnumValue) -> q::Value {
    object_value(vec![
        ("name", q::Value::String(enum_value.name.to_owned())),
        (
            "description",
            enum_value
                .description
                .as_ref()
                .map_or(q::Value::Null, |s| q::Value::String(s.to_owned())),
        ),
        ("isDeprecated", q::Value::Boolean(false)),
        ("deprecationReason", q::Value::Null),
    ])
}

fn input_object_type_object(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    input_object_type: &s::InputObjectType,
) -> q::Value {
    let input_values = input_values(schema, type_objects, &input_object_type.fields);
    object_value(vec![
        ("name", q::Value::String(input_object_type.name.to_owned())),
        ("kind", q::Value::Enum(String::from("INPUT_OBJECT"))),
        (
            "description",
            input_object_type
                .description
                .as_ref()
                .map_or(q::Value::Null, |s| q::Value::String(s.to_owned())),
        ),
        ("inputFields", q::Value::List(input_values)),
    ])
}

fn interface_type_object(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    interface_type: &s::InterfaceType,
) -> q::Value {
    object_value(vec![
        ("name", q::Value::String(interface_type.name.to_owned())),
        ("kind", q::Value::Enum(String::from("INTERFACE"))),
        (
            "description",
            interface_type
                .description
                .as_ref()
                .map_or(q::Value::Null, |s| q::Value::String(s.to_owned())),
        ),
        (
            "fields",
            field_objects(schema, type_objects, &interface_type.fields),
        ),
        (
            "possibleTypes",
            q::Value::List(
                schema.types_for_interface()[&interface_type.name]
                    .iter()
                    .map(|object_type| q::Value::String(object_type.name.to_owned()))
                    .collect(),
            ),
        ),
    ])
}

fn object_type_object(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    object_type: &s::ObjectType,
) -> q::Value {
    type_objects
        .get(&object_type.name)
        .cloned()
        .unwrap_or_else(|| {
            let type_object = object_value(vec![
                ("kind", q::Value::Enum(String::from("OBJECT"))),
                ("name", q::Value::String(object_type.name.to_owned())),
                (
                    "description",
                    object_type
                        .description
                        .as_ref()
                        .map_or(q::Value::Null, |s| q::Value::String(s.to_owned())),
                ),
                (
                    "fields",
                    field_objects(schema, type_objects, &object_type.fields),
                ),
                (
                    "interfaces",
                    object_interfaces(schema, type_objects, object_type),
                ),
            ]);

            type_objects.insert(object_type.name.to_owned(), type_object.clone());
            type_object
        })
}

fn field_objects(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    fields: &[s::Field],
) -> q::Value {
    q::Value::List(
        fields
            .into_iter()
            .map(|field| field_object(schema, type_objects, field))
            .collect(),
    )
}

fn field_object(schema: &Schema, type_objects: &mut TypeObjectsMap, field: &s::Field) -> q::Value {
    object_value(vec![
        ("name", q::Value::String(field.name.to_owned())),
        (
            "description",
            field
                .description
                .as_ref()
                .map_or(q::Value::Null, |s| q::Value::String(s.to_owned())),
        ),
        (
            "args",
            q::Value::List(input_values(schema, type_objects, &field.arguments)),
        ),
        ("type", type_object(schema, type_objects, &field.field_type)),
        ("isDeprecated", q::Value::Boolean(false)),
        ("deprecationReason", q::Value::Null),
    ])
}

fn object_interfaces(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    object_type: &s::ObjectType,
) -> q::Value {
    q::Value::List(
        schema
            .interfaces_for_type(&object_type.name)
            .unwrap_or(&vec![])
            .iter()
            .map(|typedef| interface_type_object(schema, type_objects, typedef))
            .collect(),
    )
}

fn scalar_type_object(scalar_type: &s::ScalarType) -> q::Value {
    object_value(vec![
        ("name", q::Value::String(scalar_type.name.to_owned())),
        ("kind", q::Value::Enum(String::from("SCALAR"))),
        (
            "description",
            scalar_type
                .description
                .as_ref()
                .map_or(q::Value::Null, |s| q::Value::String(s.to_owned())),
        ),
        ("isDeprecated", q::Value::Boolean(false)),
        ("deprecationReason", q::Value::Null),
    ])
}

fn union_type_object(schema: &Schema, union_type: &s::UnionType) -> q::Value {
    object_value(vec![
        ("name", q::Value::String(union_type.name.to_owned())),
        ("kind", q::Value::Enum(String::from("UNION"))),
        (
            "description",
            union_type
                .description
                .as_ref()
                .map_or(q::Value::Null, |s| q::Value::String(s.to_owned())),
        ),
        (
            "possibleTypes",
            q::Value::List(
                sast::get_object_type_definitions(&schema.document)
                    .iter()
                    .filter(|object_type| {
                        object_type
                            .implements_interfaces
                            .iter()
                            .any(|implemented_name| implemented_name == &union_type.name)
                    })
                    .map(|object_type| q::Value::String(object_type.name.to_owned()))
                    .collect(),
            ),
        ),
    ])
}

fn schema_directive_objects(schema: &Schema, type_objects: &mut TypeObjectsMap) -> q::Value {
    q::Value::List(
        schema
            .document
            .definitions
            .iter()
            .filter_map(|d| match d {
                s::Definition::DirectiveDefinition(dd) => Some(dd),
                _ => None,
            })
            .map(|dd| directive_object(schema, type_objects, dd))
            .collect(),
    )
}

fn directive_object(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    directive: &s::DirectiveDefinition,
) -> q::Value {
    object_value(vec![
        ("name", q::Value::String(directive.name.to_owned())),
        (
            "description",
            directive
                .description
                .as_ref()
                .map_or(q::Value::Null, |s| q::Value::String(s.to_owned())),
        ),
        ("locations", directive_locations(directive)),
        (
            "args",
            q::Value::List(input_values(schema, type_objects, &directive.arguments)),
        ),
    ])
}

fn directive_locations(directive: &s::DirectiveDefinition) -> q::Value {
    q::Value::List(
        directive
            .locations
            .iter()
            .map(|location| location.as_str())
            .map(|name| q::Value::Enum(name.to_owned()))
            .collect(),
    )
}

fn input_values(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    input_values: &[s::InputValue],
) -> Vec<q::Value> {
    input_values
        .iter()
        .map(|value| input_value(schema, type_objects, value))
        .collect()
}

fn input_value(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    input_value: &s::InputValue,
) -> q::Value {
    object_value(vec![
        ("name", q::Value::String(input_value.name.to_owned())),
        (
            "description",
            input_value
                .description
                .as_ref()
                .map_or(q::Value::Null, |s| q::Value::String(s.to_owned())),
        ),
        (
            "type",
            type_object(schema, type_objects, &input_value.value_type),
        ),
        (
            "defaultValue",
            input_value
                .default_value
                .as_ref()
                .map_or(q::Value::Null, |value| {
                    q::Value::String(format!("{}", value))
                }),
        ),
    ])
}

#[derive(Clone)]
pub struct IntrospectionResolver<'a> {
    logger: Logger,
    schema: &'a Schema,
    type_objects: TypeObjectsMap,
    directives: q::Value,
}

impl<'a> IntrospectionResolver<'a> {
    pub fn new(logger: &Logger, schema: &'a Schema) -> Self {
        let logger = logger.new(o!("component" => "IntrospectionResolver"));

        // Generate queryable objects for all types in the schema
        let mut type_objects = schema_type_objects(schema);

        // Generate queryable objects for all directives in the schema
        let directives = schema_directive_objects(schema, &mut type_objects);

        IntrospectionResolver {
            logger,
            schema,
            type_objects,
            directives,
        }
    }

    fn schema_object(&self) -> q::Value {
        object_value(vec![
            (
                "queryType",
                self.type_objects
                    .get(&String::from("Query"))
                    .cloned()
                    .unwrap_or(q::Value::Null),
            ),
            (
                "subscriptionType",
                self.type_objects
                    .get(&String::from("Subscription"))
                    .cloned()
                    .unwrap_or(q::Value::Null),
            ),
            ("mutationType", q::Value::Null),
            (
                "types",
                q::Value::List(self.type_objects.values().cloned().collect::<Vec<_>>()),
            ),
            ("directives", self.directives.clone()),
        ])
    }

    fn type_object(&self, name: &q::Value) -> q::Value {
        match name {
            q::Value::String(s) => Some(s),
            _ => None,
        }
        .and_then(|name| self.type_objects.get(name).cloned())
        .unwrap_or(q::Value::Null)
    }
}

/// A GraphQL resolver that can resolve entities, enum values, scalar types and interfaces/unions.
impl<'a> Resolver for IntrospectionResolver<'a> {
    fn resolve_objects(
        &self,
        parent: &Option<q::Value>,
        field: &q::Name,
        _field_definition: &s::Field,
        _object_type: ObjectOrInterface<'_>,
        _arguments: &HashMap<&q::Name, q::Value>,
        _types_for_interface: &BTreeMap<Name, Vec<ObjectType>>,
        _max_first: u32,
    ) -> Result<q::Value, QueryExecutionError> {
        match field.as_str() {
            "possibleTypes" => {
                let type_names = object_field(parent, "possibleTypes")
                    .and_then(|value| match value {
                        q::Value::List(type_names) => Some(type_names.clone()),
                        _ => None,
                    })
                    .unwrap_or_else(|| vec![]);

                if !type_names.is_empty() {
                    Ok(q::Value::List(
                        type_names
                            .iter()
                            .filter_map(|type_name| match type_name {
                                q::Value::String(ref type_name) => Some(type_name),
                                _ => None,
                            })
                            .filter_map(|type_name| self.type_objects.get(type_name).cloned())
                            .collect(),
                    ))
                } else {
                    Ok(q::Value::Null)
                }
            }
            _ => object_field(parent, field.as_str())
                .map_or(Ok(q::Value::Null), |value| Ok(value.clone())),
        }
    }

    fn resolve_object(
        &self,
        parent: &Option<q::Value>,
        field: &q::Field,
        _field_definition: &s::Field,
        _object_type: ObjectOrInterface<'_>,
        arguments: &HashMap<&q::Name, q::Value>,
        _: &BTreeMap<Name, Vec<ObjectType>>,
    ) -> Result<q::Value, QueryExecutionError> {
        let object = match field.name.as_str() {
            "__schema" => self.schema_object(),
            "__type" => {
                let name = arguments.get(&String::from("name")).ok_or_else(|| {
                    QueryExecutionError::MissingArgumentError(
                        Pos::default(),
                        "missing argument `name` in `__type(name: String!)`".to_owned(),
                    )
                })?;
                self.type_object(name)
            }
            "type" => object_field(parent, "type")
                .and_then(|value| match value {
                    q::Value::String(type_name) => self.type_objects.get(type_name).cloned(),
                    _ => Some(value.clone()),
                })
                .unwrap_or(q::Value::Null),
            "ofType" => object_field(parent, "ofType")
                .and_then(|value| match value {
                    q::Value::String(type_name) => self.type_objects.get(type_name).cloned(),
                    _ => Some(value.clone()),
                })
                .unwrap_or(q::Value::Null),
            _ => object_field(parent, field.name.as_str())
                .cloned()
                .unwrap_or(q::Value::Null),
        };
        Ok(object)
    }
}
