use graphql_parser::{query as q, schema as s, Pos};
use std::collections::{BTreeMap, HashMap};

use graph::data::graphql::ObjectOrInterface;
use graph::prelude::*;

use crate::prelude::*;
use crate::schema::ast as sast;

type TypeObjectsMap = BTreeMap<String, q::Value<'static, String>>;

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

fn type_object(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    t: &s::Type<'static, String>,
) -> q::Value<'static, String> {
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
    inner_type: &s::Type<'static, String>,
) -> q::Value<'static, String> {
    object! {
        kind: q::Value::Enum(String::from("LIST")),
        ofType: type_object(schema, type_objects, inner_type),
    }
}

fn non_null_type_object(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    inner_type: &s::Type<'static, String>,
) -> q::Value<'static, String> {
    object! {
        kind: q::Value::Enum(String::from("NON_NULL")),
        ofType: type_object(schema, type_objects, inner_type),
    }
}

fn type_definition_object(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    typedef: &s::TypeDefinition<'static, String>,
) -> q::Value<'static, String> {
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

fn enum_type_object(enum_type: &s::EnumType<'static, String>) -> q::Value<'static, String> {
    object! {
        kind: q::Value::Enum(String::from("ENUM")),
        name: enum_type.name.to_owned(),
        description: enum_type.description.clone(),
        enumValues: enum_values(enum_type),
    }
}

fn enum_values(enum_type: &s::EnumType<'static, String>) -> q::Value<'static, String> {
    q::Value::List(enum_type.values.iter().map(enum_value).collect())
}

fn enum_value(enum_value: &s::EnumValue<'static, String>) -> q::Value<'static, String> {
    object! {
        name: enum_value.name.to_owned(),
        description: enum_value.description.clone(),
        isDeprecated: false,
        deprecationReason: q::Value::Null,
    }
}

fn input_object_type_object(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    input_object_type: &s::InputObjectType<'static, String>,
) -> q::Value<'static, String> {
    let input_values = input_values(schema, type_objects, &input_object_type.fields);
    object! {
        name: input_object_type.name.to_owned(),
        kind: q::Value::Enum(String::from("INPUT_OBJECT")),
        description: input_object_type.description.clone(),
        inputFields: input_values,
    }
}

fn interface_type_object(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    interface_type: &s::InterfaceType<'static, String>,
) -> q::Value<'static, String> {
    object! {
        name: interface_type.name.to_owned(),
        kind: q::Value::Enum(String::from("INTERFACE")),
        description: interface_type.description.clone(),
        fields:
            field_objects(schema, type_objects, &interface_type.fields),
        possibleTypes: schema.types_for_interface()[&interface_type.name]
            .iter()
            .map(|object_type| q::Value::String(object_type.name.to_owned()))
            .collect::<Vec<_>>(),
    }
}

fn object_type_object(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    object_type: &s::ObjectType<'static, String>,
) -> q::Value<'static, String> {
    type_objects
        .get(&object_type.name)
        .cloned()
        .unwrap_or_else(|| {
            let type_object = object! {
                kind: q::Value::Enum(String::from("OBJECT")),
                name: object_type.name.to_owned(),
                description: object_type.description.clone(),
                fields: field_objects(schema, type_objects, &object_type.fields),
                interfaces: object_interfaces(schema, type_objects, object_type),
            };

            type_objects.insert(object_type.name.to_owned(), type_object.clone());
            type_object
        })
}

fn field_objects(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    fields: &[s::Field<'static, String>],
) -> q::Value<'static, String> {
    q::Value::List(
        fields
            .into_iter()
            .map(|field| field_object(schema, type_objects, field))
            .collect(),
    )
}

fn field_object(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    field: &s::Field<'static, String>,
) -> q::Value<'static, String> {
    object! {
        name: field.name.to_owned(),
        description: field.description.clone(),
        args: input_values(schema, type_objects, &field.arguments),
        type: type_object(schema, type_objects, &field.field_type),
        isDeprecated: false,
        deprecationReason: q::Value::Null,
    }
}

fn object_interfaces(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    object_type: &s::ObjectType<'static, String>,
) -> q::Value<'static, String> {
    q::Value::List(
        schema
            .interfaces_for_type(&object_type.name)
            .unwrap_or(&vec![])
            .iter()
            .map(|typedef| interface_type_object(schema, type_objects, typedef))
            .collect(),
    )
}

fn scalar_type_object(scalar_type: &s::ScalarType<'static, String>) -> q::Value<'static, String> {
    object! {
        name: scalar_type.name.to_owned(),
        kind: q::Value::Enum(String::from("SCALAR")),
        description: scalar_type.description.clone(),
        isDeprecated: false,
        deprecationReason: q::Value::Null,
    }
}

fn union_type_object(
    schema: &Schema,
    union_type: &s::UnionType<'static, String>,
) -> q::Value<'static, String> {
    object! {
        name: union_type.name.to_owned(),
        kind: q::Value::Enum(String::from("UNION")),
        description: union_type.description.clone(),
        possibleTypes:
            sast::get_object_type_definitions(&schema.document)
                .iter()
                .filter(|object_type| {
                    object_type
                        .implements_interfaces
                        .iter()
                        .any(|implemented_name| implemented_name == &union_type.name)
                })
                .map(|object_type| q::Value::String(object_type.name.to_owned()))
                .collect::<Vec<_>>(),
    }
}

fn schema_directive_objects(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
) -> q::Value<'static, String> {
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
    directive: &s::DirectiveDefinition<'static, String>,
) -> q::Value<'static, String> {
    object! {
        name: directive.name.to_owned(),
        description: directive.description.clone(),
        locations: directive_locations(directive),
        args: input_values(schema, type_objects, &directive.arguments),
    }
}

fn directive_locations(
    directive: &s::DirectiveDefinition<'static, String>,
) -> q::Value<'static, String> {
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
    input_values: &[s::InputValue<'static, String>],
) -> Vec<q::Value<'static, String>> {
    input_values
        .iter()
        .map(|value| input_value(schema, type_objects, value))
        .collect()
}

fn input_value(
    schema: &Schema,
    type_objects: &mut TypeObjectsMap,
    input_value: &s::InputValue<'static, String>,
) -> q::Value<'static, String> {
    object! {
        name: input_value.name.to_owned(),
        description: input_value.description.clone(),
        type: type_object(schema, type_objects, &input_value.value_type),
        defaultValue:
            input_value
                .default_value
                .as_ref()
                .map_or(q::Value::Null, |value| {
                    q::Value::String(format!("{}", value))
                }),
    }
}

#[derive(Clone)]
pub struct IntrospectionResolver {
    logger: Logger,
    type_objects: TypeObjectsMap,
    directives: q::Value<'static, String>,
}

impl IntrospectionResolver {
    pub fn new(logger: &Logger, schema: &Schema) -> Self {
        let logger = logger.new(o!("component" => "IntrospectionResolver"));

        // Generate queryable objects for all types in the schema
        let mut type_objects = schema_type_objects(schema);

        // Generate queryable objects for all directives in the schema
        let directives = schema_directive_objects(schema, &mut type_objects);

        IntrospectionResolver {
            logger,
            type_objects,
            directives,
        }
    }

    fn schema_object(&self) -> q::Value<'static, String> {
        object! {
            queryType:
                self.type_objects
                    .get(&String::from("Query"))
                    .cloned(),
            subscriptionType:
                self.type_objects
                    .get(&String::from("Subscription"))
                    .cloned(),
            mutationType: q::Value::Null,
            types: self.type_objects.values().cloned().collect::<Vec<_>>(),
            directives: self.directives.clone(),
        }
    }

    fn type_object(&self, name: &q::Value<'static, String>) -> q::Value<'static, String> {
        match name {
            q::Value::String(s) => Some(s),
            _ => None,
        }
        .and_then(|name| self.type_objects.get(name).cloned())
        .unwrap_or(q::Value::Null)
    }
}

/// A GraphQL resolver that can resolve entities, enum values, scalar types and interfaces/unions.
impl Resolver for IntrospectionResolver {
    // `IntrospectionResolver` is not used as a "top level" resolver,
    // see `fn as_introspection_context`, so this value is irrelevant.
    const CACHEABLE: bool = false;

    fn prefetch(
        &self,
        _: &ExecutionContext<Self>,
        _: &q::SelectionSet<'static, String>,
    ) -> Result<Option<q::Value<'static, String>>, Vec<QueryExecutionError>> {
        Ok(None)
    }

    fn resolve_objects(
        &self,
        prefetched_objects: Option<q::Value<'static, String>>,
        field: &q::Field<'static, String>,
        _field_definition: &s::Field<'static, String>,
        _object_type: ObjectOrInterface<'_>,
        _arguments: &HashMap<&String, q::Value<'static, String>>,
    ) -> Result<q::Value<'static, String>, QueryExecutionError> {
        match field.name.as_str() {
            "possibleTypes" => {
                let type_names = match prefetched_objects {
                    Some(q::Value::List(type_names)) => Some(type_names),
                    _ => None,
                }
                .unwrap_or_default();

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
            _ => Ok(prefetched_objects.unwrap_or(q::Value::Null)),
        }
    }

    fn resolve_object(
        &self,
        prefetched_object: Option<q::Value<'static, String>>,
        field: &q::Field<'static, String>,
        _field_definition: &s::Field<'static, String>,
        _object_type: ObjectOrInterface<'_>,
        arguments: &HashMap<&String, q::Value<'static, String>>,
    ) -> Result<q::Value<'static, String>, QueryExecutionError> {
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
            "type" | "ofType" => match prefetched_object {
                Some(q::Value::String(type_name)) => self
                    .type_objects
                    .get(&type_name)
                    .cloned()
                    .unwrap_or(q::Value::Null),
                Some(v) => v,
                None => q::Value::Null,
            },
            _ => prefetched_object.unwrap_or(q::Value::Null),
        };
        Ok(object)
    }
}
