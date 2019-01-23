use failure::Error;
use graphql_parser::schema::*;
use graphql_parser::Pos;
use lazy_static::lazy_static;
use std::ops::Deref;
use std::str::FromStr;

use graph::prelude::ValueType;

pub(crate) enum FilterOp {
    Not,
    GreaterThan,
    LessThan,
    GreaterOrEqual,
    LessOrEqual,
    In,
    NotIn,
    Contains,
    NotContains,
    StartsWith,
    NotStartsWith,
    EndsWith,
    NotEndsWith,
    Equal,
}

/// Split a "name_eq" style name into an attribute ("name") and a filter op (`Equal`).
pub(crate) fn parse_field_as_filter(key: &Name) -> (Name, FilterOp) {
    let (suffix, op) = match key {
        k if k.ends_with("_not") => ("_not", FilterOp::Not),
        k if k.ends_with("_gt") => ("_gt", FilterOp::GreaterThan),
        k if k.ends_with("_lt") => ("_lt", FilterOp::LessThan),
        k if k.ends_with("_gte") => ("_gte", FilterOp::GreaterOrEqual),
        k if k.ends_with("_lte") => ("_lte", FilterOp::LessOrEqual),
        k if k.ends_with("_not_in") => ("_not_in", FilterOp::NotIn),
        k if k.ends_with("_in") => ("_in", FilterOp::In),
        k if k.ends_with("_not_contains") => ("_not_contains", FilterOp::NotContains),
        k if k.ends_with("_contains") => ("_contains", FilterOp::Contains),
        k if k.ends_with("_not_starts_with") => ("_not_starts_with", FilterOp::NotStartsWith),
        k if k.ends_with("_not_ends_with") => ("_not_ends_with", FilterOp::NotEndsWith),
        k if k.ends_with("_starts_with") => ("_starts_with", FilterOp::StartsWith),
        k if k.ends_with("_ends_with") => ("_ends_with", FilterOp::EndsWith),
        _ => ("", FilterOp::Equal),
    };

    // Strip the operator suffix to get the attribute.
    (key.trim_right_matches(suffix).to_owned(), op)
}

/// Returns the root query type (if there is one).
pub fn get_root_query_type(schema: &Document) -> Option<&ObjectType> {
    schema
        .definitions
        .iter()
        .filter_map(|d| match d {
            Definition::TypeDefinition(TypeDefinition::Object(t)) if t.name == "Query" => Some(t),
            _ => None,
        })
        .peekable()
        .next()
}

/// Returns the root subscription type (if there is one).
pub fn get_root_subscription_type(schema: &Document) -> Option<&ObjectType> {
    schema
        .definitions
        .iter()
        .filter_map(|d| match d {
            Definition::TypeDefinition(TypeDefinition::Object(t)) if t.name == "Subscription" => {
                Some(t)
            }
            _ => None,
        })
        .peekable()
        .next()
}

/// Returns all type definitions in the schema.
pub fn get_type_definitions(schema: &Document) -> Vec<&TypeDefinition> {
    schema
        .definitions
        .iter()
        .filter_map(|d| match d {
            Definition::TypeDefinition(typedef) => Some(typedef),
            _ => None,
        })
        .collect()
}

/// Returns all object type definitions in the schema.
pub fn get_object_type_definitions(schema: &Document) -> Vec<&ObjectType> {
    schema
        .definitions
        .iter()
        .filter_map(|d| match d {
            Definition::TypeDefinition(TypeDefinition::Object(t)) => Some(t),
            _ => None,
        })
        .collect()
}

/// Returns all object type definitions in the schema.
pub fn get_object_type_definitions_mut(schema: &mut Document) -> Vec<&mut ObjectType> {
    schema
        .definitions
        .iter_mut()
        .filter_map(|d| match d {
            Definition::TypeDefinition(TypeDefinition::Object(t)) => Some(t),
            _ => None,
        })
        .collect()
}

/// Returns the object type with the given name.
pub fn get_object_type_mut<'a>(
    schema: &'a mut Document,
    name: &Name,
) -> Option<&'a mut ObjectType> {
    use self::TypeDefinition::*;

    get_named_type_definition_mut(schema, name).and_then(|type_def| match type_def {
        Object(object_type) => Some(object_type),
        _ => None,
    })
}

/// Returns all interface definitions in the schema.
pub fn get_interface_type_definitions(schema: &Document) -> Vec<&InterfaceType> {
    schema
        .definitions
        .iter()
        .filter_map(|d| match d {
            Definition::TypeDefinition(TypeDefinition::Interface(t)) => Some(t),
            _ => None,
        })
        .collect()
}

/// Returns all interface definitions in the schema.
pub fn get_interface_type_definitions_mut(schema: &mut Document) -> Vec<&mut InterfaceType> {
    schema
        .definitions
        .iter_mut()
        .filter_map(|d| match d {
            Definition::TypeDefinition(TypeDefinition::Interface(t)) => Some(t),
            _ => None,
        })
        .collect()
}

/// Returns the interface type with the given name.
pub fn get_interface_type_mut<'a>(
    schema: &'a mut Document,
    name: &Name,
) -> Option<&'a mut InterfaceType> {
    use self::TypeDefinition::*;

    get_named_type_definition_mut(schema, name).and_then(|type_def| match type_def {
        Interface(interface_type) => Some(interface_type),
        _ => None,
    })
}

/// Returns the type of a field of an object type.
pub fn get_field_type<'a>(object_type: &'a ObjectType, name: &Name) -> Option<&'a Field> {
    object_type.fields.iter().find(|field| &field.name == name)
}

/// Returns the type of a field of an interface type.
pub fn get_interface_field_type<'a>(
    interface_type: &'a InterfaceType,
    name: &Name,
) -> Option<&'a Field> {
    interface_type
        .fields
        .iter()
        .find(|field| &field.name == name)
}

/// Returns the value type for a GraphQL field type.
pub fn get_field_value_type(field_type: &Type) -> Result<ValueType, Error> {
    match field_type {
        Type::NamedType(ref name) => ValueType::from_str(&name),
        Type::NonNullType(inner) => get_field_value_type(&inner),
        Type::ListType(_) => Err(format_err!(
            "Only scalar values are supported in this context"
        )),
    }
}

/// Returns the value type for a GraphQL field type.
pub fn get_field_name(field_type: &Type) -> Name {
    match field_type {
        Type::NamedType(name) => name.to_string(),
        Type::NonNullType(inner) => get_field_name(&inner),
        Type::ListType(inner) => get_field_name(&inner),
    }
}

/// Returns the type with the given name.
pub fn get_named_type<'a>(schema: &'a Document, name: &Name) -> Option<&'a TypeDefinition> {
    schema
        .definitions
        .iter()
        .filter_map(|def| match def {
            Definition::TypeDefinition(typedef) => Some(typedef),
            _ => None,
        })
        .find(|typedef| match typedef {
            TypeDefinition::Object(t) => &t.name == name,
            TypeDefinition::Enum(t) => &t.name == name,
            TypeDefinition::InputObject(t) => &t.name == name,
            TypeDefinition::Interface(t) => &t.name == name,
            TypeDefinition::Scalar(t) => &t.name == name,
            TypeDefinition::Union(t) => &t.name == name,
        })
}

/// Returns a mutable version of the type with the given name.
pub fn get_named_type_definition_mut<'a>(
    schema: &'a mut Document,
    name: &Name,
) -> Option<&'a mut TypeDefinition> {
    schema
        .definitions
        .iter_mut()
        .filter_map(|def| match def {
            Definition::TypeDefinition(typedef) => Some(typedef),
            _ => None,
        })
        .find(|typedef| match typedef {
            TypeDefinition::Object(t) => &t.name == name,
            TypeDefinition::Enum(t) => &t.name == name,
            TypeDefinition::InputObject(t) => &t.name == name,
            TypeDefinition::Interface(t) => &t.name == name,
            TypeDefinition::Scalar(t) => &t.name == name,
            TypeDefinition::Union(t) => &t.name == name,
        })
}

/// Returns the name of a type.
pub fn get_type_name(t: &TypeDefinition) -> &Name {
    match t {
        TypeDefinition::Enum(t) => &t.name,
        TypeDefinition::InputObject(t) => &t.name,
        TypeDefinition::Interface(t) => &t.name,
        TypeDefinition::Object(t) => &t.name,
        TypeDefinition::Scalar(t) => &t.name,
        TypeDefinition::Union(t) => &t.name,
    }
}

/// Returns the description of a type.
pub fn get_type_description(t: &TypeDefinition) -> &Option<String> {
    match t {
        TypeDefinition::Enum(t) => &t.description,
        TypeDefinition::InputObject(t) => &t.description,
        TypeDefinition::Interface(t) => &t.description,
        TypeDefinition::Object(t) => &t.description,
        TypeDefinition::Scalar(t) => &t.description,
        TypeDefinition::Union(t) => &t.description,
    }
}

/// Returns the argument definitions for a field of an object type.
pub fn get_argument_definitions<'a>(
    object_type: &'a ObjectType,
    name: &Name,
) -> Option<&'a Vec<InputValue>> {
    lazy_static! {
        pub static ref NAME_ARGUMENT: Vec<InputValue> = vec![InputValue {
            position: Pos::default(),
            description: None,
            name: "name".to_owned(),
            value_type: Type::NonNullType(Box::new(Type::NamedType("String".to_owned()))),
            default_value: None,
            directives: vec![],
        }];
    }

    // Introspection: `__type(name: String!): __Type`
    if name == "__type" {
        Some(&NAME_ARGUMENT)
    } else {
        get_field_type(object_type, name).map(|field| &field.arguments)
    }
}

/// Returns the type definition that a field type corresponds to.
pub fn get_type_definition_from_field_type<'a>(
    schema: &'a Document,
    field_type: &'a Field,
) -> Option<&'a TypeDefinition> {
    get_type_definition_from_type(schema, &field_type.field_type)
}

/// Returns the type definition for a type.
pub fn get_type_definition_from_type<'a>(
    schema: &'a Document,
    t: &'a Type,
) -> Option<&'a TypeDefinition> {
    match t {
        Type::NamedType(name) => get_named_type(schema, name),
        Type::ListType(inner) => get_type_definition_from_type(schema, inner),
        Type::NonNullType(inner) => get_type_definition_from_type(schema, inner),
    }
}

/// Looks up a directive in a object type, if it is provided.
pub fn get_object_type_directive(object_type: &ObjectType, name: Name) -> Option<&Directive> {
    object_type
        .directives
        .iter()
        .find(|directive| directive.name == name)
}

// Returns true if the given type is a non-null type.
pub fn is_non_null_type(t: &Type) -> bool {
    match t {
        Type::NonNullType(_) => true,
        _ => false,
    }
}

/// Returns true if the given type is an input type.
///
/// Uses the algorithm outlined on
/// https://facebook.github.io/graphql/draft/#IsInputType().
pub fn is_input_type(schema: &Document, t: &Type) -> bool {
    use self::TypeDefinition::*;

    match t {
        Type::NamedType(name) => {
            let named_type = get_named_type(schema, name);
            named_type.map_or(false, |type_def| match type_def {
                Scalar(_) | Enum(_) | InputObject(_) => true,
                _ => false,
            })
        }
        Type::ListType(inner) => is_input_type(schema, inner),
        Type::NonNullType(inner) => is_input_type(schema, inner),
    }
}

/// Returns true if the given field is an entity relationship field.
pub fn is_entity_relationship_field(schema: &Document, field: &Field) -> bool {
    is_entity_type(schema, &field.field_type)
}

pub fn is_entity_type(schema: &Document, t: &Type) -> bool {
    use self::Type::*;

    match t {
        NamedType(name) => get_named_type(schema, &name).map_or(false, is_entity_type_definition),
        ListType(inner_type) => is_entity_type(schema, inner_type),
        NonNullType(inner_type) => is_entity_type(schema, inner_type),
    }
}

pub fn is_entity_type_definition(type_def: &TypeDefinition) -> bool {
    use self::TypeDefinition::*;

    match type_def {
        // Entity types are obvious
        Object(object_type) => {
            get_object_type_directive(object_type, Name::from("entity")).is_some()
        }

        // For now, we'll assume that only entities can implement interfaces;
        // thus, any interface type definition is automatically an entity type
        Interface(_) => true,

        // Everything else (unions, scalars, enums) are not considered entity
        // types for now
        _ => false,
    }
}

pub fn is_list_or_non_null_list_field(field: &Field) -> bool {
    use self::Type::*;

    match &field.field_type {
        ListType(_) => true,
        NonNullType(inner_type) => match inner_type.deref() {
            ListType(_) => true,
            _ => false,
        },
        _ => false,
    }
}

pub fn unpack_type<'a>(schema: &'a Document, t: &Type) -> Option<&'a TypeDefinition> {
    use self::Type::*;

    match t {
        NamedType(name) => get_named_type(schema, &name),
        ListType(inner_type) => unpack_type(schema, inner_type),
        NonNullType(inner_type) => unpack_type(schema, inner_type),
    }
}

pub fn get_referenced_entity_type<'a>(
    schema: &'a Document,
    field: &Field,
) -> Option<&'a TypeDefinition> {
    unpack_type(schema, &field.field_type).and_then(|type_def| {
        if is_entity_type_definition(type_def) {
            Some(type_def)
        } else {
            None
        }
    })
}
