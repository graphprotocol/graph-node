use graphql_parser::schema::*;
use std::collections::HashMap;

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

/// Returns all object and interface type definitions in the schema.
pub fn get_object_and_interface_type_fields(schema: &Document) -> HashMap<&Name, &Vec<Field>> {
    schema
        .definitions
        .iter()
        .filter_map(|d| match d {
            Definition::TypeDefinition(TypeDefinition::Object(t)) => Some((&t.name, &t.fields)),
            Definition::TypeDefinition(TypeDefinition::Interface(t)) => Some((&t.name, &t.fields)),
            _ => None,
        })
        .collect()
}

/// Looks up a directive in a object type, if it is provided.
pub fn get_object_type_directive(object_type: &ObjectType, name: Name) -> Option<&Directive> {
    object_type
        .directives
        .iter()
        .find(|directive| directive.name == name)
}

/// Returns the underlying type for a GraphQL field type
pub fn get_base_type(field_type: &Type) -> &Name {
    match field_type {
        Type::NamedType(name) => name,
        Type::NonNullType(inner) => get_base_type(&inner),
        Type::ListType(inner) => get_base_type(&inner),
    }
}

pub fn find_interface<'a>(schema: &'a Document, name: &str) -> Option<&'a InterfaceType> {
    schema.definitions.iter().find_map(|d| match d {
        Definition::TypeDefinition(TypeDefinition::Interface(t)) if t.name == name => Some(t),
        _ => None,
    })
}

pub fn find_derived_from<'a>(field: &'a Field) -> Option<&'a Directive> {
    field
        .directives
        .iter()
        .find(|dir| dir.name == "derivedFrom")
}
