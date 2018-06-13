use graphql_parser::schema::*;

/// Returns the root query type (if there is one).
pub fn get_root_query_type(schema: &Document) -> Option<&ObjectType> {
    schema
        .definitions
        .iter()
        .filter_map(|d| match d {
            Definition::TypeDefinition(TypeDefinition::Object(t)) => {
                if t.name == "Query".to_string() {
                    Some(t)
                } else {
                    None
                }
            }
            _ => None,
        })
        .peekable()
        .next()
}

/// Returns all type definitions in the schema.
pub fn get_type_definitions<'a>(schema: &'a Document) -> Vec<&'a TypeDefinition> {
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
pub fn get_object_type_definitions<'a>(schema: &'a Document) -> Vec<&'a ObjectType> {
    schema
        .definitions
        .iter()
        .filter_map(|d| match d {
            Definition::TypeDefinition(TypeDefinition::Object(t)) => Some(t),
            _ => None,
        })
        .collect()
}

/// Returns all interface definitions in the schema.
pub fn get_interface_type_definitions<'a>(schema: &'a Document) -> Vec<&'a InterfaceType> {
    schema
        .definitions
        .iter()
        .filter_map(|d| match d {
            Definition::TypeDefinition(TypeDefinition::Interface(t)) => Some(t),
            _ => None,
        })
        .collect()
}

/// Returns the type of a field of an object type.
pub fn get_field_type<'a>(object_type: &'a ObjectType, name: &Name) -> Option<&'a Field> {
    object_type.fields.iter().find(|field| &field.name == name)
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
    get_field_type(object_type, name).map(|field| &field.arguments)
}
