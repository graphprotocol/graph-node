use graphql_parser::schema::*;

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

/// Returns the root subscription type (if there is one).
pub fn get_root_subscription_type(schema: &Document) -> Option<&ObjectType> {
    schema
        .definitions
        .iter()
        .filter_map(|d| match d {
            Definition::TypeDefinition(TypeDefinition::Object(t)) => {
                if t.name == "Subscription".to_string() {
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
