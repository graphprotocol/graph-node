use graphql_parser::schema::*;
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Strings(Vec<String>);

impl fmt::Display for Strings {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let s = (&self.0).join(", ");
        write!(f, "{}", s)
    }
}

#[derive(Debug, Fail, PartialEq, Eq)]
pub enum SchemaValidationError {
    #[fail(display = "Interface {} not defined", _0)]
    UndefinedInterface(String),

    #[fail(display = "@entity directive missing on the following types: {}", _0)]
    EntityDirectivesMissing(Strings),

    #[fail(
        display = "Type `{}` cannot implement `{}` because it is missing \
                   the required fields {:?}",
        _0, _1, _2
    )]
    CannotImplement(String, String, Vec<String>), // (type, interface, missing_fields)
}

/// Validates whether a GraphQL schema is compatible with The Graph.
pub(crate) fn validate_schema(schema: &Document) -> Result<(), SchemaValidationError> {
    validate_schema_types(schema)
}

/// Validates whether all object types in the schema are declared with an @entity directive.
fn validate_schema_types(schema: &Document) -> Result<(), SchemaValidationError> {
    use self::SchemaValidationError::*;

    let types_without_entity_directive = get_object_type_definitions(schema)
        .iter()
        .filter(|t| get_object_type_directive(t, String::from("entity")).is_none())
        .map(|t| t.name.to_owned())
        .collect::<Vec<_>>();

    if types_without_entity_directive.is_empty() {
        Ok(())
    } else {
        Err(EntityDirectivesMissing(Strings(
            types_without_entity_directive,
        )))
    }
}

/// Validate `interfaceethat `object` implements `interface`.
pub(crate) fn validate_interface_implementation(
    object: &ObjectType,
    interface: &InterfaceType,
) -> Result<(), SchemaValidationError> {
    // Check that all fields in the interface exist in the object with same name and type.
    let mut missing_fields = vec![];
    for i in &interface.fields {
        if object
            .fields
            .iter()
            .find(|o| o.name == i.name && o.field_type == i.field_type)
            .is_none()
        {
            missing_fields.push(i.to_string().trim().to_owned());
        }
    }
    if !missing_fields.is_empty() {
        Err(SchemaValidationError::CannotImplement(
            object.name.clone(),
            interface.name.clone(),
            missing_fields,
        ))
    } else {
        Ok(())
    }
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

/// Looks up a directive in a object type, if it is provided.
pub fn get_object_type_directive(object_type: &ObjectType, name: Name) -> Option<&Directive> {
    object_type
        .directives
        .iter()
        .find(|directive| directive.name == name)
}
