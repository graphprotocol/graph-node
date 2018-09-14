use failure::Error;
use graphql_parser::schema::*;
use std::fmt;

use schema::ast;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Strings(Vec<String>);

impl fmt::Display for Strings {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let s = (&self.0).join(", ");
        write!(f, "{}", s)
    }
}

#[derive(Clone, Debug, Serialize, Fail)]
pub enum SchemaValidationError {
    #[fail(
        display = "@entity directive missing on the following types: {}",
        _0
    )]
    EntityDirectivesMissing(Strings),
}

/// Validates whether a GraphQL schema is compatible with The Graph.
pub fn validate_schema(schema: &Document) -> Result<(), Error> {
    validate_schema_types(schema)?;
    Ok(())
}

/// Validates whether all object types in the schema are declared with an @entity directive.
fn validate_schema_types(schema: &Document) -> Result<(), SchemaValidationError> {
    use self::SchemaValidationError::*;

    let types_without_entity_directive = ast::get_object_type_definitions(schema)
        .iter()
        .filter(|t| ast::get_object_type_directive(t, String::from("entity")).is_none())
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
