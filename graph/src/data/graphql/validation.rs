use crate::prelude::Fail;
use graphql_parser::schema::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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
        display = "Entity type `{}` cannot implement `{}` because it is missing \
                   the required fields: {}",
        _0, _1, _2
    )]
    CannotImplement(String, String, Strings), // (type, interface, missing_fields)
    #[fail(
        display = "Field `{}` in type `{}` has invalid @derivedFrom: {}",
        _1, _0, _2
    )]
    DerivedFromInvalid(String, String, String), // (type, field, reason)
}

/// Validates whether a GraphQL schema is compatible with The Graph.
pub(crate) fn validate_schema(schema: &Document) -> Result<(), SchemaValidationError> {
    validate_schema_types(schema)?;
    validate_derived_from(schema)
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
            Strings(missing_fields),
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

fn find_interface<'a>(schema: &'a Document, name: &str) -> Option<&'a InterfaceType> {
    schema.definitions.iter().find_map(|d| match d {
        Definition::TypeDefinition(TypeDefinition::Interface(t)) if t.name == name => Some(t),
        _ => None,
    })
}

fn find_derived_from<'a>(field: &'a Field) -> Option<&'a Directive> {
    field
        .directives
        .iter()
        .find(|dir| dir.name == "derivedFrom")
}

/// Check `@derivedFrom` annotations for various problems. This follows the
/// corresponding checks in graph-cli
fn validate_derived_from(schema: &Document) -> Result<(), SchemaValidationError> {
    // Helper to construct a DerivedFromInvalid
    fn invalid(object_type: &ObjectType, field_name: &str, reason: &str) -> SchemaValidationError {
        SchemaValidationError::DerivedFromInvalid(
            object_type.name.to_owned(),
            field_name.to_owned(),
            reason.to_owned(),
        )
    }

    let type_definitions = get_object_type_definitions(schema);
    let object_and_interface_type_fields = get_object_and_interface_type_fields(schema);

    // Iterate over all derived fields in all entity types; include the
    // interface types that the entity with the `@derivedFrom` implements
    // and the `field` argument of @derivedFrom directive
    for (object_type, interface_types, field, target_field) in type_definitions
        .clone()
        .iter()
        .flat_map(|object_type| {
            object_type
                .fields
                .iter()
                .map(move |field| (object_type, field))
        })
        .filter_map(|(object_type, field)| {
            find_derived_from(field).map(|directive| {
                (
                    object_type,
                    object_type
                        .implements_interfaces
                        .iter()
                        .filter(|iface| {
                            // Any interface that has `field` can be used
                            // as the type of the field
                            find_interface(schema, iface)
                                .map(|iface| {
                                    iface.fields.iter().any(|ifield| ifield.name == field.name)
                                })
                                .unwrap_or(false)
                        })
                        .collect::<Vec<_>>(),
                    field,
                    directive
                        .arguments
                        .iter()
                        .find(|(name, _)| name == "field")
                        .map(|(_, value)| value),
                )
            })
        })
    {
        // Turn `target_field` into the string name of the field
        let target_field = target_field.ok_or_else(|| {
            invalid(
                object_type,
                &field.name,
                "the @derivedFrom directive must have a `field` argument",
            )
        })?;
        let target_field = match target_field {
            Value::String(s) => s,
            _ => {
                return Err(invalid(
                    object_type,
                    &field.name,
                    "the value of the @derivedFrom `field` argument must be a string",
                ))
            }
        };

        // Check that the type we are deriving from exists
        let target_type_name = get_base_type(&field.field_type);
        let target_fields = object_and_interface_type_fields
            .get(target_type_name)
            .ok_or_else(|| {
                invalid(
                    object_type,
                    &field.name,
                    "the type of the field must be an existing entity or interface type",
                )
            })?;

        // Check that the type we are deriving from has a field with the
        // right name and type
        let target_field = target_fields
            .iter()
            .find(|field| &field.name == target_field)
            .ok_or_else(|| {
                let msg = format!(
                    "field `{}` does not exist on type `{}`",
                    target_field, target_type_name
                );
                invalid(object_type, &field.name, &msg)
            })?;

        // The field we are deriving from has to point back to us; as an
        // exception, we allow deriving from the `id` of another type.
        // For that, we will wind up comparing the `id`s of the two types
        // when we query, and just assume that that's ok.
        let target_field_type = get_base_type(&target_field.field_type);
        if target_field_type != &object_type.name
            && target_field_type != "ID"
            && !interface_types
                .iter()
                .any(|iface| &target_field_type == iface)
        {
            fn type_signatures(name: &String) -> Vec<String> {
                vec![
                    format!("{}", name),
                    format!("{}!", name),
                    format!("[{}!]", name),
                    format!("[{}!]!", name),
                ]
            };

            let mut valid_types = type_signatures(&object_type.name);
            valid_types.extend(
                interface_types
                    .iter()
                    .flat_map(|iface| type_signatures(iface)),
            );
            let valid_types = valid_types.join(", ");

            let msg = format!(
                "field `{tf}` on type `{tt}` must have one of the following types: {valid_types}",
                tf = target_field.name,
                tt = target_type_name,
                valid_types = valid_types,
            );
            return Err(invalid(object_type, &field.name, &msg));
        }
    }
    Ok(())
}

#[test]
fn test_derived_from_validation() {
    const OTHER_TYPES: &str = "
type B @entity { id: ID! }
type C @entity { id: ID! }
type D @entity { id: ID! }
type E @entity { id: ID! }
type F @entity { id: ID! }
type G @entity { id: ID! a: BigInt }
type H @entity { id: ID! a: A! }
# This sets up a situation where we need to allow `Transaction.from` to
# point to an interface because of `Account.txn`
type Transaction @entity { from: Address! }
interface Address { txn: Transaction! @derivedFrom(field: \"from\") }
type Account implements Address @entity { id: ID!, txn: Transaction! @derivedFrom(field: \"from\") }";

    fn validate(field: &str, errmsg: &str) {
        let raw = format!("type A @entity {{ id: ID!\n {} }}\n{}", field, OTHER_TYPES);

        let document = graphql_parser::parse_schema(&raw).expect("Failed to parse raw schema");
        match validate_derived_from(&document) {
            Err(ref e) => match e {
                SchemaValidationError::DerivedFromInvalid(_, _, msg) => assert_eq!(errmsg, msg),
                _ => panic!("expected variant SchemaValidationError::DerivedFromInvalid"),
            },
            Ok(_) => {
                if errmsg != "ok" {
                    panic!("expected validation for `{}` to fail", field)
                }
            }
        }
    }

    validate(
        "b: B @derivedFrom(field: \"a\")",
        "field `a` does not exist on type `B`",
    );
    validate(
        "c: [C!]! @derivedFrom(field: \"a\")",
        "field `a` does not exist on type `C`",
    );
    validate(
        "d: D @derivedFrom",
        "the @derivedFrom directive must have a `field` argument",
    );
    validate(
        "e: E @derivedFrom(attr: \"a\")",
        "the @derivedFrom directive must have a `field` argument",
    );
    validate(
        "f: F @derivedFrom(field: 123)",
        "the value of the @derivedFrom `field` argument must be a string",
    );
    validate(
        "g: G @derivedFrom(field: \"a\")",
        "field `a` on type `G` must have one of the following types: A, A!, [A!], [A!]!",
    );
    validate("h: H @derivedFrom(field: \"a\")", "ok");
    validate(
        "i: NotAType @derivedFrom(field: \"a\")",
        "the type of the field must be an existing entity or interface type",
    );
    validate("j: B @derivedFrom(field: \"id\")", "ok");
}
