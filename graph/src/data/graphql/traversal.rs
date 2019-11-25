use crate::data::subgraph::{SubgraphDeploymentId, SubgraphName};
use crate::prelude::Fail;
use graphql_parser::schema::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

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

// #[test]
// fn test_derived_from_validation() {
//     const OTHER_TYPES: &str = "
// type B @entity { id: ID! }
// type C @entity { id: ID! }
// type D @entity { id: ID! }
// type E @entity { id: ID! }
// type F @entity { id: ID! }
// type G @entity { id: ID! a: BigInt }
// type H @entity { id: ID! a: A! }
// # This sets up a situation where we need to allow `Transaction.from` to
// # point to an interface because of `Account.txn`
// type Transaction @entity { from: Address! }
// interface Address { txn: Transaction! @derivedFrom(field: \"from\") }
// type Account implements Address @entity { id: ID!, txn: Transaction! @derivedFrom(field: \"from\") }";

//     fn validate(field: &str, errmsg: &str) {
//         let raw = format!("type A @entity {{ id: ID!\n {} }}\n{}", field, OTHER_TYPES);

//         let document = graphql_parser::parse_schema(&raw).expect("Failed to parse raw schema");
//         match validate_derived_from(&document) {
//             Err(ref e) => match e {
//                 SchemaValidationError::DerivedFromInvalid(_, _, msg) => assert_eq!(errmsg, msg),
//                 _ => panic!("expected variant SchemaValidationError::DerivedFromInvalid"),
//             },
//             Ok(_) => {
//                 if errmsg != "ok" {
//                     panic!("expected validation for `{}` to fail", field)
//                 }
//             }
//         }
//     }

//     validate(
//         "b: B @derivedFrom(field: \"a\")",
//         "field `a` does not exist on type `B`",
//     );
//     validate(
//         "c: [C!]! @derivedFrom(field: \"a\")",
//         "field `a` does not exist on type `C`",
//     );
//     validate(
//         "d: D @derivedFrom",
//         "the @derivedFrom directive must have a `field` argument",
//     );
//     validate(
//         "e: E @derivedFrom(attr: \"a\")",
//         "the @derivedFrom directive must have a `field` argument",
//     );
//     validate(
//         "f: F @derivedFrom(field: 123)",
//         "the value of the @derivedFrom `field` argument must be a string",
//     );
//     validate(
//         "g: G @derivedFrom(field: \"a\")",
//         "field `a` on type `G` must have one of the following types: A, A!, [A!], [A!]!",
//     );
//     validate("h: H @derivedFrom(field: \"a\")", "ok");
//     validate(
//         "i: NotAType @derivedFrom(field: \"a\")",
//         "the type of the field must be an existing entity or interface type",
//     );
//     validate("j: B @derivedFrom(field: \"id\")", "ok");
// }
