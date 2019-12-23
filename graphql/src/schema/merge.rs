use graphql_parser::{
    schema::{Definition, Directive, Field, ObjectType, Type, TypeDefinition, Value},
    Pos,
};

use graph::data::graphql::ext::*;
use graph::data::graphql::scalar::BuiltInScalarType;
use graph::data::schema::{ImportedType, SchemaReference};
use graph::prelude::*;

use std::collections::HashMap;
use std::convert::TryFrom;

/// Optimistically merges a subgraph schema with all of its imports.
pub fn merged_schema(
    root_schema: &Schema,
    schemas: HashMap<SchemaReference, Arc<Schema>>,
) -> Schema {
    // Create a Vec<(ImportedType, SchemaReference)> from the imported types in the root schema
    //
    // Iterate over the Vec<(ImportedType, SchemaReference)> and in each iteration, look up the schema
    // which corresponds to the current element in the Vector.
    //
    // If the schema is not available, then add a placeholder type to the root schema.
    //
    // If the schema is available, copy the schema over.
    // Check each field in the copied type and for non scalar fields, produce an (ImportedType, SchemaRefernce)
    // tuple; the new vector element will either be for the same schema or for an imported schema.
    //
    // Copying a type:
    // 1. Clone the type
    // 2. Add a subgraph id directive
    // 3. If the type is imported with { name : "...", as: "..." }, change the name and
    //    add an @originalName(name: "...") directive
    // 4. Push it onto the schema.document.definitions
    //
    // QUESTION: How should naming conflicts be handled?
    // A subgraph developer will probably ensure that an imported type does not conflict with local subgraph types.
    // However, the non scalar fields of an imported type are also imported and those types might overlap with local
    // subgraph types. What should we do in this case?
    // Presumably overlapping type names will not be accepted by GraphQL clients.
    let mut merged = root_schema.clone();
    let mut imports: Vec<(_, _)> = merged
        .imported_types()
        .iter()
        .map(|(t, sr)| (t.clone(), sr.clone()))
        .collect();

    while let Some((import, schema_reference)) = imports.pop() {
        let (original_name, new_name) = match import {
            ImportedType::Name(name) => (name.clone(), name),
            ImportedType::NameAs(name, az) => (name, az),
        };

        match schemas.get(&schema_reference) {
            Some(schema) => {
                let subgraph_id = schema.id.clone();

                // Find the type
                let local_type = schema
                    .document
                    .definitions
                    .iter()
                    .find(|definition| {
                        if let Definition::TypeDefinition(TypeDefinition::Object(obj)) = definition
                        {
                            obj.name.eq(&original_name)
                        } else {
                            false
                        }
                    })
                    .map(|definition| match definition {
                        Definition::TypeDefinition(TypeDefinition::Object(obj)) => obj,
                        _ => unreachable!(),
                    });
                if let Some(obj) = local_type {
                    // Clone the type
                    let mut new_obj = obj.clone();

                    // Add a subgraph id directive
                    new_obj.directives.push(Directive {
                        position: Pos::default(),
                        name: String::from("subgraphId"),
                        arguments: vec![(
                            String::from("id"),
                            Value::String(subgraph_id.to_string()),
                        )],
                    });

                    // If the type is imported with { name : "...", as: "..." }, change the name and
                    //    add an @originalName(name: "...") directive
                    if !original_name.eq(&new_name) {
                        new_obj.name = new_name.clone();
                    }

                    // Push it onto the schema.document.definitions
                    merged
                        .document
                        .definitions
                        .push(Definition::TypeDefinition(TypeDefinition::Object(new_obj)));

                    // Import each none scalar field
                    obj.fields
                        .iter()
                        .filter_map(|field| {
                            let base_type = field.field_type.get_base_type();
                            match BuiltInScalarType::try_from(base_type.as_ref()) {
                                Ok(_) => None,
                                Err(_) => Some(base_type),
                            }
                        })
                        .for_each(|base_type| {
                            imports.push((
                                ImportedType::Name(base_type.to_string()),
                                schema_reference.clone(),
                            ));
                        });
                } else {
                    // Determine if the type is imported
                    // If it is imported, push a tuple onto the `imports` vector
                    if let Some((import, schema_reference)) =
                        schema
                            .imported_types()
                            .iter()
                            .find(|(import, schema_reference)| match import {
                                ImportedType::Name(name) if name.eq(&original_name) => true,
                                ImportedType::NameAs(_, az) if az.eq(&original_name) => true,
                                _ => false,
                            })
                    {
                        let import = match import {
                            ImportedType::Name(name) if new_name.eq(name) => import.clone(),
                            ImportedType::Name(name) => {
                                ImportedType::NameAs(name.clone(), new_name)
                            }
                            ImportedType::NameAs(name, _) => {
                                ImportedType::NameAs(name.clone(), new_name)
                            }
                        };
                        imports.push((import, schema_reference.clone()));
                        continue;
                    } else {
                        // If it is not imported, then add a placeholder type
                        merged.document.definitions.push(placeholder_type(
                            new_name.clone(),
                            match new_name.eq(&original_name) {
                                true => None,
                                false => Some(original_name),
                            },
                        ));
                    }
                }
            }
            None => {
                // Add a placeholder type to the root schema
                merged.document.definitions.push(placeholder_type(
                    new_name.clone(),
                    match new_name.eq(&original_name) {
                        true => None,
                        false => Some(original_name),
                    },
                ));
            }
        }
    }

    merged
}

fn placeholder_type(name: String, original_name: Option<String>) -> Definition {
    let mut obj = ObjectType::new(name);

    // Add id field
    obj.fields.push(Field {
        position: Pos::default(),
        description: None,
        name: String::from("id"),
        arguments: vec![],
        field_type: Type::NonNullType(Box::new(Type::NamedType(String::from("ID")))),
        directives: vec![],
    });

    // Add entity directive
    obj.directives.push(Directive {
        position: Pos::default(),
        name: String::from("entity"),
        arguments: vec![],
    });
    if let Some(original_name) = original_name {
        obj.directives.push(Directive {
            position: Pos::default(),
            name: String::from("originalName"),
            arguments: vec![(String::from("name"), Value::String(original_name))],
        });
    }
    Definition::TypeDefinition(TypeDefinition::Object(obj))
}

#[test]
fn test_recursive_import() {
    // Generate a root schema
    // Generate the schema lookup for the import graph
    // Call merged_schema
    // Verify the output schema is correct
}

#[test]
fn test_placeholder_for_missing_schema() {}

#[test]
fn test_placeholder_for_missing_type() {}

#[test]
fn test_original_name_directive() {}

#[test]
fn test_subgraph_id_directive_added_correctly() {}

#[test]
fn test_import_of_non_scalar_fields_for_imported_type() {}
