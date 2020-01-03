use graphql_parser::{
    schema::{Definition, Directive, Field, ObjectType, Type, TypeDefinition, Value},
    Pos,
};

use graph::data::graphql::ext::*;
use graph::data::graphql::scalar::BuiltInScalarType;
use graph::data::schema::{ImportedType, SchemaReference, SCHEMA_TYPE_NAME};
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
    // If the schema is available, copy the type over.
    // Check each field in the copied type and for non scalar fields, produce an (ImportedType, SchemaRefernce)
    // tuple.
    //
    // Copying a type:
    // 1. Clone the type
    // 2. Add a subgraph id directive
    // 3. If the type is imported with { name : "...", as: "..." }, change the name and
    //    add an @originalName(name: "...") directive

    let mut merged = root_schema.clone();
    let mut imports: Vec<(_, _)> = merged
        .imported_types()
        .iter()
        .map(|(import, schema_reference)| (import.clone(), schema_reference.clone()))
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
                if let Some(obj) = schema
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
                    })
                {
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
                    // add an @originalName(name: "...") directive
                    if !original_name.eq(&new_name) {
                        new_obj.name = new_name.clone();
                        new_obj.directives.push(Directive {
                            position: Pos::default(),
                            name: String::from("originalName"),
                            arguments: vec![(
                                String::from("name"),
                                Value::String(original_name.to_string()),
                            )],
                        });
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
                            .find(|(import, _)| match import {
                                ImportedType::Name(name) => name.eq(&original_name),
                                ImportedType::NameAs(_, az) => az.eq(&original_name),
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

    // Remove the _Schema_ type
    if let Some((idx, _)) =
        merged
            .document
            .definitions
            .iter()
            .enumerate()
            .find(|(_, definition)| match definition {
                Definition::TypeDefinition(TypeDefinition::Object(obj)) => {
                    obj.name.eq(SCHEMA_TYPE_NAME)
                }
                _ => false,
            })
    {
        merged.document.definitions.remove(idx);
    };

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
    obj.directives.push(Directive {
        position: Pos::default(),
        name: String::from("placeholder"),
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::merged_schema;

    use graph::data::graphql::ext::*;
    use graph::data::schema::{ImportedType, SchemaReference, SCHEMA_TYPE_NAME};
    use graph::prelude::*;

    use graphql_parser::schema::Value;

    fn schema_with_import(
        subgraph_id: SubgraphDeploymentId,
        type_name: String,
        subgraph_name: String,
    ) -> Schema {
        let schema = format!(
            r#"type _Schema_ @import(types: ["{}"], from: {{ name: "{}" }})"#,
            type_name, subgraph_name,
        );
        let document = graphql_parser::parse_schema(&schema).unwrap();
        Schema::new(subgraph_id, document)
    }

    fn schema_with_name_as_import(
        subgraph_id: SubgraphDeploymentId,
        type_name: String,
        type_as: String,
        subgraph_name: String,
    ) -> Schema {
        let schema = format!(
            r#"type _Schema_ @import(types: [{{ name: "{}", as: "{}" }}] from: {{ name: "{}" }})"#,
            type_name, type_as, subgraph_name,
        );
        let document = graphql_parser::parse_schema(&schema).unwrap();
        Schema::new(subgraph_id, document)
    }

    fn schema_with_type(subgraph_id: SubgraphDeploymentId, type_name: String) -> Schema {
        let schema = format!(
            r#"
type {} @entity {{
  id: ID!
  foo: String
}}
"#,
            type_name,
        );
        let document = graphql_parser::parse_schema(&schema).unwrap();
        Schema::new(subgraph_id, document)
    }

    fn schema_with_type_with_nonscalar_field(
        subgraph_id: SubgraphDeploymentId,
        type_name: String,
        field_type_name: String,
    ) -> Schema {
        let schema = format!(
            r#"
type {} @entity {{
  id: ID!
  foo: {}
}}

type {} @entity {{
  id: ID!
  bar: String
}}
"#,
            type_name, field_type_name, field_type_name,
        );
        let document = graphql_parser::parse_schema(&schema).unwrap();
        Schema::new(subgraph_id, document)
    }

    #[test]
    fn test_recursive_import() {
        let root_schema = schema_with_import(
            SubgraphDeploymentId::new("root").unwrap(),
            String::from("A"),
            String::from("c1/subgraph"),
        );
        let child_1_schema = schema_with_name_as_import(
            SubgraphDeploymentId::new("childone").unwrap(),
            String::from("T"),
            String::from("A"),
            String::from("c2/subgraph"),
        );
        let child_2_schema = schema_with_type(
            SubgraphDeploymentId::new("childtwo").unwrap(),
            String::from("T"),
        );

        let mut schemas = HashMap::new();
        schemas.insert(
            SchemaReference::ByName(SubgraphName::new("c1/subgraph").unwrap()),
            Arc::new(child_1_schema),
        );
        schemas.insert(
            SchemaReference::ByName(SubgraphName::new("c2/subgraph").unwrap()),
            Arc::new(child_2_schema),
        );

        // Call merged_schema
        let merged = merged_schema(&root_schema, schemas);

        // Verify the output schema is correctl
        match merged
            .document
            .get_object_type_definitions()
            .into_iter()
            .find(|object_type| object_type.name.eq("A"))
        {
            None => panic!("Failed to import type `A`"),
            Some(type_a) => {
                // Type A is imported with all the correct fields
                let id_field = type_a.fields.iter().find(|field| field.name.eq("id"));
                let foo_field = type_a.fields.iter().find(|field| field.name.eq("foo"));

                match (id_field, foo_field) {
                    (Some(_), Some(_)) => (),
                    _ => panic!("Imported type `A` does not have the correct fields"),
                };

                // Type A has a @subgraphId directive with the correct id
                match type_a
                    .directives
                    .iter()
                    .find(|directive| directive.name.eq("subgraphId"))
                {
                    Some(directive) => {
                        // Ensure the id argument on the directive is correct
                        match directive.arguments.iter().find(|(name, _)| name.eq("id")) {
                            Some((_, Value::String(id))) if id.eq("childtwo") => (),
                            _ => {
                                panic!(
                                "Imported type `A` needs a @subgraphId directive: @subgraphId(id: \"childtwo\")"
                            );
                            }
                        }
                    }
                    None => panic!("Imported type `A` does not have a `@subgraphId` directive"),
                };

                // Type A has an @originalName directive with the correct name
                match type_a
                    .directives
                    .iter()
                    .find(|directive| directive.name.eq("originalName"))
                {
                    Some(directive) => {
                        // Ensure the original name argument on the directive is correct
                        match directive.arguments.iter().find(|(name, _)| name.eq("name")) {
                            Some((_, Value::String(name))) if name.eq("T") => (),
                            _ => {
                                panic!(
                                "Imported type `A` needs an originalName directive: @originalName(name: \"T\")"
                            );
                            }
                        };
                    }
                    None => panic!("Imported type `A` does not have an `originalName` directive"),
                }
            }
        }
    }

    #[test]
    fn test_placeholder_for_missing_type() {
        let root_schema = schema_with_import(
            SubgraphDeploymentId::new("root").unwrap(),
            String::from("A"),
            String::from("c1/subgraph"),
        );
        let child_1_schema = schema_with_name_as_import(
            SubgraphDeploymentId::new("childone").unwrap(),
            String::from("T"),
            String::from("A"),
            String::from("c2/subgraph"),
        );
        let child_2_schema = schema_with_type(
            SubgraphDeploymentId::new("childtwo").unwrap(),
            String::from("B"),
        );

        let mut schemas = HashMap::new();
        schemas.insert(
            SchemaReference::ByName(SubgraphName::new("c1/subgraph").unwrap()),
            Arc::new(child_1_schema),
        );
        schemas.insert(
            SchemaReference::ByName(SubgraphName::new("c2/subgraph").unwrap()),
            Arc::new(child_2_schema),
        );

        // Call merged_schema
        let merged = merged_schema(&root_schema, schemas);

        match merged.document.get_object_type_definitions().iter().next() {
            None => panic!("Failed to import placeholder for type `A`"),
            Some(type_a) => {
                // Has an id field
                match type_a.fields.iter().find(|field| field.name.eq("id")) {
                    Some(_) => (),
                    _ => panic!("Placeholder for imported type does not have the correct fields"),
                };

                // Has a placeholder directive
                match type_a
                    .directives
                    .iter()
                    .find(|directive| directive.name.eq("placeholder"))
                {
                    Some(_) => (),
                    _ => {
                        panic!("Imported type `A` does not have a `@placeholder` directive");
                    }
                }
            }
        };
    }

    #[test]
    fn test_placeholder_for_missing_schema() {
        let root_schema = schema_with_import(
            SubgraphDeploymentId::new("root").unwrap(),
            String::from("A"),
            String::from("c1/subgraph"),
        );
        let child_1_schema = schema_with_name_as_import(
            SubgraphDeploymentId::new("childone").unwrap(),
            String::from("T"),
            String::from("A"),
            String::from("c2/subgraph"),
        );

        let mut schemas = HashMap::new();
        schemas.insert(
            SchemaReference::ByName(SubgraphName::new("c1/subgraph").unwrap()),
            Arc::new(child_1_schema),
        );
        // Call merged_schema
        let merged = merged_schema(&root_schema, schemas);

        match merged.document.get_object_type_definitions().iter().next() {
            None => panic!("Failed to import placeholder for type `A`"),
            Some(type_a) => {
                // Has an id field
                match type_a.fields.iter().find(|field| field.name.eq("id")) {
                    Some(_) => (),
                    _ => panic!("Placeholder for imported type does not have the correct fields"),
                };

                // Has a placeholder directive
                match type_a
                    .directives
                    .iter()
                    .find(|directive| directive.name.eq("placeholder"))
                {
                    Some(_) => (),
                    _ => {
                        panic!("Imported type `A` does not have a `@placeholder` directive");
                    }
                }
            }
        };
    }

    #[test]
    fn test_import_of_non_scalar_fields_for_imported_type() {
        let root_schema = schema_with_import(
            SubgraphDeploymentId::new("root").unwrap(),
            String::from("A"),
            String::from("c1/subgraph"),
        );
        let child_1_schema = schema_with_name_as_import(
            SubgraphDeploymentId::new("childone").unwrap(),
            String::from("T"),
            String::from("A"),
            String::from("c2/subgraph"),
        );
        let child_2_schema = schema_with_type_with_nonscalar_field(
            SubgraphDeploymentId::new("childtwo").unwrap(),
            String::from("T"),
            String::from("B"),
        );

        let mut schemas = HashMap::new();
        schemas.insert(
            SchemaReference::ByName(SubgraphName::new("c1/subgraph").unwrap()),
            Arc::new(child_1_schema),
        );
        schemas.insert(
            SchemaReference::ByName(SubgraphName::new("c2/subgraph").unwrap()),
            Arc::new(child_2_schema),
        );

        // Call merged_schema
        let merged = merged_schema(&root_schema, schemas);

        // Verify the output schema is correct
        match merged
            .document
            .get_object_type_definitions()
            .into_iter()
            .find(|object_type| object_type.name.eq("A"))
        {
            None => panic!("Failed to import type `A`"),
            Some(type_a) => {
                // Type A is imported with all the correct fields
                let id_field = type_a.fields.iter().find(|field| field.name.eq("id"));
                let foo_field = type_a.fields.iter().find(|field| field.name.eq("foo"));

                match (id_field, foo_field) {
                    (Some(_), Some(_)) => (),
                    _ => panic!("Imported type `A` does not have the correct fields"),
                };

                // Type A has a @subgraphId directive with the correct id
                match type_a
                    .directives
                    .iter()
                    .find(|directive| directive.name.eq("subgraphId"))
                {
                    Some(directive) => {
                        // Ensure the id argument on the directive is correct
                        match directive.arguments.iter().find(|(name, _)| name.eq("id")) {
                            Some((_, Value::String(id))) if id.eq("childtwo") => (),
                            _ => {
                                panic!(
                                "Imported type `A` needs a @subgraphId directive: @subgraphId(id: \"c2id\")"
                            );
                            }
                        }
                    }
                    None => panic!("Imported type `A` does not have a `@subgraphId` directive"),
                };

                // Type A has an @originalName directive with the correct name
                match type_a
                    .directives
                    .iter()
                    .find(|directive| directive.name.eq("originalName"))
                {
                    Some(directive) => {
                        // Ensure the original name argument on the directive is correct
                        match directive.arguments.iter().find(|(name, _)| name.eq("name")) {
                            Some((_, Value::String(name))) if name.eq("T") => (),
                            _ => {
                                panic!(
                                "Imported type `A` needs an originalName directive: @originalName(name: \"T\")"
                            );
                            }
                        };
                    }
                    None => panic!("Imported type `A` does not have an `originalName` directive"),
                }
            }
        }
        // Verify the output schema is correct
        match merged
            .document
            .get_object_type_definitions()
            .into_iter()
            .find(|object_type| object_type.name.eq("B"))
        {
            None => panic!("Failed to import type `B`"),
            Some(type_b) => {
                // Type A is imported with all the correct fields
                let id_field = type_b.fields.iter().find(|field| field.name.eq("id"));
                let foo_field = type_b.fields.iter().find(|field| field.name.eq("bar"));

                match (id_field, foo_field) {
                    (Some(_), Some(_)) => (),
                    _ => panic!("Imported type `B` does not have the correct fields"),
                };

                // Type A has a @subgraphId directive with the correct id
                match type_b
                    .directives
                    .iter()
                    .find(|directive| directive.name.eq("subgraphId"))
                {
                    Some(directive) => {
                        // Ensure the id argument on the directive is correct
                        match directive.arguments.iter().find(|(name, _)| name.eq("id")) {
                            Some((_, Value::String(id))) if id.eq("childtwo") => (),
                            _ => {
                                panic!(
                                "Imported type `B` needs a @subgraphId directive: @subgraphId(id: \"c2id\")"
                            );
                            }
                        }
                    }
                    None => panic!("Imported type `B` does not have a `@subgraphId` directive"),
                };
            }
        }
    }

    #[test]
    fn test_schema_type_definition_removed() {
        let root_schema = schema_with_import(
            SubgraphDeploymentId::new("root").unwrap(),
            String::from("A"),
            String::from("c1/subgraph"),
        );
        let child_1_schema = schema_with_name_as_import(
            SubgraphDeploymentId::new("childone").unwrap(),
            String::from("T"),
            String::from("A"),
            String::from("c2/subgraph"),
        );
        let child_2_schema = schema_with_type(
            SubgraphDeploymentId::new("childtwo").unwrap(),
            String::from("T"),
        );

        let mut schemas = HashMap::new();
        schemas.insert(
            SchemaReference::ByName(SubgraphName::new("childone/subgraph").unwrap()),
            Arc::new(child_1_schema),
        );
        schemas.insert(
            SchemaReference::ByName(SubgraphName::new("childtwo/subgraph").unwrap()),
            Arc::new(child_2_schema),
        );

        // Call merged_schema
        let merged = merged_schema(&root_schema, schemas);

        match merged
            .document
            .get_object_type_definitions()
            .into_iter()
            .find(|object_type| object_type.name.eq(SCHEMA_TYPE_NAME))
        {
            None => (),
            Some(_) => {
                panic!("_Schema_ type should be removed from the merged schema");
            }
        };
    }
}
