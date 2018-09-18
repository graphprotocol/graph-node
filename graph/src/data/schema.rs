use graphql_parser::{schema, Pos};

/// A GraphQL schema with additional meta data.
#[derive(Clone, Debug, PartialEq)]
pub struct Schema {
    pub name: String,
    pub id: String,
    pub document: schema::Document,
}

impl Schema {
    // Adds a @subgraphId(id: ...) directive to object/interface/enum types in the schema.
    pub fn add_subgraph_id_directives(&mut self, id: String) {
        for definition in self.document.definitions.iter_mut() {
            let subgraph_id_argument =
                (schema::Name::from("id"), schema::Value::String(id.clone()));

            let subgraph_id_directive = schema::Directive {
                name: "subgraphId".to_string(),
                position: Pos::default(),
                arguments: vec![subgraph_id_argument],
            };

            match definition {
                schema::Definition::TypeDefinition(ref mut type_definition) => {
                    match type_definition {
                        schema::TypeDefinition::Object(ref mut object_type) => {
                            object_type.directives.push(subgraph_id_directive);
                        }
                        schema::TypeDefinition::Interface(ref mut interface_type) => {
                            interface_type.directives.push(subgraph_id_directive);
                        }
                        schema::TypeDefinition::Enum(ref mut enum_type) => {
                            enum_type.directives.push(subgraph_id_directive);
                        }
                        schema::TypeDefinition::Scalar(_scalar_type) => (),
                        schema::TypeDefinition::InputObject(_input_object_type) => (),
                        schema::TypeDefinition::Union(_union_type) => (),
                    }
                }
                _ => (),
            };
        }
    }
}
