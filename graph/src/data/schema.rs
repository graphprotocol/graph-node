use data::graphql::validation::validate_schema;
use data::subgraph::SubgraphId;
use failure::Error;
use graphql_parser;
use graphql_parser::{schema, Pos};

/// A validated and preprocessed GraphQL schema for a subgraph.
#[derive(Clone, Debug, PartialEq)]
pub struct Schema {
    pub id: SubgraphId,
    pub document: schema::Document,
}

impl Schema {
    pub fn parse(raw: &str, id: SubgraphId) -> Result<Self, Error> {
        let document = graphql_parser::parse_schema(&raw)?;
        validate_schema(&document)?;
        let mut schema = Schema {
            id: id.clone(),
            document,
        };
        schema.add_subgraph_id_directives(id);
        Ok(schema)
    }

    // Adds a @subgraphId(id: ...) directive to object/interface/enum types in the schema.
    fn add_subgraph_id_directives(&mut self, id: SubgraphId) {
        for definition in self.document.definitions.iter_mut() {
            let subgraph_id_argument =
                (schema::Name::from("id"), schema::Value::String(id.clone()));

            let subgraph_id_directive = schema::Directive {
                name: "subgraphId".to_string(),
                position: Pos::default(),
                arguments: vec![subgraph_id_argument],
            };

            if let schema::Definition::TypeDefinition(ref mut type_definition) = definition {
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
            };
        }
    }
}
