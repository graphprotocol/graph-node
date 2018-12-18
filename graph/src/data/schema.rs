use data::graphql::validation::{get_object_type_definitions, validate_schema};
use data::subgraph::SubgraphId;
use failure::Error;
use graphql_parser;
use graphql_parser::{
    query,
    schema::{self, ObjectType},
    Pos,
};
use std::collections::BTreeMap;

/// A validated and preprocessed GraphQL schema for a subgraph.
#[derive(Clone, Debug, PartialEq)]
pub struct Schema {
    pub id: SubgraphId,
    pub document: schema::Document,
    // Maps an interface name to the list of entities that implement it.
    types_for_interface: BTreeMap<query::Name, Vec<ObjectType>>,
}

impl Schema {
    pub fn parse(raw: &str, id: SubgraphId) -> Result<Self, Error> {
        let document = graphql_parser::parse_schema(&raw)?;
        validate_schema(&document)?;

        let mut types_for_interface = BTreeMap::<_, Vec<_>>::new();
        for object_type in get_object_type_definitions(&document) {
            for implemented_interface in object_type.implements_interfaces.clone() {
                types_for_interface
                    .entry(implemented_interface)
                    .and_modify(|vec| vec.push(object_type.clone()))
                    .or_insert(vec![object_type.clone()]);
            }
        }

        let mut schema = Schema {
            id: id.clone(),
            document,
            types_for_interface,
        };
        schema.add_subgraph_id_directives(id);

        Ok(schema)
    }

    pub fn type_for_interface(&self, interface_name: &query::Name) -> Option<&Vec<ObjectType>> {
        self.types_for_interface.get(interface_name)
    }

    // #[cfg(test)]
    // pub fn new(id: SubgraphId, document: Schema::Document) -> Self {
    //     Schema {
    //         id,
    //         document,
    //         types_for_interface: BTreeMap::new(),
    //     }
    // }

    // Adds a @subgraphId(id: ...) directive to object/interface/enum types in the schema.
    fn add_subgraph_id_directives(&mut self, id: SubgraphId) {
        for definition in self.document.definitions.iter_mut() {
            let subgraph_id_argument = (
                schema::Name::from("id"),
                schema::Value::String(id.to_string()),
            );

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
