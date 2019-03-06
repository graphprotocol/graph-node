use data::graphql::validation::{
    get_object_type_definitions, validate_interface_implementation, validate_schema,
    SchemaValidationError,
};
use data::subgraph::SubgraphDeploymentId;
use failure::Error;
use graphql_parser;
use graphql_parser::{
    query::Name,
    schema::{self, InterfaceType, ObjectType, TypeDefinition},
    Pos,
};
use std::collections::BTreeMap;
use std::iter::FromIterator;

/// A validated and preprocessed GraphQL schema for a subgraph.
#[derive(Clone, Debug, PartialEq)]
pub struct Schema {
    pub id: SubgraphDeploymentId,
    pub document: schema::Document,

    // Maps type name to implemented interfaces.
    interfaces_for_type: BTreeMap<Name, Vec<InterfaceType>>,

    // Maps an interface name to the list of entities that implement it.
    types_for_interface: BTreeMap<Name, Vec<ObjectType>>,
}

impl Schema {
    pub fn parse(raw: &str, id: SubgraphDeploymentId) -> Result<Self, Error> {
        let document = graphql_parser::parse_schema(&raw)?;
        validate_schema(&document)?;

        // Initialize with an empty vec for each interface, so we don't
        // miss interfaces that have no implementors.
        let mut types_for_interface =
            BTreeMap::from_iter(document.definitions.iter().filter_map(|d| match d {
                schema::Definition::TypeDefinition(TypeDefinition::Interface(t)) => {
                    Some((t.name.clone(), vec![]))
                }
                _ => None,
            }));
        let mut interfaces_for_type = BTreeMap::<_, Vec<_>>::new();

        for object_type in get_object_type_definitions(&document) {
            for implemented_interface in object_type.implements_interfaces.clone() {
                let interface_type = document
                    .definitions
                    .iter()
                    .find_map(|def| match def {
                        schema::Definition::TypeDefinition(TypeDefinition::Interface(i))
                            if i.name == implemented_interface =>
                        {
                            Some(i.clone())
                        }
                        _ => None,
                    })
                    .ok_or_else(|| {
                        SchemaValidationError::UndefinedInterface(implemented_interface.clone())
                    })?;

                validate_interface_implementation(object_type, &interface_type)?;

                interfaces_for_type
                    .entry(object_type.name.clone())
                    .or_default()
                    .push(interface_type);
                types_for_interface
                    .get_mut(&implemented_interface)
                    .unwrap()
                    .push(object_type.clone());
            }
        }

        let mut schema = Schema {
            id: id.clone(),
            document,
            interfaces_for_type,
            types_for_interface,
        };
        schema.add_subgraph_id_directives(id);

        Ok(schema)
    }

    /// Returned map has one an entry for each interface in the schema.
    pub fn types_for_interface(&self) -> &BTreeMap<Name, Vec<ObjectType>> {
        &self.types_for_interface
    }

    /// Returns `None` if the type implements no interfaces.
    pub fn interfaces_for_type(&self, type_name: &Name) -> Option<&Vec<InterfaceType>> {
        self.interfaces_for_type.get(type_name)
    }

    // Adds a @subgraphId(id: ...) directive to object/interface/enum types in the schema.
    fn add_subgraph_id_directives(&mut self, id: SubgraphDeploymentId) {
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
                    TypeDefinition::Object(ref mut object_type) => {
                        object_type.directives.push(subgraph_id_directive);
                    }
                    TypeDefinition::Interface(ref mut interface_type) => {
                        interface_type.directives.push(subgraph_id_directive);
                    }
                    TypeDefinition::Enum(ref mut enum_type) => {
                        enum_type.directives.push(subgraph_id_directive);
                    }
                    TypeDefinition::Scalar(_scalar_type) => (),
                    TypeDefinition::InputObject(_input_object_type) => (),
                    TypeDefinition::Union(_union_type) => (),
                }
            };
        }
    }
}

#[test]
fn non_existing_interface() {
    let schema = "type Foo implements Bar @entity { foo: Int }";
    let res = Schema::parse(schema, SubgraphDeploymentId::new("dummy").unwrap());
    let error = res
        .unwrap_err()
        .downcast::<SchemaValidationError>()
        .unwrap();
    assert_eq!(
        error,
        SchemaValidationError::UndefinedInterface("Bar".to_owned())
    );
}

#[test]
fn invalid_interface_implementation() {
    let schema = "
        interface Foo {
            x: Int,
            y: Int
        }

        type Bar implements Foo @entity {
            x: Boolean
        }
    ";
    let res = Schema::parse(schema, SubgraphDeploymentId::new("dummy").unwrap());
    assert_eq!(
        res.unwrap_err().to_string(),
        "Entity type `Bar` cannot implement `Foo` because it is missing the \
         required fields: x: Int, y: Int"
    );
}
