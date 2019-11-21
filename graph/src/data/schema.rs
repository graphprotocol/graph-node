use crate::components::store::{Store, SubgraphDeploymentStore};
use crate::data::graphql::validation::{
    get_object_type_definitions, validate_interface_implementation, validate_schema,
    SchemaValidationError,
};
use crate::data::subgraph::{SubgraphDeploymentId, SubgraphName};
use crate::prelude::future::{self, *};
use crate::prelude::Fail;
use failure::Error;
use graphql_parser;
use graphql_parser::{
    query::Name,
    schema::{self, InterfaceType, ObjectType, TypeDefinition, Value},
    Pos,
};

use std::collections::BTreeMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;
use std::sync::Arc;

pub const SUBGRAPH_SCHEMA_TYPE_NAME: &str = "_SubgraphSchema_";

#[derive(Debug, Fail, PartialEq, Eq, Clone)]
pub enum SchemaImportError {
    #[fail(display = "Schema for imported subgraph `{}` was not found", _0)]
    ImportedSchemaNotFound(SchemaReference),
    #[fail(display = "Subgraph for imported schema `{}` is not deployed", _0)]
    ImportedSubgraphNotFound(SchemaReference),
    #[fail(display = "Name for imported subgraph `{}` is invalid", _0)]
    ImportedSubgraphNameInvalid(String),
    #[fail(display = "Id for imported subgraph `{}` is invalid", _0)]
    ImportedSubgraphIdInvalid(String),
}

impl SchemaImportError {
    pub fn is_failure(error: &Self) -> bool {
        match error {
            SchemaImportError::ImportedSubgraphNameInvalid(_)
            | SchemaImportError::ImportedSubgraphIdInvalid(_) => true,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaReference {
    ByName(String),
    ById(String),
}

impl Hash for SchemaReference {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::ById(id) => id.hash(state),
            Self::ByName(name) => name.hash(state),
        };
    }
}

impl fmt::Display for SchemaReference {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            SchemaReference::ByName(name) => write!(f, "{}", name),
            SchemaReference::ById(id) => write!(f, "{}", id),
        }
    }
}

impl SchemaReference {
    pub fn resolve<S: Store + SubgraphDeploymentStore>(
        self,
        store: Arc<S>,
    ) -> Result<Arc<Schema>, SchemaImportError> {
        let subgraph_id = match &self {
            SchemaReference::ByName(name) => {
                let subgraph_name = SubgraphName::new(name.clone())
                    .map_err(|err| SchemaImportError::ImportedSubgraphNameInvalid(name.clone()))?;
                store
                    .resolve_subgraph_name_to_id(subgraph_name.clone())
                    .map_err(|_| SchemaImportError::ImportedSubgraphNotFound(self.clone()))
                    .and_then(|subgraph_id_opt| {
                        subgraph_id_opt
                            .ok_or(SchemaImportError::ImportedSubgraphNotFound(self.clone()))
                    })?
            }
            SchemaReference::ById(id) => SubgraphDeploymentId::new(id.clone())
                .map_err(|err| SchemaImportError::ImportedSubgraphIdInvalid(id.clone()))?,
        };

        store
            .input_schema(&subgraph_id)
            .map_err(|err| SchemaImportError::ImportedSchemaNotFound(self.clone()))
    }
}

/// A validated and preprocessed GraphQL schema for a subgraph.
#[derive(Clone, Debug, PartialEq)]
pub struct Schema {
    pub id: SubgraphDeploymentId,
    pub document: schema::Document,

    // Maps type name to implemented interfaces.
    pub interfaces_for_type: BTreeMap<Name, Vec<InterfaceType>>,

    // Maps an interface name to the list of entities that implement it.
    pub types_for_interface: BTreeMap<Name, Vec<ObjectType>>,
}

impl Schema {
    /// Create a new schema. The document must already have been
    /// validated. This function is only useful for creating an introspection
    /// schema, and should not be used otherwise
    pub fn new(id: SubgraphDeploymentId, document: schema::Document) -> Self {
        Schema {
            id,
            document,
            interfaces_for_type: BTreeMap::new(),
            types_for_interface: BTreeMap::new(),
        }
    }

    pub fn collect_interfaces(
        document: &schema::Document,
    ) -> Result<
        (
            BTreeMap<Name, Vec<InterfaceType>>,
            BTreeMap<Name, Vec<ObjectType>>,
        ),
        SchemaValidationError,
    > {
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

        return Ok((interfaces_for_type, types_for_interface));
    }

    pub fn parse(raw: &str, id: SubgraphDeploymentId) -> Result<Self, Error> {
        let document = graphql_parser::parse_schema(&raw)?;
        validate_schema(&document)?;

        let (interfaces_for_type, types_for_interface) = Self::collect_interfaces(&document)?;

        let mut schema = Schema {
            id: id.clone(),
            document,
            interfaces_for_type,
            types_for_interface,
        };
        schema.add_subgraph_id_directives(id);

        Ok(schema)
    }

    pub fn imported_schemas(&self) -> Vec<SchemaReference> {
        self.subgraph_schema_object_type().map_or(vec![], |object| {
            object
                .directives
                .iter()
                .filter_map(|directive| directive.arguments.iter().find(|(name, _)| name == "from"))
                .filter_map(|(_, value)| match value {
                    Value::Object(map) => {
                        let id = map
                            .get("id")
                            .filter(|id| match id {
                                Value::String(_) => true,
                                _ => false,
                            })
                            .map(|id| match id {
                                Value::String(i) => SchemaReference::ById(i.to_string()),
                                _ => unreachable!(),
                            });
                        let name = map
                            .get("name")
                            .filter(|name| match name {
                                Value::String(_) => true,
                                _ => false,
                            })
                            .map(|name| match name {
                                Value::String(n) => SchemaReference::ByName(n.to_string()),
                                _ => unreachable!(),
                            });
                        id.or(name)
                    }
                    _ => None,
                })
                .collect()
        })
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
    pub fn add_subgraph_id_directives(&mut self, id: SubgraphDeploymentId) {
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
                let directives = match type_definition {
                    TypeDefinition::Object(object_type) => &mut object_type.directives,
                    TypeDefinition::Interface(interface_type) => &mut interface_type.directives,
                    TypeDefinition::Enum(enum_type) => &mut enum_type.directives,
                    TypeDefinition::Scalar(scalar_type) => &mut scalar_type.directives,
                    TypeDefinition::InputObject(input_object_type) => {
                        &mut input_object_type.directives
                    }
                    TypeDefinition::Union(union_type) => &mut union_type.directives,
                };

                if directives
                    .iter()
                    .find(|directive| directive.name == "subgraphId")
                    .is_none()
                {
                    directives.push(subgraph_id_directive);
                }
            };
        }
    }

    fn subgraph_schema_object_type(&self) -> Option<&ObjectType> {
        self.document.definitions.iter().find_map(|def| match def {
            schema::Definition::TypeDefinition(type_def) => match type_def {
                schema::TypeDefinition::Object(object_type) => {
                    if object_type.name == SUBGRAPH_SCHEMA_TYPE_NAME {
                        Some(object_type)
                    } else {
                        None
                    }
                }
                _ => None,
            },
            _ => None,
        })
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
