use crate::components::store::SubgraphStore;
use crate::data::graphql::ext::{DirectiveExt, DirectiveFinder, DocumentExt, TypeExt, ValueExt};
use crate::data::store::ValueType;
use crate::data::subgraph::{SubgraphDeploymentId, SubgraphName};
use crate::prelude::{
    q::Value,
    s::{self, Definition, InterfaceType, ObjectType, TypeDefinition, *},
};

use anyhow::{Context, Error};
use graphql_parser::{self, Pos};
use inflector::Inflector;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::TryFrom;
use std::fmt;
use std::hash::Hash;
use std::iter::FromIterator;
use std::str::FromStr;
use std::sync::Arc;

pub const SCHEMA_TYPE_NAME: &str = "_Schema_";

pub const META_FIELD_TYPE: &str = "_Meta_";
pub const META_FIELD_NAME: &str = "_meta";

pub const BLOCK_FIELD_TYPE: &str = "_Block_";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Strings(Vec<String>);

impl fmt::Display for Strings {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let s = (&self.0).join(", ");
        write!(f, "{}", s)
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SchemaValidationError {
    #[error("Interface `{0}` not defined")]
    InterfaceUndefined(String),

    #[error("@entity directive missing on the following types: `{0}`")]
    EntityDirectivesMissing(Strings),

    #[error(
        "Entity type `{0}` does not satisfy interface `{1}` because it is missing \
         the following fields: {2}"
    )]
    InterfaceFieldsMissing(String, String, Strings), // (type, interface, missing_fields)
    #[error("Field `{1}` in type `{0}` has invalid @derivedFrom: {2}")]
    InvalidDerivedFrom(String, String, String), // (type, field, reason)
    #[error("_Schema_ type is only for @imports and must not have any fields")]
    SchemaTypeWithFields,
    #[error("Imported subgraph name `{0}` is invalid")]
    ImportedSubgraphNameInvalid(String),
    #[error("Imported subgraph id `{0}` is invalid")]
    ImportedSubgraphIdInvalid(String),
    #[error("The _Schema_ type only allows @import directives")]
    InvalidSchemaTypeDirectives,
    #[error(
        r#"@import directives must have the form \
@import(types: ["A", {{ name: "B", as: "C"}}], from: {{ name: "org/subgraph"}}) or \
@import(types: ["A", {{ name: "B", as: "C"}}], from: {{ id: "Qm..."}})"#
    )]
    ImportDirectiveInvalid,
    #[error("Type `{0}`, field `{1}`: type `{2}` is neither defined nor imported")]
    FieldTypeUnknown(String, String, String), // (type_name, field_name, field_type)
    #[error("Imported type `{0}` does not exist in the `{1}` schema")]
    ImportedTypeUndefined(String, String), // (type_name, schema)
    #[error("Fulltext directive name undefined")]
    FulltextNameUndefined,
    #[error("Fulltext directive name overlaps with type: {0}")]
    FulltextNameConflict(String),
    #[error("Fulltext directive name overlaps with an existing entity field or a top-level query field: {0}")]
    FulltextNameCollision(String),
    #[error("Fulltext language is undefined")]
    FulltextLanguageUndefined,
    #[error("Fulltext language is invalid: {0}")]
    FulltextLanguageInvalid(String),
    #[error("Fulltext algorithm is undefined")]
    FulltextAlgorithmUndefined,
    #[error("Fulltext algorithm is invalid: {0}")]
    FulltextAlgorithmInvalid(String),
    #[error("Fulltext include is invalid")]
    FulltextIncludeInvalid,
    #[error("Fulltext directive requires an 'include' list")]
    FulltextIncludeUndefined,
    #[error("Fulltext 'include' list must contain an object")]
    FulltextIncludeObjectMissing,
    #[error(
        "Fulltext 'include' object must contain 'entity' (String) and 'fields' (List) attributes"
    )]
    FulltextIncludeEntityMissingOrIncorrectAttributes,
    #[error("Fulltext directive includes an entity not found on the subgraph schema")]
    FulltextIncludedEntityNotFound,
    #[error("Fulltext include field must have a 'name' attribute")]
    FulltextIncludedFieldMissingRequiredProperty,
    #[error("Fulltext entity field, {0}, not found or not a string")]
    FulltextIncludedFieldInvalid(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum FulltextLanguage {
    Simple,
    Danish,
    Dutch,
    English,
    Finnish,
    French,
    German,
    Hungarian,
    Italian,
    Norwegian,
    Portugese,
    Romanian,
    Russian,
    Spanish,
    Swedish,
    Turkish,
}

impl TryFrom<&String> for FulltextLanguage {
    type Error = String;
    fn try_from(language: &String) -> Result<Self, Self::Error> {
        match &language[..] {
            "simple" => Ok(FulltextLanguage::Simple),
            "da" => Ok(FulltextLanguage::Danish),
            "nl" => Ok(FulltextLanguage::Dutch),
            "en" => Ok(FulltextLanguage::English),
            "fi" => Ok(FulltextLanguage::Finnish),
            "fr" => Ok(FulltextLanguage::French),
            "de" => Ok(FulltextLanguage::German),
            "hu" => Ok(FulltextLanguage::Hungarian),
            "it" => Ok(FulltextLanguage::Italian),
            "no" => Ok(FulltextLanguage::Norwegian),
            "pt" => Ok(FulltextLanguage::Portugese),
            "ro" => Ok(FulltextLanguage::Romanian),
            "ru" => Ok(FulltextLanguage::Russian),
            "es" => Ok(FulltextLanguage::Spanish),
            "sv" => Ok(FulltextLanguage::Swedish),
            "tr" => Ok(FulltextLanguage::Turkish),
            invalid => Err(format!(
                "Provided language for fulltext search is invalid: {}",
                invalid
            )),
        }
    }
}

impl FulltextLanguage {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Simple => "simple",
            Self::Danish => "danish",
            Self::Dutch => "dutch",
            Self::English => "english",
            Self::Finnish => "finnish",
            Self::French => "french",
            Self::German => "german",
            Self::Hungarian => "hungarian",
            Self::Italian => "italian",
            Self::Norwegian => "norwegian",
            Self::Portugese => "portugese",
            Self::Romanian => "romanian",
            Self::Russian => "russian",
            Self::Spanish => "spanish",
            Self::Swedish => "swedish",
            Self::Turkish => "turkish",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum FulltextAlgorithm {
    Rank,
    ProximityRank,
}

impl TryFrom<&String> for FulltextAlgorithm {
    type Error = String;
    fn try_from(algorithm: &String) -> Result<Self, Self::Error> {
        match &algorithm[..] {
            "rank" => Ok(FulltextAlgorithm::Rank),
            "proximityRank" => Ok(FulltextAlgorithm::ProximityRank),
            invalid => Err(format!(
                "The provided fulltext search algorithm {} is invalid. It must be one of: rank, proximityRank",
                invalid,
            )),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FulltextConfig {
    pub language: FulltextLanguage,
    pub algorithm: FulltextAlgorithm,
}

pub struct FulltextDefinition {
    pub config: FulltextConfig,
    pub included_fields: HashSet<String>,
    pub name: String,
}

impl From<&s::Directive> for FulltextDefinition {
    // Assumes the input is a Fulltext Directive that has already been validated because it makes
    // liberal use of unwrap() where specific types are expected
    fn from(directive: &Directive) -> Self {
        let name = directive
            .argument("name")
            .unwrap()
            .as_string()
            .unwrap()
            .clone();

        let algorithm = FulltextAlgorithm::try_from(
            directive.argument("algorithm").unwrap().as_enum().unwrap(),
        )
        .unwrap();

        let language =
            FulltextLanguage::try_from(directive.argument("language").unwrap().as_enum().unwrap())
                .unwrap();

        let included_entity_list = directive.argument("include").unwrap().as_list().unwrap();
        // Currently fulltext query fields are limited to 1 entity, so we just take the first (and only) included Entity
        let included_entity = included_entity_list.first().unwrap().as_object().unwrap();
        let included_field_values = included_entity.get("fields").unwrap().as_list().unwrap();
        let included_fields: HashSet<String> = included_field_values
            .into_iter()
            .map(|field| {
                field
                    .as_object()
                    .unwrap()
                    .get("name")
                    .unwrap()
                    .as_string()
                    .unwrap()
                    .clone()
            })
            .collect();

        FulltextDefinition {
            config: FulltextConfig {
                language,
                algorithm,
            },
            included_fields,
            name,
        }
    }
}
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum SchemaImportError {
    #[error("Schema for imported subgraph `{0}` was not found")]
    ImportedSchemaNotFound(SchemaReference),
    #[error("Subgraph for imported schema `{0}` is not deployed")]
    ImportedSubgraphNotFound(SchemaReference),
}

/// The representation of a single type from an import statement. This
/// corresponds either to a string `"Thing"` or an object
/// `{name: "Thing", as: "Stuff"}`. The first form is equivalent to
/// `{name: "Thing", as: "Thing"}`
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ImportedType {
    /// The 'name'
    name: String,
    /// The 'as' alias or a copy of `name` if the user did not specify an alias
    alias: String,
    /// Whether the alias was explicitly given or is just a copy of the name
    explicit: bool,
}

impl fmt::Display for ImportedType {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        if self.explicit {
            write!(f, "name: {}, as: {}", self.name, self.alias)
        } else {
            write!(f, "{}", self.name)
        }
    }
}

impl ImportedType {
    fn parse(type_import: &Value) -> Option<Self> {
        match type_import {
            Value::String(type_name) => Some(ImportedType {
                name: type_name.to_string(),
                alias: type_name.to_string(),
                explicit: false,
            }),
            Value::Object(type_name_as) => {
                match (type_name_as.get("name"), type_name_as.get("as")) {
                    (Some(name), Some(az)) => Some(ImportedType {
                        name: name.to_string(),
                        alias: az.to_string(),
                        explicit: true,
                    }),
                    _ => None,
                }
            }
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SchemaReference {
    subgraph: SubgraphDeploymentId,
}

impl fmt::Display for SchemaReference {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.subgraph)
    }
}

impl SchemaReference {
    fn new(subgraph: SubgraphDeploymentId) -> Self {
        SchemaReference { subgraph }
    }

    pub fn resolve<S: SubgraphStore>(
        &self,
        store: Arc<S>,
    ) -> Result<Arc<Schema>, SchemaImportError> {
        store
            .input_schema(&self.subgraph)
            .map_err(|_| SchemaImportError::ImportedSchemaNotFound(self.clone()))
    }

    fn parse(value: &Value) -> Option<Self> {
        match value {
            Value::Object(map) => match map.get("id") {
                Some(Value::String(id)) => match SubgraphDeploymentId::new(id) {
                    Ok(id) => Some(SchemaReference::new(id)),
                    _ => None,
                },
                _ => None,
            },
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct ApiSchema {
    pub schema: Schema,

    // Root types for the api schema.
    pub query_type: Arc<ObjectType>,
    pub subscription_type: Option<Arc<ObjectType>>,
}

impl ApiSchema {
    /// `api_schema` will typically come from `fn api_schema` in the graphql crate.
    pub fn from_api_schema(api_schema: Schema) -> Result<Self, anyhow::Error> {
        let query_type = api_schema
            .document
            .get_root_query_type()
            .context("no root `Query` in the schema")?
            .clone();
        let subscription_type = api_schema
            .document
            .get_root_subscription_type()
            .cloned()
            .map(Arc::new);

        Ok(Self {
            schema: api_schema,
            query_type: Arc::new(query_type),
            subscription_type,
        })
    }

    pub fn document(&self) -> &s::Document {
        &self.schema.document
    }

    pub fn id(&self) -> &SubgraphDeploymentId {
        &self.schema.id
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn types_for_interface(&self) -> &BTreeMap<String, Vec<ObjectType>> {
        &self.schema.types_for_interface
    }

    /// Returns `None` if the type implements no interfaces.
    pub fn interfaces_for_type(&self, type_name: &String) -> Option<&Vec<InterfaceType>> {
        self.schema.interfaces_for_type(type_name)
    }
}

/// A validated and preprocessed GraphQL schema for a subgraph.
#[derive(Clone, Debug, PartialEq)]
pub struct Schema {
    pub id: SubgraphDeploymentId,
    pub document: s::Document,

    // Maps type name to implemented interfaces.
    pub interfaces_for_type: BTreeMap<String, Vec<InterfaceType>>,

    // Maps an interface name to the list of entities that implement it.
    pub types_for_interface: BTreeMap<String, Vec<ObjectType>>,
}

impl Schema {
    /// Create a new schema. The document must already have been
    /// validated. This function is only useful for creating an introspection
    /// schema, and should not be used otherwise
    pub fn new(id: SubgraphDeploymentId, document: s::Document) -> Self {
        Schema {
            id,
            document,
            interfaces_for_type: BTreeMap::new(),
            types_for_interface: BTreeMap::new(),
        }
    }

    pub fn resolve_schema_references<S: SubgraphStore>(
        &self,
        store: Arc<S>,
    ) -> (
        HashMap<SchemaReference, Arc<Schema>>,
        Vec<SchemaImportError>,
    ) {
        let mut schemas = HashMap::new();
        let mut visit_log = HashSet::new();
        let import_errors = self.resolve_import_graph(store, &mut schemas, &mut visit_log);
        (schemas, import_errors)
    }

    fn resolve_import_graph<S: SubgraphStore>(
        &self,
        store: Arc<S>,
        schemas: &mut HashMap<SchemaReference, Arc<Schema>>,
        visit_log: &mut HashSet<SubgraphDeploymentId>,
    ) -> Vec<SchemaImportError> {
        // Use the visit log to detect cycles in the import graph
        self.imported_schemas()
            .into_iter()
            .fold(vec![], |mut errors, schema_ref| {
                match schema_ref.resolve(store.clone()) {
                    Ok(schema) => {
                        schemas.insert(schema_ref, schema.clone());
                        // If this node in the graph has already been visited stop traversing
                        if !visit_log.contains(&schema.id) {
                            visit_log.insert(schema.id.clone());
                            errors.extend(schema.resolve_import_graph(
                                store.clone(),
                                schemas,
                                visit_log,
                            ));
                        }
                    }
                    Err(err) => {
                        errors.push(err);
                    }
                }
                errors
            })
    }

    pub fn collect_interfaces(
        document: &s::Document,
    ) -> Result<
        (
            BTreeMap<String, Vec<InterfaceType>>,
            BTreeMap<String, Vec<ObjectType>>,
        ),
        SchemaValidationError,
    > {
        // Initialize with an empty vec for each interface, so we don't
        // miss interfaces that have no implementors.
        let mut types_for_interface =
            BTreeMap::from_iter(document.definitions.iter().filter_map(|d| match d {
                Definition::TypeDefinition(TypeDefinition::Interface(t)) => {
                    Some((t.name.clone(), vec![]))
                }
                _ => None,
            }));
        let mut interfaces_for_type = BTreeMap::<_, Vec<_>>::new();

        for object_type in document.get_object_type_definitions() {
            for implemented_interface in object_type.implements_interfaces.clone() {
                let interface_type = document
                    .definitions
                    .iter()
                    .find_map(|def| match def {
                        Definition::TypeDefinition(TypeDefinition::Interface(i))
                            if i.name.eq(&implemented_interface) =>
                        {
                            Some(i.clone())
                        }
                        _ => None,
                    })
                    .ok_or_else(|| {
                        SchemaValidationError::InterfaceUndefined(implemented_interface.clone())
                    })?;

                Self::validate_interface_implementation(object_type, &interface_type)?;

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

        Ok((interfaces_for_type, types_for_interface))
    }

    pub fn parse(raw: &str, id: SubgraphDeploymentId) -> Result<Self, Error> {
        let document = graphql_parser::parse_schema(&raw)?.into_static();

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

    fn imported_types(&self) -> HashMap<ImportedType, SchemaReference> {
        fn parse_types(import: &Directive) -> Vec<ImportedType> {
            import
                .argument("types")
                .map_or(vec![], |value| match value {
                    Value::List(types) => types.iter().filter_map(ImportedType::parse).collect(),
                    _ => vec![],
                })
        }

        self.subgraph_schema_object_type()
            .map_or(HashMap::new(), |object| {
                object
                    .directives
                    .iter()
                    .filter(|directive| directive.name.eq("import"))
                    .map(|import| {
                        import.argument("from").map_or(vec![], |from| {
                            SchemaReference::parse(from).map_or(vec![], |schema_ref| {
                                parse_types(import)
                                    .into_iter()
                                    .map(|imported_type| (imported_type, schema_ref.clone()))
                                    .collect()
                            })
                        })
                    })
                    .flatten()
                    .collect::<HashMap<ImportedType, SchemaReference>>()
            })
    }

    pub fn imported_schemas(&self) -> Vec<SchemaReference> {
        self.subgraph_schema_object_type().map_or(vec![], |object| {
            object
                .directives
                .iter()
                .filter(|directive| directive.name.eq("import"))
                .filter_map(|directive| directive.argument("from"))
                .filter_map(SchemaReference::parse)
                .collect()
        })
    }

    pub fn name_argument_value_from_directive(directive: &Directive) -> Value {
        directive
            .argument("name")
            .expect("fulltext directive must have name argument")
            .clone()
    }

    /// Returned map has one an entry for each interface in the schema.
    pub fn types_for_interface(&self) -> &BTreeMap<String, Vec<ObjectType>> {
        &self.types_for_interface
    }

    /// Returns `None` if the type implements no interfaces.
    pub fn interfaces_for_type(&self, type_name: &String) -> Option<&Vec<InterfaceType>> {
        self.interfaces_for_type.get(type_name)
    }

    // Adds a @subgraphId(id: ...) directive to object/interface/enum types in the schema.
    pub fn add_subgraph_id_directives(&mut self, id: SubgraphDeploymentId) {
        for definition in self.document.definitions.iter_mut() {
            let subgraph_id_argument = (String::from("id"), s::Value::String(id.to_string()));

            let subgraph_id_directive = s::Directive {
                name: "subgraphId".to_string(),
                position: Pos::default(),
                arguments: vec![subgraph_id_argument],
            };

            if let Definition::TypeDefinition(ref mut type_definition) = definition {
                let (name, directives) = match type_definition {
                    TypeDefinition::Object(object_type) => {
                        (&object_type.name, &mut object_type.directives)
                    }
                    TypeDefinition::Interface(interface_type) => {
                        (&interface_type.name, &mut interface_type.directives)
                    }
                    TypeDefinition::Enum(enum_type) => (&enum_type.name, &mut enum_type.directives),
                    TypeDefinition::Scalar(scalar_type) => {
                        (&scalar_type.name, &mut scalar_type.directives)
                    }
                    TypeDefinition::InputObject(input_object_type) => {
                        (&input_object_type.name, &mut input_object_type.directives)
                    }
                    TypeDefinition::Union(union_type) => {
                        (&union_type.name, &mut union_type.directives)
                    }
                };

                if !name.eq(SCHEMA_TYPE_NAME)
                    && directives
                        .iter()
                        .find(|directive| directive.name.eq("subgraphId"))
                        .is_none()
                {
                    directives.push(subgraph_id_directive);
                }
            };
        }
    }

    pub fn validate(
        &self,
        schemas: &HashMap<SchemaReference, Arc<Schema>>,
    ) -> Result<(), Vec<SchemaValidationError>> {
        let mut errors = vec![];
        self.validate_schema_types()
            .unwrap_or_else(|err| errors.push(err));
        self.validate_derived_from()
            .unwrap_or_else(|err| errors.push(err));
        self.validate_schema_type_has_no_fields()
            .unwrap_or_else(|err| errors.push(err));
        self.validate_directives_on_schema_type()
            .unwrap_or_else(|err| errors.push(err));
        errors.append(&mut self.validate_fields());
        errors.append(&mut self.validate_import_directives());
        errors.append(&mut self.validate_fulltext_directives());
        errors.append(&mut self.validate_imported_types(schemas));
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    fn validate_schema_type_has_no_fields(&self) -> Result<(), SchemaValidationError> {
        match self
            .subgraph_schema_object_type()
            .and_then(|subgraph_schema_type| {
                if !subgraph_schema_type.fields.is_empty() {
                    Some(SchemaValidationError::SchemaTypeWithFields)
                } else {
                    None
                }
            }) {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    fn validate_directives_on_schema_type(&self) -> Result<(), SchemaValidationError> {
        match self
            .subgraph_schema_object_type()
            .and_then(|subgraph_schema_type| {
                if !subgraph_schema_type
                    .directives
                    .iter()
                    .filter(|directive| {
                        !directive.name.eq("import") && !directive.name.eq("fulltext")
                    })
                    .collect::<Vec<&Directive>>()
                    .is_empty()
                {
                    Some(SchemaValidationError::InvalidSchemaTypeDirectives)
                } else {
                    None
                }
            }) {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    /// Check the syntax of a single `@import` directive
    fn validate_import_directive_arguments(import: &Directive) -> Option<SchemaValidationError> {
        fn validate_import_type(typ: &Value) -> Result<(), ()> {
            match typ {
                Value::String(_) => Ok(()),
                Value::Object(typ) => match (typ.get("name"), typ.get("as")) {
                    (Some(Value::String(_)), Some(Value::String(_))) => Ok(()),
                    _ => Err(()),
                },
                _ => Err(()),
            }
        }

        fn types_are_valid(types: Option<&Value>) -> bool {
            // All of the elements in the `types` field are valid: either
            // a string or an object with keys `name` and `as` which are strings
            if let Some(Value::List(types)) = types {
                types
                    .iter()
                    .try_for_each(validate_import_type)
                    .err()
                    .is_none()
            } else {
                false
            }
        }

        fn from_is_valid(from: Option<&Value>) -> bool {
            if let Some(Value::Object(from)) = from {
                let has_id = match from.get("id") {
                    Some(Value::String(_)) => true,
                    _ => false,
                };
                let has_name = match from.get("name") {
                    Some(Value::String(_)) => true,
                    _ => false,
                };
                has_id ^ has_name
            } else {
                false
            }
        }

        if from_is_valid(import.argument("from")) && types_are_valid(import.argument("types")) {
            None
        } else {
            Some(SchemaValidationError::ImportDirectiveInvalid)
        }
    }

    fn validate_import_directive_schema_reference_parses(
        directive: &Directive,
    ) -> Option<SchemaValidationError> {
        directive.argument("from").and_then(|from| match from {
            Value::Object(from) => {
                let id_parse_error = match from.get("id") {
                    Some(Value::String(id)) => match SubgraphDeploymentId::new(id) {
                        Err(_) => {
                            Some(SchemaValidationError::ImportedSubgraphIdInvalid(id.clone()))
                        }
                        _ => None,
                    },
                    _ => None,
                };
                let name_parse_error = match from.get("name") {
                    Some(Value::String(name)) => match SubgraphName::new(name) {
                        Err(_) => Some(SchemaValidationError::ImportedSubgraphNameInvalid(
                            name.clone(),
                        )),
                        _ => None,
                    },
                    _ => None,
                };
                id_parse_error.or(name_parse_error)
            }
            _ => None,
        })
    }

    fn validate_fulltext_directives(&self) -> Vec<SchemaValidationError> {
        self.subgraph_schema_object_type()
            .map_or(vec![], |subgraph_schema_type| {
                subgraph_schema_type
                    .directives
                    .iter()
                    .filter(|directives| directives.name.eq("fulltext"))
                    .fold(vec![], |mut errors, fulltext| {
                        errors.extend(self.validate_fulltext_directive_name(fulltext).into_iter());
                        errors.extend(
                            self.validate_fulltext_directive_language(fulltext)
                                .into_iter(),
                        );
                        errors.extend(
                            self.validate_fulltext_directive_algorithm(fulltext)
                                .into_iter(),
                        );
                        errors.extend(
                            self.validate_fulltext_directive_includes(fulltext)
                                .into_iter(),
                        );
                        errors
                    })
            })
    }

    fn validate_fulltext_directive_name(&self, fulltext: &Directive) -> Vec<SchemaValidationError> {
        let name = match fulltext.argument("name") {
            Some(Value::String(name)) => name,
            _ => return vec![SchemaValidationError::FulltextNameUndefined],
        };

        let local_types: Vec<&ObjectType> = self
            .document
            .get_object_type_definitions()
            .into_iter()
            .collect();

        // Validate that the fulltext field doesn't collide with any top-level Query fields
        // generated for entity types. The field name conversions should always align with those used
        // to create the field names in `graphql::schema::api::query_fields_for_type()`.
        if local_types
            .iter()
            .find(|typ| {
                typ.fields
                    .iter()
                    .find(|field| {
                        name == &field.name.as_str().to_camel_case()
                            || name == &field.name.to_plural().to_camel_case()
                            || field.name.eq(name)
                    })
                    .is_some()
            })
            .is_some()
        {
            return vec![SchemaValidationError::FulltextNameCollision(
                name.to_string(),
            )];
        }

        // Validate that each fulltext directive has a distinct name
        if self
            .subgraph_schema_object_type()
            .unwrap()
            .directives
            .iter()
            .filter(|directive| directive.name.eq("fulltext"))
            .filter_map(|fulltext| {
                // Collect all @fulltext directives with the same name
                match fulltext.argument("name") {
                    Some(Value::String(n)) if name.eq(n) => Some(n.as_str()),
                    _ => None,
                }
            })
            .collect::<Vec<&_>>()
            .len()
            > 1
        {
            return vec![SchemaValidationError::FulltextNameConflict(
                name.to_string(),
            )];
        } else {
            return vec![];
        }
    }

    fn validate_fulltext_directive_language(
        &self,
        fulltext: &Directive,
    ) -> Vec<SchemaValidationError> {
        let language = match fulltext.argument("language") {
            Some(Value::Enum(language)) => language,
            _ => return vec![SchemaValidationError::FulltextLanguageUndefined],
        };
        match FulltextLanguage::try_from(language) {
            Ok(_) => vec![],
            Err(_) => vec![SchemaValidationError::FulltextLanguageInvalid(
                language.to_string(),
            )],
        }
    }

    fn validate_fulltext_directive_algorithm(
        &self,
        fulltext: &Directive,
    ) -> Vec<SchemaValidationError> {
        let algorithm = match fulltext.argument("algorithm") {
            Some(Value::Enum(algorithm)) => algorithm,
            _ => return vec![SchemaValidationError::FulltextAlgorithmUndefined],
        };
        match FulltextAlgorithm::try_from(algorithm) {
            Ok(_) => vec![],
            Err(_) => vec![SchemaValidationError::FulltextAlgorithmInvalid(
                algorithm.to_string(),
            )],
        }
    }

    fn validate_fulltext_directive_includes(
        &self,
        fulltext: &Directive,
    ) -> Vec<SchemaValidationError> {
        // Only allow fulltext directive on local types
        let local_types: Vec<&ObjectType> = self
            .document
            .get_object_type_definitions()
            .into_iter()
            .collect();

        // Validate that each entity in fulltext.include exists
        let includes = match fulltext.argument("include") {
            Some(Value::List(includes)) if includes.len() > 0 => includes,
            _ => return vec![SchemaValidationError::FulltextIncludeUndefined],
        };

        for include in includes {
            match include.as_object() {
                None => return vec![SchemaValidationError::FulltextIncludeObjectMissing],
                Some(include_entity) => {
                    let (entity, fields) =
                        match (include_entity.get("entity"), include_entity.get("fields")) {
                            (Some(Value::String(entity)), Some(Value::List(fields))) => {
                                (entity, fields)
                            }
                            _ => return vec![SchemaValidationError::FulltextIncludeEntityMissingOrIncorrectAttributes],
                        };

                    // Validate the included entity type is one of the local types
                    let entity_type = match local_types
                        .iter()
                        .cloned()
                        .find(|typ| typ.name[..].eq(entity))
                    {
                        None => return vec![SchemaValidationError::FulltextIncludedEntityNotFound],
                        Some(t) => t.clone(),
                    };

                    for field_value in fields {
                        let field_name = match field_value {
                            Value::Object(field_map) => match field_map.get("name") {
                                Some(Value::String(name)) => name,
                                _ => return vec![SchemaValidationError::FulltextIncludedFieldMissingRequiredProperty],
                            },
                            _ => return vec![SchemaValidationError::FulltextIncludeEntityMissingOrIncorrectAttributes],
                        };

                        // Validate the included field is a String field on the local entity types specified
                        if !&entity_type
                            .fields
                            .iter()
                            .find(|field| {
                                match ValueType::from_str(field.field_type.get_base_type().as_ref())
                                {
                                    Ok(ValueType::String) if field.name.eq(field_name) => true,
                                    _ => false,
                                }
                            })
                            .is_some()
                        {
                            return vec![SchemaValidationError::FulltextIncludedFieldInvalid(
                                field_name.clone(),
                            )];
                        };
                    }
                }
            }
        }
        // Fulltext include validations all passed, so we return an empty vector
        return vec![];
    }

    fn validate_import_directives(&self) -> Vec<SchemaValidationError> {
        self.subgraph_schema_object_type()
            .map_or(vec![], |subgraph_schema_type| {
                subgraph_schema_type
                    .directives
                    .iter()
                    .filter(|directives| directives.name.eq("import"))
                    .fold(vec![], |mut errors, import| {
                        Self::validate_import_directive_arguments(import)
                            .into_iter()
                            .for_each(|err| errors.push(err));
                        Self::validate_import_directive_schema_reference_parses(import)
                            .into_iter()
                            .for_each(|err| errors.push(err));
                        errors
                    })
            })
    }

    fn validate_imported_types(
        &self,
        schemas: &HashMap<SchemaReference, Arc<Schema>>,
    ) -> Vec<SchemaValidationError> {
        self.imported_types()
            .iter()
            .fold(vec![], |mut errors, (imported_type, schema_ref)| {
                schemas
                    .get(schema_ref)
                    .and_then(|schema| {
                        let local_types = schema.document.get_object_type_definitions();
                        let imported_types = schema.imported_types();

                        // Ensure that the imported type is either local to
                        // the respective schema or is itself imported
                        // If the imported type is itself imported, do not
                        // recursively check the schema
                        let schema_handle = schema_ref.subgraph.to_string();
                        let name = imported_type.name.as_str();

                        let is_local = local_types.iter().any(|object| object.name == name);
                        let is_imported = imported_types
                            .iter()
                            .any(|(import, _)| name == import.alias);
                        if !is_local && !is_imported {
                            Some(SchemaValidationError::ImportedTypeUndefined(
                                name.to_string(),
                                schema_handle.to_string(),
                            ))
                        } else {
                            None
                        }
                    })
                    .into_iter()
                    .for_each(|err| errors.push(err));
                errors
            })
    }

    fn validate_fields(&self) -> Vec<SchemaValidationError> {
        let local_types = self.document.get_object_and_interface_type_fields();
        let local_enums = self
            .document
            .get_enum_definitions()
            .iter()
            .map(|enu| enu.name.clone())
            .collect::<Vec<String>>();
        let imported_types = self.imported_types();
        local_types
            .iter()
            .fold(vec![], |errors, (type_name, fields)| {
                fields.iter().fold(errors, |mut errors, field| {
                    let base = field.field_type.get_base_type();
                    if ValueType::is_scalar(base.as_ref()) {
                        return errors;
                    }
                    if local_types.contains_key(base) {
                        return errors;
                    }
                    if imported_types
                        .iter()
                        .any(|(imported_type, _)| &imported_type.alias == base)
                    {
                        return errors;
                    }
                    if local_enums.iter().any(|enu| enu.eq(base)) {
                        return errors;
                    }
                    errors.push(SchemaValidationError::FieldTypeUnknown(
                        type_name.to_string(),
                        field.name.to_string(),
                        base.to_string(),
                    ));
                    errors
                })
            })
    }

    fn validate_schema_types(&self) -> Result<(), SchemaValidationError> {
        let types_without_entity_directive = self
            .document
            .get_object_type_definitions()
            .iter()
            .filter(|t| {
                t.find_directive(String::from("entity")).is_none() && !t.name.eq(SCHEMA_TYPE_NAME)
            })
            .map(|t| t.name.to_owned())
            .collect::<Vec<_>>();
        if types_without_entity_directive.is_empty() {
            Ok(())
        } else {
            Err(SchemaValidationError::EntityDirectivesMissing(Strings(
                types_without_entity_directive,
            )))
        }
    }

    fn validate_derived_from(&self) -> Result<(), SchemaValidationError> {
        // Helper to construct a DerivedFromInvalid
        fn invalid(
            object_type: &ObjectType,
            field_name: &str,
            reason: &str,
        ) -> SchemaValidationError {
            SchemaValidationError::InvalidDerivedFrom(
                object_type.name.to_owned(),
                field_name.to_owned(),
                reason.to_owned(),
            )
        }

        let type_definitions = self.document.get_object_type_definitions();
        let object_and_interface_type_fields = self.document.get_object_and_interface_type_fields();

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
                field
                    .find_directive(String::from("derivedFrom"))
                    .map(|directive| {
                        (
                            object_type,
                            object_type
                                .implements_interfaces
                                .iter()
                                .filter(|iface| {
                                    // Any interface that has `field` can be used
                                    // as the type of the field
                                    self.document
                                        .find_interface(iface)
                                        .map(|iface| {
                                            iface
                                                .fields
                                                .iter()
                                                .any(|ifield| ifield.name.eq(&field.name))
                                        })
                                        .unwrap_or(false)
                                })
                                .collect::<Vec<_>>(),
                            field,
                            directive.argument("field"),
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
                        "the @derivedFrom `field` argument must be a string",
                    ))
                }
            };

            // Check that the type we are deriving from exists
            let target_type_name = field.field_type.get_base_type();
            let target_fields = object_and_interface_type_fields
                .get(target_type_name)
                .ok_or_else(|| {
                    invalid(
                        object_type,
                        &field.name,
                        "type must be an existing entity or interface",
                    )
                })?;

            // Check that the type we are deriving from has a field with the
            // right name and type
            let target_field = target_fields
                .iter()
                .find(|field| field.name.eq(target_field))
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
            let target_field_type = target_field.field_type.get_base_type();
            if target_field_type != &object_type.name
                && target_field_type != "ID"
                && !interface_types
                    .iter()
                    .any(|iface| target_field_type.eq(iface.clone()))
            {
                fn type_signatures(name: &String) -> Vec<String> {
                    vec![
                        format!("{}", name),
                        format!("{}!", name),
                        format!("[{}!]", name),
                        format!("[{}!]!", name),
                    ]
                }

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

    /// Validate that `object` implements `interface`.
    fn validate_interface_implementation(
        object: &ObjectType,
        interface: &InterfaceType,
    ) -> Result<(), SchemaValidationError> {
        // Check that all fields in the interface exist in the object with same name and type.
        let mut missing_fields = vec![];
        for i in &interface.fields {
            if object
                .fields
                .iter()
                .find(|o| o.name.eq(&i.name) && o.field_type.eq(&i.field_type))
                .is_none()
            {
                missing_fields.push(i.to_string().trim().to_owned());
            }
        }
        if !missing_fields.is_empty() {
            Err(SchemaValidationError::InterfaceFieldsMissing(
                object.name.clone(),
                interface.name.clone(),
                Strings(missing_fields),
            ))
        } else {
            Ok(())
        }
    }

    fn subgraph_schema_object_type(&self) -> Option<&ObjectType> {
        self.document
            .get_object_type_definitions()
            .into_iter()
            .find(|object_type| object_type.name.eq(SCHEMA_TYPE_NAME))
    }

    pub fn entity_fulltext_definitions<'a>(
        entity: &str,
        document: &'a Document,
    ) -> Result<Vec<FulltextDefinition>, anyhow::Error> {
        Ok(document
            .get_fulltext_directives()?
            .into_iter()
            .filter(|directive| match directive.argument("include") {
                Some(Value::List(includes)) if includes.len() > 0 => includes
                    .iter()
                    .find(|include| match include {
                        Value::Object(include) => match include.get("entity") {
                            Some(Value::String(fulltext_entity)) if fulltext_entity == entity => {
                                true
                            }
                            _ => false,
                        },
                        _ => false,
                    })
                    .is_some(),
                _ => false,
            })
            .map(|directive| FulltextDefinition::from(directive))
            .collect())
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
        SchemaValidationError::InterfaceUndefined("Bar".to_owned())
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
        "Entity type `Bar` does not satisfy interface `Foo` because it is missing \
         the following fields: x: Int, y: Int",
    );
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

        let document = graphql_parser::parse_schema(&raw)
            .expect("Failed to parse raw schema")
            .into_static();
        let schema = Schema::new(SubgraphDeploymentId::new("id").unwrap(), document);
        match schema.validate_derived_from() {
            Err(ref e) => match e {
                SchemaValidationError::InvalidDerivedFrom(_, _, msg) => assert_eq!(errmsg, msg),
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
        "the @derivedFrom `field` argument must be a string",
    );
    validate(
        "g: G @derivedFrom(field: \"a\")",
        "field `a` on type `G` must have one of the following types: A, A!, [A!], [A!]!",
    );
    validate("h: H @derivedFrom(field: \"a\")", "ok");
    validate(
        "i: NotAType @derivedFrom(field: \"a\")",
        "type must be an existing entity or interface",
    );
    validate("j: B @derivedFrom(field: \"id\")", "ok");
}

#[test]
fn test_reserved_type_with_fields() {
    const ROOT_SCHEMA: &str = "
type _Schema_ { id: ID! }";

    let document = graphql_parser::parse_schema(ROOT_SCHEMA).expect("Failed to parse root schema");
    let schema = Schema::new(SubgraphDeploymentId::new("id").unwrap(), document);
    assert_eq!(
        schema
            .validate_schema_type_has_no_fields()
            .expect_err("Expected validation to fail due to fields defined on the reserved type"),
        SchemaValidationError::SchemaTypeWithFields
    )
}

#[test]
fn test_reserved_type_directives() {
    const ROOT_SCHEMA: &str = "
type _Schema_ @illegal";

    let document = graphql_parser::parse_schema(ROOT_SCHEMA).expect("Failed to parse root schema");
    let schema = Schema::new(SubgraphDeploymentId::new("id").unwrap(), document);
    assert_eq!(
        schema.validate_directives_on_schema_type().expect_err(
            "Expected validation to fail due to extra imports defined on the reserved type"
        ),
        SchemaValidationError::InvalidSchemaTypeDirectives
    )
}

#[test]
fn test_imports_directive_from_argument() {
    const ROOT_SCHEMA: &str = r#"
type _Schema_ @import(types: ["T", "A", "C"])"#;

    let document = graphql_parser::parse_schema(ROOT_SCHEMA).expect("Failed to parse root schema");
    let schema = Schema::new(SubgraphDeploymentId::new("id").unwrap(), document);
    match schema
        .validate_import_directives()
        .into_iter()
        .find(|err| *err == SchemaValidationError::ImportDirectiveInvalid) {
            None => panic!(
                "Expected validation for `{}` to fail due to an @imports directive without a `from` argument",
                ROOT_SCHEMA,
            ),
            _ => (),
    }
}

#[test]
fn test_enums_pass_field_validation() {
    const ROOT_SCHEMA: &str = r#"
enum Color {
  RED
  GREEN
}

type A @entity {
  id: ID!
  color: Color
}"#;

    let document = graphql_parser::parse_schema(ROOT_SCHEMA).expect("Failed to parse root schema");
    let schema = Schema::new(SubgraphDeploymentId::new("id").unwrap(), document);
    assert_eq!(schema.validate_fields().len(), 0);
}

#[test]
fn test_recursively_imported_type_validates() {
    const ROOT_SCHEMA: &str = r#"
type _Schema_ @import(types: ["T"], from: { id: "c1id" })"#;
    const CHILD_1_SCHEMA: &str = r#"
type _Schema_ @import(types: ["T"], from: { id: "c2id" })"#;
    const CHILD_2_SCHEMA: &str = r#"
type T @entity { id: ID! }
"#;

    let root_document =
        graphql_parser::parse_schema(ROOT_SCHEMA).expect("Failed to parse root schema");
    let child_1_document =
        graphql_parser::parse_schema(CHILD_1_SCHEMA).expect("Failed to parse child 1 schema");
    let child_2_document =
        graphql_parser::parse_schema(CHILD_2_SCHEMA).expect("Failed to parse child 2 schema");

    let c1id = SubgraphDeploymentId::new("c1id").unwrap();
    let c2id = SubgraphDeploymentId::new("c2id").unwrap();
    let root_schema = Schema::new(SubgraphDeploymentId::new("rid").unwrap(), root_document);
    let child_1_schema = Schema::new(c1id.clone(), child_1_document);
    let child_2_schema = Schema::new(c2id.clone(), child_2_document);

    let mut schemas = HashMap::new();
    schemas.insert(SchemaReference::new(c1id), Arc::new(child_1_schema));
    schemas.insert(SchemaReference::new(c2id), Arc::new(child_2_schema));

    match root_schema.validate_imported_types(&schemas).is_empty() {
        false => panic!(
            "Expected imported types validation for `{}` to suceed",
            ROOT_SCHEMA,
        ),
        true => (),
    }
}

#[test]
fn test_recursively_imported_type_which_dne_fails_validation() {
    const ROOT_SCHEMA: &str = r#"
type _Schema_ @import(types: ["T"], from: { id:"c1id"})"#;
    const CHILD_1_SCHEMA: &str = r#"
type _Schema_ @import(types: [{name: "T", as: "A"}], from: { id:"c2id"})"#;
    const CHILD_2_SCHEMA: &str = r#"
type T @entity { id: ID! }
"#;
    let root_document =
        graphql_parser::parse_schema(ROOT_SCHEMA).expect("Failed to parse root schema");
    let child_1_document =
        graphql_parser::parse_schema(CHILD_1_SCHEMA).expect("Failed to parse child 1 schema");
    let child_2_document =
        graphql_parser::parse_schema(CHILD_2_SCHEMA).expect("Failed to parse child 2 schema");

    let c1id = SubgraphDeploymentId::new("c1id").unwrap();
    let c2id = SubgraphDeploymentId::new("c2id").unwrap();
    let root_schema = Schema::new(SubgraphDeploymentId::new("rid").unwrap(), root_document);
    let child_1_schema = Schema::new(c1id.clone(), child_1_document);
    let child_2_schema = Schema::new(c2id.clone(), child_2_document);

    let mut schemas = HashMap::new();
    schemas.insert(SchemaReference::new(c1id), Arc::new(child_1_schema));
    schemas.insert(SchemaReference::new(c2id), Arc::new(child_2_schema));

    match root_schema.validate_imported_types(&schemas).into_iter().find(|err| match err {
        SchemaValidationError::ImportedTypeUndefined(_, _) => true,
        _ => false,
    }) {
        None => panic!(
            "Expected imported types validation to fail because an imported type was missing in the target schema",
        ),
        _ => (),
    }
}

#[test]
fn test_fulltext_directive_validation() {
    const SCHEMA: &str = r#"
type _Schema_ @fulltext(
  name: "metadata"
  language: en
  algorithm: rank
  include: [
    {
      entity: "Gravatar",
      fields: [
        { name: "displayName"},
        { name: "imageUrl"},
      ]
    }
  ]
)
type Gravatar @entity {
  id: ID!
  owner: Bytes!
  displayName: String!
  imageUrl: String!
}"#;

    let document = graphql_parser::parse_schema(SCHEMA).expect("Failed to parse schema");
    let schema = Schema::new(SubgraphDeploymentId::new("id1").unwrap(), document);

    assert_eq!(schema.validate_fulltext_directives(), vec![]);
}
