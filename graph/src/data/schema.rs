use crate::data::graphql::ext::{DirectiveFinder, DocumentExt, TypeExt};
use crate::data::graphql::scalar::BuiltInScalarType;
use crate::data::subgraph::{SubgraphDeploymentId, SubgraphName};
use crate::prelude::Fail;

use failure::Error;
use graphql_parser;
use graphql_parser::{
    query::{Name, Value},
    schema::{self, InterfaceType, ObjectType, TypeDefinition, *},
    Pos,
};
use serde::{Deserialize, Serialize};

use std::collections::{BTreeMap, HashMap};
use std::convert::TryFrom;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;
use std::sync::Arc;

pub const SCHEMA_TYPE_NAME: &str = "_Schema_";

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
    #[fail(display = "Interface `{}` not defined", _0)]
    InterfaceUndefined(String),

    #[fail(display = "@entity directive missing on the following types: `{}`", _0)]
    EntityDirectivesMissing(Strings),

    #[fail(
        display = "Entity type `{}` does not satisfy interface `{}` because it is missing \
                   the following fields: {}",
        _0, _1, _2
    )]
    InterfaceFieldsMissing(String, String, Strings), // (type, interface, missing_fields)
    #[fail(
        display = "Field `{}` in type `{}` has invalid @derivedFrom: {}",
        _1, _0, _2
    )]
    InvalidDerivedFrom(String, String, String), // (type, field, reason)
    #[fail(display = "_Schema_ type is only for @imports and must not have any fields")]
    SchemaTypeWithFields,
    #[fail(display = "Imported subgraph name `{}` is invalid", _0)]
    ImportedSubgraphNameInvalid(String),
    #[fail(display = "Imported subgraph id `{}` is invalid", _0)]
    ImportedSubgraphIdInvalid(String),
    #[fail(display = "The _Schema_ type only allows @import directives")]
    InvalidSchemaTypeDirectives,
    #[fail(
        display = "@import directives must have the form @import(types: ['A', {{ name: 'B', as: 'C'}}], from: {{ name: 'org/subgraph'}}) or @import(types: ['A', {{ name: 'B', as: 'C'}}], from: {{ id: 'Qm...'}})"
    )]
    ImportDirectiveInvalid,
    #[fail(
        display = "Type `{}`, field `{}`, type `{}` is neither defined or imported",
        _0, _1, _2
    )]
    FieldTypeUnknown(String, String, String), // (type_name, field_name, field_type)
    #[fail(
        display = "Imported type `{}` does not exist in the `{}` schema",
        _0, _1
    )]
    ImportedTypeUndefined(String, String), // (type_name, schema)
}

#[derive(Debug, Fail, PartialEq, Eq, Clone)]
pub enum SchemaImportError {
    #[fail(display = "Schema for imported subgraph `{}` was not found", _0)]
    ImportedSchemaNotFound(SchemaReference),
    #[fail(display = "Subgraph for imported schema `{}` is not deployed", _0)]
    ImportedSubgraphNotFound(SchemaReference),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ImportedType {
    Name(String),
    NameAs(String, String),
}

impl Hash for ImportedType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Name(name) => name.hash(state),
            Self::NameAs(name, az) => {
                name.hash(state);
                String::from(" as ").hash(state);
                az.hash(state);
            }
        };
    }
}

impl fmt::Display for ImportedType {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            Self::Name(name) => write!(f, "{}", name),
            Self::NameAs(name, az) => write!(f, "name: {}, as: {}", name, az),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaReference {
    ByName(SubgraphName),
    ById(SubgraphDeploymentId),
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

        for object_type in document.get_object_type_definitions() {
            for implemented_interface in object_type.implements_interfaces.clone() {
                let interface_type = document
                    .definitions
                    .iter()
                    .find_map(|def| match def {
                        schema::Definition::TypeDefinition(TypeDefinition::Interface(i))
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

        return Ok((interfaces_for_type, types_for_interface));
    }

    pub fn parse(raw: &str, id: SubgraphDeploymentId) -> Result<Self, Error> {
        let document = graphql_parser::parse_schema(&raw)?;

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

    pub fn imported_types(&self) -> HashMap<ImportedType, SchemaReference> {
        self.subgraph_schema_object_type()
            .map_or(HashMap::new(), |object| {
                object
                    .directives
                    .iter()
                    .filter(|directive| directive.name.eq("import"))
                    .map(|imports| {
                        imports
                            .arguments
                            .iter()
                            .find(|(name, _)| name.eq("from"))
                            .map_or(vec![], |from| {
                                self.schema_reference_from_directive_argument(from).map_or(
                                    vec![],
                                    |schema_ref| {
                                        self.imported_types_from_import_directive(imports)
                                            .iter()
                                            .map(|imported_type| {
                                                (imported_type.clone(), schema_ref.clone())
                                            })
                                            .collect()
                                    },
                                )
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
                .filter_map(|directive| {
                    directive.arguments.iter().find(|(name, _)| name.eq("from"))
                })
                .filter_map(|from| self.schema_reference_from_directive_argument(from))
                .collect()
        })
    }

    fn imported_types_from_import_directive(&self, import: &Directive) -> Vec<ImportedType> {
        import
            .arguments
            .iter()
            .find(|(name, _)| name.eq("types"))
            .map_or(vec![], |(_, value)| match value {
                Value::List(types) => types
                    .iter()
                    .filter_map(|type_import| match type_import {
                        Value::String(type_name) => Some(ImportedType::Name(type_name.to_string())),
                        Value::Object(type_name_as) => {
                            match (type_name_as.get("name"), type_name_as.get("as")) {
                                (Some(Value::String(name)), Some(Value::String(az))) => {
                                    Some(ImportedType::NameAs(name.to_string(), az.to_string()))
                                }
                                _ => None,
                            }
                        }
                        _ => None,
                    })
                    .collect(),
                _ => vec![],
            })
    }

    fn schema_reference_from_directive_argument(
        &self,
        from: &(Name, Value),
    ) -> Option<SchemaReference> {
        let (name, value) = from;
        if !name.eq("from") {
            return None;
        }
        match value {
            Value::Object(map) => {
                let id = match map.get("id") {
                    Some(Value::String(id)) => match SubgraphDeploymentId::new(id) {
                        Ok(id) => Some(SchemaReference::ById(id)),
                        _ => None,
                    },
                    _ => None,
                };
                let name = match map.get("name") {
                    Some(Value::String(name)) => match SubgraphName::new(name) {
                        Ok(name) => Some(SchemaReference::ByName(name)),
                        _ => None,
                    },
                    _ => None,
                };
                id.or(name)
            }
            _ => None,
        }
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
        self.validate_only_import_directives_on_schema_type()
            .unwrap_or_else(|err| errors.push(err));
        errors.append(&mut self.validate_fields());
        errors.append(&mut self.validate_import_directives());
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

    fn validate_only_import_directives_on_schema_type(&self) -> Result<(), SchemaValidationError> {
        match self
            .subgraph_schema_object_type()
            .and_then(|subgraph_schema_type| {
                if !subgraph_schema_type
                    .directives
                    .iter()
                    .filter(|directive| !directive.name.eq("import"))
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

    fn is_import_directive_argument_types_valid(types: &Value) -> bool {
        // All of the elements in the `types` field are valid: either a string or an object with keys `name` and `as` which are strings'
        if let Value::List(types) = types {
            types
                .iter()
                .try_for_each(Self::validate_import_type)
                .err()
                .is_none()
        } else {
            false
        }
    }

    fn is_import_directive_argument_from_valid(from: &Value) -> bool {
        if let Value::Object(from) = from {
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

    fn validate_import_directive_arguments(directive: &Directive) -> Option<SchemaValidationError> {
        let from_is_valid = directive
            .arguments
            .iter()
            .find(|(name, _)| name.eq("from"))
            .map_or(false, |(_, from)| {
                Self::is_import_directive_argument_from_valid(from)
            });
        let types_are_valid = directive
            .arguments
            .iter()
            .find(|(name, _)| name.eq("types"))
            .map_or(false, |(_, types)| {
                Self::is_import_directive_argument_types_valid(types)
            });
        if from_is_valid && types_are_valid {
            None
        } else {
            Some(SchemaValidationError::ImportDirectiveInvalid)
        }
    }

    fn validate_import_directive_schema_reference_parses(
        directive: &Directive,
    ) -> Option<SchemaValidationError> {
        directive
            .arguments
            .iter()
            .find(|(name, _)| name.eq("from"))
            .and_then(|(_, from)| match from {
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
                        let native_types = schema.document.get_object_type_definitions();
                        let imported_types = schema.imported_types();

                        // Ensure that the imported type is either native to
                        // the respective schema or is itself imported
                        // If the imported type is itself imported, do not
                        // recursively check the schema
                        let schema_handle = match schema_ref {
                            SchemaReference::ById(id) => id.to_string(),
                            SchemaReference::ByName(name) => name.to_string(),
                        };
                        let name = match imported_type {
                            ImportedType::Name(name) => name,
                            ImportedType::NameAs(name, _) => name,
                        };

                        let is_native = native_types.iter().any(|object| object.name.eq(name));
                        let is_imported = imported_types.iter().any(|(import, _)| match import {
                            ImportedType::Name(n) => name.eq(n),
                            ImportedType::NameAs(_, az) => name.eq(az),
                        });
                        if !is_native && !is_imported {
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
        let native_types = self.document.get_object_and_interface_type_fields();
        let imported_types = self.imported_types();
        native_types
            .iter()
            .fold(vec![], |errors, (type_name, fields)| {
                fields.iter().fold(errors, |mut errors, field| {
                    let base = field.field_type.get_base_type();
                    if let Ok(_) = BuiltInScalarType::try_from(base.as_ref()) {
                        return errors;
                    }
                    if native_types.contains_key(base) {
                        return errors;
                    }
                    if imported_types
                        .iter()
                        .any(|(imported_type, _)| match imported_type {
                            ImportedType::Name(name) if name.eq(base) => true,
                            ImportedType::NameAs(_, az) if az.eq(base) => true,
                            _ => false,
                        })
                    {
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
            .filter(|t| t.find_directive(String::from("entity")).is_none())
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
                            directive
                                .arguments
                                .iter()
                                .find(|(name, _)| name.eq("field"))
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

    /// Validate `interfaceethat `object` implements `interface`.
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
        "Entity type `Bar` does not satisfy interface `Foo` because it is missing the following fields: x: Int, y: Int",
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

        let document = graphql_parser::parse_schema(&raw).expect("Failed to parse raw schema");
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

    let document = graphql_parser::parse_schema(ROOT_SCHEMA).unwrap();
    let schema = Schema::new(SubgraphDeploymentId::new("id").unwrap(), document);
    match schema.validate_schema_type_has_no_fields() {
        Err(e) => assert_eq!(e, SchemaValidationError::SchemaTypeWithFields),
        Ok(_) => panic!(
            "Expected validation for `{}` to fail due to fields defined on the reserved type",
            ROOT_SCHEMA,
        ),
    }
}

#[test]
fn test_reserved_type_directives() {
    const ROOT_SCHEMA: &str = "
type _Schema_ @illegal";

    let document = graphql_parser::parse_schema(ROOT_SCHEMA).unwrap();
    let schema = Schema::new(SubgraphDeploymentId::new("id").unwrap(), document);
    match schema.validate_only_import_directives_on_schema_type() {
        Err(e) => assert_eq!(e, SchemaValidationError::InvalidSchemaTypeDirectives),
        Ok(_) => panic!(
            "Expected validation for `{}` to fail due to extra imports defined on the reserved type",
            ROOT_SCHEMA,
        ),
    }
}

#[test]
fn test_imports_directive_from_argument() {
    const ROOT_SCHEMA: &str = "type _Schema_ @import(types: [\"T\", \"A\", \"C\"])";

    let document = graphql_parser::parse_schema(ROOT_SCHEMA).unwrap();
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
fn test_recursively_imported_type_validates() {
    const ROOT_SCHEMA: &str = r#"
type _Schema_ @import(types: ["T"], from: { name: "child1/subgraph" })"#;
    const CHILD_1_SCHEMA: &str = r#"
type _Schema_ @import(types: ["T"], from: { name: "child2/subgraph" })"#;
    const CHILD_2_SCHEMA: &str = r#"
type T @entity { id: ID! }
"#;

    let root_document =
        graphql_parser::parse_schema(ROOT_SCHEMA).expect("Failed to parse root schema");
    let child_1_document =
        graphql_parser::parse_schema(CHILD_1_SCHEMA).expect("Failed to parse child 1 schema");
    let child_2_document =
        graphql_parser::parse_schema(CHILD_2_SCHEMA).expect("Failed to parse child 2 schema");

    let root_schema = Schema::new(SubgraphDeploymentId::new("rid").unwrap(), root_document);
    let child_1_schema = Schema::new(SubgraphDeploymentId::new("c1id").unwrap(), child_1_document);
    let child_2_schema = Schema::new(SubgraphDeploymentId::new("c2id").unwrap(), child_2_document);

    let mut schemas = HashMap::new();
    schemas.insert(
        SchemaReference::ByName(SubgraphName::new("childone/subgraph").unwrap()),
        Arc::new(child_1_schema),
    );
    schemas.insert(
        SchemaReference::ByName(SubgraphName::new("childtwo/subgraph").unwrap()),
        Arc::new(child_2_schema),
    );

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
type _Schema_ @import(types: ["T"], from: { name: "childone/subgraph" })"#;
    const CHILD_1_SCHEMA: &str = r#"
type _Schema_ @import(types: [{name: "T", as: "A"}], from: { name: "childtwo/subgraph" })"#;
    const CHILD_2_SCHEMA: &str = r#"
type T @entity { id: ID! }"#;

    let root_document = graphql_parser::parse_schema(ROOT_SCHEMA).unwrap();
    let child_1_document = graphql_parser::parse_schema(CHILD_1_SCHEMA).unwrap();
    let child_2_document = graphql_parser::parse_schema(CHILD_2_SCHEMA).unwrap();

    let root_schema = Schema::new(SubgraphDeploymentId::new("rid").unwrap(), root_document);
    let child_1_schema = Schema::new(SubgraphDeploymentId::new("c1id").unwrap(), child_1_document);
    let child_2_schema = Schema::new(SubgraphDeploymentId::new("c2id").unwrap(), child_2_document);

    let mut schemas = HashMap::new();
    schemas.insert(
        SchemaReference::ByName(SubgraphName::new("childone/subgraph").unwrap()),
        Arc::new(child_1_schema),
    );
    schemas.insert(
        SchemaReference::ByName(SubgraphName::new("childtwo/subgraph").unwrap()),
        Arc::new(child_2_schema),
    );

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
