use crate::components::store::EntityType;
use crate::data::graphql::ext::{DirectiveExt, DirectiveFinder, DocumentExt, TypeExt, ValueExt};
use crate::data::graphql::ObjectTypeExt;
use crate::data::store::ValueType;
use crate::data::subgraph::DeploymentHash;
use crate::prelude::{
    anyhow,
    q::Value,
    s::{self, Definition, InterfaceType, ObjectType, TypeDefinition, *},
};

use anyhow::Error;
use graphql_parser::{self, Pos};
use inflector::Inflector;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use std::collections::{BTreeMap, HashSet};
use std::convert::TryFrom;
use std::fmt;
use std::iter::FromIterator;
use std::str::FromStr;

/// Generate full-fledged API schemas from existing GraphQL schemas.
mod api;

/// Utilities for working with GraphQL schema ASTs.
pub mod ast;

mod fulltext;
mod input_schema;

pub use api::{api_schema, APISchemaError};

pub use api::{ApiSchema, ErrorPolicy};
pub use fulltext::{FulltextAlgorithm, FulltextConfig, FulltextDefinition, FulltextLanguage};
pub use input_schema::InputSchema;

pub const SCHEMA_TYPE_NAME: &str = "_Schema_";

pub const META_FIELD_TYPE: &str = "_Meta_";
pub const META_FIELD_NAME: &str = "_meta";

pub const BLOCK_FIELD_TYPE: &str = "_Block_";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Strings(Vec<String>);

impl fmt::Display for Strings {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let s = self.0.join(", ");
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
    #[error("Implementors of interface `{0}` use different id types `{1}`. They must all use the same type")]
    InterfaceImplementorsMixId(String, String),
    #[error("Field `{1}` in type `{0}` has invalid @derivedFrom: {2}")]
    InvalidDerivedFrom(String, String, String), // (type, field, reason)
    #[error("The following type names are reserved: `{0}`")]
    UsageOfReservedTypes(Strings),
    #[error("_Schema_ type is only for @fulltext and must not have any fields")]
    SchemaTypeWithFields,
    #[error("The _Schema_ type only allows @fulltext directives")]
    InvalidSchemaTypeDirectives,
    #[error("Type `{0}`, field `{1}`: type `{2}` is not defined")]
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

/// A validated and preprocessed GraphQL schema for a subgraph.
#[derive(Clone, Debug, PartialEq)]
pub struct Schema {
    pub id: DeploymentHash,
    pub document: s::Document,

    // Maps type name to implemented interfaces.
    pub interfaces_for_type: BTreeMap<EntityType, Vec<InterfaceType>>,

    // Maps an interface name to the list of entities that implement it.
    pub types_for_interface: BTreeMap<EntityType, Vec<ObjectType>>,
}

impl Schema {
    /// Create a new schema. The document must already have been validated
    //
    // TODO: The way some validation is expected to be done beforehand, and
    // some is done here makes it incredibly murky whether a `Schema` is
    // fully validated. The code should be changed to make sure that a
    // `Schema` is always fully valid
    pub fn new(id: DeploymentHash, document: s::Document) -> Result<Self, SchemaValidationError> {
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

    fn collect_interfaces(
        document: &s::Document,
    ) -> Result<
        (
            BTreeMap<EntityType, Vec<InterfaceType>>,
            BTreeMap<EntityType, Vec<ObjectType>>,
        ),
        SchemaValidationError,
    > {
        // Initialize with an empty vec for each interface, so we don't
        // miss interfaces that have no implementors.
        let mut types_for_interface =
            BTreeMap::from_iter(document.definitions.iter().filter_map(|d| match d {
                Definition::TypeDefinition(TypeDefinition::Interface(t)) => {
                    Some((EntityType::from(t), vec![]))
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
                    .entry(EntityType::from(object_type))
                    .or_default()
                    .push(interface_type);
                types_for_interface
                    .get_mut(&EntityType::new(implemented_interface))
                    .unwrap()
                    .push(object_type.clone());
            }
        }

        Ok((interfaces_for_type, types_for_interface))
    }

    pub fn parse(raw: &str, id: DeploymentHash) -> Result<Self, Error> {
        let document = graphql_parser::parse_schema(raw)?.into_static();

        Schema::new(id, document).map_err(Into::into)
    }

    /// Returned map has one an entry for each interface in the schema.
    pub fn types_for_interface(&self) -> &BTreeMap<EntityType, Vec<ObjectType>> {
        &self.types_for_interface
    }

    /// Returns `None` if the type implements no interfaces.
    pub fn interfaces_for_type(&self, type_name: &EntityType) -> Option<&Vec<InterfaceType>> {
        self.interfaces_for_type.get(type_name)
    }

    // Adds a @subgraphId(id: ...) directive to object/interface/enum types in the schema.
    pub fn add_subgraph_id_directives(&mut self, id: DeploymentHash) {
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
                    && !directives
                        .iter()
                        .any(|directive| directive.name.eq("subgraphId"))
                {
                    directives.push(subgraph_id_directive);
                }
            };
        }
    }

    pub fn validate(&self) -> Result<(), Vec<SchemaValidationError>> {
        let mut errors: Vec<SchemaValidationError> = [
            self.validate_schema_types(),
            self.validate_derived_from(),
            self.validate_schema_type_has_no_fields(),
            self.validate_directives_on_schema_type(),
            self.validate_reserved_types_usage(),
            self.validate_interface_id_type(),
        ]
        .into_iter()
        .filter(Result::is_err)
        // Safe unwrap due to the filter above
        .map(Result::unwrap_err)
        .collect();

        errors.append(&mut self.validate_fields());
        errors.append(&mut self.validate_fulltext_directives());

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
                if subgraph_schema_type
                    .directives
                    .iter()
                    .filter(|directive| !directive.name.eq("fulltext"))
                    .next()
                    .is_some()
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
        if local_types.iter().any(|typ| {
            typ.fields.iter().any(|field| {
                name == &field.name.as_str().to_camel_case()
                    || name == &field.name.to_plural().to_camel_case()
                    || field.name.eq(name)
            })
        }) {
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
            .count()
            > 1
        {
            vec![SchemaValidationError::FulltextNameConflict(
                name.to_string(),
            )]
        } else {
            vec![]
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
        match FulltextLanguage::try_from(language.as_str()) {
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
        match FulltextAlgorithm::try_from(algorithm.as_str()) {
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
            Some(Value::List(includes)) if !includes.is_empty() => includes,
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
                            .any(|field| {
                                let base_type: &str = field.field_type.get_base_type();
                                matches!(ValueType::from_str(base_type), Ok(ValueType::String) if field.name.eq(field_name))
                            })
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
        vec![]
    }

    fn validate_fields(&self) -> Vec<SchemaValidationError> {
        let local_types = self.document.get_object_and_interface_type_fields();
        let local_enums = self
            .document
            .get_enum_definitions()
            .iter()
            .map(|enu| enu.name.clone())
            .collect::<Vec<String>>();
        local_types
            .iter()
            .fold(vec![], |errors, (type_name, fields)| {
                fields.iter().fold(errors, |mut errors, field| {
                    let base = field.field_type.get_base_type();
                    if ValueType::is_scalar(base) {
                        return errors;
                    }
                    if local_types.contains_key(base) {
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

    /// Checks if the schema is using types that are reserved
    /// by `graph-node`
    fn validate_reserved_types_usage(&self) -> Result<(), SchemaValidationError> {
        let document = &self.document;
        let object_types: Vec<_> = document
            .get_object_type_definitions()
            .into_iter()
            .map(|obj_type| &obj_type.name)
            .collect();

        let interface_types: Vec<_> = document
            .get_interface_type_definitions()
            .into_iter()
            .map(|iface_type| &iface_type.name)
            .collect();

        // TYPE_NAME_filter types for all object and interface types
        let mut filter_types: Vec<String> = object_types
            .iter()
            .chain(interface_types.iter())
            .map(|type_name| format!("{}_filter", type_name))
            .collect();

        // TYPE_NAME_orderBy types for all object and interface types
        let mut order_by_types: Vec<_> = object_types
            .iter()
            .chain(interface_types.iter())
            .map(|type_name| format!("{}_orderBy", type_name))
            .collect();

        let mut reserved_types: Vec<String> = vec![
            // The built-in scalar types
            "Boolean".into(),
            "ID".into(),
            "Int".into(),
            "BigDecimal".into(),
            "String".into(),
            "Bytes".into(),
            "BigInt".into(),
            // Reserved Query and Subscription types
            "Query".into(),
            "Subscription".into(),
        ];

        reserved_types.append(&mut filter_types);
        reserved_types.append(&mut order_by_types);

        // `reserved_types` will now only contain
        // the reserved types that the given schema *is* using.
        //
        // That is, if the schema is compliant and not using any reserved
        // types, then it'll become an empty vector
        reserved_types.retain(|reserved_type| document.get_named_type(reserved_type).is_some());

        if reserved_types.is_empty() {
            Ok(())
        } else {
            Err(SchemaValidationError::UsageOfReservedTypes(Strings(
                reserved_types,
            )))
        }
    }

    fn validate_schema_types(&self) -> Result<(), SchemaValidationError> {
        let types_without_entity_directive = self
            .document
            .get_object_type_definitions()
            .iter()
            .filter(|t| t.find_directive("entity").is_none() && !t.name.eq(SCHEMA_TYPE_NAME))
            .map(|t| t.name.clone())
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
                object_type.name.clone(),
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
                field.find_directive("derivedFrom").map(|directive| {
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
            if target_field_type != object_type.name
                && target_field_type != "ID"
                && !interface_types
                    .iter()
                    .any(|iface| target_field_type.eq(iface.as_str()))
            {
                fn type_signatures(name: &str) -> Vec<String> {
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
            if !object
                .fields
                .iter()
                .any(|o| o.name.eq(&i.name) && o.field_type.eq(&i.field_type))
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

    fn validate_interface_id_type(&self) -> Result<(), SchemaValidationError> {
        for (intf, obj_types) in &self.types_for_interface {
            let id_types: HashSet<&str> = HashSet::from_iter(
                obj_types
                    .iter()
                    .filter_map(|obj_type| obj_type.field("id"))
                    .map(|f| f.field_type.get_base_type())
                    .map(|name| if name == "ID" { "String" } else { name }),
            );
            if id_types.len() > 1 {
                return Err(SchemaValidationError::InterfaceImplementorsMixId(
                    intf.to_string(),
                    id_types.iter().join(", "),
                ));
            }
        }
        Ok(())
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
    let res = Schema::parse(schema, DeploymentHash::new("dummy").unwrap());
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
    let res = Schema::parse(schema, DeploymentHash::new("dummy").unwrap());
    assert_eq!(
        res.unwrap_err().to_string(),
        "Entity type `Bar` does not satisfy interface `Foo` because it is missing \
         the following fields: x: Int, y: Int",
    );
}

#[test]
fn interface_implementations_id_type() {
    fn check_schema(bar_id: &str, baz_id: &str, ok: bool) {
        let schema = format!(
            "interface Foo {{ x: Int }}
             type Bar implements Foo @entity {{
                id: {bar_id}!
                x: Int
             }}

             type Baz implements Foo @entity {{
                id: {baz_id}!
                x: Int
            }}"
        );
        let schema = Schema::parse(&schema, DeploymentHash::new("dummy").unwrap()).unwrap();
        let res = schema.validate();
        if ok {
            assert!(matches!(res, Ok(_)));
        } else {
            assert!(matches!(res, Err(_)));
            assert!(matches!(
                res.unwrap_err()[0],
                SchemaValidationError::InterfaceImplementorsMixId(_, _)
            ));
        }
    }
    check_schema("ID", "ID", true);
    check_schema("ID", "String", true);
    check_schema("ID", "Bytes", false);
    check_schema("Bytes", "String", false);
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
        let schema = Schema::new(DeploymentHash::new("id").unwrap(), document).unwrap();
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
    let schema = Schema::new(DeploymentHash::new("id").unwrap(), document).unwrap();
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
    let schema = Schema::new(DeploymentHash::new("id").unwrap(), document).unwrap();
    assert_eq!(
        schema.validate_directives_on_schema_type().expect_err(
            "Expected validation to fail due to extra imports defined on the reserved type"
        ),
        SchemaValidationError::InvalidSchemaTypeDirectives
    )
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
    let schema = Schema::new(DeploymentHash::new("id").unwrap(), document).unwrap();
    assert_eq!(schema.validate_fields().len(), 0);
}

#[test]
fn test_reserved_types_validation() {
    let reserved_types = [
        // Built-in scalars
        "Boolean",
        "ID",
        "Int",
        "BigDecimal",
        "String",
        "Bytes",
        "BigInt",
        // Reserved keywords
        "Query",
        "Subscription",
    ];

    let dummy_hash = DeploymentHash::new("dummy").unwrap();

    for reserved_type in reserved_types {
        let schema = format!("type {} @entity {{ _: Boolean }}\n", reserved_type);

        let schema = Schema::parse(&schema, dummy_hash.clone()).unwrap();

        let errors = schema.validate().unwrap_err();
        for error in errors {
            assert!(matches!(
                error,
                SchemaValidationError::UsageOfReservedTypes(_)
            ))
        }
    }
}

#[test]
fn test_reserved_filter_and_group_by_types_validation() {
    const SCHEMA: &str = r#"
    type Gravatar @entity {
        _: Boolean
      }
    type Gravatar_filter @entity {
        _: Boolean
    }
    type Gravatar_orderBy @entity {
        _: Boolean
    }
    "#;

    let dummy_hash = DeploymentHash::new("dummy").unwrap();

    let schema = Schema::parse(SCHEMA, dummy_hash).unwrap();

    let errors = schema.validate().unwrap_err();

    // The only problem in the schema is the usage of reserved types
    assert_eq!(errors.len(), 1);

    assert!(matches!(
        &errors[0],
        SchemaValidationError::UsageOfReservedTypes(Strings(_))
    ));

    // We know this will match due to the assertion above
    match &errors[0] {
        SchemaValidationError::UsageOfReservedTypes(Strings(reserved_types)) => {
            let expected_types: Vec<String> =
                vec!["Gravatar_filter".into(), "Gravatar_orderBy".into()];
            assert_eq!(reserved_types, &expected_types);
        }
        _ => unreachable!(),
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
    let schema = Schema::new(DeploymentHash::new("id1").unwrap(), document).unwrap();

    assert_eq!(schema.validate_fulltext_directives(), vec![]);
}
