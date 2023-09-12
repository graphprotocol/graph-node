use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{anyhow, Error};
use store::Entity;

use crate::cheap_clone::CheapClone;
use crate::components::store::LoadRelatedRequest;
use crate::data::graphql::ext::DirectiveFinder;
use crate::data::graphql::{
    DirectiveExt, DocumentExt, ObjectOrInterface, ObjectTypeExt, TypeExt, ValueExt,
};
use crate::data::store::{
    self, EntityValidationError, IdType, IntoEntityIterator, TryIntoEntityIterator, ID,
};
use crate::data::value::Word;
use crate::prelude::q::Value;
use crate::prelude::{s, DeploymentHash};
use crate::schema::api_schema;
use crate::util::intern::{Atom, AtomPool};

use super::fulltext::FulltextDefinition;
use super::{
    ApiSchema, AsEntityTypeName, EntityType, Schema, SchemaValidationError, SCHEMA_TYPE_NAME,
};

/// The name of the PoI entity type
pub(crate) const POI_OBJECT: &str = "Poi$";
/// The name of the digest attribute of POI entities
const POI_DIGEST: &str = "digest";

/// The internal representation of a subgraph schema, i.e., the
/// `schema.graphql` file that is part of a subgraph. Any code that deals
/// with writing a subgraph should use this struct. Code that deals with
/// querying subgraphs will instead want to use an `ApiSchema` which can be
/// generated with the `api_schema` method on `InputSchema`
///
/// There's no need to put this into an `Arc`, since `InputSchema` already
/// does that internally and is `CheapClone`
#[derive(Clone, Debug, PartialEq)]
pub struct InputSchema {
    inner: Arc<Inner>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum TypeKind {
    MutableObject(IdType),
    ImmutableObject(IdType),
    Interface,
}

impl TypeKind {
    fn is_object(&self) -> bool {
        match self {
            TypeKind::MutableObject(_) | TypeKind::ImmutableObject(_) => true,
            TypeKind::Interface => false,
        }
    }

    fn id_type(&self) -> Option<IdType> {
        match self {
            TypeKind::MutableObject(id_type) | TypeKind::ImmutableObject(id_type) => Some(*id_type),
            TypeKind::Interface => None,
        }
    }
}

#[derive(Debug, PartialEq)]
struct TypeInfo {
    name: Atom,
    kind: TypeKind,
    fields: Box<[Atom]>,
}

impl TypeInfo {
    fn new(pool: &AtomPool, typ: ObjectOrInterface<'_>, kind: TypeKind) -> Self {
        // The `unwrap` of `lookup` is safe because the pool was just
        // constructed against the underlying schema
        let name = pool.lookup(typ.name()).unwrap();
        let fields = typ
            .fields()
            .iter()
            .map(|field| pool.lookup(&field.name).unwrap())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { name, kind, fields }
    }

    fn for_object(pool: &AtomPool, obj_type: &s::ObjectType) -> Self {
        let id_type = IdType::try_from(obj_type).expect("validation caught any issues here");
        let kind = if obj_type.is_immutable() {
            TypeKind::ImmutableObject(id_type)
        } else {
            TypeKind::MutableObject(id_type)
        };
        Self::new(pool, obj_type.into(), kind)
    }

    fn for_interface(pool: &AtomPool, intf_type: &s::InterfaceType) -> Self {
        Self::new(pool, intf_type.into(), TypeKind::Interface)
    }

    fn for_poi(pool: &AtomPool) -> Self {
        // The way we handle the PoI type is a bit of a hack. We pretend
        // it's an object type, but trying to look up the `s::ObjectType`
        // for it will turn up nothing.
        // See also https://github.com/graphprotocol/graph-node/issues/4873
        let name = pool.lookup(POI_OBJECT).unwrap();
        let fields =
            vec![pool.lookup(&ID).unwrap(), pool.lookup(POI_DIGEST).unwrap()].into_boxed_slice();
        Self {
            name,
            kind: TypeKind::MutableObject(IdType::String),
            fields,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Inner {
    schema: Schema,
    /// A list of all the object and interface types in the `schema` with
    /// some important information extracted from the schema. The list is
    /// sorted by the name atom (not the string name) of the types
    type_infos: Box<[TypeInfo]>,
    pool: Arc<AtomPool>,
}

impl CheapClone for InputSchema {
    fn cheap_clone(&self) -> Self {
        InputSchema {
            inner: self.inner.cheap_clone(),
        }
    }
}

impl InputSchema {
    fn create(schema: Schema) -> Self {
        let pool = Arc::new(atom_pool(&schema.document));

        let obj_types = schema
            .document
            .get_object_type_definitions()
            .into_iter()
            .filter(|obj_type| obj_type.name != SCHEMA_TYPE_NAME)
            .map(|obj_type| TypeInfo::for_object(&pool, obj_type));
        let intf_types = schema
            .document
            .get_interface_type_definitions()
            .into_iter()
            .map(|intf_type| TypeInfo::for_interface(&pool, intf_type));
        let mut type_infos: Vec<_> = obj_types
            .chain(intf_types)
            .chain(vec![TypeInfo::for_poi(&pool)])
            .collect();
        type_infos.sort_by_key(|ti| ti.name);
        let type_infos = type_infos.into_boxed_slice();

        Self {
            inner: Arc::new(Inner {
                schema,
                type_infos,
                pool,
            }),
        }
    }

    /// Create a new `InputSchema` from the GraphQL document that resulted
    /// from parsing a subgraph's `schema.graphql`. The document must have
    /// already been validated.
    pub fn new(id: DeploymentHash, document: s::Document) -> Result<Self, SchemaValidationError> {
        let schema = Schema::new(id, document)?;
        Ok(Self::create(schema))
    }

    /// A convenience function for creating an `InputSchema` from the string
    /// representation of the subgraph's GraphQL schema `raw` and its
    /// deployment hash `id`. An `InputSchema` that is constructed with this
    /// function still has to be validated after construction.
    pub fn parse(raw: &str, id: DeploymentHash) -> Result<Self, Error> {
        let schema = Schema::parse(raw, id)?;

        Ok(Self::create(schema))
    }

    /// Convenience for tests to construct an `InputSchema`
    ///
    /// # Panics
    ///
    /// If the `document` or `hash` can not be successfully converted
    #[cfg(debug_assertions)]
    #[track_caller]
    pub fn raw(document: &str, hash: &str) -> Self {
        let hash = DeploymentHash::new(hash).unwrap();
        Self::parse(document, hash).unwrap()
    }

    pub fn schema(&self) -> &Schema {
        &self.inner.schema
    }

    /// Generate the `ApiSchema` for use with GraphQL queries for this
    /// `InputSchema`
    pub fn api_schema(&self) -> Result<ApiSchema, anyhow::Error> {
        let mut schema = self.inner.schema.clone();
        schema.document = api_schema(&self.inner.schema.document)?;
        schema.add_subgraph_id_directives(schema.id.clone());
        ApiSchema::from_api_schema(schema)
    }

    /// Returns the field that has the relationship with the key requested
    /// This works as a reverse search for the Field related to the query
    ///
    /// example:
    ///
    /// type Account @entity {
    ///     wallets: [Wallet!]! @derivedFrom(field: "account")
    /// }
    /// type Wallet {
    ///     account: Account!
    ///     balance: Int!
    /// }
    ///
    /// When asked to load the related entities from "Account" in the field "wallets"
    /// This function will return the type "Wallet" with the field "account"
    pub fn get_field_related(&self, key: &LoadRelatedRequest) -> Result<(&str, &s::Field), Error> {
        let field = self
            .inner
            .schema
            .document
            .get_object_type_definition(key.entity_type.as_str())
            .ok_or_else(|| {
                anyhow!(
                    "Entity {}[{}]: unknown entity type `{}`",
                    key.entity_type,
                    key.entity_id,
                    key.entity_type,
                )
            })?
            .field(&key.entity_field)
            .ok_or_else(|| {
                anyhow!(
                    "Entity {}[{}]: unknown field `{}`",
                    key.entity_type,
                    key.entity_id,
                    key.entity_field,
                )
            })?;
        if field.is_derived() {
            let derived_from = field.find_directive("derivedFrom").unwrap();
            let base_type = field.field_type.get_base_type();
            let field_name = derived_from.argument("field").unwrap();

            let field = self
                .inner
                .schema
                .document
                .get_object_type_definition(base_type)
                .ok_or_else(|| {
                    anyhow!(
                        "Entity {}[{}]: unknown entity type `{}`",
                        key.entity_type,
                        key.entity_id,
                        key.entity_type,
                    )
                })?
                .field(field_name.as_str().unwrap())
                .ok_or_else(|| {
                    anyhow!(
                        "Entity {}[{}]: unknown field `{}`",
                        key.entity_type,
                        key.entity_id,
                        key.entity_field,
                    )
                })?;

            Ok((base_type, field))
        } else {
            Err(anyhow!(
                "Entity {}[{}]: field `{}` is not derived",
                key.entity_type,
                key.entity_id,
                key.entity_field,
            ))
        }
    }

    fn type_info(&self, atom: Atom) -> Option<&TypeInfo> {
        self.inner
            .type_infos
            .binary_search_by_key(&atom, |ti| ti.name)
            .map(|idx| &self.inner.type_infos[idx])
            .ok()
    }

    pub(in crate::schema) fn id_type(&self, entity_type: Atom) -> Result<store::IdType, Error> {
        fn unknown_name(pool: &AtomPool, atom: Atom) -> Error {
            match pool.get(atom) {
                Some(name) => anyhow!("Entity type `{name}` is not defined in the schema"),
                None => anyhow!(
                    "Invalid atom for id_type lookup (atom is probably from a different pool)"
                ),
            }
        }

        let type_info = self
            .type_info(entity_type)
            .ok_or_else(|| unknown_name(&self.inner.pool, entity_type))?;

        type_info.kind.id_type().ok_or_else(|| {
            let name = self.inner.pool.get(entity_type).unwrap();
            anyhow!("Entity type `{}` does not have an `id` field", name)
        })
    }

    /// Check if `entity_type` is an immutable object type
    pub(in crate::schema) fn is_immutable(&self, entity_type: Atom) -> bool {
        self.type_info(entity_type)
            .map(|ti| matches!(ti.kind, TypeKind::ImmutableObject(_)))
            .unwrap_or(false)
    }

    pub fn get_named_type(&self, name: &str) -> Option<&s::TypeDefinition> {
        self.inner.schema.document.get_named_type(name)
    }

    pub fn types_for_interface(&self, intf: &s::InterfaceType) -> Option<&Vec<s::ObjectType>> {
        self.inner.schema.types_for_interface.get(&intf.name)
    }

    /// Returns `None` if the type implements no interfaces.
    pub fn interfaces_for_type(&self, type_name: &str) -> Option<&Vec<s::InterfaceType>> {
        self.inner.schema.interfaces_for_type(type_name)
    }

    pub(in crate::schema) fn find_object_type(
        &self,
        entity_type: &EntityType,
    ) -> Option<&s::ObjectType> {
        self.inner
            .schema
            .document
            .definitions
            .iter()
            .filter_map(|d| match d {
                s::Definition::TypeDefinition(s::TypeDefinition::Object(t)) => Some(t),
                _ => None,
            })
            .find(|object_type| entity_type.as_str() == object_type.name)
    }

    pub fn get_enum_definitions(&self) -> Vec<&s::EnumType> {
        self.inner.schema.document.get_enum_definitions()
    }

    pub fn get_object_type_definitions(&self) -> Vec<&s::ObjectType> {
        self.inner.schema.document.get_object_type_definitions()
    }

    pub fn interface_types(&self) -> &BTreeMap<String, Vec<s::ObjectType>> {
        &self.inner.schema.types_for_interface
    }

    pub fn entity_fulltext_definitions(
        &self,
        entity: &str,
    ) -> Result<Vec<FulltextDefinition>, anyhow::Error> {
        Self::fulltext_definitions(&self.inner.schema.document, entity)
    }

    fn fulltext_definitions(
        document: &s::Document,
        entity: &str,
    ) -> Result<Vec<FulltextDefinition>, anyhow::Error> {
        Ok(document
            .get_fulltext_directives()?
            .into_iter()
            .filter(|directive| match directive.argument("include") {
                Some(Value::List(includes)) if !includes.is_empty() => {
                    includes.iter().any(|include| match include {
                        Value::Object(include) => match include.get("entity") {
                            Some(Value::String(fulltext_entity)) if fulltext_entity == entity => {
                                true
                            }
                            _ => false,
                        },
                        _ => false,
                    })
                }
                _ => false,
            })
            .map(FulltextDefinition::from)
            .collect())
    }

    pub fn id(&self) -> &DeploymentHash {
        &self.inner.schema.id
    }

    pub fn document_string(&self) -> String {
        self.inner.schema.document.to_string()
    }

    pub fn get_fulltext_directives(&self) -> Result<Vec<&s::Directive>, Error> {
        self.inner.schema.document.get_fulltext_directives()
    }

    pub(crate) fn validate(&self) -> Result<(), Vec<SchemaValidationError>> {
        self.inner.schema.validate()
    }

    pub fn make_entity<I: IntoEntityIterator>(
        &self,
        iter: I,
    ) -> Result<Entity, EntityValidationError> {
        Entity::make(self.inner.pool.clone(), iter)
    }

    pub fn try_make_entity<
        E: std::error::Error + Send + Sync + 'static,
        I: TryIntoEntityIterator<E>,
    >(
        &self,
        iter: I,
    ) -> Result<Entity, Error> {
        Entity::try_make(self.inner.pool.clone(), iter)
    }

    /// Check if `entity_type` is an object type and has a field `field`
    pub(in crate::schema) fn has_field(&self, entity_type: Atom, field: Atom) -> bool {
        self.type_info(entity_type)
            .map(|ti| ti.kind.is_object() && ti.fields.contains(&field))
            .unwrap_or(false)
    }

    pub fn poi_type(&self) -> EntityType {
        // unwrap: we make sure to put POI_OBJECT into the pool
        let atom = self.inner.pool.lookup(POI_OBJECT).unwrap();
        EntityType::new(self.cheap_clone(), atom)
    }

    pub fn poi_digest(&self) -> Word {
        Word::from(POI_DIGEST)
    }

    // A helper for the `EntityType` constructor
    pub(in crate::schema) fn pool(&self) -> &Arc<AtomPool> {
        &self.inner.pool
    }

    /// Return the entity type for `named`. If the entity type does not
    /// exist, return an error. Generally, an error should only be possible
    /// if `named` is based on user input. If `named` is an internal object,
    /// like a `ObjectType`, it is safe to unwrap the result
    pub fn entity_type<N: AsEntityTypeName>(&self, named: N) -> Result<EntityType, Error> {
        let name = named.name();
        self.inner
            .pool
            .lookup(name)
            .and_then(|atom| self.type_info(atom))
            .map(|ti| EntityType::new(self.cheap_clone(), ti.name))
            .ok_or_else(|| {
                anyhow!(
                    "internal error: entity type `{}` does not exist in {}",
                    name,
                    self.inner.schema.id
                )
            })
    }
}

/// Create a new pool that contains the names of all the types defined
/// in the document and the names of all their fields
fn atom_pool(document: &s::Document) -> AtomPool {
    let mut pool = AtomPool::new();

    pool.intern(POI_OBJECT); // Name of PoI entity type
    pool.intern(POI_DIGEST); // Attribute of PoI object

    for definition in &document.definitions {
        match definition {
            s::Definition::TypeDefinition(typedef) => match typedef {
                s::TypeDefinition::Object(t) => {
                    pool.intern(&t.name);
                    for field in &t.fields {
                        pool.intern(&field.name);
                    }
                }
                s::TypeDefinition::Enum(t) => {
                    pool.intern(&t.name);
                }
                s::TypeDefinition::Interface(t) => {
                    pool.intern(&t.name);
                    for field in &t.fields {
                        pool.intern(&field.name);
                    }
                }
                s::TypeDefinition::InputObject(input_object) => {
                    pool.intern(&input_object.name);
                    for field in &input_object.fields {
                        pool.intern(&field.name);
                    }
                }
                s::TypeDefinition::Scalar(scalar_type) => {
                    pool.intern(&scalar_type.name);
                }
                s::TypeDefinition::Union(union_type) => {
                    pool.intern(&union_type.name);
                    for typ in &union_type.types {
                        pool.intern(typ);
                    }
                }
            },
            s::Definition::SchemaDefinition(_)
            | s::Definition::TypeExtension(_)
            | s::Definition::DirectiveDefinition(_) => { /* ignore, these only happen for introspection schemas */
            }
        }
    }

    for object_type in document.get_object_type_definitions() {
        for defn in InputSchema::fulltext_definitions(&document, &object_type.name).unwrap() {
            pool.intern(defn.name.as_str());
        }
    }

    pool
}

#[cfg(test)]
mod tests {
    use crate::{
        data::store::ID,
        prelude::DeploymentHash,
        schema::input_schema::{POI_DIGEST, POI_OBJECT},
    };

    use super::InputSchema;

    const SCHEMA: &str = r#"
      type Thing @entity {
        id: ID!
        name: String!
      }
    "#;

    #[test]
    fn entity_type() {
        let id = DeploymentHash::new("test").unwrap();
        let schema = InputSchema::parse(SCHEMA, id).unwrap();

        assert_eq!("Thing", schema.entity_type("Thing").unwrap().as_str());

        let poi = schema.entity_type(POI_OBJECT).unwrap();
        assert_eq!(POI_OBJECT, poi.as_str());
        assert!(poi.has_field(schema.pool().lookup(&ID).unwrap()));
        assert!(poi.has_field(schema.pool().lookup(POI_DIGEST).unwrap()));
        // This is not ideal, but we don't have an object type for the PoI
        assert_eq!(None, poi.object_type());

        assert!(schema.entity_type("NonExistent").is_err());
    }
}
