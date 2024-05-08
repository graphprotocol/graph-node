use std::{borrow::Borrow, fmt, sync::Arc};

use anyhow::{Context, Error};

use crate::{
    cheap_clone::CheapClone,
    data::store::{Id, IdList},
    data::{graphql::QueryableType, store::IdType, value::Word},
    data_source::causality_region::CausalityRegion,
    prelude::s,
    util::intern::Atom,
};

use super::{EntityKey, Field, InputSchema, InterfaceType, ObjectType, POI_OBJECT};

use graph_derive::CheapClone;

/// A reference to a type in the input schema. It should mostly be the
/// reference to a concrete entity type, either one declared with `@entity`
/// in the input schema, or the object type that stores aggregations for a
/// certain interval, in other words a type that is actually backed by a
/// database table. However, it can also be a reference to an interface type
/// for historical reasons.
///
/// Even though it is not implemented as a string type, it behaves as if it
/// were the string name of the type for all external purposes like
/// comparison, ordering, and serialization
#[derive(Clone, CheapClone)]
pub struct EntityType {
    schema: InputSchema,
    pub(in crate::schema) atom: Atom,
}

impl EntityType {
    pub(in crate::schema) fn new(schema: InputSchema, atom: Atom) -> Self {
        EntityType { schema, atom }
    }

    /// Return the name of this type as a string.
    pub fn as_str(&self) -> &str {
        // unwrap: we constructed the entity type from the schema's pool
        self.schema.pool().get(self.atom).unwrap()
    }

    /// Return the name of the declared type from the input schema that this
    /// type belongs to. For object and interface types, that's the same as
    /// `as_str()`, but for aggregations it's the name of the aggregation
    /// rather than the name of the specific aggregation for an interval. In
    /// that case, `as_str()` might return `Stats_hour` whereas `typename()`
    /// returns `Stats`
    pub fn typename(&self) -> &str {
        self.schema.typename(self.atom)
    }

    pub fn is_poi(&self) -> bool {
        self.as_str() == POI_OBJECT
    }

    pub fn has_field(&self, field: Atom) -> bool {
        self.schema.has_field(self.atom, field)
    }

    pub fn field(&self, name: &str) -> Option<&Field> {
        self.schema.field(self.atom, name)
    }

    pub fn is_immutable(&self) -> bool {
        self.schema.is_immutable(self.atom)
    }

    pub fn id_type(&self) -> Result<IdType, Error> {
        self.schema.id_type(self.atom)
    }

    /// Return the object type for this entity type. It is an error to call
    /// this if `entity_type` refers to an interface or an aggregation as
    /// they don't have an underlying type that stores daa directly
    pub fn object_type(&self) -> Result<&ObjectType, Error> {
        self.schema.object_type(self.atom)
    }

    /// Create a key from this type for an onchain entity
    pub fn key(&self, id: Id) -> EntityKey {
        self.key_in(id, CausalityRegion::ONCHAIN)
    }

    /// Create a key from this type for an entity in the given causality region
    pub fn key_in(&self, id: Id, causality_region: CausalityRegion) -> EntityKey {
        EntityKey::new(self.cheap_clone(), id, causality_region)
    }

    /// Construct an `Id` from the given string and parse it into the
    /// correct type if necessary
    pub fn parse_id(&self, id: impl Into<Word>) -> Result<Id, Error> {
        let id = id.into();
        let id_type = self
            .schema
            .id_type(self.atom)
            .with_context(|| format!("error determining id_type for {}[{}]", self.as_str(), id))?;
        id_type.parse(id)
    }

    /// Construct an `IdList` from a list of given strings and parse them
    /// into the correct type if necessary
    pub fn parse_ids(&self, ids: Vec<impl Into<Word>>) -> Result<IdList, Error> {
        let ids: Vec<_> = ids
            .into_iter()
            .map(|id| self.parse_id(id))
            .collect::<Result<_, _>>()?;
        IdList::try_from_iter(self.id_type()?, ids.into_iter())
            .map_err(|e| anyhow::anyhow!("error: {}", e))
    }

    /// Parse the given `id` into an `Id` and construct a key for an onchain
    /// entity from it
    pub fn parse_key(&self, id: impl Into<Word>) -> Result<EntityKey, Error> {
        let id_value = self.parse_id(id)?;
        Ok(self.key(id_value))
    }

    /// Parse the given `id` into an `Id` and construct a key for an entity
    /// in the give causality region from it
    pub fn parse_key_in(
        &self,
        id: impl Into<Word>,
        causality_region: CausalityRegion,
    ) -> Result<EntityKey, Error> {
        let id_value = self.parse_id(id.into())?;
        Ok(self.key_in(id_value, causality_region))
    }

    fn same_pool(&self, other: &EntityType) -> bool {
        Arc::ptr_eq(self.schema.pool(), other.schema.pool())
    }

    pub fn interfaces(&self) -> impl Iterator<Item = &InterfaceType> {
        self.schema.interfaces(self.atom)
    }

    /// Return a list of all entity types that implement one of the
    /// interfaces that `self` implements; the result does not include
    /// `self`
    pub fn share_interfaces(&self) -> Result<Vec<EntityType>, Error> {
        self.schema.share_interfaces(self.atom)
    }

    /// Return `true` if `self` is an object type, i.e., a type that is
    /// declared with an `@entity` directive in the input schema. This
    /// specifically excludes interfaces and aggregations.
    pub fn is_object_type(&self) -> bool {
        self.schema.is_object_type(self.atom)
    }
}

impl fmt::Display for EntityType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Borrow<str> for EntityType {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Debug for EntityType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EntityType({})", self.as_str())
    }
}

impl PartialEq for EntityType {
    fn eq(&self, other: &Self) -> bool {
        if self.same_pool(other) && self.atom == other.atom {
            return true;
        }
        self.as_str() == other.as_str()
    }
}

impl Eq for EntityType {}

impl PartialOrd for EntityType {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.as_str().partial_cmp(other.as_str())
    }
}

impl Ord for EntityType {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl std::hash::Hash for EntityType {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_str().hash(state)
    }
}

/// A trait to mark types that can reasonably turned into the name of an
/// entity type
pub trait AsEntityTypeName {
    fn name(&self) -> &str;
}

impl AsEntityTypeName for &str {
    fn name(&self) -> &str {
        self
    }
}

impl AsEntityTypeName for &String {
    fn name(&self) -> &str {
        self.as_str()
    }
}

impl AsEntityTypeName for &Word {
    fn name(&self) -> &str {
        self.as_str()
    }
}

impl AsEntityTypeName for &s::ObjectType {
    fn name(&self) -> &str {
        &self.name
    }
}

impl AsEntityTypeName for &s::InterfaceType {
    fn name(&self) -> &str {
        &self.name
    }
}

impl AsEntityTypeName for QueryableType<'_> {
    fn name(&self) -> &str {
        match self {
            QueryableType::Object(object) => &object.name,
            QueryableType::Interface(interface) => &interface.name,
            QueryableType::Union(union) => &union.name,
        }
    }
}
