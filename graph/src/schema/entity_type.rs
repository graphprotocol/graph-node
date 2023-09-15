use std::{borrow::Borrow, fmt, sync::Arc};

use anyhow::{bail, Error};
use serde::Serialize;

use crate::{
    cheap_clone::CheapClone,
    data::{graphql::ObjectOrInterface, store::IdType},
    prelude::s,
    util::intern::Atom,
};

use super::{input_schema::POI_OBJECT, InputSchema};

/// The type name of an entity. This is the string that is used in the
/// subgraph's GraphQL schema as `type NAME @entity { .. }`
///
/// Even though it is not implemented as a string type, it behaves as if it
/// were the string name of the type for all external purposes like
/// comparison, ordering, and serialization
#[derive(Clone)]
pub struct EntityType {
    schema: InputSchema,
    atom: Atom,
}

impl EntityType {
    /// Construct a new entity type. Ideally, this is only called when
    /// `entity_type` either comes from the GraphQL schema, or from
    /// the database from fields that are known to contain a valid entity type
    // This method is only meant to be used in `InputSchema`; all external
    // constructions of an `EntityType` need to go through that struct
    pub(in crate::schema) fn new(schema: InputSchema, name: &str) -> Result<Self, Error> {
        let atom = match schema.pool().lookup(name) {
            Some(atom) => atom,
            None => bail!("entity type `{name}` is not interned"),
        };

        Ok(EntityType { schema, atom })
    }

    pub fn as_str(&self) -> &str {
        // unwrap: we constructed the entity type from the schema's pool
        self.schema.pool().get(self.atom).unwrap()
    }

    pub fn is_poi(&self) -> bool {
        self.as_str() == POI_OBJECT
    }

    pub fn has_field(&self, field: Atom) -> bool {
        self.schema.has_field(self.atom, field)
    }

    pub fn is_immutable(&self) -> bool {
        self.schema.is_immutable(self.atom)
    }

    pub fn id_type(&self) -> Result<IdType, Error> {
        self.schema.id_type(self)
    }

    fn same_pool(&self, other: &EntityType) -> bool {
        Arc::ptr_eq(self.schema.pool(), other.schema.pool())
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

impl CheapClone for EntityType {}

impl std::fmt::Debug for EntityType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "EntityType({})", self.as_str())
    }
}

impl Serialize for EntityType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.as_str().serialize(serializer)
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

impl AsEntityTypeName for ObjectOrInterface<'_> {
    fn name(&self) -> &str {
        match self {
            ObjectOrInterface::Object(object) => &object.name,
            ObjectOrInterface::Interface(interface) => &interface.name,
        }
    }
}
