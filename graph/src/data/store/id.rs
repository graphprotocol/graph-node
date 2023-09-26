//! Types and helpers to deal with entity IDs which support a subset of the
//! types that more general values support
use anyhow::{anyhow, Error};
use stable_hash::{StableHash, StableHasher};
use std::convert::TryFrom;
use std::fmt;

use crate::{
    data::graphql::{ObjectTypeExt, TypeExt},
    prelude::s,
};

use crate::{
    components::store::StoreError,
    constraint_violation,
    data::value::Word,
    prelude::{CacheWeight, QueryExecutionError},
    runtime::gas::{Gas, GasSizeOf},
    schema::EntityType,
};

use super::{scalar, Value, ID};

/// The types that can be used for the `id` of an entity
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum IdType {
    String,
    Bytes,
}

impl IdType {
    /// Parse the given string into an ID of this type
    pub fn parse(&self, s: Word) -> Result<Id, Error> {
        match self {
            IdType::String => Ok(Id::String(s)),
            IdType::Bytes => Ok(Id::Bytes(s.parse()?)),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            IdType::String => "String",
            IdType::Bytes => "Bytes",
        }
    }
}

impl<'a> TryFrom<&s::ObjectType> for IdType {
    type Error = Error;

    fn try_from(obj_type: &s::ObjectType) -> Result<Self, Self::Error> {
        let base_type = obj_type.field(&*ID).unwrap().field_type.get_base_type();

        match base_type {
            "ID" | "String" => Ok(IdType::String),
            "Bytes" => Ok(IdType::Bytes),
            s => Err(anyhow!(
                "Entity type {} uses illegal type {} for id column",
                obj_type.name,
                s
            )),
        }
    }
}

impl std::fmt::Display for IdType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Values for the ids of entities
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Id {
    String(Word),
    Bytes(scalar::Bytes),
}

impl Id {
    pub fn id_type(&self) -> IdType {
        match self {
            Id::String(_) => IdType::String,
            Id::Bytes(_) => IdType::Bytes,
        }
    }
}

impl std::hash::Hash for Id {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);
        match self {
            Id::String(s) => s.hash(state),
            Id::Bytes(b) => b.hash(state),
        }
    }
}

impl PartialEq<Value> for Id {
    fn eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Id::String(s), Value::String(v)) => s.as_str() == v.as_str(),
            (Id::Bytes(s), Value::Bytes(v)) => s == v,
            _ => false,
        }
    }
}

impl PartialEq<Id> for Value {
    fn eq(&self, other: &Id) -> bool {
        other.eq(self)
    }
}

impl TryFrom<Value> for Id {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::String(s) => Ok(Id::String(Word::from(s))),
            Value::Bytes(b) => Ok(Id::Bytes(b)),
            _ => Err(anyhow!(
                "expected string or bytes for id but found {:?}",
                value
            )),
        }
    }
}

impl From<Id> for Value {
    fn from(value: Id) -> Self {
        match value {
            Id::String(s) => Value::String(s.into()),
            Id::Bytes(b) => Value::Bytes(b),
        }
    }
}

impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Id::String(s) => write!(f, "{}", s),
            Id::Bytes(b) => write!(f, "{}", b),
        }
    }
}

impl CacheWeight for Id {
    fn indirect_weight(&self) -> usize {
        match self {
            Id::String(s) => s.indirect_weight(),
            Id::Bytes(b) => b.indirect_weight(),
        }
    }
}

impl GasSizeOf for Id {
    fn gas_size_of(&self) -> Gas {
        match self {
            Id::String(s) => s.gas_size_of(),
            Id::Bytes(b) => b.gas_size_of(),
        }
    }
}

impl StableHash for Id {
    fn stable_hash<H: StableHasher>(&self, field_address: H::Addr, state: &mut H) {
        match self {
            Id::String(s) => stable_hash::StableHash::stable_hash(s, field_address, state),
            Id::Bytes(b) => {
                // We have to convert here to a string `0xdeadbeef` for
                // backwards compatibility. It would be nice to avoid that
                // allocation and just use the bytes directly, but that will
                // break PoI compatibility
                stable_hash::StableHash::stable_hash(&b.to_string(), field_address, state)
            }
        }
    }
}

impl stable_hash_legacy::StableHash for Id {
    fn stable_hash<H: stable_hash_legacy::StableHasher>(
        &self,
        sequence_number: H::Seq,
        state: &mut H,
    ) {
        match self {
            Id::String(s) => stable_hash_legacy::StableHash::stable_hash(s, sequence_number, state),
            Id::Bytes(b) => {
                stable_hash_legacy::StableHash::stable_hash(&b.to_string(), sequence_number, state)
            }
        }
    }
}

/// A value that contains a reference to the underlying data for an entity
/// ID. This is used to avoid cloning the ID when it is not necessary.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum IdRef<'a> {
    String(&'a str),
    Bytes(&'a [u8]),
}

impl std::fmt::Display for IdRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IdRef::String(s) => write!(f, "{}", s),
            IdRef::Bytes(b) => write!(f, "0x{}", hex::encode(b)),
        }
    }
}

impl<'a> IdRef<'a> {
    pub fn to_value(self) -> Id {
        match self {
            IdRef::String(s) => Id::String(Word::from(s.to_owned())),
            IdRef::Bytes(b) => Id::Bytes(scalar::Bytes::from(b)),
        }
    }
}

/// A homogeneous list of entity ids, i.e., all ids in the list are of the
/// same `IdType`
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IdList {
    String(Vec<Word>),
    Bytes(Vec<scalar::Bytes>),
}

impl IdList {
    pub fn new(typ: IdType) -> Self {
        match typ {
            IdType::String => IdList::String(Vec::new()),
            IdType::Bytes => IdList::Bytes(Vec::new()),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            IdList::String(ids) => ids.len(),
            IdList::Bytes(ids) => ids.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Turn a list of ids into an `IdList` and check that they are all the
    /// same type
    pub fn try_from_iter<I: Iterator<Item = Id>>(
        entity_type: &EntityType,
        mut iter: I,
    ) -> Result<Self, QueryExecutionError> {
        let id_type = entity_type.id_type()?;
        match id_type {
            IdType::String => {
                let ids: Vec<Word> = iter.try_fold(vec![], |mut ids, id| match id {
                    Id::String(id) => {
                        ids.push(id);
                        Ok(ids)
                    }
                    Id::Bytes(id) => Err(constraint_violation!(
                        "expected string id, got bytes: {}",
                        id,
                    )),
                })?;
                Ok(IdList::String(ids))
            }
            IdType::Bytes => {
                let ids: Vec<scalar::Bytes> = iter.try_fold(vec![], |mut ids, id| match id {
                    Id::String(id) => Err(constraint_violation!(
                        "expected bytes id, got string: {}",
                        id,
                    )),
                    Id::Bytes(id) => {
                        ids.push(id);
                        Ok(ids)
                    }
                })?;
                Ok(IdList::Bytes(ids))
            }
        }
    }

    /// Turn a list of references to ids into an `IdList` and check that
    /// they are all the same type. Note that this method clones all the ids
    /// and `try_from_iter` is therefore preferrable
    pub fn try_from_iter_ref<'a, I: Iterator<Item = IdRef<'a>>>(
        mut iter: I,
    ) -> Result<Self, QueryExecutionError> {
        let first = match iter.next() {
            Some(id) => id,
            None => return Ok(IdList::String(Vec::new())),
        };
        match first {
            IdRef::String(s) => {
                let ids: Vec<_> = iter.try_fold(vec![Word::from(s)], |mut ids, id| match id {
                    IdRef::String(id) => {
                        ids.push(Word::from(id));
                        Ok(ids)
                    }
                    IdRef::Bytes(id) => Err(constraint_violation!(
                        "expected string id, got bytes: 0x{}",
                        hex::encode(id),
                    )),
                })?;
                Ok(IdList::String(ids))
            }
            IdRef::Bytes(b) => {
                let ids: Vec<_> =
                    iter.try_fold(vec![scalar::Bytes::from(b)], |mut ids, id| match id {
                        IdRef::String(id) => Err(constraint_violation!(
                            "expected bytes id, got string: {}",
                            id,
                        )),
                        IdRef::Bytes(id) => {
                            ids.push(scalar::Bytes::from(id));
                            Ok(ids)
                        }
                    })?;
                Ok(IdList::Bytes(ids))
            }
        }
    }

    pub fn index(&self, index: usize) -> IdRef<'_> {
        match self {
            IdList::String(ids) => IdRef::String(&ids[index]),
            IdList::Bytes(ids) => IdRef::Bytes(ids[index].as_slice()),
        }
    }

    pub fn first(&self) -> Option<IdRef<'_>> {
        if self.len() > 0 {
            Some(self.index(0))
        } else {
            None
        }
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = IdRef<'_>> + '_> {
        match self {
            IdList::String(ids) => Box::new(ids.iter().map(|id| IdRef::String(id))),
            IdList::Bytes(ids) => Box::new(ids.iter().map(|id| IdRef::Bytes(id))),
        }
    }

    pub fn as_unique(self) -> Self {
        match self {
            IdList::String(mut ids) => {
                ids.sort_unstable();
                ids.dedup();
                IdList::String(ids)
            }
            IdList::Bytes(mut ids) => {
                ids.sort_unstable_by(|id1, id2| id1.as_slice().cmp(id2.as_slice()));
                ids.dedup();
                IdList::Bytes(ids)
            }
        }
    }

    pub fn push(&mut self, entity_id: Id) -> Result<(), StoreError> {
        match (self, entity_id) {
            (IdList::String(ids), Id::String(id)) => {
                ids.push(id);
                Ok(())
            }
            (IdList::Bytes(ids), Id::Bytes(id)) => {
                ids.push(id);
                Ok(())
            }
            (IdList::String(_), Id::Bytes(b)) => Err(constraint_violation!(
                "expected id of type string, but got Bytes[{}]",
                b
            )),
            (IdList::Bytes(_), Id::String(s)) => Err(constraint_violation!(
                "expected id of type bytes, but got String[{}]",
                s
            )),
        }
    }

    pub fn as_ids(self) -> Vec<Id> {
        match self {
            IdList::String(ids) => ids.into_iter().map(Id::String).collect(),
            IdList::Bytes(ids) => ids.into_iter().map(Id::Bytes).collect(),
        }
    }
}
