use crate::prelude::s;
use crate::schema::{EntityType, Schema};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::mem;

use super::{DocumentExt, ObjectTypeExt};

#[derive(Copy, Clone, Debug)]
pub enum QueryableType<'a> {
    Object(&'a s::ObjectType),
    Interface(&'a s::InterfaceType),
    Union(&'a s::UnionType),
}

impl<'a> PartialEq for QueryableType<'a> {
    fn eq(&self, other: &Self) -> bool {
        use QueryableType::*;
        match (self, other) {
            (Object(a), Object(b)) => a.name == b.name,
            (Interface(a), Interface(b)) => a.name == b.name,
            (Union(a), Union(b)) => a.name == b.name,
            (Object(_), Union(_))
            | (Interface(_), Union(_))
            | (Union(_), Object(_))
            | (Union(_), Interface(_))
            | (Interface(_), Object(_))
            | (Object(_), Interface(_)) => false,
        }
    }
}

impl<'a> Eq for QueryableType<'a> {}

impl<'a> Hash for QueryableType<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        mem::discriminant(self).hash(state);
        self.name().hash(state)
    }
}

impl<'a> PartialOrd for QueryableType<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for QueryableType<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use QueryableType::*;
        match (self, other) {
            (Object(a), Object(b)) => a.name.cmp(&b.name),
            (Interface(a), Interface(b)) => a.name.cmp(&b.name),
            (Union(a), Union(b)) => a.name.cmp(&b.name),
            (Interface(_), Object(_)) => Ordering::Less,
            (Object(_), Interface(_)) => Ordering::Greater,
            // TODO what is the correct order here?
            (Object(_), Union(_)) => Ordering::Less,
            (Interface(_), Union(_)) => Ordering::Less,
            (Union(_), Object(_)) => Ordering::Greater,
            (Union(_), Interface(_)) => Ordering::Greater,
        }
    }
}

impl<'a> From<&'a s::ObjectType> for QueryableType<'a> {
    fn from(object: &'a s::ObjectType) -> Self {
        QueryableType::Object(object)
    }
}

impl<'a> From<&'a s::InterfaceType> for QueryableType<'a> {
    fn from(interface: &'a s::InterfaceType) -> Self {
        QueryableType::Interface(interface)
    }
}

impl<'a> From<&'a s::UnionType> for QueryableType<'a> {
    fn from(union: &'a s::UnionType) -> Self {
        QueryableType::Union(union)
    }
}

impl<'a> QueryableType<'a> {
    pub fn is_object(self) -> bool {
        match self {
            QueryableType::Object(_) => true,
            QueryableType::Interface(_) => false,
            QueryableType::Union(_) => false,
        }
    }

    pub fn is_interface(self) -> bool {
        match self {
            QueryableType::Object(_) => false,
            QueryableType::Interface(_) => true,
            QueryableType::Union(_) => false,
        }
    }

    pub fn name(self) -> &'a str {
        match self {
            QueryableType::Object(object) => &object.name,
            QueryableType::Interface(interface) => &interface.name,
            QueryableType::Union(union) => &union.name,
        }
    }

    pub fn directives(self) -> &'a Vec<s::Directive> {
        match self {
            QueryableType::Object(object) => &object.directives,
            QueryableType::Interface(interface) => &interface.directives,
            QueryableType::Union(union) => &union.directives,
        }
    }

    pub fn fields(self) -> &'a Vec<s::Field> {
        match self {
            QueryableType::Object(object) => &object.fields,
            QueryableType::Interface(interface) => &interface.fields,
            QueryableType::Union(_) => unreachable!("union type has no fields"),
        }
    }

    pub fn field(&self, name: &str) -> Option<&s::Field> {
        self.fields().iter().find(|field| &field.name == name)
    }

    pub fn object_types(self, schema: &'a Schema) -> Option<Vec<&'a s::ObjectType>> {
        match self {
            QueryableType::Object(object) => Some(vec![object]),
            QueryableType::Interface(interface) => schema
                .types_for_interface_or_union()
                .get(interface.name.as_str())
                .map(|object_types| object_types.iter().collect()),
            QueryableType::Union(union) => Some(
                schema
                    .document
                    .get_object_type_definitions()
                    .into_iter()
                    .filter(|object_type| union.types.contains(&object_type.name))
                    .collect(),
            ),
        }
    }

    /// `typename` is the name of an object type. Matches if `self` is an object and has the same
    /// name, or if self is an interface implemented by `typename`.
    pub fn matches(
        self,
        typename: &str,
        types_for_interface: &BTreeMap<EntityType, Vec<s::ObjectType>>,
    ) -> bool {
        match self {
            QueryableType::Object(o) => o.name == typename,
            QueryableType::Interface(s::InterfaceType { name, .. })
            | QueryableType::Union(s::UnionType { name, .. }) => types_for_interface[name.as_str()]
                .iter()
                .any(|o| o.name == typename),
        }
    }

    pub fn is_meta(&self) -> bool {
        match self {
            QueryableType::Object(o) => o.is_meta(),
            QueryableType::Interface(i) => i.is_meta(),
            QueryableType::Union(u) => u.is_meta(),
        }
    }

    pub fn is_sql(&self) -> bool {
        match self {
            QueryableType::Object(o) => o.is_sql(),
            QueryableType::Interface(i) => i.is_sql(),
            QueryableType::Union(u) => u.is_sql(),
        }
    }
}
