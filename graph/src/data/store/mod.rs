use crate::{
    components::store::{DeploymentLocator, EntityType},
    data::graphql::ObjectTypeExt,
    prelude::{anyhow::Context, q, r, s, CacheWeight, EntityKey, QueryExecutionError, Schema},
    runtime::gas::{Gas, GasSizeOf},
};
use crate::{data::subgraph::DeploymentHash, prelude::EntityChange};
use anyhow::{anyhow, Error};
use itertools::Itertools;
use serde::de;
use serde::{Deserialize, Serialize};
use stable_hash::{FieldAddress, StableHash, StableHasher};
use std::convert::TryFrom;
use std::fmt;
use std::iter::FromIterator;
use std::str::FromStr;
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
};
use strum::AsStaticRef as _;
use strum_macros::AsStaticStr;

use super::graphql::{ext::DirectiveFinder, DocumentExt as _, TypeExt as _};

/// Custom scalars in GraphQL.
pub mod scalar;

// Ethereum compatibility.
pub mod ethereum;

/// Filter subscriptions
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SubscriptionFilter {
    /// Receive updates about all entities from the given deployment of the
    /// given type
    Entities(DeploymentHash, EntityType),
    /// Subscripe to changes in deployment assignments
    Assignment,
}

impl SubscriptionFilter {
    pub fn matches(&self, change: &EntityChange) -> bool {
        match (self, change) {
            (
                Self::Entities(eid, etype),
                EntityChange::Data {
                    subgraph_id,
                    entity_type,
                    ..
                },
            ) => subgraph_id == eid && entity_type == etype,
            (Self::Assignment, EntityChange::Assignment { .. }) => true,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeId(String);

impl NodeId {
    pub fn new(s: impl Into<String>) -> Result<Self, ()> {
        let s = s.into();

        // Enforce length limit
        if s.len() > 63 {
            return Err(());
        }

        // Check that the ID contains only allowed characters.
        // Note: these restrictions are relied upon to prevent SQL injection
        if !s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(());
        }

        Ok(NodeId(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl slog::Value for NodeId {
    fn serialize(
        &self,
        _record: &slog::Record,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        serializer.emit_str(key, self.0.as_str())
    }
}

impl<'de> de::Deserialize<'de> for NodeId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s: String = de::Deserialize::deserialize(deserializer)?;
        NodeId::new(s.clone())
            .map_err(|()| de::Error::invalid_value(de::Unexpected::Str(&s), &"valid node ID"))
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "type")]
pub enum AssignmentEvent {
    Add {
        deployment: DeploymentLocator,
        node_id: NodeId,
    },
    Remove {
        deployment: DeploymentLocator,
        node_id: NodeId,
    },
}

impl AssignmentEvent {
    pub fn node_id(&self) -> &NodeId {
        match self {
            AssignmentEvent::Add { node_id, .. } => node_id,
            AssignmentEvent::Remove { node_id, .. } => node_id,
        }
    }
}

/// An entity attribute name is represented as a string.
pub type Attribute = String;

pub const ID: &str = "ID";
pub const BYTES_SCALAR: &str = "Bytes";
pub const BIG_INT_SCALAR: &str = "BigInt";
pub const BIG_DECIMAL_SCALAR: &str = "BigDecimal";

#[derive(Clone, Debug, PartialEq)]
pub enum ValueType {
    Boolean,
    BigInt,
    Bytes,
    BigDecimal,
    Int,
    String,
}

impl FromStr for ValueType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Boolean" => Ok(ValueType::Boolean),
            "BigInt" => Ok(ValueType::BigInt),
            "Bytes" => Ok(ValueType::Bytes),
            "BigDecimal" => Ok(ValueType::BigDecimal),
            "Int" => Ok(ValueType::Int),
            "String" | "ID" => Ok(ValueType::String),
            s => Err(anyhow!("Type not available in this context: {}", s)),
        }
    }
}

impl ValueType {
    /// Return `true` if `s` is the name of a builtin scalar type
    pub fn is_scalar(s: &str) -> bool {
        Self::from_str(s).is_ok()
    }
}

// Note: Do not modify fields without also making a backward compatible change to the StableHash impl (below)
/// An attribute value is represented as an enum with variants for all supported value types.
#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", content = "data")]
#[derive(AsStaticStr)]
pub enum Value {
    String(String),
    Int(i32),
    BigDecimal(scalar::BigDecimal),
    Bool(bool),
    List(Vec<Value>),
    Null,
    Bytes(scalar::Bytes),
    BigInt(scalar::BigInt),
}

impl stable_hash_legacy::StableHash for Value {
    fn stable_hash<H: stable_hash_legacy::StableHasher>(
        &self,
        mut sequence_number: H::Seq,
        state: &mut H,
    ) {
        use stable_hash_legacy::prelude::*;
        use Value::*;

        // This is the default, so write nothing.
        if self == &Null {
            return;
        }
        stable_hash_legacy::StableHash::stable_hash(
            &self.as_static().to_string(),
            sequence_number.next_child(),
            state,
        );

        match self {
            Null => unreachable!(),
            String(inner) => {
                stable_hash_legacy::StableHash::stable_hash(inner, sequence_number, state)
            }
            Int(inner) => {
                stable_hash_legacy::StableHash::stable_hash(inner, sequence_number, state)
            }
            BigDecimal(inner) => {
                stable_hash_legacy::StableHash::stable_hash(inner, sequence_number, state)
            }
            Bool(inner) => {
                stable_hash_legacy::StableHash::stable_hash(inner, sequence_number, state)
            }
            List(inner) => {
                stable_hash_legacy::StableHash::stable_hash(inner, sequence_number, state)
            }
            Bytes(inner) => {
                stable_hash_legacy::StableHash::stable_hash(inner, sequence_number, state)
            }
            BigInt(inner) => {
                stable_hash_legacy::StableHash::stable_hash(inner, sequence_number, state)
            }
        }
    }
}

impl StableHash for Value {
    fn stable_hash<H: StableHasher>(&self, field_address: H::Addr, state: &mut H) {
        use Value::*;

        // This is the default, so write nothing.
        if self == &Null {
            return;
        }

        let variant = match self {
            Null => unreachable!(),
            String(inner) => {
                inner.stable_hash(field_address.child(0), state);
                1
            }
            Int(inner) => {
                inner.stable_hash(field_address.child(0), state);
                2
            }
            BigDecimal(inner) => {
                inner.stable_hash(field_address.child(0), state);
                3
            }
            Bool(inner) => {
                inner.stable_hash(field_address.child(0), state);
                4
            }
            List(inner) => {
                inner.stable_hash(field_address.child(0), state);
                5
            }
            Bytes(inner) => {
                inner.stable_hash(field_address.child(0), state);
                6
            }
            BigInt(inner) => {
                inner.stable_hash(field_address.child(0), state);
                7
            }
        };

        state.write(field_address, &[variant])
    }
}

impl Value {
    pub fn from_query_value(value: &r::Value, ty: &s::Type) -> Result<Value, QueryExecutionError> {
        use graphql_parser::schema::Type::{ListType, NamedType, NonNullType};

        Ok(match (value, ty) {
            // When dealing with non-null types, use the inner type to convert the value
            (value, NonNullType(t)) => Value::from_query_value(value, t)?,

            (r::Value::List(values), ListType(ty)) => Value::List(
                values
                    .iter()
                    .map(|value| Self::from_query_value(value, ty))
                    .collect::<Result<Vec<_>, _>>()?,
            ),

            (r::Value::List(values), NamedType(n)) => Value::List(
                values
                    .iter()
                    .map(|value| Self::from_query_value(value, &NamedType(n.to_string())))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            (r::Value::Enum(e), NamedType(_)) => Value::String(e.clone()),
            (r::Value::String(s), NamedType(n)) => {
                // Check if `ty` is a custom scalar type, otherwise assume it's
                // just a string.
                match n.as_str() {
                    BYTES_SCALAR => Value::Bytes(scalar::Bytes::from_str(s)?),
                    BIG_INT_SCALAR => Value::BigInt(scalar::BigInt::from_str(s)?),
                    BIG_DECIMAL_SCALAR => Value::BigDecimal(scalar::BigDecimal::from_str(s)?),
                    _ => Value::String(s.clone()),
                }
            }
            (r::Value::Int(i), _) => Value::Int(*i as i32),
            (r::Value::Boolean(b), _) => Value::Bool(b.to_owned()),
            (r::Value::Null, _) => Value::Null,
            _ => {
                return Err(QueryExecutionError::AttributeTypeError(
                    value.to_string(),
                    ty.to_string(),
                ));
            }
        })
    }

    pub fn as_string(self) -> Option<String> {
        if let Value::String(s) = self {
            Some(s)
        } else {
            None
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        if let Value::String(s) = self {
            Some(s.as_str())
        } else {
            None
        }
    }

    pub fn is_string(&self) -> bool {
        matches!(self, Value::String(_))
    }

    pub fn as_int(&self) -> Option<i32> {
        if let Value::Int(i) = self {
            Some(*i)
        } else {
            None
        }
    }

    pub fn as_big_decimal(self) -> Option<scalar::BigDecimal> {
        if let Value::BigDecimal(d) = self {
            Some(d)
        } else {
            None
        }
    }

    pub fn as_bool(self) -> Option<bool> {
        if let Value::Bool(b) = self {
            Some(b)
        } else {
            None
        }
    }

    pub fn as_list(self) -> Option<Vec<Value>> {
        if let Value::List(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn as_bytes(self) -> Option<scalar::Bytes> {
        if let Value::Bytes(b) = self {
            Some(b)
        } else {
            None
        }
    }

    pub fn as_bigint(self) -> Option<scalar::BigInt> {
        if let Value::BigInt(b) = self {
            Some(b)
        } else {
            None
        }
    }

    /// Return the name of the type of this value for display to the user
    pub fn type_name(&self) -> String {
        match self {
            Value::BigDecimal(_) => "BigDecimal".to_owned(),
            Value::BigInt(_) => "BigInt".to_owned(),
            Value::Bool(_) => "Boolean".to_owned(),
            Value::Bytes(_) => "Bytes".to_owned(),
            Value::Int(_) => "Int".to_owned(),
            Value::List(values) => {
                if let Some(v) = values.first() {
                    format!("[{}]", v.type_name())
                } else {
                    "[Any]".to_owned()
                }
            }
            Value::Null => "Null".to_owned(),
            Value::String(_) => "String".to_owned(),
        }
    }

    pub fn is_assignable(&self, scalar_type: &ValueType, is_list: bool) -> bool {
        match (self, scalar_type) {
            (Value::String(_), ValueType::String)
            | (Value::BigDecimal(_), ValueType::BigDecimal)
            | (Value::BigInt(_), ValueType::BigInt)
            | (Value::Bool(_), ValueType::Boolean)
            | (Value::Bytes(_), ValueType::Bytes)
            | (Value::Int(_), ValueType::Int)
            | (Value::Null, _) => true,
            (Value::List(values), _) if is_list => values
                .iter()
                .all(|value| value.is_assignable(scalar_type, false)),
            _ => false,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Value::String(s) => s.to_string(),
                Value::Int(i) => i.to_string(),
                Value::BigDecimal(d) => d.to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Null => "null".to_string(),
                Value::List(ref values) =>
                    format!("[{}]", values.iter().map(ToString::to_string).join(", ")),
                Value::Bytes(ref bytes) => bytes.to_string(),
                Value::BigInt(ref number) => number.to_string(),
            }
        )
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String(s) => f.debug_tuple("String").field(s).finish(),
            Self::Int(i) => f.debug_tuple("Int").field(i).finish(),
            Self::BigDecimal(d) => d.fmt(f),
            Self::Bool(arg0) => f.debug_tuple("Bool").field(arg0).finish(),
            Self::List(arg0) => f.debug_tuple("List").field(arg0).finish(),
            Self::Null => write!(f, "Null"),
            Self::Bytes(bytes) => bytes.fmt(f),
            Self::BigInt(number) => number.fmt(f),
        }
    }
}

impl From<Value> for q::Value {
    fn from(value: Value) -> Self {
        match value {
            Value::String(s) => q::Value::String(s),
            Value::Int(i) => q::Value::Int(q::Number::from(i)),
            Value::BigDecimal(d) => q::Value::String(d.to_string()),
            Value::Bool(b) => q::Value::Boolean(b),
            Value::Null => q::Value::Null,
            Value::List(values) => {
                q::Value::List(values.into_iter().map(|value| value.into()).collect())
            }
            Value::Bytes(bytes) => q::Value::String(bytes.to_string()),
            Value::BigInt(number) => q::Value::String(number.to_string()),
        }
    }
}

impl From<Value> for r::Value {
    fn from(value: Value) -> Self {
        match value {
            Value::String(s) => r::Value::String(s),
            Value::Int(i) => r::Value::Int(i as i64),
            Value::BigDecimal(d) => r::Value::String(d.to_string()),
            Value::Bool(b) => r::Value::Boolean(b),
            Value::Null => r::Value::Null,
            Value::List(values) => {
                r::Value::List(values.into_iter().map(|value| value.into()).collect())
            }
            Value::Bytes(bytes) => r::Value::String(bytes.to_string()),
            Value::BigInt(number) => r::Value::String(number.to_string()),
        }
    }
}

impl<'a> From<&'a str> for Value {
    fn from(value: &'a str) -> Value {
        Value::String(value.to_owned())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Value {
        Value::String(value)
    }
}

impl<'a> From<&'a String> for Value {
    fn from(value: &'a String) -> Value {
        Value::String(value.clone())
    }
}

impl From<scalar::Bytes> for Value {
    fn from(value: scalar::Bytes) -> Value {
        Value::Bytes(value)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Value {
        Value::Bool(value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Value {
        Value::Int(value)
    }
}

impl From<scalar::BigDecimal> for Value {
    fn from(value: scalar::BigDecimal) -> Value {
        Value::BigDecimal(value)
    }
}

impl From<scalar::BigInt> for Value {
    fn from(value: scalar::BigInt) -> Value {
        Value::BigInt(value)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Value {
        Value::BigInt(value.into())
    }
}

impl TryFrom<Value> for Option<scalar::BigInt> {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::BigInt(n) => Ok(Some(n)),
            Value::Null => Ok(None),
            _ => Err(anyhow!("Value is not an BigInt")),
        }
    }
}

impl<T> From<Vec<T>> for Value
where
    T: Into<Value>,
{
    fn from(values: Vec<T>) -> Value {
        Value::List(values.into_iter().map(Into::into).collect())
    }
}

impl<T> From<Option<T>> for Value
where
    Value: From<T>,
{
    fn from(x: Option<T>) -> Value {
        match x {
            Some(x) => x.into(),
            None => Value::Null,
        }
    }
}

/// An entity is represented as a map of attribute names to values.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
pub struct Entity(HashMap<Attribute, Value>);

impl stable_hash_legacy::StableHash for Entity {
    #[inline]
    fn stable_hash<H: stable_hash_legacy::StableHasher>(
        &self,
        mut sequence_number: H::Seq,
        state: &mut H,
    ) {
        use stable_hash_legacy::SequenceNumber;
        let Self(inner) = self;
        stable_hash_legacy::StableHash::stable_hash(inner, sequence_number.next_child(), state);
    }
}

impl StableHash for Entity {
    fn stable_hash<H: StableHasher>(&self, field_address: H::Addr, state: &mut H) {
        let Self(inner) = self;
        StableHash::stable_hash(inner, field_address.child(0), state);
    }
}

#[macro_export]
macro_rules! entity {
    ($($name:ident: $value:expr,)*) => {
        {
            let mut result = $crate::data::store::Entity::new();
            $(
                result.set(stringify!($name), $crate::data::store::Value::from($value));
            )*
            result
        }
    };
    ($($name:ident: $value:expr),*) => {
        entity! {$($name: $value,)*}
    };
}

impl Entity {
    /// Creates a new entity with no attributes set.
    pub fn new() -> Self {
        Default::default()
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.get(key)
    }

    pub fn insert(&mut self, key: String, value: Value) -> Option<Value> {
        self.0.insert(key, value)
    }

    pub fn remove(&mut self, key: &str) -> Option<Value> {
        self.0.remove(key)
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }

    // This collects the entity into an ordered vector so that it can be iterated deterministically.
    pub fn sorted(self) -> Vec<(String, Value)> {
        let mut v: Vec<_> = self.0.into_iter().collect();
        v.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
        v
    }

    /// Return the ID of this entity. If the ID is a string, return the
    /// string. If it is `Bytes`, return it as a hex string with a `0x`
    /// prefix. If the ID is not set or anything but a `String` or `Bytes`,
    /// return an error
    pub fn id(&self) -> Result<String, Error> {
        match self.get("id") {
            None => Err(anyhow!("Entity is missing an `id` attribute")),
            Some(Value::String(s)) => Ok(s.to_owned()),
            Some(Value::Bytes(b)) => Ok(b.to_string()),
            _ => Err(anyhow!("Entity has non-string `id` attribute")),
        }
    }

    /// Convenience method to save having to `.into()` the arguments.
    pub fn set(&mut self, name: impl Into<Attribute>, value: impl Into<Value>) -> Option<Value> {
        self.0.insert(name.into(), value.into())
    }

    /// Merges an entity update `update` into this entity.
    ///
    /// If a key exists in both entities, the value from `update` is chosen.
    /// If a key only exists on one entity, the value from that entity is chosen.
    /// If a key is set to `Value::Null` in `update`, the key/value pair is set to `Value::Null`.
    pub fn merge(&mut self, update: Entity) {
        for (key, value) in update.0.into_iter() {
            self.insert(key, value);
        }
    }

    /// Merges an entity update `update` into this entity, removing `Value::Null` values.
    ///
    /// If a key exists in both entities, the value from `update` is chosen.
    /// If a key only exists on one entity, the value from that entity is chosen.
    /// If a key is set to `Value::Null` in `update`, the key/value pair is removed.
    pub fn merge_remove_null_fields(&mut self, update: Entity) {
        for (key, value) in update.0.into_iter() {
            match value {
                Value::Null => self.remove(&key),
                _ => self.insert(key, value),
            };
        }
    }

    /// Validate that this entity matches the object type definition in the
    /// schema. An entity that passes these checks can be stored
    /// successfully in the subgraph's database schema
    pub fn validate(&self, schema: &Schema, key: &EntityKey) -> Result<(), anyhow::Error> {
        fn scalar_value_type(schema: &Schema, field_type: &s::Type) -> ValueType {
            use s::TypeDefinition as t;
            match field_type {
                s::Type::NamedType(name) => ValueType::from_str(name).unwrap_or_else(|_| {
                    match schema.document.get_named_type(name) {
                        Some(t::Object(obj_type)) => {
                            let id = obj_type.field("id").expect("all object types have an id");
                            scalar_value_type(schema, &id.field_type)
                        }
                        Some(t::Interface(intf)) => {
                            // Validation checks that all implementors of an
                            // interface use the same type for `id`. It is
                            // therefore enough to use the id type of one of
                            // the implementors
                            match schema
                                .types_for_interface()
                                .get(&EntityType::new(intf.name.clone()))
                                .expect("interface type names are known")
                                .first()
                            {
                                None => {
                                    // Nothing is implementing this interface; we assume it's of type string
                                    // see also: id-type-for-unimplemented-interfaces
                                    ValueType::String
                                }
                                Some(obj_type) => {
                                    let id =
                                        obj_type.field("id").expect("all object types have an id");
                                    scalar_value_type(schema, &id.field_type)
                                }
                            }
                        }
                        Some(t::Enum(_)) => ValueType::String,
                        Some(t::Scalar(_)) => unreachable!("user-defined scalars are not used"),
                        Some(t::Union(_)) => unreachable!("unions are not used"),
                        Some(t::InputObject(_)) => unreachable!("inputObjects are not used"),
                        None => unreachable!("names of field types have been validated"),
                    }
                }),
                s::Type::NonNullType(inner) => scalar_value_type(schema, inner),
                s::Type::ListType(inner) => scalar_value_type(schema, inner),
            }
        }

        if key.entity_type.is_poi() {
            // Users can't modify Poi entities, and therefore they do not
            // need to be validated. In addition, the schema has no object
            // type for them, and validation would therefore fail
            return Ok(());
        }
        let object_type_definitions = schema.document.get_object_type_definitions();
        let object_type = object_type_definitions
            .iter()
            .find(|object_type| key.entity_type.as_str() == object_type.name)
            .with_context(|| {
                format!(
                    "Entity {}[{}]: unknown entity type `{}`",
                    key.entity_type, key.entity_id, key.entity_type
                )
            })?;

        for field in &object_type.fields {
            let is_derived = field.is_derived();
            match (self.get(&field.name), is_derived) {
                (Some(value), false) => {
                    let scalar_type = scalar_value_type(schema, &field.field_type);
                    if field.field_type.is_list() {
                        // Check for inhomgeneous lists to produce a better
                        // error message for them; other problems, like
                        // assigning a scalar to a list will be caught below
                        if let Value::List(elts) = value {
                            for (index, elt) in elts.iter().enumerate() {
                                if !elt.is_assignable(&scalar_type, false) {
                                    anyhow::bail!(
                                        "Entity {}[{}]: field `{}` is of type {}, but the value `{}` \
                                        contains a {} at index {}",
                                        key.entity_type,
                                        key.entity_id,
                                        field.name,
                                        &field.field_type,
                                        value,
                                        elt.type_name(),
                                        index
                                    );
                                }
                            }
                        }
                    }
                    if !value.is_assignable(&scalar_type, field.field_type.is_list()) {
                        anyhow::bail!(
                            "Entity {}[{}]: the value `{}` for field `{}` must have type {} but has type {}",
                            key.entity_type,
                            key.entity_id,
                            value,
                            field.name,
                            &field.field_type,
                            value.type_name()
                        );
                    }
                }
                (None, false) => {
                    if field.field_type.is_non_null() {
                        anyhow::bail!(
                            "Entity {}[{}]: missing value for non-nullable field `{}`",
                            key.entity_type,
                            key.entity_id,
                            field.name,
                        );
                    }
                }
                (Some(_), true) => {
                    anyhow::bail!(
                        "Entity {}[{}]: field `{}` is derived and can not be set",
                        key.entity_type,
                        key.entity_id,
                        field.name,
                    );
                }
                (None, true) => {
                    // derived fields should not be set
                }
            }
        }
        Ok(())
    }
}

impl From<Entity> for BTreeMap<String, q::Value> {
    fn from(entity: Entity) -> BTreeMap<String, q::Value> {
        entity.0.into_iter().map(|(k, v)| (k, v.into())).collect()
    }
}

impl From<Entity> for q::Value {
    fn from(entity: Entity) -> q::Value {
        q::Value::Object(entity.into())
    }
}

impl From<HashMap<Attribute, Value>> for Entity {
    fn from(m: HashMap<Attribute, Value>) -> Entity {
        Entity(m)
    }
}

impl<'a> From<&'a Entity> for Cow<'a, Entity> {
    fn from(entity: &'a Entity) -> Self {
        Cow::Borrowed(entity)
    }
}

impl<'a> From<Vec<(&'a str, Value)>> for Entity {
    fn from(entries: Vec<(&'a str, Value)>) -> Entity {
        Entity::from(HashMap::from_iter(
            entries.into_iter().map(|(k, v)| (String::from(k), v)),
        ))
    }
}

impl CacheWeight for Entity {
    fn indirect_weight(&self) -> usize {
        self.0.indirect_weight()
    }
}

impl GasSizeOf for Entity {
    fn gas_size_of(&self) -> Gas {
        self.0.gas_size_of()
    }
}

/// A value that can (maybe) be converted to an `Entity`.
pub trait TryIntoEntity {
    fn try_into_entity(self) -> Result<Entity, Error>;
}

/// A value that can be converted to an `Entity` ID.
pub trait ToEntityId {
    fn to_entity_id(&self) -> String;
}

/// A value that can be converted to an `Entity` key.
pub trait ToEntityKey {
    fn to_entity_key(&self, subgraph: DeploymentHash) -> EntityKey;
}

#[test]
fn value_bytes() {
    let graphql_value = r::Value::String("0x8f494c66afc1d3f8ac1b45df21f02a46".to_owned());
    let ty = q::Type::NamedType(BYTES_SCALAR.to_owned());
    let from_query = Value::from_query_value(&graphql_value, &ty).unwrap();
    assert_eq!(
        from_query,
        Value::Bytes(scalar::Bytes::from(
            &[143, 73, 76, 102, 175, 193, 211, 248, 172, 27, 69, 223, 33, 240, 42, 70][..]
        ))
    );
    assert_eq!(r::Value::from(from_query), graphql_value);
}

#[test]
fn value_bigint() {
    let big_num = "340282366920938463463374607431768211456";
    let graphql_value = r::Value::String(big_num.to_owned());
    let ty = q::Type::NamedType(BIG_INT_SCALAR.to_owned());
    let from_query = Value::from_query_value(&graphql_value, &ty).unwrap();
    assert_eq!(
        from_query,
        Value::BigInt(FromStr::from_str(big_num).unwrap())
    );
    assert_eq!(r::Value::from(from_query), graphql_value);
}

#[test]
fn entity_validation() {
    fn make_thing(name: &str) -> Entity {
        let mut thing = Entity::new();
        thing.set("id", name);
        thing.set("name", name);
        thing.set("stuff", "less");
        thing.set("favorite_color", "red");
        thing.set("things", Value::List(vec![]));
        thing
    }

    fn check(thing: Entity, errmsg: &str) {
        const DOCUMENT: &str = "
      enum Color { red, yellow, blue }
      interface Stuff { id: ID!, name: String! }
      type Cruft @entity {
          id: ID!,
          thing: Thing!
      }
      type Thing @entity {
          id: ID!,
          name: String!,
          favorite_color: Color,
          stuff: Stuff,
          things: [Thing!]!
          # Make sure we do not validate derived fields; it's ok
          # to store a thing with a null Cruft
          cruft: Cruft! @derivedFrom(field: \"thing\")
      }";
        let subgraph = DeploymentHash::new("doesntmatter").unwrap();
        let schema =
            crate::prelude::Schema::parse(DOCUMENT, subgraph).expect("Failed to parse test schema");
        let id = thing.id().unwrap_or("none".to_owned());
        let key = EntityKey::data(
            DeploymentHash::new("doesntmatter").unwrap(),
            "Thing".to_owned(),
            id.to_owned(),
        );

        let err = thing.validate(&schema, &key);
        if errmsg == "" {
            assert!(
                err.is_ok(),
                "checking entity {}: expected ok but got {}",
                id,
                err.unwrap_err()
            );
        } else {
            if let Err(e) = err {
                assert_eq!(errmsg, e.to_string(), "checking entity {}", id);
            } else {
                panic!(
                    "Expected error `{}` but got ok when checking entity {}",
                    errmsg, id
                );
            }
        }
    }

    let mut thing = make_thing("t1");
    thing.set("things", Value::from(vec!["thing1", "thing2"]));
    check(thing, "");

    let thing = make_thing("t2");
    check(thing, "");

    let mut thing = make_thing("t3");
    thing.remove("name");
    check(
        thing,
        "Entity Thing[t3]: missing value for non-nullable field `name`",
    );

    let mut thing = make_thing("t4");
    thing.remove("things");
    check(
        thing,
        "Entity Thing[t4]: missing value for non-nullable field `things`",
    );

    let mut thing = make_thing("t5");
    thing.set("name", Value::Int(32));
    check(
        thing,
        "Entity Thing[t5]: the value `32` for field `name` must \
         have type String! but has type Int",
    );

    let mut thing = make_thing("t6");
    thing.set("things", Value::List(vec!["thing1".into(), 17.into()]));
    check(
        thing,
        "Entity Thing[t6]: field `things` is of type [Thing!]!, \
         but the value `[thing1, 17]` contains a Int at index 1",
    );

    let mut thing = make_thing("t7");
    thing.remove("favorite_color");
    thing.remove("stuff");
    check(thing, "");

    let mut thing = make_thing("t8");
    thing.set("cruft", "wat");
    check(
        thing,
        "Entity Thing[t8]: field `cruft` is derived and can not be set",
    );
}

#[test]
fn fmt_debug() {
    assert_eq!("String(\"hello\")", format!("{:?}", Value::from("hello")));
    assert_eq!("Int(17)", format!("{:?}", Value::Int(17)));
    assert_eq!("Bool(false)", format!("{:?}", Value::Bool(false)));
    assert_eq!("Null", format!("{:?}", Value::Null));

    let bd = Value::BigDecimal(scalar::BigDecimal::from(-0.17));
    assert_eq!("BigDecimal(-0.17)", format!("{:?}", bd));

    let bytes = Value::Bytes(scalar::Bytes::from([222, 173, 190, 239].as_slice()));
    assert_eq!("Bytes(0xdeadbeef)", format!("{:?}", bytes));

    let bi = Value::BigInt(scalar::BigInt::from(-17i32));
    assert_eq!("BigInt(-17)", format!("{:?}", bi));
}
