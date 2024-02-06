use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Error};
use semver::Version;
use store::Entity;

use crate::bail;
use crate::blockchain::BlockTime;
use crate::cheap_clone::CheapClone;
use crate::components::store::LoadRelatedRequest;
use crate::data::graphql::ext::DirectiveFinder;
use crate::data::graphql::{DirectiveExt, DocumentExt, ObjectTypeExt, TypeExt, ValueExt};
use crate::data::store::{
    self, EntityValidationError, IdType, IntoEntityIterator, TryIntoEntityIterator, ValueType, ID,
};
use crate::data::value::Word;
use crate::prelude::q::Value;
use crate::prelude::{s, DeploymentHash};
use crate::schema::api::api_schema;
use crate::util::intern::{Atom, AtomPool};

use super::fulltext::FulltextDefinition;
use super::{ApiSchema, AsEntityTypeName, EntityType, Schema};

/// The name of the PoI entity type
pub(crate) const POI_OBJECT: &str = "Poi$";
/// The name of the digest attribute of POI entities
const POI_DIGEST: &str = "digest";
/// The name of the PoI attribute for storing the block time
const POI_BLOCK_TIME: &str = "blockTime";

pub mod kw {
    pub const ENTITY: &str = "entity";
    pub const IMMUTABLE: &str = "immutable";
    pub const TIMESERIES: &str = "timeseries";
    pub const TIMESTAMP: &str = "timestamp";
    pub const AGGREGATE: &str = "aggregate";
    pub const AGGREGATION: &str = "aggregation";
    pub const SOURCE: &str = "source";
    pub const FUNC: &str = "fn";
    pub const ARG: &str = "arg";
    pub const INTERVALS: &str = "intervals";
    pub const INTERVAL: &str = "interval";
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeKind {
    /// The type is a normal @entity
    Object,
    /// The type is an interface
    Interface,
    /// The type is an aggregation
    Aggregation,
}

#[derive(Debug, PartialEq)]
enum TypeInfo {
    Object(ObjectType),
    Interface(InterfaceType),
    Aggregation(Aggregation),
}

impl TypeInfo {
    fn is_object(&self) -> bool {
        match self {
            TypeInfo::Object(_) => true,
            TypeInfo::Interface(_) | TypeInfo::Aggregation(_) => false,
        }
    }

    fn is_interface(&self) -> bool {
        match self {
            TypeInfo::Object(_) | TypeInfo::Aggregation(_) => false,
            TypeInfo::Interface(_) => true,
        }
    }

    fn id_type(&self) -> Option<IdType> {
        match self {
            TypeInfo::Object(obj_type) => Some(obj_type.id_type),
            TypeInfo::Interface(intf_type) => Some(intf_type.id_type),
            TypeInfo::Aggregation(agg_type) => Some(agg_type.id_type),
        }
    }

    fn fields(&self) -> &[Field] {
        match self {
            TypeInfo::Object(obj_type) => &obj_type.fields,
            TypeInfo::Interface(intf_type) => &intf_type.fields,
            TypeInfo::Aggregation(agg_type) => &agg_type.fields,
        }
    }

    fn name(&self) -> Atom {
        match self {
            TypeInfo::Object(obj_type) => obj_type.name,
            TypeInfo::Interface(intf_type) => intf_type.name,
            TypeInfo::Aggregation(agg_type) => agg_type.name,
        }
    }

    fn is_immutable(&self) -> bool {
        match self {
            TypeInfo::Object(obj_type) => obj_type.immutable,
            TypeInfo::Interface(_) => false,
            TypeInfo::Aggregation(_) => true,
        }
    }

    fn kind(&self) -> TypeKind {
        match self {
            TypeInfo::Object(_) => TypeKind::Object,
            TypeInfo::Interface(_) => TypeKind::Interface,
            TypeInfo::Aggregation(_) => TypeKind::Aggregation,
        }
    }

    fn object_type(&self) -> Option<&ObjectType> {
        match self {
            TypeInfo::Object(obj_type) => Some(obj_type),
            TypeInfo::Interface(_) | TypeInfo::Aggregation(_) => None,
        }
    }

    fn interface_type(&self) -> Option<&InterfaceType> {
        match self {
            TypeInfo::Interface(intf_type) => Some(intf_type),
            TypeInfo::Object(_) | TypeInfo::Aggregation(_) => None,
        }
    }

    fn aggregation(&self) -> Option<&Aggregation> {
        match self {
            TypeInfo::Aggregation(agg_type) => Some(agg_type),
            TypeInfo::Interface(_) | TypeInfo::Object(_) => None,
        }
    }
}

impl TypeInfo {
    fn for_object(schema: &Schema, pool: &AtomPool, obj_type: &s::ObjectType) -> Self {
        let shared_interfaces: Vec<_> = match schema.interfaces_for_type(&obj_type.name) {
            Some(intfs) => {
                let mut shared_interfaces: Vec<_> = intfs
                    .iter()
                    .flat_map(|intf| &schema.types_for_interface[&intf.name])
                    .filter(|other| other.name != obj_type.name)
                    .map(|obj_type| pool.lookup(&obj_type.name).unwrap())
                    .collect();
                shared_interfaces.sort();
                shared_interfaces.dedup();
                shared_interfaces
            }
            None => Vec::new(),
        };
        let object_type =
            ObjectType::new(schema, pool, obj_type, shared_interfaces.into_boxed_slice());
        TypeInfo::Object(object_type)
    }

    fn for_interface(schema: &Schema, pool: &AtomPool, intf_type: &s::InterfaceType) -> Self {
        static EMPTY_VEC: [s::ObjectType; 0] = [];
        let implementers = schema
            .types_for_interface
            .get(&intf_type.name)
            .map(|impls| impls.as_slice())
            .unwrap_or_else(|| EMPTY_VEC.as_slice());
        let intf_type = InterfaceType::new(schema, pool, intf_type, implementers);
        TypeInfo::Interface(intf_type)
    }

    fn for_poi(pool: &AtomPool) -> Self {
        // The way we handle the PoI type is a bit of a hack. We pretend
        // it's an object type, but trying to look up the `s::ObjectType`
        // for it will turn up nothing.
        // See also https://github.com/graphprotocol/graph-node/issues/4873
        TypeInfo::Object(ObjectType::for_poi(pool))
    }

    fn for_aggregation(schema: &Schema, pool: &AtomPool, agg_type: &s::ObjectType) -> Self {
        let agg_type = Aggregation::new(&schema, &pool, agg_type);
        TypeInfo::Aggregation(agg_type)
    }

    fn interfaces(&self) -> impl Iterator<Item = &str> {
        const NO_INTF: [Word; 0] = [];
        let interfaces = match &self {
            TypeInfo::Object(obj_type) => &obj_type.interfaces,
            TypeInfo::Interface(_) | TypeInfo::Aggregation(_) => NO_INTF.as_slice(),
        };
        interfaces.iter().map(|interface| interface.as_str())
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct Field {
    pub name: Word,
    pub field_type: s::Type,
    pub value_type: ValueType,
    derived_from: Option<Word>,
}

impl Field {
    pub fn new(
        schema: &Schema,
        name: &str,
        field_type: &s::Type,
        derived_from: Option<Word>,
    ) -> Self {
        let value_type = Self::scalar_value_type(&schema, field_type);
        Self {
            name: Word::from(name),
            field_type: field_type.clone(),
            value_type,
            derived_from,
        }
    }

    fn scalar_value_type(schema: &Schema, field_type: &s::Type) -> ValueType {
        use s::TypeDefinition as t;
        match field_type {
            s::Type::NamedType(name) => name.parse::<ValueType>().unwrap_or_else(|_| {
                match schema.document.get_named_type(name) {
                    Some(t::Object(obj_type)) => {
                        let id = obj_type.field(&*ID).expect("all object types have an id");
                        Self::scalar_value_type(schema, &id.field_type)
                    }
                    Some(t::Interface(intf)) => {
                        // Validation checks that all implementors of an
                        // interface use the same type for `id`. It is
                        // therefore enough to use the id type of one of
                        // the implementors
                        match schema
                            .types_for_interface
                            .get(&intf.name)
                            .expect("interface type names are known")
                            .first()
                        {
                            None => {
                                // Nothing is implementing this interface; we assume it's of type string
                                // see also: id-type-for-unimplemented-interfaces
                                ValueType::String
                            }
                            Some(obj_type) => {
                                let id = obj_type.field(&*ID).expect("all object types have an id");
                                Self::scalar_value_type(schema, &id.field_type)
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
            s::Type::NonNullType(inner) => Self::scalar_value_type(schema, inner),
            s::Type::ListType(inner) => Self::scalar_value_type(schema, inner),
        }
    }

    pub fn is_list(&self) -> bool {
        self.field_type.is_list()
    }

    pub fn derived_from<'a>(&self, schema: &'a InputSchema) -> Option<&'a Field> {
        let derived_from = self.derived_from.as_ref()?;
        let name = schema
            .pool()
            .lookup(&self.field_type.get_base_type())
            .unwrap();
        schema.field(name, derived_from)
    }

    pub fn is_derived(&self) -> bool {
        self.derived_from.is_some()
    }
}

#[derive(Copy, Clone)]
pub enum ObjectOrInterface<'a> {
    Object(&'a InputSchema, &'a ObjectType),
    Interface(&'a InputSchema, &'a InterfaceType),
}

impl<'a> ObjectOrInterface<'a> {
    pub fn object_types(self) -> Vec<EntityType> {
        let (schema, object_types) = match self {
            ObjectOrInterface::Object(schema, object) => (schema, vec![object]),
            ObjectOrInterface::Interface(schema, interface) => {
                (schema, schema.implementers(interface).collect())
            }
        };
        object_types
            .into_iter()
            .map(|object_type| EntityType::new(schema.cheap_clone(), object_type.name))
            .collect()
    }

    pub fn typename(&self) -> &str {
        let (schema, atom) = self.unpack();
        schema.pool().get(atom).unwrap()
    }

    /// Return the field with the given name. For interfaces, that is the
    /// field with that name declared in the interface, not in the
    /// implementing object types
    pub fn field(&self, name: &str) -> Option<&Field> {
        match self {
            ObjectOrInterface::Object(_, object) => object.field(name),
            ObjectOrInterface::Interface(_, interface) => interface.field(name),
        }
    }

    /// Return the field with the given name. For object types, that's the
    /// field with that name. For interfaces, it's the field with that name
    /// in the first object type that implements the interface; to be
    /// useful, this tacitly assumes that all implementers of an interface
    /// declare that field in the same way
    pub fn implemented_field(&self, name: &str) -> Option<&Field> {
        let object_type = match self {
            ObjectOrInterface::Object(_, object_type) => Some(*object_type),
            ObjectOrInterface::Interface(schema, interface) => {
                schema.implementers(&interface).next()
            }
        };
        object_type.and_then(|object_type| object_type.field(name))
    }

    pub fn is_interface(&self) -> bool {
        match self {
            ObjectOrInterface::Object(_, _) => false,
            ObjectOrInterface::Interface(_, _) => true,
        }
    }

    pub fn derived_from(&self, field_name: &str) -> Option<&str> {
        let field = self.field(field_name)?;
        field.derived_from.as_ref().map(|name| name.as_str())
    }

    pub fn entity_type(&self) -> EntityType {
        let (schema, atom) = self.unpack();
        EntityType::new(schema.cheap_clone(), atom)
    }

    fn unpack(&self) -> (&InputSchema, Atom) {
        match self {
            ObjectOrInterface::Object(schema, object) => (schema, object.name),
            ObjectOrInterface::Interface(schema, interface) => (schema, interface.name),
        }
    }

    pub fn is_aggregation(&self) -> bool {
        match self {
            ObjectOrInterface::Object(_, object) => object.is_aggregation(),
            ObjectOrInterface::Interface(_, _) => false,
        }
    }
}

impl CheapClone for ObjectOrInterface<'_> {}

impl std::fmt::Debug for ObjectOrInterface<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (schema, name) = match self {
            ObjectOrInterface::Object(schema, object) => (schema, object.name),
            ObjectOrInterface::Interface(schema, interface) => (schema, interface.name),
        };
        write!(f, "ObjectOrInterface({})", schema.pool().get(name).unwrap())
    }
}

#[derive(PartialEq, Debug)]
pub struct ObjectType {
    pub name: Atom,
    pub id_type: IdType,
    pub fields: Box<[Field]>,
    pub immutable: bool,
    /// The name of the aggregation to which this object type belongs if it
    /// is part of an aggregation
    aggregation: Option<Atom>,
    pub timeseries: bool,
    interfaces: Box<[Word]>,
    shared_interfaces: Box<[Atom]>,
}

impl ObjectType {
    fn new(
        schema: &Schema,
        pool: &AtomPool,
        object_type: &s::ObjectType,
        shared_interfaces: Box<[Atom]>,
    ) -> Self {
        let id_type = IdType::try_from(object_type).expect("validation caught any issues here");
        let fields = object_type
            .fields
            .iter()
            .map(|field| {
                let derived_from = field.derived_from().map(|name| Word::from(name));
                Field::new(schema, &field.name, &field.field_type, derived_from)
            })
            .collect();
        let interfaces = object_type
            .implements_interfaces
            .iter()
            .map(|intf| Word::from(intf.to_owned()))
            .collect();
        let name = pool
            .lookup(&object_type.name)
            .expect("object type names have been interned");
        let dir = object_type.find_directive("entity").unwrap();
        let timeseries = match dir.argument("timeseries") {
            Some(Value::Boolean(ts)) => *ts,
            None => false,
            _ => unreachable!("validations ensure we don't get here"),
        };
        let immutable = match dir.argument("immutable") {
            Some(Value::Boolean(im)) => *im,
            None => timeseries,
            _ => unreachable!("validations ensure we don't get here"),
        };
        Self {
            name,
            fields,
            id_type,
            immutable,
            aggregation: None,
            timeseries,
            interfaces,
            shared_interfaces,
        }
    }

    fn for_poi(pool: &AtomPool) -> Self {
        let fields = vec![
            Field {
                name: ID.clone(),
                field_type: s::Type::NamedType("ID".to_string()),
                value_type: ValueType::String,
                derived_from: None,
            },
            Field {
                name: Word::from(POI_DIGEST),
                field_type: s::Type::NamedType("String".to_string()),
                value_type: ValueType::String,
                derived_from: None,
            },
        ]
        .into_boxed_slice();
        let name = pool
            .lookup(POI_OBJECT)
            .expect("POI_OBJECT has been interned");
        Self {
            name,
            interfaces: Box::new([]),
            id_type: IdType::String,
            immutable: false,
            aggregation: None,
            timeseries: false,
            fields,
            shared_interfaces: Box::new([]),
        }
    }

    pub fn field(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|field| field.name == name)
    }

    /// Return `true` if this object type is part of an aggregation
    pub fn is_aggregation(&self) -> bool {
        self.aggregation.is_some()
    }
}

#[derive(PartialEq, Debug)]
pub struct InterfaceType {
    pub name: Atom,
    /// For interfaces, the type of the `id` field is the type of the `id`
    /// field of the object types that implement it; validations ensure that
    /// it is the same for all implementers of an interface. If an interface
    /// is not implemented at all, we arbitrarily use `String`
    pub id_type: IdType,
    pub fields: Box<[Field]>,
    implementers: Box<[Atom]>,
}

impl InterfaceType {
    fn new(
        schema: &Schema,
        pool: &AtomPool,
        interface_type: &s::InterfaceType,
        implementers: &[s::ObjectType],
    ) -> Self {
        let fields = interface_type
            .fields
            .iter()
            .map(|field| {
                // It's very unclear what it means for an interface field to
                // be derived; but for legacy reasons, we need to allow it
                // since the API schema does not contain certain filters for
                // derived fields on interfaces that it would for
                // non-derived fields
                let derived_from = field.derived_from().map(|name| Word::from(name));
                Field::new(schema, &field.name, &field.field_type, derived_from)
            })
            .collect();
        let name = pool
            .lookup(&interface_type.name)
            .expect("interface type names have been interned");
        let id_type = implementers
            .first()
            .map(|obj_type| IdType::try_from(obj_type).expect("validation caught any issues here"))
            .unwrap_or(IdType::String);
        let implementers = implementers
            .iter()
            .map(|obj_type| {
                pool.lookup(&obj_type.name)
                    .expect("object type names have been interned")
            })
            .collect();
        Self {
            name,
            id_type,
            fields,
            implementers,
        }
    }

    fn field(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|field| field.name == name)
    }
}

#[derive(Debug, PartialEq)]
struct EnumMap(BTreeMap<String, Arc<BTreeSet<String>>>);

impl EnumMap {
    fn new(schema: &Schema) -> Self {
        let map = schema
            .document
            .get_enum_definitions()
            .iter()
            .map(|enum_type| {
                (
                    enum_type.name.clone(),
                    Arc::new(
                        enum_type
                            .values
                            .iter()
                            .map(|value| value.name.clone())
                            .collect::<BTreeSet<_>>(),
                    ),
                )
            })
            .collect();
        EnumMap(map)
    }

    fn names(&self) -> impl Iterator<Item = &str> {
        self.0.keys().map(|name| name.as_str())
    }

    fn contains_key(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }

    fn values(&self, name: &str) -> Option<Arc<BTreeSet<String>>> {
        self.0.get(name).cloned()
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum AggregateFn {
    Sum,
    Max,
    Min,
    Count,
    First,
    Last,
}

impl FromStr for AggregateFn {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "sum" => Ok(AggregateFn::Sum),
            "max" => Ok(AggregateFn::Max),
            "min" => Ok(AggregateFn::Min),
            "count" => Ok(AggregateFn::Count),
            "first" => Ok(AggregateFn::First),
            "last" => Ok(AggregateFn::Last),
            _ => Err(anyhow!("invalid aggregate function `{}`", s)),
        }
    }
}

impl AggregateFn {
    pub fn has_arg(&self) -> bool {
        use AggregateFn::*;
        match self {
            Sum | Max | Min | First | Last => true,
            Count => false,
        }
    }

    fn as_str(&self) -> &'static str {
        use AggregateFn::*;
        match self {
            Sum => "sum",
            Max => "max",
            Min => "min",
            Count => "count",
            First => "first",
            Last => "last",
        }
    }
}

/// The supported intervals for timeseries in order of decreasing
/// granularity. The intervals must all be divisible by the smallest
/// interval
#[derive(Clone, Copy, PartialEq, Eq, Debug, PartialOrd, Ord, Hash)]
pub enum AggregationInterval {
    Hour,
    Day,
}

impl AggregationInterval {
    pub fn as_str(&self) -> &'static str {
        match self {
            AggregationInterval::Hour => "hour",
            AggregationInterval::Day => "day",
        }
    }

    pub fn as_duration(&self) -> Duration {
        use AggregationInterval::*;
        match self {
            Hour => Duration::from_secs(3600),
            Day => Duration::from_secs(3600 * 24),
        }
    }

    /// Return time ranges for all buckets that intersect `from..to` except
    /// the last one. In other words, return time ranges for all buckets
    /// that overlap `from..to` and end before `to`. The ranges are in
    /// increasing order of the start time
    pub fn buckets(&self, from: BlockTime, to: BlockTime) -> Vec<Range<BlockTime>> {
        let first = from.bucket(self.as_duration());
        let last = to.bucket(self.as_duration());
        (first..last)
            .map(|nr| self.as_duration() * nr as u32)
            .map(|start| {
                let lower = BlockTime::from(start);
                let upper = BlockTime::from(start + self.as_duration());
                lower..upper
            })
            .collect()
    }
}

impl std::fmt::Display for AggregationInterval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[test]
fn buckets() {
    // 2006-07-16 07:40Z
    const START: i64 = 1153035600;
    // 2006-07-16 08:00Z, the start of the next hourly bucket after `START`
    const EIGHT_AM: i64 = 1153036800;

    let start = BlockTime::since_epoch(START, 0);
    let seven_am = BlockTime::since_epoch(START - 40 * 60, 0);
    let eight_am = BlockTime::since_epoch(EIGHT_AM, 0);
    let nine_am = BlockTime::since_epoch(EIGHT_AM + 3600, 0);

    // One hour and two hours after `START`
    let one_hour = BlockTime::since_epoch(START + 3600, 0);
    let two_hour = BlockTime::since_epoch(START + 2 * 3600, 0);

    use AggregationInterval::*;
    assert_eq!(vec![seven_am..eight_am], Hour.buckets(start, eight_am));
    assert_eq!(vec![seven_am..eight_am], Hour.buckets(start, one_hour),);
    assert_eq!(
        vec![seven_am..eight_am, eight_am..nine_am],
        Hour.buckets(start, two_hour),
    );
    assert_eq!(vec![eight_am..nine_am], Hour.buckets(one_hour, two_hour));
    assert_eq!(Vec::<Range<BlockTime>>::new(), Day.buckets(start, two_hour));
}

impl FromStr for AggregationInterval {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "hour" => Ok(AggregationInterval::Hour),
            "day" => Ok(AggregationInterval::Day),
            _ => Err(anyhow!("invalid aggregation interval `{}`", s)),
        }
    }
}

/// The connection between the object type that stores the data points for
/// an aggregation and the type that stores the finalised aggregations.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct AggregationMapping {
    pub interval: AggregationInterval,
    // Index of aggregation type in `type_infos`
    aggregation: usize,
    // Index of the object type for the interval in the aggregation's `obj_types`
    agg_type: usize,
}

impl AggregationMapping {
    pub fn source_type(&self, schema: &InputSchema) -> EntityType {
        let source = self.aggregation(schema).source;
        EntityType::new(schema.cheap_clone(), source)
    }

    pub fn aggregation<'a>(&self, schema: &'a InputSchema) -> &'a Aggregation {
        schema.inner.type_infos[self.aggregation]
            .aggregation()
            .expect("the aggregation source is an object type")
    }

    pub fn agg_type(&self, schema: &InputSchema) -> EntityType {
        let agg_type = self.aggregation(schema).obj_types[self.agg_type].name;
        EntityType::new(schema.cheap_clone(), agg_type)
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct Arg {
    pub name: Word,
    pub value_type: ValueType,
}

impl Arg {
    fn new(name: Word, src_type: &s::ObjectType) -> Self {
        let value_type = src_type
            .field(&name)
            .unwrap()
            .field_type
            .value_type()
            .unwrap();
        Self { name, value_type }
    }
}
#[derive(PartialEq, Debug)]
pub struct Aggregate {
    pub name: Word,
    pub func: AggregateFn,
    pub arg: Option<Arg>,
    pub field_type: s::Type,
    pub value_type: ValueType,
}

impl Aggregate {
    fn new(
        _schema: &Schema,
        src_type: &s::ObjectType,
        name: &str,
        field_type: &s::Type,
        dir: &s::Directive,
    ) -> Self {
        let func = dir
            .argument("fn")
            .unwrap()
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        let arg = dir
            .argument("arg")
            .map(|arg| Word::from(arg.as_str().unwrap()))
            .map(|arg| Arg::new(arg, src_type));
        Aggregate {
            name: Word::from(name),
            func,
            arg,
            field_type: field_type.clone(),
            value_type: field_type.get_base_type().parse().unwrap(),
        }
    }

    /// The field needed for the finalised aggregation for hourly/daily
    /// values
    fn as_agg_field(&self) -> Field {
        Field {
            name: self.name.clone(),
            field_type: self.field_type.clone(),
            value_type: self.value_type,
            derived_from: None,
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct Aggregation {
    pub name: Atom,
    pub id_type: IdType,
    pub intervals: Box<[AggregationInterval]>,
    pub source: Atom,
    /// The non-aggregation fields of the time series
    pub fields: Box<[Field]>,
    pub aggregates: Box<[Aggregate]>,
    /// The object types for the aggregated data, one for each interval, in
    /// the same order as `intervals`
    obj_types: Box<[ObjectType]>,
}

impl Aggregation {
    pub fn new(schema: &Schema, pool: &AtomPool, agg_type: &s::ObjectType) -> Self {
        let name = pool.lookup(&agg_type.name).unwrap();
        let id_type = IdType::try_from(agg_type).expect("validation caught any issues here");
        let intervals = Self::parse_intervals(agg_type).into_boxed_slice();
        let source = agg_type
            .find_directive(kw::AGGREGATION)
            .unwrap()
            .argument("source")
            .unwrap()
            .as_str()
            .unwrap();
        let src_type = schema.document.get_object_type_definition(source).unwrap();
        let source = pool.lookup(source).unwrap();
        let fields: Box<[_]> = agg_type
            .fields
            .iter()
            .filter(|field| field.find_directive(kw::AGGREGATE).is_none())
            .map(|field| Field::new(schema, &field.name, &field.field_type, None))
            .collect();
        let aggregates: Box<[_]> = agg_type
            .fields
            .iter()
            .filter_map(|field| field.find_directive(kw::AGGREGATE).map(|dir| (field, dir)))
            .map(|(field, dir)| {
                Aggregate::new(schema, src_type, &field.name, &field.field_type, dir)
            })
            .collect();

        let obj_types = intervals
            .iter()
            .map(|interval| {
                let name = format!("{}_{}", &agg_type.name, interval.as_str());
                let name = pool.lookup(&name).unwrap();
                ObjectType {
                    name,
                    id_type,
                    fields: fields
                        .iter()
                        .cloned()
                        .chain(aggregates.iter().map(Aggregate::as_agg_field))
                        .collect(),
                    immutable: true,
                    aggregation: Some(name),
                    timeseries: false,
                    interfaces: Box::new([]),
                    shared_interfaces: Box::new([]),
                }
            })
            .collect();
        Self {
            name,
            id_type,
            intervals,
            source,
            fields,
            aggregates,
            obj_types,
        }
    }

    fn parse_intervals(agg_type: &s::ObjectType) -> Vec<AggregationInterval> {
        let dir = agg_type.find_directive(kw::AGGREGATION).unwrap();
        let mut intervals: Vec<_> = dir
            .argument(kw::INTERVALS)
            .unwrap()
            .as_list()
            .unwrap()
            .iter()
            .map(|interval| interval.as_str().unwrap().parse().unwrap())
            .collect();
        intervals.sort();
        intervals.dedup();
        intervals
    }

    fn has_object_type(&self, atom: Atom) -> bool {
        self.obj_types.iter().any(|obj_type| obj_type.name == atom)
    }

    fn aggregated_type(&self, atom: Atom) -> Option<&ObjectType> {
        self.obj_types.iter().find(|obj_type| obj_type.name == atom)
    }

    pub fn dimensions(&self) -> impl Iterator<Item = &Field> {
        self.fields
            .into_iter()
            .filter(|field| &field.name != &*ID && field.name != kw::TIMESTAMP)
    }

    fn object_type(&self, interval: AggregationInterval) -> Option<&ObjectType> {
        let pos = self.intervals.iter().position(|i| *i == interval)?;
        Some(&self.obj_types[pos])
    }

    fn field(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|field| field.name == name)
    }
}

#[derive(Debug, PartialEq)]
pub struct Inner {
    schema: Schema,
    /// A list of all the object and interface types in the `schema` with
    /// some important information extracted from the schema. The list is
    /// sorted by the name atom (not the string name) of the types
    type_infos: Box<[TypeInfo]>,
    enum_map: EnumMap,
    pool: Arc<AtomPool>,
    /// A list of all timeseries types by interval
    agg_mappings: Box<[AggregationMapping]>,
}

impl CheapClone for InputSchema {
    fn cheap_clone(&self) -> Self {
        InputSchema {
            inner: self.inner.cheap_clone(),
        }
    }
}

impl InputSchema {
    /// A convenience function for creating an `InputSchema` from the string
    /// representation of the subgraph's GraphQL schema `raw` and its
    /// deployment hash `id`. The returned schema is fully validated.
    pub fn parse(spec_version: &Version, raw: &str, id: DeploymentHash) -> Result<Self, Error> {
        fn agg_mappings(ts_types: &[TypeInfo]) -> Box<[AggregationMapping]> {
            let mut mappings: Vec<_> = ts_types
                .iter()
                .enumerate()
                .filter_map(|(idx, ti)| ti.aggregation().map(|agg_type| (idx, agg_type)))
                .map(|(aggregation, agg_type)| {
                    agg_type
                        .intervals
                        .iter()
                        .enumerate()
                        .map(move |(agg_type, interval)| AggregationMapping {
                            interval: *interval,
                            aggregation,
                            agg_type,
                        })
                })
                .flatten()
                .collect();
            mappings.sort();
            mappings.into_boxed_slice()
        }

        let schema = Schema::parse(raw, id.clone())?;
        validations::validate(spec_version, &schema).map_err(|errors| {
            anyhow!(
                "Validation errors in subgraph `{}`:\n{}",
                id,
                errors
                    .into_iter()
                    .enumerate()
                    .map(|(n, e)| format!("  ({}) - {}", n + 1, e))
                    .collect::<Vec<_>>()
                    .join("\n")
            )
        })?;

        let pool = Arc::new(atom_pool(&schema.document));

        // There are a lot of unwraps in this code; they are all safe
        // because the validations check for all the ways in which the
        // unwrapping could go wrong. It would be better to rewrite all this
        // code so that validation and construction of the internal data
        // structures happen in one pass which would eliminate the need for
        // unwrapping
        let obj_types = schema
            .document
            .get_object_type_definitions()
            .into_iter()
            .filter(|obj_type| obj_type.find_directive("entity").is_some())
            .map(|obj_type| TypeInfo::for_object(&schema, &pool, obj_type));
        let intf_types = schema
            .document
            .get_interface_type_definitions()
            .into_iter()
            .map(|intf_type| TypeInfo::for_interface(&schema, &pool, intf_type));
        let agg_types = schema
            .document
            .get_object_type_definitions()
            .into_iter()
            .filter(|obj_type| obj_type.find_directive(kw::AGGREGATION).is_some())
            .map(|agg_type| TypeInfo::for_aggregation(&schema, &pool, agg_type));
        let mut type_infos: Vec<_> = obj_types
            .chain(intf_types)
            .chain(agg_types)
            .chain(vec![TypeInfo::for_poi(&pool)])
            .collect();
        type_infos.sort_by_key(|ti| ti.name());
        let type_infos = type_infos.into_boxed_slice();

        let enum_map = EnumMap::new(&schema);

        let agg_mappings = agg_mappings(&type_infos);

        Ok(Self {
            inner: Arc::new(Inner {
                schema,
                type_infos,
                enum_map,
                pool,
                agg_mappings,
            }),
        })
    }

    /// Parse with the latest spec version
    pub fn parse_latest(raw: &str, id: DeploymentHash) -> Result<Self, Error> {
        use crate::data::subgraph::LATEST_VERSION;

        Self::parse(LATEST_VERSION, raw, id)
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
        Self::parse_latest(document, hash).unwrap()
    }

    pub fn schema(&self) -> &Schema {
        &self.inner.schema
    }

    /// Generate the `ApiSchema` for use with GraphQL queries for this
    /// `InputSchema`
    pub fn api_schema(&self) -> Result<ApiSchema, anyhow::Error> {
        let mut schema = self.inner.schema.clone();
        schema.document = api_schema(self)?;
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
    pub fn get_field_related(
        &self,
        key: &LoadRelatedRequest,
    ) -> Result<(EntityType, &Field), Error> {
        fn field_err(key: &LoadRelatedRequest, err: &str) -> Error {
            anyhow!(
                "Entity {}[{}]: {err} `{}`",
                key.entity_type,
                key.entity_id,
                key.entity_field,
            )
        }

        let field = self
            .inner
            .schema
            .document
            .get_object_type_definition(key.entity_type.typename())
            .ok_or_else(|| field_err(key, "unknown entity type"))?
            .field(&key.entity_field)
            .ok_or_else(|| field_err(key, "unknown field"))?;
        if !field.is_derived() {
            return Err(field_err(key, "field is not derived"));
        }

        let derived_from = field.find_directive("derivedFrom").unwrap();
        let entity_type = self.entity_type(field.field_type.get_base_type())?;
        let field_name = derived_from.argument("field").unwrap();

        let field = self
            .object_type(entity_type.atom)?
            .field(field_name.as_str().unwrap())
            .ok_or_else(|| field_err(key, "unknown field"))?;

        Ok((entity_type, field))
    }

    /// Return the `TypeInfo` for the type with name `atom`. For object and
    /// interface types, `atom` must be the name of the type. For
    /// aggregations, `atom` must be either the name of the aggregation or
    /// the name of one of the object types that are part of the
    /// aggregation.
    fn type_info(&self, atom: Atom) -> Result<&TypeInfo, Error> {
        for ti in self.inner.type_infos.iter() {
            match ti {
                TypeInfo::Object(obj_type) => {
                    if obj_type.name == atom {
                        return Ok(ti);
                    }
                }
                TypeInfo::Interface(intf_type) => {
                    if intf_type.name == atom {
                        return Ok(ti);
                    }
                }
                TypeInfo::Aggregation(agg_type) => {
                    if agg_type.name == atom || agg_type.has_object_type(atom) {
                        return Ok(ti);
                    }
                }
            }
        }

        let err = match self.inner.pool.get(atom) {
            Some(name) => anyhow!(
                "internal error: entity type `{}` does not exist in {}",
                name,
                self.inner.schema.id
            ),
            None => anyhow!(
                "Invalid atom {atom:?} for type_info lookup in {} (atom is probably from a different pool)",
                self.inner.schema.id
            ),
        };
        Err(err)
    }

    pub(in crate::schema) fn id_type(&self, entity_type: Atom) -> Result<store::IdType, Error> {
        let type_info = self.type_info(entity_type)?;

        type_info.id_type().ok_or_else(|| {
            let name = self.inner.pool.get(entity_type).unwrap();
            anyhow!("Entity type `{}` does not have an `id` field", name)
        })
    }

    /// Check if `entity_type` is an immutable object type
    pub(in crate::schema) fn is_immutable(&self, entity_type: Atom) -> bool {
        self.type_info(entity_type)
            .ok()
            .map(|ti| ti.is_immutable())
            .unwrap_or(false)
    }

    /// Return true if `type_name` is the name of an object or interface type
    pub fn is_reference(&self, type_name: &str) -> bool {
        self.inner
            .pool
            .lookup(type_name)
            .and_then(|atom| {
                self.type_info(atom)
                    .ok()
                    .map(|ti| ti.is_object() || ti.is_interface())
            })
            .unwrap_or(false)
    }

    /// Return a list of the interfaces that `entity_type` implements
    pub fn interfaces(&self, entity_type: Atom) -> impl Iterator<Item = &InterfaceType> {
        let obj_type = self.type_info(entity_type).unwrap();
        obj_type.interfaces().map(|intf| {
            let atom = self.inner.pool.lookup(intf).unwrap();
            match self.type_info(atom).unwrap() {
                TypeInfo::Interface(ref intf_type) => intf_type,
                _ => unreachable!("expected `{intf}` to refer to an interface"),
            }
        })
    }

    fn implementers<'a>(
        &'a self,
        interface: &'a InterfaceType,
    ) -> impl Iterator<Item = &'a ObjectType> {
        interface
            .implementers
            .iter()
            .map(|atom| self.object_type(*atom).unwrap())
    }

    /// Return a list of all entity types that implement one of the
    /// interfaces that `entity_type` implements
    pub(in crate::schema) fn share_interfaces(
        &self,
        entity_type: Atom,
    ) -> Result<Vec<EntityType>, Error> {
        let obj_type = match &self.type_info(entity_type)? {
            TypeInfo::Object(obj_type) => obj_type,
            TypeInfo::Aggregation(_) => {
                /* aggregations don't implement interfaces */
                return Ok(Vec::new());
            }
            _ => bail!(
                "expected `{}` to refer to an object type",
                self.inner.pool.get(entity_type).unwrap_or("<unknown>")
            ),
        };
        Ok(obj_type
            .shared_interfaces
            .into_iter()
            .map(|atom| EntityType::new(self.cheap_clone(), *atom))
            .collect())
    }

    /// Return the object type with name `entity_type`. It is an error to
    /// call this if `entity_type` refers to an interface or an aggregation
    /// as they don't have an underlying type that stores data directly
    pub(in crate::schema) fn object_type(&self, entity_type: Atom) -> Result<&ObjectType, Error> {
        let ti = self.type_info(entity_type)?;
        match ti {
            TypeInfo::Object(obj_type) => Ok(obj_type),
            TypeInfo::Interface(_) => {
                let name = self.inner.pool.get(entity_type).unwrap();
                bail!(
                    "expected `{}` to refer to an object type but it's an interface",
                    name
                )
            }
            TypeInfo::Aggregation(agg_type) => {
                agg_type.obj_types
                    .iter()
                    .find(|obj_type| obj_type.name == entity_type)
                    .ok_or_else(|| anyhow!("type_info returns an aggregation only when it has the requested object type"))
            }
        }
    }

    fn types_with_kind(&self, kind: TypeKind) -> impl Iterator<Item = (&str, &TypeInfo)> {
        self.inner
            .type_infos
            .iter()
            .filter(move |ti| ti.kind() == kind)
            .map(|ti| {
                let name = self.inner.pool.get(ti.name()).unwrap();
                (name, ti)
            })
    }

    /// Return a list of all object types, i.e., types defined with an
    /// `@entity` annotation. This does not include the type for the PoI
    pub(in crate::schema) fn object_types(&self) -> impl Iterator<Item = (&str, &ObjectType)> {
        self.types_with_kind(TypeKind::Object)
            .filter(|(name, _)| {
                // Filter out the POI object type
                name != &POI_OBJECT
            })
            .filter_map(|(name, ti)| ti.object_type().map(|obj| (name, obj)))
    }

    pub(in crate::schema) fn interface_types(
        &self,
    ) -> impl Iterator<Item = (&str, &InterfaceType)> {
        self.types_with_kind(TypeKind::Interface)
            .filter_map(|(name, ti)| ti.interface_type().map(|intf| (name, intf)))
    }

    pub(in crate::schema) fn aggregation_types(
        &self,
    ) -> impl Iterator<Item = (&str, &Aggregation)> {
        self.types_with_kind(TypeKind::Aggregation)
            .filter_map(|(name, ti)| ti.aggregation().map(|intf| (name, intf)))
    }

    /// Return a list of the names of all enum types
    pub fn enum_types(&self) -> impl Iterator<Item = &str> {
        self.inner.enum_map.names()
    }

    /// Check if `name` is the name of an enum type
    pub fn is_enum_type(&self, name: &str) -> bool {
        self.inner.enum_map.contains_key(name)
    }

    /// Return a list of the values of the enum type `name`
    pub fn enum_values(&self, name: &str) -> Option<Arc<BTreeSet<String>>> {
        self.inner.enum_map.values(name)
    }

    /// Return a list of the entity types defined in the schema, i.e., the
    /// types that have a `@entity` annotation. This does not include the
    /// type for the PoI
    pub fn entity_types(&self) -> Vec<EntityType> {
        self.inner
            .type_infos
            .iter()
            .filter_map(|ti| match ti {
                TypeInfo::Object(obj_type) => Some(obj_type),
                TypeInfo::Interface(_) | TypeInfo::Aggregation(_) => None,
            })
            .map(|obj_type| EntityType::new(self.cheap_clone(), obj_type.name))
            .filter(|entity_type| !entity_type.is_poi())
            .collect()
    }

    /// Return a list of all the entity types for aggregations; these are
    /// types derived from types with `@aggregation` annotations
    pub fn ts_entity_types(&self) -> Vec<EntityType> {
        self.inner
            .type_infos
            .iter()
            .filter_map(TypeInfo::aggregation)
            .flat_map(|ts_type| ts_type.obj_types.iter())
            .map(|obj_type| EntityType::new(self.cheap_clone(), obj_type.name))
            .collect()
    }

    /// Return a list of all the aggregation mappings for this schema. The
    /// `interval` of the aggregations are non-decreasing
    pub fn agg_mappings(&self) -> impl Iterator<Item = &AggregationMapping> {
        self.inner.agg_mappings.iter()
    }

    pub fn has_aggregations(&self) -> bool {
        self.inner
            .type_infos
            .iter()
            .any(|ti| matches!(ti, TypeInfo::Aggregation(_)))
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
    pub(in crate::schema) fn has_field(&self, entity_type: Atom, name: Atom) -> bool {
        let name = self.inner.pool.get(name).unwrap();
        self.type_info(entity_type)
            .map(|ti| ti.is_object() && ti.fields().iter().any(|field| field.name == name))
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

    pub fn poi_block_time(&self) -> Word {
        Word::from(POI_BLOCK_TIME)
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
        let atom = self.inner.pool.lookup(name).ok_or_else(|| {
            anyhow!("internal error: unknown name {name} when looking up entity type")
        })?;

        // This is a little subtle: we use `type_info` to check that `atom`
        // is a known type, but use `atom` and not the name of the
        // `TypeInfo` in the returned entity type so that passing the name
        // of the object type from an aggregation, say `Stats_hour`
        // references that object type, and not the aggregation.
        self.type_info(atom)
            .map(|_| EntityType::new(self.cheap_clone(), atom))
    }

    pub fn has_field_with_name(&self, entity_type: &EntityType, field: &str) -> bool {
        let field = self.inner.pool.lookup(field);

        match field {
            Some(field) => self.has_field(entity_type.atom, field),
            None => false,
        }
    }

    /// For the `name` of a type declared in the input schema, return
    /// whether it is a normal object, declared with `@entity`, an
    /// interface, or an aggregation. If there is no type `name`, or it is
    /// not one of these three kinds, return `None`
    pub fn kind_of_declared_type(&self, name: &str) -> Option<TypeKind> {
        let name = self.inner.pool.lookup(name)?;
        self.inner.type_infos.iter().find_map(|ti| {
            if ti.name() == name {
                Some(ti.kind())
            } else {
                None
            }
        })
    }

    /// Return `true` if `atom` is an object type, i.e., a type that is
    /// declared with an `@entity` directive in the input schema. This
    /// specifically excludes interfaces and aggregations.
    pub(crate) fn is_object_type(&self, atom: Atom) -> bool {
        self.inner.type_infos.iter().any(|ti| match ti {
            TypeInfo::Object(obj_type) => obj_type.name == atom,
            _ => false,
        })
    }

    pub(crate) fn typename(&self, atom: Atom) -> &str {
        let name = self.type_info(atom).unwrap().name();
        self.inner.pool.get(name).unwrap()
    }

    pub(in crate::schema) fn field(&self, type_name: Atom, name: &str) -> Option<&Field> {
        let ti = self.type_info(type_name).ok()?;
        match ti {
            TypeInfo::Object(obj_type) => obj_type.field(name),
            TypeInfo::Aggregation(agg_type) => {
                if agg_type.name == type_name {
                    agg_type.field(name)
                } else {
                    agg_type
                        .aggregated_type(type_name)
                        .and_then(|obj_type| obj_type.field(name))
                }
            }
            TypeInfo::Interface(intf_type) => intf_type.field(name),
        }
    }

    /// Resolve the given name and interval into an object or interface
    /// type. If `name` refers to an object or interface type, return that
    /// regardless of the value of `interval`. If `name` refers to an
    /// aggregation, return the object type of that aggregation for the
    /// given `interval`
    pub fn object_or_interface(
        &self,
        name: &str,
        interval: Option<AggregationInterval>,
    ) -> Option<ObjectOrInterface<'_>> {
        let name = self.inner.pool.lookup(name)?;
        let ti = self.inner.type_infos.iter().find(|ti| ti.name() == name)?;
        match (ti, interval) {
            (TypeInfo::Object(obj_type), _) => Some(ObjectOrInterface::Object(self, obj_type)),
            (TypeInfo::Interface(intf_type), _) => {
                Some(ObjectOrInterface::Interface(self, intf_type))
            }
            (TypeInfo::Aggregation(agg_type), Some(interval)) => agg_type
                .object_type(interval)
                .map(|object_type| ObjectOrInterface::Object(self, object_type)),
            (TypeInfo::Aggregation(_), None) => None,
        }
    }

    /// Return an `EntityType` that either references the object type `name`
    /// or, if `name` references an aggregation, return the object type of
    /// that aggregation for the given `interval`
    pub fn object_or_aggregation(
        &self,
        name: &str,
        interval: Option<AggregationInterval>,
    ) -> Option<EntityType> {
        let name = self.inner.pool.lookup(name)?;
        let ti = self.inner.type_infos.iter().find(|ti| ti.name() == name)?;
        let obj_type = match (ti, interval) {
            (TypeInfo::Object(obj_type), _) => Some(obj_type),
            (TypeInfo::Interface(_), _) => None,
            (TypeInfo::Aggregation(agg_type), Some(interval)) => agg_type.object_type(interval),
            (TypeInfo::Aggregation(_), None) => None,
        }?;
        Some(EntityType::new(self.cheap_clone(), obj_type.name))
    }
}

/// Create a new pool that contains the names of all the types defined
/// in the document and the names of all their fields
fn atom_pool(document: &s::Document) -> AtomPool {
    let mut pool = AtomPool::new();

    pool.intern(&*ID);
    // Name and attributes of PoI entity type
    pool.intern(POI_OBJECT);
    pool.intern(POI_DIGEST);
    pool.intern(POI_BLOCK_TIME);

    for definition in &document.definitions {
        match definition {
            s::Definition::TypeDefinition(typedef) => match typedef {
                s::TypeDefinition::Object(t) => {
                    static NO_VALUE: Vec<Value> = Vec::new();

                    pool.intern(&t.name);

                    // For timeseries, also intern the names of the
                    // additional types we generate.
                    let intervals = t
                        .find_directive(kw::AGGREGATION)
                        .and_then(|dir| dir.argument(kw::INTERVALS))
                        .and_then(Value::as_list)
                        .unwrap_or(&NO_VALUE);
                    for interval in intervals {
                        if let Some(interval) = interval.as_str() {
                            pool.intern(&format!("{}_{}", t.name, interval));
                        }
                    }
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

/// Validations for an `InputSchema`.
mod validations {
    use std::{collections::HashSet, str::FromStr};

    use itertools::Itertools;
    use semver::Version;

    use crate::{
        data::{
            graphql::{
                ext::{DirectiveFinder, FieldExt},
                DirectiveExt, DocumentExt, ObjectTypeExt, TypeExt, ValueExt,
            },
            store::{IdType, ValueType, ID},
            subgraph::SPEC_VERSION_1_1_0,
        },
        prelude::s,
        schema::{
            input_schema::{kw, AggregateFn, AggregationInterval},
            FulltextAlgorithm, FulltextLanguage, Schema as BaseSchema, SchemaValidationError,
            SchemaValidationError as Err, Strings, SCHEMA_TYPE_NAME,
        },
    };

    /// Helper struct for validations
    struct Schema<'a> {
        #[allow(dead_code)]
        spec_version: &'a Version,
        schema: &'a BaseSchema,
        subgraph_schema_type: Option<&'a s::ObjectType>,
        // All entity types, excluding the subgraph schema type
        entity_types: Vec<&'a s::ObjectType>,
        aggregations: Vec<&'a s::ObjectType>,
    }

    pub(super) fn validate(
        spec_version: &Version,
        schema: &BaseSchema,
    ) -> Result<(), Vec<SchemaValidationError>> {
        let schema = Schema::new(spec_version, schema);

        let mut errors: Vec<SchemaValidationError> = [
            schema.validate_no_extra_types(),
            schema.validate_derived_from(),
            schema.validate_schema_type_has_no_fields(),
            schema.validate_directives_on_schema_type(),
            schema.validate_reserved_types_usage(),
            schema.validate_interface_id_type(),
        ]
        .into_iter()
        .filter(Result::is_err)
        // Safe unwrap due to the filter above
        .map(Result::unwrap_err)
        .collect();

        errors.append(&mut schema.validate_entity_directives());
        errors.append(&mut schema.validate_entity_type_ids());
        errors.append(&mut schema.validate_fields());
        errors.append(&mut schema.validate_fulltext_directives());
        errors.append(&mut schema.validate_aggregations());
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    impl<'a> Schema<'a> {
        fn new(spec_version: &'a Version, schema: &'a BaseSchema) -> Self {
            let subgraph_schema_type = schema.subgraph_schema_object_type();
            let mut entity_types = schema.document.get_object_type_definitions();
            entity_types.retain(|obj_type| obj_type.find_directive(kw::ENTITY).is_some());
            let mut aggregations = schema.document.get_object_type_definitions();
            aggregations.retain(|obj_type| obj_type.find_directive(kw::AGGREGATION).is_some());

            Schema {
                spec_version,
                schema,
                subgraph_schema_type,
                entity_types,
                aggregations,
            }
        }

        fn validate_schema_type_has_no_fields(&self) -> Result<(), SchemaValidationError> {
            match self.subgraph_schema_type.and_then(|subgraph_schema_type| {
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
            match self.subgraph_schema_type.and_then(|subgraph_schema_type| {
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
            self.subgraph_schema_type
                .map_or(vec![], |subgraph_schema_type| {
                    subgraph_schema_type
                        .directives
                        .iter()
                        .filter(|directives| directives.name.eq("fulltext"))
                        .fold(vec![], |mut errors, fulltext| {
                            errors.extend(self.validate_fulltext_directive_name(fulltext));
                            errors.extend(self.validate_fulltext_directive_language(fulltext));
                            errors.extend(self.validate_fulltext_directive_algorithm(fulltext));
                            errors.extend(self.validate_fulltext_directive_includes(fulltext));
                            errors
                        })
                })
        }

        fn validate_fulltext_directive_name(
            &self,
            fulltext: &s::Directive,
        ) -> Vec<SchemaValidationError> {
            let name = match fulltext.argument("name") {
                Some(s::Value::String(name)) => name,
                _ => return vec![SchemaValidationError::FulltextNameUndefined],
            };

            // Validate that the fulltext field doesn't collide with any top-level Query fields
            // generated for entity types. The field name conversions should always align with those used
            // to create the field names in `graphql::schema::api::query_fields_for_type()`.
            if self.entity_types.iter().any(|typ| {
                typ.fields.iter().any(|field| {
                    let (singular, plural) = field.camel_cased_names();
                    name == &singular || name == &plural || field.name.eq(name)
                })
            }) {
                return vec![SchemaValidationError::FulltextNameCollision(
                    name.to_string(),
                )];
            }

            // Validate that each fulltext directive has a distinct name
            if self
                .subgraph_schema_type
                .unwrap()
                .directives
                .iter()
                .filter(|directive| directive.name.eq("fulltext"))
                .filter_map(|fulltext| {
                    // Collect all @fulltext directives with the same name
                    match fulltext.argument("name") {
                        Some(s::Value::String(n)) if name.eq(n) => Some(n.as_str()),
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
            fulltext: &s::Directive,
        ) -> Vec<SchemaValidationError> {
            let language = match fulltext.argument("language") {
                Some(s::Value::Enum(language)) => language,
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
            fulltext: &s::Directive,
        ) -> Vec<SchemaValidationError> {
            let algorithm = match fulltext.argument("algorithm") {
                Some(s::Value::Enum(algorithm)) => algorithm,
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
            fulltext: &s::Directive,
        ) -> Vec<SchemaValidationError> {
            // Validate that each entity in fulltext.include exists
            let includes = match fulltext.argument("include") {
                Some(s::Value::List(includes)) if !includes.is_empty() => includes,
                _ => return vec![SchemaValidationError::FulltextIncludeUndefined],
            };

            for include in includes {
                match include.as_object() {
                    None => return vec![SchemaValidationError::FulltextIncludeObjectMissing],
                    Some(include_entity) => {
                        let (entity, fields) =
                        match (include_entity.get("entity"), include_entity.get("fields")) {
                            (Some(s::Value::String(entity)), Some(s::Value::List(fields))) => {
                                (entity, fields)
                            }
                            _ => return vec![SchemaValidationError::FulltextIncludeEntityMissingOrIncorrectAttributes],
                        };

                        // Validate the included entity type is one of the local types
                        let entity_type = match self
                            .entity_types
                            .iter()
                            .cloned()
                            .find(|typ| typ.name[..].eq(entity))
                        {
                            None => {
                                return vec![SchemaValidationError::FulltextIncludedEntityNotFound]
                            }
                            Some(t) => t,
                        };

                        for field_value in fields {
                            let field_name = match field_value {
                            s::Value::Object(field_map) => match field_map.get("name") {
                                Some(s::Value::String(name)) => name,
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
            let local_types = self.schema.document.get_object_and_interface_type_fields();
            let local_enums = self
                .schema
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

        /// The `@entity` directive accepts two flags `immutable` and
        /// `timeseries`, and when `timeseries` is `true`, `immutable` can
        /// not be `false`.
        ///
        /// For timeseries, also check that there is a `timestamp` field of
        /// type `Int8` and that the `id` field has type `Int8`
        fn validate_entity_directives(&self) -> Vec<SchemaValidationError> {
            fn id_type_is_int8(object_type: &s::ObjectType) -> Option<SchemaValidationError> {
                let field = match object_type.field(&*ID) {
                    Some(field) => field,
                    None => {
                        return Some(Err::IdFieldMissing(object_type.name.to_owned()));
                    }
                };

                match field.field_type.value_type() {
                    Ok(ValueType::Int8) => None,
                    Ok(_) | Err(_) => Some(Err::IllegalIdType(format!(
                        "Timeseries `{}` must have an `id` field of type `Int8`",
                        object_type.name
                    ))),
                }
            }

            fn bool_arg(
                dir: &s::Directive,
                name: &str,
            ) -> Result<Option<bool>, SchemaValidationError> {
                let arg = dir.argument(name);
                match arg {
                    Some(s::Value::Boolean(b)) => Ok(Some(*b)),
                    Some(_) => Err(SchemaValidationError::EntityDirectiveNonBooleanArgValue(
                        name.to_owned(),
                    )),
                    None => Ok(None),
                }
            }

            self.entity_types
                .iter()
                .filter_map(|object_type| {
                    let dir = object_type.find_directive(kw::ENTITY).unwrap();
                    let timeseries = match bool_arg(dir, kw::TIMESERIES) {
                        Ok(b) => b.unwrap_or(false),
                        Err(e) => return Some(e),
                    };
                    let immutable = match bool_arg(dir, kw::IMMUTABLE) {
                        Ok(b) => b.unwrap_or(timeseries),
                        Err(e) => return Some(e),
                    };
                    if timeseries {
                        if !immutable {
                            Some(SchemaValidationError::MutableTimeseries(
                                object_type.name.clone(),
                            ))
                        } else {
                            id_type_is_int8(object_type)
                                .or_else(|| Self::valid_timestamp_field(object_type))
                        }
                    } else {
                        None
                    }
                })
                .collect()
        }

        /// 1. All object types besides `_Schema_` must have an id field
        /// 2. The id field must be recognized by IdType
        fn validate_entity_type_ids(&self) -> Vec<SchemaValidationError> {
            self.entity_types
                .iter()
                .fold(vec![], |mut errors, object_type| {
                    match object_type.field(&*ID) {
                        None => errors.push(SchemaValidationError::IdFieldMissing(
                            object_type.name.clone(),
                        )),
                        Some(_) => match IdType::try_from(*object_type) {
                            Ok(IdType::Int8) => {
                                if self.spec_version < &SPEC_VERSION_1_1_0 {
                                    errors.push(SchemaValidationError::IdTypeInt8NotSupported(
                                        self.spec_version.clone(),
                                    ))
                                }
                            }
                            Ok(IdType::String | IdType::Bytes) => { /* ok at any spec version */ }
                            Err(e) => {
                                errors.push(SchemaValidationError::IllegalIdType(e.to_string()))
                            }
                        },
                    }
                    errors
                })
        }

        /// Checks if the schema is using types that are reserved
        /// by `graph-node`
        fn validate_reserved_types_usage(&self) -> Result<(), SchemaValidationError> {
            let document = &self.schema.document;
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

        fn validate_no_extra_types(&self) -> Result<(), SchemaValidationError> {
            let extra_type = |t: &&s::ObjectType| {
                t.find_directive(kw::ENTITY).is_none()
                    && t.find_directive(kw::AGGREGATION).is_none()
                    && !t.name.eq(SCHEMA_TYPE_NAME)
            };
            let types_without_entity_directive = self
                .schema
                .document
                .get_object_type_definitions()
                .into_iter()
                .filter(extra_type)
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
                object_type: &s::ObjectType,
                field_name: &str,
                reason: &str,
            ) -> SchemaValidationError {
                SchemaValidationError::InvalidDerivedFrom(
                    object_type.name.clone(),
                    field_name.to_owned(),
                    reason.to_owned(),
                )
            }

            let object_and_interface_type_fields =
                self.schema.document.get_object_and_interface_type_fields();

            // Iterate over all derived fields in all entity types; include the
            // interface types that the entity with the `@derivedFrom` implements
            // and the `field` argument of @derivedFrom directive
            for (object_type, interface_types, field, target_field) in self
                .entity_types
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
                                    self.schema
                                        .document
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
                    s::Value::String(s) => s,
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
                    && &target_field.name != ID.as_str()
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

        fn validate_interface_id_type(&self) -> Result<(), SchemaValidationError> {
            for (intf, obj_types) in &self.schema.types_for_interface {
                let id_types: HashSet<&str> = HashSet::from_iter(
                    obj_types
                        .iter()
                        .filter_map(|obj_type| obj_type.field(&*ID))
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

        fn validate_aggregations(&self) -> Vec<SchemaValidationError> {
            /// Aggregations must have an `id` field with the same type as
            /// the id field for the source type
            fn valid_id_field(
                agg_type: &s::ObjectType,
                src_id_type: IdType,
                errors: &mut Vec<Err>,
            ) {
                match IdType::try_from(agg_type) {
                    Ok(agg_id_type) => {
                        if agg_id_type != src_id_type {
                            errors.push(Err::IllegalIdType(format!(
                                "The type of the `id` field for aggregation {} must be {}, the same as in the source, but is {}",
                                agg_type.name, src_id_type, agg_id_type
                            )))
                        }
                    }
                    Err(e) => errors.push(Err::IllegalIdType(e.to_string())),
                }
            }

            fn no_derived_fields(agg_type: &s::ObjectType, errors: &mut Vec<Err>) {
                for field in &agg_type.fields {
                    if field.find_directive("derivedFrom").is_some() {
                        errors.push(Err::AggregationDerivedField(
                            agg_type.name.to_owned(),
                            field.name.to_owned(),
                        ));
                    }
                }
            }

            fn aggregate_fields_are_numbers(agg_type: &s::ObjectType, errors: &mut Vec<Err>) {
                let errs = agg_type
                    .fields
                    .iter()
                    .filter(|field| field.find_directive(kw::AGGREGATE).is_some())
                    .map(|field| match field.field_type.value_type() {
                        Ok(vt) => {
                            if vt.is_numeric() {
                                Ok(())
                            } else {
                                Err(Err::NonNumericAggregate(
                                    agg_type.name.to_owned(),
                                    field.name.to_owned(),
                                ))
                            }
                        }
                        Err(_) => Err(Err::FieldTypeUnknown(
                            agg_type.name.to_owned(),
                            field.name.to_owned(),
                            field.field_type.get_base_type().to_owned(),
                        )),
                    })
                    .filter_map(|err| err.err());
                errors.extend(errs);
            }

            /// * `source` is an existing timeseries type
            /// * all non-aggregate fields are also fields on the `source`
            /// type and have the same type
            /// * `arg` for each `@aggregate` is a numeric type in the
            /// timeseries, coercible to the type of the field (e.g. `Int ->
            /// BigDecimal`, but not `BigInt -> Int8`)
            fn aggregate_directive(
                schema: &Schema,
                agg_type: &s::ObjectType,
                errors: &mut Vec<Err>,
            ) {
                let source = match agg_type
                    .find_directive(kw::AGGREGATION)
                    .and_then(|dir| dir.argument(kw::SOURCE))
                {
                    Some(s::Value::String(source)) => source,
                    Some(_) => {
                        errors.push(Err::AggregationInvalidSource(agg_type.name.to_owned()));
                        return;
                    }
                    None => {
                        errors.push(Err::AggregationMissingSource(agg_type.name.to_owned()));
                        return;
                    }
                };
                let source = match schema.entity_types.iter().find(|ty| &ty.name == source) {
                    Some(source) => *source,
                    None => {
                        errors.push(Err::AggregationUnknownSource(
                            agg_type.name.to_owned(),
                            source.to_owned(),
                        ));
                        return;
                    }
                };
                match source
                    .find_directive(kw::ENTITY)
                    .and_then(|dir| dir.argument(kw::TIMESERIES))
                {
                    Some(s::Value::Boolean(true)) => { /* ok */ }
                    Some(_) | None => {
                        errors.push(Err::AggregationNonTimeseriesSource(
                            agg_type.name.to_owned(),
                            source.name.to_owned(),
                        ));
                        return;
                    }
                }
                match IdType::try_from(source) {
                    Ok(id_type) => valid_id_field(agg_type, id_type, errors),
                    Err(e) => errors.push(Err::IllegalIdType(e.to_string())),
                };

                let mut has_aggregate = false;
                for field in agg_type
                    .fields
                    .iter()
                    .filter(|field| field.name != ID.as_str() && field.name != kw::TIMESTAMP)
                {
                    match field.find_directive(kw::AGGREGATE) {
                        Some(agg) => {
                            // The source field for an aggregate
                            // must have the same type as the arg
                            has_aggregate = true;
                            let func = match agg.argument(kw::FUNC) {
                                Some(s::Value::Enum(func) | s::Value::String(func)) => func,
                                Some(v) => {
                                    errors.push(Err::AggregationInvalidFn(
                                        agg_type.name.to_owned(),
                                        field.name.to_owned(),
                                        v.to_string(),
                                    ));
                                    continue;
                                }
                                None => {
                                    errors.push(Err::AggregationMissingFn(
                                        agg_type.name.to_owned(),
                                        field.name.to_owned(),
                                    ));
                                    continue;
                                }
                            };
                            let func = match func.parse::<AggregateFn>() {
                                Ok(func) => func,
                                Err(_) => {
                                    errors.push(Err::AggregationInvalidFn(
                                        agg_type.name.to_owned(),
                                        field.name.to_owned(),
                                        func.to_owned(),
                                    ));
                                    continue;
                                }
                            };
                            let arg = match agg.argument(kw::ARG) {
                                Some(s::Value::String(arg)) => arg,
                                Some(_) => {
                                    errors.push(Err::AggregationInvalidArg(
                                        agg_type.name.to_owned(),
                                        field.name.to_owned(),
                                    ));
                                    continue;
                                }
                                None => {
                                    if func.has_arg() {
                                        errors.push(Err::AggregationMissingArg(
                                            agg_type.name.to_owned(),
                                            field.name.to_owned(),
                                            func.as_str().to_owned(),
                                        ));
                                        continue;
                                    } else {
                                        // No arg for a function
                                        // that does not take an arg
                                        continue;
                                    }
                                }
                            };
                            let arg_type = match source.field(arg) {
                                Some(arg_field) => match arg_field.field_type.value_type() {
                                    Ok(arg_type) if arg_type.is_numeric() => arg_type,
                                    Ok(_) | Err(_) => {
                                        errors.push(Err::AggregationNonNumericArg(
                                            agg_type.name.to_owned(),
                                            field.name.to_owned(),
                                            source.name.to_owned(),
                                            arg.to_owned(),
                                        ));
                                        continue;
                                    }
                                },
                                None => {
                                    errors.push(Err::AggregationUnknownArg(
                                        agg_type.name.to_owned(),
                                        field.name.to_owned(),
                                        arg.to_owned(),
                                    ));
                                    continue;
                                }
                            };
                            let field_type = match field.field_type.value_type() {
                                Ok(field_type) => field_type,
                                Err(_) => {
                                    errors.push(Err::NonNumericAggregate(
                                        agg_type.name.to_owned(),
                                        field.name.to_owned(),
                                    ));
                                    continue;
                                }
                            };
                            if arg_type > field_type {
                                errors.push(Err::AggregationNonMatchingArg(
                                    agg_type.name.to_owned(),
                                    field.name.to_owned(),
                                    arg.to_owned(),
                                    arg_type.to_str().to_owned(),
                                    field_type.to_str().to_owned(),
                                ));
                            }
                        }
                        None => {
                            // Non-aggregate fields must have the
                            // same type as the type in the source
                            let src_field = match source.field(&field.name) {
                                Some(src_field) => src_field,
                                None => {
                                    errors.push(Err::AggregationUnknownField(
                                        agg_type.name.to_owned(),
                                        source.name.to_owned(),
                                        field.name.to_owned(),
                                    ));
                                    continue;
                                }
                            };
                            if field.field_type.get_base_type()
                                != src_field.field_type.get_base_type()
                            {
                                errors.push(Err::AggregationNonMatchingType(
                                    agg_type.name.to_owned(),
                                    field.name.to_owned(),
                                    field.field_type.get_base_type().to_owned(),
                                    src_field.field_type.get_base_type().to_owned(),
                                ));
                            }
                        }
                    }
                }
                if !has_aggregate {
                    errors.push(Err::PointlessAggregation(agg_type.name.to_owned()));
                }
            }

            fn aggregation_intervals(agg_type: &s::ObjectType, errors: &mut Vec<Err>) {
                let intervals = match agg_type
                    .find_directive(kw::AGGREGATION)
                    .and_then(|dir| dir.argument(kw::INTERVALS))
                {
                    Some(s::Value::List(intervals)) => intervals,
                    Some(_) => {
                        errors.push(Err::AggregationWrongIntervals(agg_type.name.to_owned()));
                        return;
                    }
                    None => {
                        errors.push(Err::AggregationMissingIntervals(agg_type.name.to_owned()));
                        return;
                    }
                };
                let intervals = intervals
                    .iter()
                    .map(|interval| match interval {
                        s::Value::String(s) => Ok(s),
                        _ => Err(Err::AggregationWrongIntervals(agg_type.name.to_owned())),
                    })
                    .collect::<Result<Vec<_>, _>>();
                let intervals = match intervals {
                    Ok(intervals) => intervals,
                    Err(err) => {
                        errors.push(err);
                        return;
                    }
                };
                if intervals.is_empty() {
                    errors.push(Err::AggregationWrongIntervals(agg_type.name.to_owned()));
                    return;
                }
                for interval in intervals {
                    if let Err(_) = interval.parse::<AggregationInterval>() {
                        errors.push(Err::AggregationInvalidInterval(
                            agg_type.name.to_owned(),
                            interval.to_owned(),
                        ));
                    }
                }
            }

            if !self.aggregations.is_empty() && self.spec_version < &SPEC_VERSION_1_1_0 {
                return vec![SchemaValidationError::AggregationsNotSupported(
                    self.spec_version.clone(),
                )];
            }

            let mut errors = Vec::new();
            for agg_type in &self.aggregations {
                // FIXME: We could make it so that we silently add the `id` and
                // `timestamp` fields instead of requiring users to always
                // list them.
                if let Some(err) = Self::valid_timestamp_field(agg_type) {
                    errors.push(err);
                }
                no_derived_fields(agg_type, &mut errors);
                aggregate_fields_are_numbers(agg_type, &mut errors);
                aggregate_directive(self, agg_type, &mut errors);
                // check timeseries directive has intervals and args
                aggregation_intervals(agg_type, &mut errors);
            }
            errors
        }

        /// Aggregations must have a `timestamp` field of type Int8
        /// FIXME: introduce a timestamp type and use that
        fn valid_timestamp_field(agg_type: &s::ObjectType) -> Option<Err> {
            let field = match agg_type.field(kw::TIMESTAMP) {
                Some(field) => field,
                None => {
                    return Some(Err::TimestampFieldMissing(agg_type.name.to_owned()));
                }
            };

            match field.field_type.value_type() {
                Ok(ValueType::Int8) => None,
                Ok(_) | Err(_) => Some(Err::InvalidTimestampType(
                    agg_type.name.to_owned(),
                    field.field_type.get_base_type().to_owned(),
                )),
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use std::ffi::OsString;

        use regex::Regex;

        use crate::{data::subgraph::LATEST_VERSION, prelude::DeploymentHash};

        use super::*;

        fn parse(schema: &str) -> BaseSchema {
            let hash = DeploymentHash::new("test").unwrap();
            BaseSchema::parse(schema, hash).unwrap()
        }

        fn validate(schema: &BaseSchema) -> Result<(), Vec<SchemaValidationError>> {
            super::validate(LATEST_VERSION, schema)
        }

        #[test]
        fn object_types_have_id() {
            const NO_ID: &str = "type User @entity { name: String! }";
            const ID_BIGINT: &str = "type User @entity { id: BigInt! }";
            const INTF_NO_ID: &str = "interface Person { name: String! }";
            const ROOT_SCHEMA: &str = "type _Schema_";

            let res = validate(&parse(NO_ID));
            assert_eq!(
                res,
                Err(vec![SchemaValidationError::IdFieldMissing(
                    "User".to_string()
                )])
            );

            let res = validate(&parse(ID_BIGINT));
            let errs = res.unwrap_err();
            assert_eq!(1, errs.len());
            assert!(matches!(errs[0], SchemaValidationError::IllegalIdType(_)));

            let res = validate(&parse(INTF_NO_ID));
            assert_eq!(Ok(()), res);

            let res = validate(&parse(ROOT_SCHEMA));
            assert_eq!(Ok(()), res);
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
                let schema =
                    BaseSchema::parse(&schema, DeploymentHash::new("dummy").unwrap()).unwrap();
                let res = validate(&schema);
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
                let schema = BaseSchema::new(DeploymentHash::new("id").unwrap(), document).unwrap();
                let schema = Schema::new(LATEST_VERSION, &schema);
                match schema.validate_derived_from() {
                    Err(ref e) => match e {
                        SchemaValidationError::InvalidDerivedFrom(_, _, msg) => {
                            assert_eq!(errmsg, msg)
                        }
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

            let document =
                graphql_parser::parse_schema(ROOT_SCHEMA).expect("Failed to parse root schema");
            let schema = BaseSchema::new(DeploymentHash::new("id").unwrap(), document).unwrap();
            let schema = Schema::new(LATEST_VERSION, &schema);
            assert_eq!(
                schema.validate_schema_type_has_no_fields().expect_err(
                    "Expected validation to fail due to fields defined on the reserved type"
                ),
                SchemaValidationError::SchemaTypeWithFields
            )
        }

        #[test]
        fn test_reserved_type_directives() {
            const ROOT_SCHEMA: &str = "
type _Schema_ @illegal";

            let document =
                graphql_parser::parse_schema(ROOT_SCHEMA).expect("Failed to parse root schema");
            let schema = BaseSchema::new(DeploymentHash::new("id").unwrap(), document).unwrap();
            let schema = Schema::new(LATEST_VERSION, &schema);
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

            let document =
                graphql_parser::parse_schema(ROOT_SCHEMA).expect("Failed to parse root schema");
            let schema = BaseSchema::new(DeploymentHash::new("id").unwrap(), document).unwrap();
            let schema = Schema::new(LATEST_VERSION, &schema);
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
                let schema = format!(
                    "type {} @entity {{ id: String! _: Boolean }}\n",
                    reserved_type
                );

                let schema = BaseSchema::parse(&schema, dummy_hash.clone()).unwrap();

                let errors = validate(&schema).unwrap_err();
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
        id: String!
        _: Boolean
      }
    type Gravatar_filter @entity {
        id: String!
        _: Boolean
    }
    type Gravatar_orderBy @entity {
        id: String!
        _: Boolean
    }
    "#;

            let dummy_hash = DeploymentHash::new("dummy").unwrap();

            let schema = BaseSchema::parse(SCHEMA, dummy_hash).unwrap();

            let errors = validate(&schema).unwrap_err();

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
            let schema = BaseSchema::new(DeploymentHash::new("id1").unwrap(), document).unwrap();
            let schema = Schema::new(LATEST_VERSION, &schema);
            assert_eq!(schema.validate_fulltext_directives(), vec![]);
        }

        #[test]
        fn agg() {
            fn parse_annotation(file_name: &str, line: &str) -> (bool, Version, String) {
                let bad_annotation = |msg: &str| -> ! {
                    panic!("test case {file_name} has an invalid annotation `{line}`: {msg}")
                };

                let header_rx = Regex::new(
                    r"^#\s*(?P<exp>valid|fail)\s*(@\s*(?P<version>[0-9.]+))?\s*:\s*(?P<rest>.*)$",
                )
                .unwrap();
                let Some(caps) = header_rx.captures(line) else {
                    bad_annotation("must match the regex `^# (valid|fail) (@ ([0-9.]+))? : .*$`")
                };
                let valid = match caps.name("exp").map(|mtch| mtch.as_str()) {
                    Some("valid") => true,
                    Some("fail") => false,
                    Some(other) => {
                        bad_annotation(&format!("expected 'valid' or 'fail' but got {other}"))
                    }
                    None => bad_annotation("missing 'valid' or 'fail'"),
                };
                let version = match caps
                    .name("version")
                    .map(|mtch| Version::parse(mtch.as_str()))
                    .transpose()
                {
                    Ok(Some(version)) => version,
                    Ok(None) => LATEST_VERSION.clone(),
                    Err(err) => bad_annotation(&err.to_string()),
                };
                let rest = match caps.name("rest").map(|mtch| mtch.as_str()) {
                    Some(rest) => rest.to_string(),
                    None => bad_annotation("missing message"),
                };
                (valid, version, rest)
            }

            // The test files for this test are all GraphQL schemas that
            // must all have a comment as the first line. For a test that is
            // expected to succeed, the comment must be `# valid: ..`. For
            // tests that are expected to fail validation, the comment must
            // be `# fail: <err>` where <err> must appear in one of the
            // error messages when they are formatted as debug output.
            let dir = std::path::PathBuf::from_iter([
                env!("CARGO_MANIFEST_DIR"),
                "src",
                "schema",
                "test_schemas",
            ]);
            let files = {
                let mut files = std::fs::read_dir(dir)
                    .unwrap()
                    .into_iter()
                    .filter_map(|entry| entry.ok())
                    .map(|entry| entry.path())
                    .filter(|path| path.extension() == Some(OsString::from("graphql").as_os_str()))
                    .collect::<Vec<_>>();
                files.sort();
                files
            };
            for file in files {
                let schema = std::fs::read_to_string(&file).unwrap();
                let file_name = file.file_name().unwrap().to_str().unwrap();
                let first_line = schema.lines().next().unwrap();
                let (valid, version, msg) = parse_annotation(file_name, first_line);
                let schema = {
                    let hash = DeploymentHash::new("test").unwrap();
                    match BaseSchema::parse(&schema, hash) {
                        Ok(schema) => schema,
                        Err(e) => panic!("test case {file_name} failed to parse: {e}"),
                    }
                };
                let res = super::validate(&version, &schema);
                match (valid, res) {
                    (true, Err(errs)) => {
                        panic!("{file_name} should validate: {errs:?}",);
                    }
                    (false, Ok(_)) => {
                        panic!("{file_name} should fail validation");
                    }
                    (false, Err(errs)) => {
                        if errs.iter().any(|err| {
                            err.to_string().contains(&msg) || format!("{err:?}").contains(&msg)
                        }) {
                            println!("{file_name} failed as expected: {errs:?}",)
                        } else {
                            let msgs: Vec<_> = errs.iter().map(|err| err.to_string()).collect();
                            panic!(
                                    "{file_name} failed but not with the expected error `{msg}`: {errs:?} {msgs:?}",
                                )
                        }
                    }
                    (true, Ok(_)) => {
                        println!("{file_name} validated as expected")
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        data::store::ID,
        prelude::DeploymentHash,
        schema::{
            input_schema::{POI_DIGEST, POI_OBJECT},
            EntityType,
        },
    };

    use super::InputSchema;

    const SCHEMA: &str = r#"
      type Thing @entity {
        id: ID!
        name: String!
      }

      interface Animal {
        name: String!
      }

      type Hippo implements Animal @entity {
        id: ID!
        name: String!
      }

      type Rhino implements Animal @entity {
        id: ID!
        name: String!
      }

      type HippoData @entity(timeseries: true) {
        id: Int8!
        hippo: Hippo!
        timestamp: Int8!
        weight: BigDecimal!
      }

      type HippoStats @aggregation(intervals: ["hour"], source: "HippoData") {
        id: Int8!
        timestamp: Int8!
        hippo: Hippo!
        maxWeight: BigDecimal! @aggregate(fn: "max", arg:"weight")
      }
    "#;

    fn make_schema() -> InputSchema {
        let id = DeploymentHash::new("test").unwrap();
        InputSchema::parse_latest(SCHEMA, id).unwrap()
    }

    #[test]
    fn entity_type() {
        let schema = make_schema();

        assert_eq!("Thing", schema.entity_type("Thing").unwrap().typename());

        let poi = schema.entity_type(POI_OBJECT).unwrap();
        assert_eq!(POI_OBJECT, poi.typename());
        assert!(poi.has_field(schema.pool().lookup(&ID).unwrap()));
        assert!(poi.has_field(schema.pool().lookup(POI_DIGEST).unwrap()));
        assert!(poi.object_type().is_ok());

        assert!(schema.entity_type("NonExistent").is_err());
    }

    #[test]
    fn share_interfaces() {
        const SCHEMA: &str = r#"
    interface Animal {
        name: String!
    }

    type Dog implements Animal @entity {
        id: ID!
        name: String!
    }

    type Cat implements Animal @entity {
        id: ID!
        name: String!
    }

    type Person @entity {
        id: ID!
        name: String!
    }
        "#;

        let id = DeploymentHash::new("test").unwrap();
        let schema = InputSchema::parse_latest(SCHEMA, id).unwrap();

        let dog = schema.entity_type("Dog").unwrap();
        let cat = schema.entity_type("Cat").unwrap();
        let person = schema.entity_type("Person").unwrap();
        assert_eq!(vec![cat.clone()], dog.share_interfaces().unwrap());
        assert_eq!(vec![dog], cat.share_interfaces().unwrap());
        assert!(person.share_interfaces().unwrap().is_empty());
    }

    #[test]
    fn intern() {
        static NAMES: &[&str] = &[
            "Thing",
            "Animal",
            "Hippo",
            "HippoStats",
            "HippoStats_hour",
            "id",
            "name",
            "timestamp",
            "hippo",
            "maxWeight",
        ];

        let schema = make_schema();
        let pool = schema.pool();

        for name in NAMES {
            assert!(pool.lookup(name).is_some(), "The string {name} is interned");
        }
    }

    #[test]
    fn object_type() {
        let schema = make_schema();
        let pool = schema.pool();

        let animal = pool.lookup("Animal").unwrap();
        let hippo = pool.lookup("Hippo").unwrap();
        let rhino = pool.lookup("Rhino").unwrap();
        let hippo_data = pool.lookup("HippoData").unwrap();
        let hippo_stats_hour = pool.lookup("HippoStats_hour").unwrap();

        let animal_ent = EntityType::new(schema.clone(), animal);
        // Interfaces don't have an object type
        assert!(animal_ent.object_type().is_err());

        let hippo_ent = EntityType::new(schema.clone(), hippo);
        let hippo_obj = hippo_ent.object_type().unwrap();
        assert_eq!(hippo_obj.name, hippo);
        assert!(!hippo_ent.is_immutable());

        let rhino_ent = EntityType::new(schema.clone(), rhino);
        assert_eq!(hippo_ent.share_interfaces().unwrap(), vec![rhino_ent]);

        let hippo_data_ent = EntityType::new(schema.clone(), hippo_data);
        let hippo_data_obj = hippo_data_ent.object_type().unwrap();
        assert_eq!(hippo_data_obj.name, hippo_data);
        assert!(hippo_data_ent.share_interfaces().unwrap().is_empty());
        assert!(hippo_data_ent.is_immutable());

        let hippo_stats_hour_ent = EntityType::new(schema.clone(), hippo_stats_hour);
        let hippo_stats_hour_obj = schema.object_type(hippo_stats_hour).unwrap();
        assert_eq!(hippo_stats_hour_obj.name, hippo_stats_hour);
        assert!(hippo_stats_hour_ent.share_interfaces().unwrap().is_empty());
        assert!(hippo_stats_hour_ent.is_immutable());
    }
}
