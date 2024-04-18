//! Helpers to use diesel dynamic schema to retrieve values from Postgres

use std::num::NonZeroU32;

use diesel::sql_types::{Array, BigInt, Binary, Bool, Integer, Numeric, Text, Timestamptz};
use diesel::{deserialize::FromSql, pg::Pg};
use diesel_dynamic_schema::dynamic_value::{Any, DynamicRow};

use graph::{
    components::store::StoreError,
    data::{
        store::{
            scalar::{BigDecimal, Bytes, Timestamp},
            Entity, QueryObject,
        },
        value::{Object, Word},
    },
    prelude::r,
    schema::InputSchema,
};

use super::ColumnType;
use crate::relational::Column;

/// Represent values of the database types we care about as a single value.
/// The deserialization of these values is completely governed by the oid we
/// get from Postgres; in a second step, these values need to be transformed
/// into our internal values using the underlying `ColumnType`. Diesel's API
/// doesn't let us do that in one go, so we do a first transformation into
/// `OidValue` and then use `FromOidValue` to transform guided by the
/// `ColumnType`
#[derive(Debug)]
pub enum OidValue {
    String(String),
    StringArray(Vec<String>),
    Bytes(Bytes),
    BytesArray(Vec<Bytes>),
    Bool(bool),
    BoolArray(Vec<bool>),
    Int(i32),
    Ints(Vec<i32>),
    Int8(i64),
    Int8Array(Vec<i64>),
    BigDecimal(BigDecimal),
    BigDecimalArray(Vec<BigDecimal>),
    Timestamp(Timestamp),
    TimestampArray(Vec<Timestamp>),
    Null,
}

impl FromSql<Any, Pg> for OidValue {
    fn from_sql(value: diesel::pg::PgValue) -> diesel::deserialize::Result<Self> {
        const VARCHAR_OID: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(1043) };
        const VARCHAR_ARY_OID: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(1015) };
        const TEXT_OID: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(25) };
        const TEXT_ARY_OID: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(1009) };
        const BYTEA_OID: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(17) };
        const BYTEA_ARY_OID: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(1001) };
        const BOOL_OID: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(16) };
        const BOOL_ARY_OID: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(1000) };
        const INTEGER_OID: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(23) };
        const INTEGER_ARY_OID: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(1007) };
        const INT8_OID: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(20) };
        const INT8_ARY_OID: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(1016) };
        const NUMERIC_OID: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(1700) };
        const NUMERIC_ARY_OID: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(1231) };
        const TIMESTAMPTZ_OID: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(1184) };
        const TIMESTAMPTZ_ARY_OID: NonZeroU32 = unsafe { NonZeroU32::new_unchecked(1185) };

        match value.get_oid() {
            VARCHAR_OID | TEXT_OID => {
                <String as FromSql<Text, Pg>>::from_sql(value).map(OidValue::String)
            }
            VARCHAR_ARY_OID | TEXT_ARY_OID => {
                <Vec<String> as FromSql<Array<Text>, Pg>>::from_sql(value)
                    .map(OidValue::StringArray)
            }
            BYTEA_OID => <Bytes as FromSql<Binary, Pg>>::from_sql(value).map(OidValue::Bytes),
            BYTEA_ARY_OID => <Vec<Bytes> as FromSql<Array<Binary>, Pg>>::from_sql(value)
                .map(OidValue::BytesArray),
            BOOL_OID => <bool as FromSql<Bool, Pg>>::from_sql(value).map(OidValue::Bool),
            BOOL_ARY_OID => {
                <Vec<bool> as FromSql<Array<Bool>, Pg>>::from_sql(value).map(OidValue::BoolArray)
            }
            INTEGER_OID => <i32 as FromSql<Integer, Pg>>::from_sql(value).map(OidValue::Int),
            INTEGER_ARY_OID => {
                <Vec<i32> as FromSql<Array<Integer>, Pg>>::from_sql(value).map(OidValue::Ints)
            }
            INT8_OID => <i64 as FromSql<BigInt, Pg>>::from_sql(value).map(OidValue::Int8),
            INT8_ARY_OID => {
                <Vec<i64> as FromSql<Array<BigInt>, Pg>>::from_sql(value).map(OidValue::Int8Array)
            }
            NUMERIC_OID => {
                <BigDecimal as FromSql<Numeric, Pg>>::from_sql(value).map(OidValue::BigDecimal)
            }
            NUMERIC_ARY_OID => <Vec<BigDecimal> as FromSql<Array<Numeric>, Pg>>::from_sql(value)
                .map(OidValue::BigDecimalArray),
            TIMESTAMPTZ_OID => {
                <Timestamp as FromSql<Timestamptz, Pg>>::from_sql(value).map(OidValue::Timestamp)
            }
            TIMESTAMPTZ_ARY_OID => {
                <Vec<Timestamp> as FromSql<Array<Timestamptz>, Pg>>::from_sql(value)
                    .map(OidValue::TimestampArray)
            }
            e => Err(format!("Unknown type: {e}").into()),
        }
    }

    fn from_nullable_sql(bytes: Option<diesel::pg::PgValue>) -> diesel::deserialize::Result<Self> {
        match bytes {
            Some(bytes) => Self::from_sql(bytes),
            None => Ok(OidValue::Null),
        }
    }
}

pub trait FromOidValue: Sized {
    fn from_oid_value(value: OidValue, column_type: &ColumnType) -> Result<Self, StoreError>;
}

impl FromOidValue for r::Value {
    fn from_oid_value(value: OidValue, _: &ColumnType) -> Result<Self, StoreError> {
        fn as_list<T, F>(values: Vec<T>, f: F) -> r::Value
        where
            F: Fn(T) -> r::Value,
        {
            r::Value::List(values.into_iter().map(f).collect())
        }

        use OidValue as O;
        let value = match value {
            O::String(s) => Self::String(s),
            O::StringArray(s) => as_list(s, Self::String),
            O::Bytes(b) => Self::String(b.to_string()),
            O::BytesArray(b) => as_list(b, |b| Self::String(b.to_string())),
            O::Bool(b) => Self::Boolean(b),
            O::BoolArray(b) => as_list(b, Self::Boolean),
            O::Int(i) => Self::Int(i as i64),
            O::Ints(i) => as_list(i, |i| Self::Int(i as i64)),
            O::Int8(i) => Self::String(i.to_string()),
            O::Int8Array(i) => as_list(i, |i| Self::String(i.to_string())),
            O::BigDecimal(b) => Self::String(b.to_string()),
            O::BigDecimalArray(b) => as_list(b, |b| Self::String(b.to_string())),
            O::Timestamp(t) => Self::Timestamp(t),
            O::TimestampArray(t) => as_list(t, Self::Timestamp),
            O::Null => Self::Null,
        };
        Ok(value)
    }
}

impl FromOidValue for graph::prelude::Value {
    fn from_oid_value(value: OidValue, column_type: &ColumnType) -> Result<Self, StoreError> {
        fn as_list<T, F>(values: Vec<T>, f: F) -> graph::prelude::Value
        where
            F: Fn(T) -> graph::prelude::Value,
        {
            graph::prelude::Value::List(values.into_iter().map(f).collect())
        }

        fn as_list_err<T, F>(values: Vec<T>, f: F) -> Result<graph::prelude::Value, StoreError>
        where
            F: Fn(T) -> Result<graph::prelude::Value, StoreError>,
        {
            values
                .into_iter()
                .map(f)
                .collect::<Result<_, _>>()
                .map(graph::prelude::Value::List)
        }

        use OidValue as O;
        let value = match value {
            O::String(s) => Self::String(s),
            O::StringArray(s) => as_list(s, Self::String),
            O::Bytes(b) => Self::Bytes(b),
            O::BytesArray(b) => as_list(b, Self::Bytes),
            O::Bool(b) => Self::Bool(b),
            O::BoolArray(b) => as_list(b, Self::Bool),
            O::Int(i) => Self::Int(i),
            O::Ints(i) => as_list(i, Self::Int),
            O::Int8(i) => Self::Int8(i),
            O::Int8Array(i) => as_list(i, Self::Int8),
            O::BigDecimal(b) => match column_type {
                ColumnType::BigDecimal => Self::BigDecimal(b),
                ColumnType::BigInt => Self::BigInt(b.to_bigint()?),
                _ => unreachable!("only BigInt and BigDecimal are stored as numeric"),
            },
            O::BigDecimalArray(b) => match column_type {
                ColumnType::BigDecimal => as_list(b, Self::BigDecimal),
                ColumnType::BigInt => as_list_err(b, |b| {
                    b.to_bigint().map(Self::BigInt).map_err(StoreError::from)
                })?,
                _ => unreachable!("only BigInt and BigDecimal are stored as numeric[]"),
            },
            O::Timestamp(t) => Self::Timestamp(t),
            O::TimestampArray(t) => as_list(t, Self::Timestamp),
            O::Null => Self::Null,
        };
        Ok(value)
    }
}

pub type OidRow = DynamicRow<OidValue>;

pub trait FromOidRow: Sized {
    // Should the columns for `__typename` and `g$parent_id` be selected
    const WITH_INTERNAL_KEYS: bool;
    // Should the system columns for block/block_range and vid be selected
    const WITH_SYSTEM_COLUMNS: bool = false;

    fn from_oid_row(
        row: DynamicRow<OidValue>,
        schema: &InputSchema,
        columns: &[&Column],
    ) -> Result<Self, StoreError>;
}

impl FromOidRow for Entity {
    const WITH_INTERNAL_KEYS: bool = false;

    fn from_oid_row(
        row: DynamicRow<OidValue>,
        schema: &InputSchema,
        columns: &[&Column],
    ) -> Result<Self, StoreError> {
        let x = row
            .into_iter()
            .zip(columns)
            .filter(|(value, _)| !matches!(value, OidValue::Null))
            .map(|(value, column)| {
                graph::prelude::Value::from_oid_value(value, &column.column_type)
                    .map(|value| (Word::from(column.field.clone()), value))
            });
        schema.try_make_entity(x).map_err(StoreError::from)
    }
}

impl FromOidRow for QueryObject {
    const WITH_INTERNAL_KEYS: bool = true;

    fn from_oid_row(
        row: DynamicRow<OidValue>,
        _schema: &InputSchema,
        columns: &[&Column],
    ) -> Result<Self, StoreError> {
        let pairs = row
            .into_iter()
            .zip(columns)
            .filter(|(value, _)| !matches!(value, OidValue::Null))
            .map(|(value, column)| -> Result<_, StoreError> {
                let name = &column.name;
                let value = r::Value::from_oid_value(value, &column.column_type)?;
                Ok((Word::from(name.clone()), value))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let entity = Object::from_iter(pairs);
        Ok(QueryObject {
            entity,
            parent: None,
        })
    }
}
