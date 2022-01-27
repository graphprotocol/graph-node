///! This module contains the gory details of using Diesel to query
///! a database schema that is not known at compile time. The code in this
///! module is mostly concerned with constructing SQL queries and some
///! helpers for serializing and deserializing entities.
///!
///! Code in this module works very hard to minimize the number of allocations
///! that it performs
use diesel::pg::{Pg, PgConnection};
use diesel::query_builder::{AstPass, QueryFragment, QueryId};
use diesel::query_dsl::{LoadQuery, RunQueryDsl};
use diesel::result::{Error as DieselError, QueryResult};
use diesel::sql_types::{Array, BigInt, Binary, Bool, Integer, Jsonb, Range, Text};
use diesel::Connection;
use lazy_static::lazy_static;

use graph::prelude::{
    anyhow, r, serde_json, Attribute, BlockNumber, ChildMultiplicity, Entity, EntityCollection,
    EntityFilter, EntityKey, EntityLink, EntityOrder, EntityRange, EntityWindow, ParentLink,
    QueryExecutionError, StoreError, Value,
};
use graph::{
    components::store::{AttributeNames, EntityType},
    data::{schema::FulltextAlgorithm, store::scalar},
};
use itertools::Itertools;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::convert::TryFrom;
use std::env;
use std::fmt::{self, Display};
use std::iter::FromIterator;
use std::str::FromStr;

use crate::relational::{
    Column, ColumnType, IdType, Layout, SqlName, Table, PRIMARY_KEY_COLUMN, STRING_PREFIX_SIZE,
};
use crate::sql_value::SqlValue;
use crate::{
    block_range::{BlockRange, BlockRangeContainsClause, BLOCK_RANGE_COLUMN, BLOCK_RANGE_CURRENT},
    primary::Namespace,
};

lazy_static! {
    /// Use a variant of the query for child_type_a when we are looking up
    /// fewer than this many entities. This variable is only here temporarily
    /// until we can settle on the right batch size through experimentation
    /// and should then just become an ordinary constant
    static ref TYPEA_BATCH_SIZE: usize = {
        env::var("TYPEA_BATCH_SIZE")
            .ok()
            .map(|s| {
                usize::from_str(&s)
                    .unwrap_or_else(|_| panic!("TYPE_BATCH_SIZE must be a number, but is `{}`", s))
            })
            .unwrap_or(150)
    };
    /// Include a constraint on the child ids as a set in child_type_d
    /// queries if the size of the set is below this threshold. Set this to
    /// 0 to turn off this optimization
    static ref TYPED_CHILDREN_SET_SIZE: usize = {
        env::var("TYPED_CHILDREN_SET_SIZE")
            .ok()
            .map(|s| {
                usize::from_str(&s)
                    .unwrap_or_else(|_| panic!("TYPED_CHILDREN_SET_SIZE must be a number, but is `{}`", s))
            })
            .unwrap_or(150)
    };
    /// When we add `order by id` to a query should we add instead
    /// `order by id, block_range`
    static ref ORDER_BY_BLOCK_RANGE: bool = {
        env::var("ORDER_BY_BLOCK_RANGE")
            .ok()
            .map(|s| {
                s == "1"
            })
            .unwrap_or(false)
    };
    /// Reversible order by. Change our `order by` clauses so that `asc`
    /// and `desc` ordering produce reverse orders. Setting this
    /// turns the new, correct behavior off
    static ref REVERSIBLE_ORDER_BY_OFF: bool = {
        env::var("REVERSIBLE_ORDER_BY_OFF")
            .ok()
            .map(|s| {
                s == "1"
            })
            .unwrap_or(false)
    };
}

/// Those are columns that we always want to fetch from the database.
const BASE_SQL_COLUMNS: [&'static str; 2] = ["id", "vid"];

#[derive(Debug)]
pub(crate) struct UnsupportedFilter {
    pub filter: String,
    pub value: Value,
}

impl Display for UnsupportedFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "unsupported filter `{}` for value `{}`",
            self.filter, self.value
        )
    }
}

impl std::error::Error for UnsupportedFilter {}

impl From<UnsupportedFilter> for diesel::result::Error {
    fn from(error: UnsupportedFilter) -> Self {
        diesel::result::Error::QueryBuilderError(Box::new(error))
    }
}

// Similar to graph::prelude::constraint_violation, but returns a Diesel
// error for use in the guts of query generation
macro_rules! constraint_violation {
    ($msg:expr) => {{
        diesel::result::Error::QueryBuilderError(anyhow!("{}", $msg).into())
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        diesel::result::Error::QueryBuilderError(anyhow!("{}", $fmt, $($arg)*))
    }}
}

fn str_as_bytes(id: &str) -> QueryResult<scalar::Bytes> {
    scalar::Bytes::from_str(&id).map_err(|e| DieselError::SerializationError(Box::new(e)))
}

/// Convert Postgres string representation of bytes "\xdeadbeef"
/// to ours of just "deadbeef".
fn bytes_as_str(id: &str) -> String {
    id.trim_start_matches("\\x").to_owned()
}

/// Conveniences for handling foreign keys depending on whether we are using
/// `IdType::Bytes` or `IdType::String` as the primary key
///
/// This trait adds some capabilities to `Column` that are very specific to
/// how we generate SQL queries. Using a method like `bind_ids` from this
/// trait on a given column means "send these values to the database in a form
/// that can later be used for comparisons with that column"
trait ForeignKeyClauses {
    /// The type of the column
    fn column_type(&self) -> &ColumnType;

    /// The name of the column
    fn name(&self) -> &str;

    /// Add `id` as a bind variable to `out`, using the right SQL type
    fn bind_id(&self, id: &str, out: &mut AstPass<Pg>) -> QueryResult<()> {
        match self.column_type().id_type() {
            IdType::String => out.push_bind_param::<Text, _>(&id)?,
            IdType::Bytes => out.push_bind_param::<Binary, _>(&str_as_bytes(&id)?.as_slice())?,
        }
        // Generate '::text' or '::bytea'
        out.push_sql("::");
        out.push_sql(self.column_type().sql_type());
        Ok(())
    }

    /// Add `ids`  as a bind variable to `out`, using the right SQL type
    fn bind_ids<S>(&self, ids: &[S], out: &mut AstPass<Pg>) -> QueryResult<()>
    where
        S: AsRef<str> + diesel::serialize::ToSql<Text, Pg>,
    {
        match self.column_type().id_type() {
            IdType::String => out.push_bind_param::<Array<Text>, _>(&ids)?,
            IdType::Bytes => {
                let ids = ids
                    .into_iter()
                    .map(|id| str_as_bytes(id.as_ref()))
                    .collect::<Result<Vec<scalar::Bytes>, _>>()?;
                let id_slices = ids.iter().map(|id| id.as_slice()).collect::<Vec<_>>();
                out.push_bind_param::<Array<Binary>, _>(&id_slices)?;
            }
        }
        // Generate '::text[]' or '::bytea[]'
        out.push_sql("::");
        out.push_sql(self.column_type().sql_type());
        out.push_sql("[]");
        Ok(())
    }

    /// Generate a clause `{name()} = $id` using the right types to bind `$id`
    /// into `out`
    fn eq(&self, id: &str, out: &mut AstPass<Pg>) -> QueryResult<()> {
        out.push_sql(self.name());
        out.push_sql(" = ");
        self.bind_id(id, out)
    }

    /// Generate a clause
    ///    `exists (select 1 from unnest($ids) as p(g$id) where id = p.g$id)`
    /// using the right types to bind `$ids` into `out`
    fn is_in<S>(&self, ids: &[S], out: &mut AstPass<Pg>) -> QueryResult<()>
    where
        S: AsRef<str> + diesel::serialize::ToSql<Text, Pg>,
    {
        out.push_sql("exists (select 1 from unnest(");
        self.bind_ids(ids, out)?;
        out.push_sql(") as p(g$id) where id = p.g$id)");
        Ok(())
    }

    /// Generate an array of arrays as literal SQL. The `ids` must form a
    /// valid matrix, i.e. the same numbe of entries in each row. This can
    /// be achieved by padding them with `None` values. Diesel does not support
    /// arrays of arrays as bind variables, nor arrays containing nulls, so
    /// we have to manually serialize the `ids` as literal SQL.
    fn push_matrix(
        &self,
        matrix: &Vec<Vec<Option<SafeString>>>,
        out: &mut AstPass<Pg>,
    ) -> QueryResult<()> {
        out.push_sql("array[");
        if matrix.is_empty() {
            // If there are no ids, make sure we are producing an
            // empty array of arrays
            out.push_sql("array[null]");
        } else {
            for (i, ids) in matrix.iter().enumerate() {
                if i > 0 {
                    out.push_sql(", ");
                }
                out.push_sql("array[");
                for (j, id) in ids.iter().enumerate() {
                    if j > 0 {
                        out.push_sql(", ");
                    }
                    match id {
                        None => out.push_sql("null"),
                        Some(id) => match self.column_type().id_type() {
                            IdType::String => {
                                out.push_sql("'");
                                out.push_sql(&id.0);
                                out.push_sql("'");
                            }
                            IdType::Bytes => {
                                out.push_sql("'\\x");
                                out.push_sql(&id.0.trim_start_matches("0x"));
                                out.push_sql("'");
                            }
                        },
                    }
                }
                out.push_sql("]");
            }
        }
        // Generate '::text[][]' or '::bytea[][]'
        out.push_sql("]::");
        out.push_sql(self.column_type().sql_type());
        out.push_sql("[][]");
        Ok(())
    }
}

impl ForeignKeyClauses for Column {
    fn column_type(&self) -> &ColumnType {
        &self.column_type
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }
}

pub trait FromEntityData {
    type Value: FromColumnValue;

    fn new_entity(typename: String) -> Self;

    fn insert_entity_data(&mut self, key: String, v: Self::Value);
}

impl FromEntityData for Entity {
    type Value = graph::prelude::Value;

    fn new_entity(typename: String) -> Self {
        let mut entity = Entity::new();
        entity.insert("__typename".to_string(), Self::Value::String(typename));
        entity
    }

    fn insert_entity_data(&mut self, key: String, v: Self::Value) {
        self.insert(key, v);
    }
}

impl FromEntityData for BTreeMap<String, r::Value> {
    type Value = r::Value;

    fn new_entity(typename: String) -> Self {
        let mut map = BTreeMap::new();
        map.insert("__typename".to_string(), Self::Value::from_string(typename));
        map
    }

    fn insert_entity_data(&mut self, key: String, v: Self::Value) {
        self.insert(key, v);
    }
}

pub trait FromColumnValue: Sized {
    fn is_null(&self) -> bool;

    fn null() -> Self;

    fn from_string(s: String) -> Self;

    fn from_bool(b: bool) -> Self;

    fn from_i32(i: i32) -> Self;

    fn from_big_decimal(d: scalar::BigDecimal) -> Self;

    fn from_big_int(i: serde_json::Number) -> Result<Self, StoreError>;

    // The string returned by the DB, without the leading '\x'
    fn from_bytes(i: &str) -> Result<Self, StoreError>;

    fn from_vec(v: Vec<Self>) -> Self;

    fn from_column_value(
        column_type: &ColumnType,
        json: serde_json::Value,
    ) -> Result<Self, StoreError> {
        use serde_json::Value as j;
        // Many possible conversion errors are already caught by how
        // we define the schema; for example, we can only get a NULL for
        // a column that is actually nullable
        match (json, column_type) {
            (j::Null, _) => Ok(Self::null()),
            (j::Bool(b), _) => Ok(Self::from_bool(b)),
            (j::Number(number), ColumnType::Int) => match number.as_i64() {
                Some(i) => i32::try_from(i).map(Self::from_i32).map_err(|e| {
                    StoreError::Unknown(anyhow!("failed to convert {} to Int: {}", number, e))
                }),
                None => Err(StoreError::Unknown(anyhow!(
                    "failed to convert {} to Int",
                    number
                ))),
            },
            (j::Number(number), ColumnType::BigDecimal) => {
                let s = number.to_string();
                scalar::BigDecimal::from_str(s.as_str())
                    .map(Self::from_big_decimal)
                    .map_err(|e| {
                        StoreError::Unknown(anyhow!(
                            "failed to convert {} to BigDecimal: {}",
                            number,
                            e
                        ))
                    })
            }
            (j::Number(number), ColumnType::BigInt) => Self::from_big_int(number),
            (j::Number(number), column_type) => Err(StoreError::Unknown(anyhow!(
                "can not convert number {} to {:?}",
                number,
                column_type
            ))),
            (j::String(s), ColumnType::String) | (j::String(s), ColumnType::Enum(_)) => {
                Ok(Self::from_string(s))
            }
            (j::String(s), ColumnType::Bytes) => Self::from_bytes(s.trim_start_matches("\\x")),
            (j::String(s), ColumnType::BytesId) => Ok(Self::from_string(bytes_as_str(&s))),
            (j::String(s), column_type) => Err(StoreError::Unknown(anyhow!(
                "can not convert string {} to {:?}",
                s,
                column_type
            ))),
            (j::Array(values), _) => Ok(Self::from_vec(
                values
                    .into_iter()
                    .map(|v| Self::from_column_value(column_type, v))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            (j::Object(_), _) => {
                unimplemented!("objects as entity attributes are not needed/supported")
            }
        }
    }
}

impl FromColumnValue for r::Value {
    fn is_null(&self) -> bool {
        matches!(self, r::Value::Null)
    }

    fn null() -> Self {
        Self::Null
    }

    fn from_string(s: String) -> Self {
        r::Value::String(s)
    }

    fn from_bool(b: bool) -> Self {
        r::Value::Boolean(b)
    }

    fn from_i32(i: i32) -> Self {
        r::Value::Int(i.into())
    }

    fn from_big_decimal(d: scalar::BigDecimal) -> Self {
        r::Value::String(d.to_string())
    }

    fn from_big_int(i: serde_json::Number) -> Result<Self, StoreError> {
        Ok(r::Value::String(i.to_string()))
    }

    fn from_bytes(b: &str) -> Result<Self, StoreError> {
        Ok(r::Value::String(format!("0x{}", b)))
    }

    fn from_vec(v: Vec<Self>) -> Self {
        r::Value::List(v)
    }
}

impl FromColumnValue for graph::prelude::Value {
    fn is_null(&self) -> bool {
        self == &Value::Null
    }

    fn null() -> Self {
        Self::Null
    }

    fn from_string(s: String) -> Self {
        graph::prelude::Value::String(s)
    }

    fn from_bool(b: bool) -> Self {
        graph::prelude::Value::Bool(b)
    }

    fn from_i32(i: i32) -> Self {
        graph::prelude::Value::Int(i)
    }

    fn from_big_decimal(d: scalar::BigDecimal) -> Self {
        graph::prelude::Value::BigDecimal(d)
    }

    fn from_big_int(i: serde_json::Number) -> Result<Self, StoreError> {
        scalar::BigInt::from_str(&i.to_string())
            .map(graph::prelude::Value::BigInt)
            .map_err(|e| StoreError::Unknown(anyhow!("failed to convert {} to BigInt: {}", i, e)))
    }

    fn from_bytes(b: &str) -> Result<Self, StoreError> {
        scalar::Bytes::from_str(b)
            .map(graph::prelude::Value::Bytes)
            .map_err(|e| StoreError::Unknown(anyhow!("failed to convert {} to Bytes: {}", b, e)))
    }

    fn from_vec(v: Vec<Self>) -> Self {
        graph::prelude::Value::List(v)
    }
}

/// Helper struct for retrieving entities from the database. With diesel, we
/// can only run queries that return columns whose number and type are known
/// at compile time. Because of that, we retrieve the actual data for an
/// entity as Jsonb by converting the row containing the entity using the
/// `to_jsonb` function.
#[derive(QueryableByName)]
pub struct EntityData {
    #[sql_type = "Text"]
    entity: String,
    #[sql_type = "Jsonb"]
    data: serde_json::Value,
}

impl EntityData {
    pub fn entity_type(&self) -> EntityType {
        EntityType::new(self.entity.clone())
    }

    /// Map the `EntityData` using the schema information in `Layout`
    pub fn deserialize_with_layout<T: FromEntityData>(
        self,
        layout: &Layout,
    ) -> Result<T, StoreError> {
        let entity_type = EntityType::new(self.entity);
        let table = layout.table_for_entity(&entity_type)?;

        use serde_json::Value as j;
        match self.data {
            j::Object(map) => {
                let mut out = T::new_entity(entity_type.into_string());
                for (key, json) in map {
                    // Simply ignore keys that do not have an underlying table
                    // column; those will be things like the block_range that
                    // is used internally for versioning
                    if key == "g$parent_id" {
                        let value = T::Value::from_column_value(&ColumnType::String, json)?;
                        out.insert_entity_data("g$parent_id".to_owned(), value);
                    } else if let Some(column) = table.column(&SqlName::verbatim(key)) {
                        let value = T::Value::from_column_value(&column.column_type, json)?;
                        if !value.is_null() {
                            out.insert_entity_data(column.field.clone(), value);
                        }
                    }
                }
                Ok(out)
            }
            _ => unreachable!(
                "we use `to_json` in our queries, and will therefore always get an object back"
            ),
        }
    }
}

/// A `QueryValue` makes it possible to bind a `Value` into a SQL query
/// using the metadata from Column
struct QueryValue<'a>(&'a Value, &'a ColumnType);

impl<'a> QueryFragment<Pg> for QueryValue<'a> {
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();
        let column_type = self.1;

        match self.0 {
            Value::String(s) => match &column_type {
                ColumnType::String => out.push_bind_param::<Text, _>(s),
                ColumnType::Enum(enum_type) => {
                    out.push_bind_param::<Text, _>(s)?;
                    out.push_sql("::");
                    out.push_sql(enum_type.name.as_str());
                    Ok(())
                }
                ColumnType::TSVector(_) => {
                    out.push_sql("to_tsquery(");
                    out.push_bind_param::<Text, _>(s)?;
                    out.push_sql(")");
                    Ok(())
                }
                ColumnType::Bytes | ColumnType::BytesId => {
                    let bytes = scalar::Bytes::from_str(&s)
                        .map_err(|e| DieselError::SerializationError(Box::new(e)))?;
                    out.push_bind_param::<Binary, _>(&bytes.as_slice())
                }
                _ => unreachable!(
                    "only string, enum and tsvector columns have values of type string"
                ),
            },
            Value::Int(i) => out.push_bind_param::<Integer, _>(i),
            Value::BigDecimal(d) => {
                out.push_bind_param::<Text, _>(&d.to_string())?;
                out.push_sql("::numeric");
                Ok(())
            }
            Value::Bool(b) => out.push_bind_param::<Bool, _>(b),
            Value::List(values) => {
                let sql_values = SqlValue::new_array(values.clone());
                match &column_type {
                    ColumnType::BigDecimal | ColumnType::BigInt => {
                        let text_values: Vec<_> = values.iter().map(|v| v.to_string()).collect();
                        out.push_bind_param::<Array<Text>, _>(&text_values)?;
                        out.push_sql("::numeric[]");
                        Ok(())
                    }
                    ColumnType::Boolean => out.push_bind_param::<Array<Bool>, _>(&sql_values),
                    ColumnType::Bytes => out.push_bind_param::<Array<Binary>, _>(&sql_values),
                    ColumnType::Int => out.push_bind_param::<Array<Integer>, _>(&sql_values),
                    ColumnType::String => out.push_bind_param::<Array<Text>, _>(&sql_values),
                    ColumnType::Enum(enum_type) => {
                        out.push_bind_param::<Array<Text>, _>(&sql_values)?;
                        out.push_sql("::");
                        out.push_sql(enum_type.name.as_str());
                        out.push_sql("[]");
                        Ok(())
                    }
                    // TSVector will only be in a Value::List() for inserts so "to_tsvector" can always be used here
                    ColumnType::TSVector(config) => {
                        if sql_values.is_empty() {
                            out.push_sql("''::tsvector");
                        } else {
                            out.push_sql("(");
                            for (i, value) in sql_values.iter().enumerate() {
                                if i > 0 {
                                    out.push_sql(") || ");
                                }
                                out.push_sql("to_tsvector(");
                                out.push_bind_param::<Text, _>(
                                    &config.language.as_str().to_string(),
                                )?;
                                out.push_sql("::regconfig, ");
                                out.push_bind_param::<Text, _>(&value)?;
                            }
                            out.push_sql("))");
                        }

                        Ok(())
                    }
                    ColumnType::BytesId => out.push_bind_param::<Array<Binary>, _>(&sql_values),
                }
            }
            Value::Null => {
                out.push_sql("null");
                Ok(())
            }
            Value::Bytes(b) => out.push_bind_param::<Binary, _>(&b.as_slice()),
            Value::BigInt(i) => {
                out.push_bind_param::<Text, _>(&i.to_string())?;
                out.push_sql("::numeric");
                Ok(())
            }
        }
    }
}

#[derive(Copy, Clone, PartialEq)]
enum Comparison {
    Less,
    LessOrEqual,
    Equal,
    NotEqual,
    GreaterOrEqual,
    Greater,
    Match,
}

impl Comparison {
    fn as_str(&self) -> &str {
        use Comparison::*;
        match self {
            Less => " < ",
            LessOrEqual => " <= ",
            Equal => " = ",
            NotEqual => " != ",
            GreaterOrEqual => " >= ",
            Greater => " > ",
            Match => " @@ ",
        }
    }
}

/// Produce a comparison between the string column `column` and the string
/// value `text` that makes it obvious to Postgres' optimizer that it can
/// first consult the partial index on `left(column, STRING_PREFIX_SIZE)`
/// instead of going straight to a sequential scan of the underlying table.
/// We do this by writing the comparison `column op text` in a way that
/// involves `left(column, STRING_PREFIX_SIZE)`
#[derive(Constructor)]
struct PrefixComparison<'a> {
    op: Comparison,
    column: &'a Column,
    text: &'a Value,
}

impl<'a> PrefixComparison<'a> {
    fn push_column_prefix(column: &Column, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.push_sql("left(");
        out.push_identifier(column.name.as_str())?;
        out.push_sql(", ");
        out.push_sql(&STRING_PREFIX_SIZE.to_string());
        out.push_sql(")");
        Ok(())
    }

    fn push_value_prefix(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.push_sql("left(");
        QueryValue(self.text, &self.column.column_type).walk_ast(out.reborrow())?;
        out.push_sql(", ");
        out.push_sql(&STRING_PREFIX_SIZE.to_string());
        out.push_sql(")");
        Ok(())
    }

    fn push_prefix_cmp(&self, op: Comparison, mut out: AstPass<Pg>) -> QueryResult<()> {
        Self::push_column_prefix(self.column, out.reborrow())?;
        out.push_sql(op.as_str());
        self.push_value_prefix(out.reborrow())
    }

    fn push_full_cmp(&self, op: Comparison, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.push_identifier(self.column.name.as_str())?;
        out.push_sql(op.as_str());
        QueryValue(self.text, &self.column.column_type).walk_ast(out)
    }
}

impl<'a> QueryFragment<Pg> for PrefixComparison<'a> {
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        use Comparison::*;

        // For the various comparison operators, we want to write the condition
        // `column op text` in a way that lets Postgres use the index on
        // `left(column, STRING_PREFIX_SIZE)`. If at all possible, we also want
        // the condition in a form that only uses the index, or if that's not
        // possible, in a form where Postgres can first reduce the number of
        // rows where a full comparison between `column` and `text` is needed
        // by consulting the index.
        //
        // To ease notation, let `N = STRING_PREFIX_SIZE` and write a string stored in
        // `column` as `uv` where `len(u) <= N`; that means that `v` is only
        // nonempty if `len(uv) > N`. We similarly split `text` into `st` where
        // `len(s) <= N`. In other words, `u = left(column, N)` and
        // `s = left(text, N)`
        //
        // In all these comparisons, if `len(st) <= N - 1`, we can reduce
        // checking `uv op st` to `u op s`, since in that case `t` is the empty
        // string, we have `uv op s`. If `len(uv) <= N - 1`, then `v` will
        // also be the empty string. If `len(uv) >= N`, then `len(u) = N`,
        // and since `u` will be one character longer than `s`, that character
        // will decide the outcome of `u op s`, even if `u` and `s` agree on
        // the first `N-1` characters.
        //
        // For equality, we can expand `uv = st` into `u = s && uv = st` which
        // lets Postgres use the index for the first comparison, and `uv = st`
        // only needs to be checked for rows that pass the check on the index.
        //
        // For inequality, we can write `uv != st` as `u != s || uv != st`,
        // but that doesn't buy us much since Postgres always needs to check
        // `uv != st`, for which the index is of little help.
        //
        // For `op` either `<` or `>`, we have
        //   uv op st <=> u op s || u = s && uv op st
        //
        // For `op` either `<=` or `>=`, we can write (using '<=' as an example)
        //   uv <= st <=> u < s || u = s && uv <= st
        let large = if let Value::String(s) = self.text {
            // We need to check the entire string
            s.len() > STRING_PREFIX_SIZE - 1
        } else {
            unreachable!("text columns are only ever compared to strings");
        };
        match self.op {
            Equal => {
                if large {
                    out.push_sql("(");
                    self.push_prefix_cmp(self.op, out.reborrow())?;
                    out.push_sql(" and ");
                    self.push_full_cmp(self.op, out.reborrow())?;
                    out.push_sql(")");
                } else {
                    self.push_prefix_cmp(self.op, out.reborrow())?;
                }
            }
            Match => {
                self.push_full_cmp(self.op, out.reborrow())?;
            }
            NotEqual => {
                if large {
                    self.push_full_cmp(self.op, out.reborrow())?;
                } else {
                    self.push_prefix_cmp(self.op, out.reborrow())?;
                }
            }
            LessOrEqual | Less | GreaterOrEqual | Greater => {
                let prefix_op = match self.op {
                    LessOrEqual => Less,
                    GreaterOrEqual => Greater,
                    op => op,
                };
                if large {
                    out.push_sql("(");
                    self.push_prefix_cmp(prefix_op, out.reborrow())?;
                    out.push_sql(" or (");
                    self.push_prefix_cmp(Equal, out.reborrow())?;
                    out.push_sql(" and ");
                    self.push_full_cmp(self.op, out.reborrow())?;
                    out.push_sql("))");
                } else {
                    self.push_prefix_cmp(self.op, out.reborrow())?;
                }
            }
        }
        Ok(())
    }
}

/// A `QueryFilter` adds the conditions represented by the `filter` to
/// the `where` clause of a SQL query. The attributes mentioned in
/// the `filter` must all come from the given `table`, which is used to
/// map GraphQL names to column names, and to determine the type of the
/// column an attribute refers to
#[derive(Debug, Clone)]
pub struct QueryFilter<'a> {
    filter: &'a EntityFilter,
    table: &'a Table,
}

impl<'a> QueryFilter<'a> {
    pub fn new(filter: &'a EntityFilter, table: &'a Table) -> Result<Self, StoreError> {
        Self::valid_attributes(filter, table)?;
        Ok(QueryFilter { filter, table })
    }

    fn valid_attributes(filter: &'a EntityFilter, table: &'a Table) -> Result<(), StoreError> {
        use EntityFilter::*;
        match filter {
            And(filters) | Or(filters) => {
                for filter in filters {
                    Self::valid_attributes(filter, table)?;
                }
            }

            Contains(attr, _)
            | NotContains(attr, _)
            | Equal(attr, _)
            | Not(attr, _)
            | GreaterThan(attr, _)
            | LessThan(attr, _)
            | GreaterOrEqual(attr, _)
            | LessOrEqual(attr, _)
            | In(attr, _)
            | NotIn(attr, _)
            | StartsWith(attr, _)
            | NotStartsWith(attr, _)
            | EndsWith(attr, _)
            | NotEndsWith(attr, _) => {
                table.column_for_field(attr)?;
            }
        }
        Ok(())
    }

    fn with(&self, filter: &'a EntityFilter) -> Self {
        QueryFilter {
            filter,
            table: self.table,
        }
    }

    fn column(&self, attribute: &Attribute) -> &'a Column {
        self.table
            .column_for_field(attribute)
            .expect("the constructor already checked that all attribute names are valid")
    }

    fn binary_op(
        &self,
        filters: &Vec<EntityFilter>,
        op: &str,
        on_empty: &str,
        mut out: AstPass<Pg>,
    ) -> QueryResult<()> {
        if !filters.is_empty() {
            out.push_sql("(");
            for (i, filter) in filters.iter().enumerate() {
                if i > 0 {
                    out.push_sql(op);
                }
                self.with(&filter).walk_ast(out.reborrow())?;
            }
            out.push_sql(")");
        } else {
            out.push_sql(on_empty);
        }
        Ok(())
    }

    fn contains(
        &self,
        attribute: &Attribute,
        value: &Value,
        negated: bool,
        mut out: AstPass<Pg>,
    ) -> QueryResult<()> {
        let column = self.column(attribute);

        match value {
            Value::String(s) => {
                out.push_identifier(column.name.as_str())?;
                if negated {
                    out.push_sql(" not like ");
                } else {
                    out.push_sql(" like ")
                };
                if s.starts_with('%') || s.ends_with('%') {
                    out.push_bind_param::<Text, _>(s)?;
                } else {
                    let s = format!("%{}%", s);
                    out.push_bind_param::<Text, _>(&s)?;
                }
            }
            Value::Bytes(b) => {
                out.push_sql("position(");
                out.push_bind_param::<Binary, _>(&b.as_slice())?;
                out.push_sql(" in ");
                out.push_identifier(column.name.as_str())?;
                if negated {
                    out.push_sql(") = 0")
                } else {
                    out.push_sql(") > 0");
                }
            }
            Value::List(_) => {
                if negated {
                    out.push_sql(" not ");
                    out.push_identifier(column.name.as_str())?;
                    out.push_sql(" && ");
                } else {
                    out.push_identifier(column.name.as_str())?;
                    out.push_sql(" @> ");
                }
                QueryValue(value, &column.column_type).walk_ast(out)?;
            }
            Value::Null
            | Value::BigDecimal(_)
            | Value::Int(_)
            | Value::Bool(_)
            | Value::BigInt(_) => {
                let filter = match negated {
                    false => "contains",
                    true => "not_contains",
                };
                return Err(UnsupportedFilter {
                    filter: filter.to_owned(),
                    value: value.clone(),
                }
                .into());
            }
        }
        Ok(())
    }

    fn equals(
        &self,
        attribute: &Attribute,
        value: &Value,
        op: Comparison,
        mut out: AstPass<Pg>,
    ) -> QueryResult<()> {
        let column = self.column(attribute);

        if column.is_text() && value.is_string() {
            PrefixComparison::new(op, column, value).walk_ast(out.reborrow())?;
        } else if column.is_fulltext() {
            out.push_identifier(column.name.as_str())?;
            out.push_sql(Comparison::Match.as_str());
            QueryValue(value, &column.column_type).walk_ast(out)?;
        } else {
            out.push_identifier(column.name.as_str())?;

            match value {
                Value::String(_)
                | Value::BigInt(_)
                | Value::Bool(_)
                | Value::Bytes(_)
                | Value::BigDecimal(_)
                | Value::Int(_)
                | Value::List(_) => {
                    out.push_sql(op.as_str());
                    QueryValue(value, &column.column_type).walk_ast(out)?;
                }
                Value::Null => {
                    use Comparison as c;
                    match op {
                        c::Equal => out.push_sql(" is null"),
                        c::NotEqual => out.push_sql(" is not null"),
                        _ => unreachable!("we only call equals with '=' or '!='"),
                    }
                }
            }
        }
        Ok(())
    }

    fn compare(
        &self,
        attribute: &Attribute,
        value: &Value,
        op: Comparison,
        mut out: AstPass<Pg>,
    ) -> QueryResult<()> {
        let column = self.column(attribute);

        if column.is_text() && value.is_string() {
            PrefixComparison::new(op, column, value).walk_ast(out.reborrow())?;
        } else {
            out.push_identifier(column.name.as_str())?;
            out.push_sql(op.as_str());
            match value {
                Value::BigInt(_) | Value::BigDecimal(_) | Value::Int(_) | Value::String(_) => {
                    QueryValue(value, &column.column_type).walk_ast(out)?
                }
                Value::Bool(_) | Value::Bytes(_) | Value::List(_) | Value::Null => {
                    return Err(UnsupportedFilter {
                        filter: op.as_str().to_owned(),
                        value: value.clone(),
                    }
                    .into());
                }
            }
        }
        Ok(())
    }

    fn in_array(
        &self,
        attribute: &Attribute,
        values: &Vec<Value>,
        negated: bool,
        mut out: AstPass<Pg>,
    ) -> QueryResult<()> {
        let column = self.column(attribute);

        if values.is_empty() {
            out.push_sql("false");
            return Ok(());
        }

        // NULLs in SQL are very special creatures, and we need to treat
        // them special. For non-NULL values, we generate
        //   attribute {in|not in} (value1, value2, ...)
        // and for NULL values we generate
        //   attribute {is|is not} null
        // If we have both NULL and non-NULL values we join these
        // two clauses with OR.
        //
        // Note that when we have no non-NULL values at all, we must
        // not generate `attribute {in|not in} ()` since the empty `()`
        // is a syntax error
        //
        // Because we checked above, one of these two will be true
        let have_nulls = values.iter().any(|value| value == &Value::Null);
        let have_non_nulls = values.iter().any(|value| value != &Value::Null);

        if have_nulls && have_non_nulls {
            out.push_sql("(");
        }

        if have_nulls {
            out.push_identifier(column.name.as_str())?;
            if negated {
                out.push_sql(" is not null");
            } else {
                out.push_sql(" is null")
            }
        }

        if have_nulls && have_non_nulls {
            out.push_sql(" or ");
        }

        if have_non_nulls {
            if column.is_text()
                && values.iter().all(|v| match v {
                    Value::String(s) => s.len() <= STRING_PREFIX_SIZE - 1,
                    _ => false,
                })
            {
                // If all values are shorter than STRING_PREFIX_SIZE - 1,
                // only check the prefix of the column; that's a fairly common
                // case and we present it in the best possible way for
                // Postgres' query optimizer
                // See PrefixComparison for a more detailed discussion of what
                // is happening here
                PrefixComparison::push_column_prefix(&column, out.reborrow())?;
            } else {
                out.push_identifier(column.name.as_str())?;
            }
            if negated {
                out.push_sql(" not in (");
            } else {
                out.push_sql(" in (");
            }
            for (i, value) in values
                .iter()
                .filter(|value| value != &&Value::Null)
                .enumerate()
            {
                if i > 0 {
                    out.push_sql(", ");
                }
                QueryValue(&value, &column.column_type).walk_ast(out.reborrow())?;
            }
            out.push_sql(")");
        }

        if have_nulls && have_non_nulls {
            out.push_sql(")");
        }
        Ok(())
    }

    fn starts_or_ends_with(
        &self,
        attribute: &Attribute,
        value: &Value,
        op: &str,
        starts_with: bool,
        mut out: AstPass<Pg>,
    ) -> QueryResult<()> {
        let column = self.column(attribute);

        out.push_identifier(column.name.as_str())?;
        out.push_sql(op);
        match value {
            Value::String(s) => {
                let s = if starts_with {
                    format!("{}%", s)
                } else {
                    format!("%{}", s)
                };
                out.push_bind_param::<Text, _>(&s)?
            }
            Value::Bool(_)
            | Value::BigInt(_)
            | Value::Bytes(_)
            | Value::BigDecimal(_)
            | Value::Int(_)
            | Value::List(_)
            | Value::Null => {
                return Err(UnsupportedFilter {
                    filter: op.to_owned(),
                    value: value.clone(),
                }
                .into());
            }
        }
        Ok(())
    }
}

impl<'a> QueryFragment<Pg> for QueryFilter<'a> {
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();

        use Comparison as c;
        use EntityFilter::*;
        match &self.filter {
            And(filters) => self.binary_op(filters, " and ", " true ", out)?,
            Or(filters) => self.binary_op(filters, " or ", " false ", out)?,

            Contains(attr, value) => self.contains(attr, value, false, out)?,
            NotContains(attr, value) => self.contains(attr, value, true, out)?,

            Equal(attr, value) => self.equals(attr, value, c::Equal, out)?,
            Not(attr, value) => self.equals(attr, value, c::NotEqual, out)?,

            GreaterThan(attr, value) => self.compare(attr, value, c::Greater, out)?,
            LessThan(attr, value) => self.compare(attr, value, c::Less, out)?,
            GreaterOrEqual(attr, value) => self.compare(attr, value, c::GreaterOrEqual, out)?,
            LessOrEqual(attr, value) => self.compare(attr, value, c::LessOrEqual, out)?,

            In(attr, values) => self.in_array(attr, values, false, out)?,
            NotIn(attr, values) => self.in_array(attr, values, true, out)?,

            StartsWith(attr, value) => {
                self.starts_or_ends_with(attr, value, " like ", true, out)?
            }
            NotStartsWith(attr, value) => {
                self.starts_or_ends_with(attr, value, " not like ", true, out)?
            }
            EndsWith(attr, value) => self.starts_or_ends_with(attr, value, " like ", false, out)?,
            NotEndsWith(attr, value) => {
                self.starts_or_ends_with(attr, value, " not like ", false, out)?
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Constructor)]
pub struct FindQuery<'a> {
    table: &'a Table,
    id: &'a str,
    block: BlockNumber,
}

impl<'a> QueryFragment<Pg> for FindQuery<'a> {
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();

        // Generate
        //    select '..' as entity, to_jsonb(e.*) as data
        //      from schema.table e where id = $1
        out.push_sql("select ");
        out.push_bind_param::<Text, _>(&self.table.object.as_str())?;
        out.push_sql(" as entity, to_jsonb(e.*) as data\n");
        out.push_sql("  from ");
        out.push_sql(self.table.qualified_name.as_str());
        out.push_sql(" e\n where ");
        self.table.primary_key().eq(&self.id, &mut out)?;
        out.push_sql(" and ");
        BlockRangeContainsClause::new(&self.table, "e.", self.block).walk_ast(out)
    }
}

impl<'a> QueryId for FindQuery<'a> {
    type QueryId = ();

    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<'a> LoadQuery<PgConnection, EntityData> for FindQuery<'a> {
    fn internal_load(self, conn: &PgConnection) -> QueryResult<Vec<EntityData>> {
        conn.query_by_name(&self)
    }
}

impl<'a, Conn> RunQueryDsl<Conn> for FindQuery<'a> {}

#[derive(Debug, Clone, Constructor)]
pub struct FindManyQuery<'a> {
    pub(crate) _namespace: &'a Namespace,
    pub(crate) tables: Vec<&'a Table>,

    // Maps object name to ids.
    pub(crate) ids_for_type: &'a BTreeMap<&'a EntityType, Vec<&'a str>>,
    pub(crate) block: BlockNumber,
}

impl<'a> QueryFragment<Pg> for FindManyQuery<'a> {
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();

        // Generate
        //    select $object0 as entity, to_jsonb(e.*) as data
        //      from schema.<table0> e where {id.is_in($ids0)}
        //    union all
        //    select $object1 as entity, to_jsonb(e.*) as data
        //      from schema.<table1> e where {id.is_in($ids1))
        //    union all
        //    ...
        for (i, table) in self.tables.iter().enumerate() {
            if i > 0 {
                out.push_sql("\nunion all\n");
            }
            out.push_sql("select ");
            out.push_bind_param::<Text, _>(&table.object.as_str())?;
            out.push_sql(" as entity, to_jsonb(e.*) as data\n");
            out.push_sql("  from ");
            out.push_sql(table.qualified_name.as_str());
            out.push_sql(" e\n where ");
            table
                .primary_key()
                .is_in(&self.ids_for_type[&table.object], &mut out)?;
            out.push_sql(" and ");
            BlockRangeContainsClause::new(&table, "e.", self.block).walk_ast(out.reborrow())?;
        }
        Ok(())
    }
}

impl<'a> QueryId for FindManyQuery<'a> {
    type QueryId = ();

    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<'a> LoadQuery<PgConnection, EntityData> for FindManyQuery<'a> {
    fn internal_load(self, conn: &PgConnection) -> QueryResult<Vec<EntityData>> {
        conn.query_by_name(&self)
    }
}

impl<'a, Conn> RunQueryDsl<Conn> for FindManyQuery<'a> {}

#[derive(Debug)]
pub struct InsertQuery<'a> {
    table: &'a Table,
    entities: &'a [(&'a EntityKey, Cow<'a, Entity>)],
    unique_columns: Vec<&'a Column>,
    block: BlockNumber,
}

impl<'a> InsertQuery<'a> {
    pub fn new(
        table: &'a Table,
        entities: &'a mut [(&'a EntityKey, Cow<Entity>)],
        block: BlockNumber,
    ) -> Result<InsertQuery<'a>, StoreError> {
        for (entity_key, entity) in entities.iter_mut() {
            for column in table.columns.iter() {
                match column.fulltext_fields.as_ref() {
                    Some(fields) => {
                        let fulltext_field_values = fields
                            .iter()
                            .filter_map(|field| entity.get(field))
                            .cloned()
                            .collect::<Vec<Value>>();
                        if !fulltext_field_values.is_empty() {
                            entity.to_mut().insert(
                                column.field.to_string(),
                                Value::List(fulltext_field_values),
                            );
                        }
                    }
                    None => (),
                }
                if !column.is_nullable() && !entity.contains_key(&column.field) {
                    return Err(StoreError::QueryExecutionError(format!(
                    "can not insert entity {}[{}] since value for non-nullable attribute {} is missing. \
                     To fix this, mark the attribute as nullable in the GraphQL schema or change the \
                     mapping code to always set this attribute.",
                    entity_key.entity_type, entity_key.entity_id, column.field
                )));
                }
            }
        }
        let unique_columns = InsertQuery::unique_columns(table, entities);

        Ok(InsertQuery {
            table,
            entities,
            unique_columns,
            block,
        })
    }

    /// Build the column name list using the subset of all keys among present entities.
    fn unique_columns(
        table: &'a Table,
        entities: &'a [(&'a EntityKey, Cow<'a, Entity>)],
    ) -> Vec<&'a Column> {
        let mut hashmap = HashMap::new();
        for (_key, entity) in entities.iter() {
            for column in &table.columns {
                if entity.get(&column.field).is_some() {
                    hashmap.entry(column.name.as_str()).or_insert(column);
                }
            }
        }
        hashmap.into_iter().map(|(_key, value)| value).collect()
    }
}

impl<'a> QueryFragment<Pg> for InsertQuery<'a> {
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();

        // Construct a query
        //   insert into schema.table(column, ...)
        //   values
        //   (a, b, c),
        //   (d, e, f)
        //   [...]
        //   (x, y, z)
        //
        // and convert and bind the entity's values into it
        out.push_sql("insert into ");
        out.push_sql(self.table.qualified_name.as_str());

        out.push_sql("(");

        for &column in &self.unique_columns {
            out.push_identifier(column.name.as_str())?;
            out.push_sql(", ");
        }
        out.push_identifier(BLOCK_RANGE_COLUMN)?;

        out.push_sql(") values\n");

        // Use a `Peekable` iterator to help us decide how to finalize each line.
        let mut iter = self.entities.iter().map(|(_key, entity)| entity).peekable();
        while let Some(entity) = iter.next() {
            out.push_sql("(");
            for column in &self.unique_columns {
                // If the column name is not within this entity's fields, we will issue the
                // null value in its place
                if let Some(value) = entity.get(&column.field) {
                    QueryValue(value, &column.column_type).walk_ast(out.reborrow())?;
                } else {
                    out.push_sql("null");
                }
                out.push_sql(", ");
            }
            let block_range: BlockRange = (self.block..).into();
            out.push_bind_param::<Range<Integer>, _>(&block_range)?;
            out.push_sql(")");

            // finalize line according to remaining entities to insert
            if iter.peek().is_some() {
                out.push_sql(",\n");
            }
        }
        out.push_sql("\nreturning ");
        out.push_sql(PRIMARY_KEY_COLUMN);
        out.push_sql("::text");

        Ok(())
    }
}

impl<'a> QueryId for InsertQuery<'a> {
    type QueryId = ();

    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<'a> LoadQuery<PgConnection, ReturnedEntityData> for InsertQuery<'a> {
    fn internal_load(self, conn: &PgConnection) -> QueryResult<Vec<ReturnedEntityData>> {
        conn.query_by_name(&self)
            .map(|data| ReturnedEntityData::bytes_as_str(&self.table, data))
    }
}

impl<'a, Conn> RunQueryDsl<Conn> for InsertQuery<'a> {}

#[derive(Debug, Clone)]
pub struct ConflictingEntityQuery<'a> {
    _layout: &'a Layout,
    tables: Vec<&'a Table>,
    entity_id: &'a str,
}
impl<'a> ConflictingEntityQuery<'a> {
    pub fn new(
        layout: &'a Layout,
        entities: Vec<EntityType>,
        entity_id: &'a str,
    ) -> Result<Self, StoreError> {
        let tables = entities
            .iter()
            .map(|entity| layout.table_for_entity(entity).map(|table| table.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ConflictingEntityQuery {
            _layout: layout,
            tables,
            entity_id,
        })
    }
}

impl<'a> QueryFragment<Pg> for ConflictingEntityQuery<'a> {
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();

        // Construct a query
        //   select 'Type1' as entity from schema.table1 where id = $1
        //   union all
        //   select 'Type2' as entity from schema.table2 where id = $1
        //   union all
        //   ...
        for (i, table) in self.tables.iter().enumerate() {
            if i > 0 {
                out.push_sql("\nunion all\n");
            }
            out.push_sql("select ");
            out.push_bind_param::<Text, _>(&table.object.as_str())?;
            out.push_sql(" as entity from ");
            out.push_sql(table.qualified_name.as_str());
            out.push_sql(" where id = ");
            out.push_bind_param::<Text, _>(&self.entity_id)?;
        }
        Ok(())
    }
}

impl<'a> QueryId for ConflictingEntityQuery<'a> {
    type QueryId = ();

    const HAS_STATIC_QUERY_ID: bool = false;
}

#[derive(QueryableByName)]
pub struct ConflictingEntityData {
    #[sql_type = "Text"]
    pub entity: String,
}

impl<'a> LoadQuery<PgConnection, ConflictingEntityData> for ConflictingEntityQuery<'a> {
    fn internal_load(self, conn: &PgConnection) -> QueryResult<Vec<ConflictingEntityData>> {
        conn.query_by_name(&self)
    }
}

impl<'a, Conn> RunQueryDsl<Conn> for ConflictingEntityQuery<'a> {}

/// A string where we have checked that it is safe to embed it literally
/// in a string in a SQL query. In particular, we have escaped any use
/// of the string delimiter `'`.
///
/// This is only needed for `ParentIds::List` since we can't send those to
/// the database as a bind variable, and therefore need to embed them in
/// the query literally
#[derive(Debug, Clone)]
struct SafeString(String);

/// A `ParentLink` where we've made sure for the `List` variant that each
/// `Vec<Option<String>>` has the same length
/// Use the provided constructors to make sure this invariant holds
#[derive(Debug, Clone)]
enum ParentIds {
    List(Vec<Vec<Option<SafeString>>>),
    Scalar(Vec<String>),
}

impl ParentIds {
    fn new(link: ParentLink) -> Self {
        match link {
            ParentLink::Scalar(child_ids) => ParentIds::Scalar(child_ids),
            ParentLink::List(child_ids) => {
                // Postgres will only accept child_ids, which is a Vec<Vec<String>>
                // if all Vec<String> are the same length. We therefore pad
                // shorter ones with None, which become nulls in the database
                let maxlen = child_ids.iter().map(|ids| ids.len()).max().unwrap_or(0);
                let child_ids = child_ids
                    .into_iter()
                    .map(|ids| {
                        let mut ids: Vec<_> = ids
                            .into_iter()
                            .map(|s| {
                                if s.contains('\'') {
                                    SafeString(s.replace('\'', "''"))
                                } else {
                                    SafeString(s)
                                }
                            })
                            .map(Some)
                            .collect();
                        ids.resize_with(maxlen, || None);
                        ids
                    })
                    .collect();
                ParentIds::List(child_ids)
            }
        }
    }
}

/// An `EntityLink` where we've resolved the entity type and attribute to the
/// corresponding table and column
#[derive(Debug, Clone)]
enum TableLink<'a> {
    Direct(&'a Column, ChildMultiplicity),
    Parent(ParentIds),
}

impl<'a> TableLink<'a> {
    fn new(child_table: &'a Table, link: EntityLink) -> Result<Self, QueryExecutionError> {
        match link {
            EntityLink::Direct(attribute, multiplicity) => {
                let column = child_table.column_for_field(attribute.name())?;
                Ok(TableLink::Direct(column, multiplicity))
            }
            EntityLink::Parent(parent_link) => Ok(TableLink::Parent(ParentIds::new(parent_link))),
        }
    }
}

/// When we expand the parents for a specific query for children, we
/// sometimes (aka interfaces) need to restrict them to a specific
/// parent `q.id` that an outer query has already set up. In all other
/// cases, we restrict the children to the top n by ordering by a specific
/// sort key and limiting
#[derive(Copy, Clone)]
enum ParentLimit<'a> {
    /// Limit children to a specific parent
    Outer,
    /// Limit children by sorting and picking top n
    Ranked(&'a SortKey<'a>, &'a FilterRange),
}

impl<'a> ParentLimit<'a> {
    fn filter(&self, out: &mut AstPass<Pg>) {
        match self {
            ParentLimit::Outer => out.push_sql(" and q.id = p.id"),
            ParentLimit::Ranked(_, _) => (),
        }
    }

    fn restrict(&self, out: &mut AstPass<Pg>) -> QueryResult<()> {
        if let ParentLimit::Ranked(sort_key, range) = self {
            out.push_sql(" ");
            sort_key.order_by(out)?;
            range.walk_ast(out.reborrow())?;
        }
        Ok(())
    }

    /// Include a 'limit {num_parents}+1' clause for single-object queries
    /// if that is needed
    fn single_limit(&self, num_parents: usize, out: &mut AstPass<Pg>) {
        match self {
            ParentLimit::Ranked(_, _) => {
                out.push_sql(" limit ");
                out.push_sql(&(num_parents + 1).to_string());
            }
            ParentLimit::Outer => {
                // limiting is taken care of in a wrapper around
                // the query we are currently building
            }
        }
    }
}

/// This is the parallel to `EntityWindow`, with names translated to
/// the relational layout, and checked against it
#[derive(Debug, Clone)]
pub struct FilterWindow<'a> {
    /// The table from which we take entities
    table: &'a Table,
    /// The overall filter for the entire query
    query_filter: Option<QueryFilter<'a>>,
    /// The parent ids we are interested in. The type in the database
    /// for these is determined by the `IdType` of the parent table. Since
    /// we always compare these ids with a column in `table`, and that
    /// column must have the same type as the primary key of the parent
    /// table, we can deduce the correct `IdType` that way
    ids: Vec<String>,
    /// How to filter by a set of parents
    link: TableLink<'a>,
    column_names: AttributeNames,
}

impl<'a> FilterWindow<'a> {
    fn new(
        layout: &'a Layout,
        window: EntityWindow,
        query_filter: Option<&'a EntityFilter>,
    ) -> Result<Self, QueryExecutionError> {
        let EntityWindow {
            child_type,
            ids,
            link,
            column_names,
        } = window;
        let table = layout.table_for_entity(&child_type).map(|rc| rc.as_ref())?;

        // Confidence check: ensure that all selected column names exist in the table
        if let AttributeNames::Select(ref selected_field_names) = column_names {
            for field in selected_field_names {
                let _ = table.column_for_field(&field)?;
            }
        }

        let query_filter = query_filter
            .map(|filter| QueryFilter::new(filter, table))
            .transpose()?;
        let link = TableLink::new(table, link)?;
        Ok(FilterWindow {
            table,
            query_filter,
            ids,
            link,
            column_names,
        })
    }

    fn and_filter(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        if let Some(filter) = &self.query_filter {
            out.push_sql("\n   and ");
            filter.walk_ast(out)?
        }
        Ok(())
    }

    fn children_type_a(
        &self,
        column: &Column,
        limit: ParentLimit<'_>,
        block: BlockNumber,
        out: &mut AstPass<Pg>,
    ) -> QueryResult<()> {
        assert!(column.is_list());

        // Generate
        //      from unnest({parent_ids}) as p(id)
        //           cross join lateral
        //           (select {column names}
        //              from children c
        //             where p.id = any(c.{parent_field})
        //               and .. other conditions on c ..
        //             order by c.{sort_key}
        //             limit {first} offset {skip}) c
        //     order by c.{sort_key}

        out.push_sql("\n/* children_type_a */  from unnest(");
        column.bind_ids(&self.ids, out)?;
        out.push_sql(") as p(id) cross join lateral (select ");
        write_column_names(&self.column_names, &self.table, out)?;
        out.push_sql(" from ");
        out.push_sql(self.table.qualified_name.as_str());
        out.push_sql(" c where ");
        BlockRangeContainsClause::new(&self.table, "c.", block).walk_ast(out.reborrow())?;
        limit.filter(out);
        out.push_sql(" and p.id = any(c.");
        out.push_identifier(column.name.as_str())?;
        out.push_sql(")");
        self.and_filter(out.reborrow())?;
        limit.restrict(out)?;
        out.push_sql(") c");
        Ok(())
    }

    fn child_type_a(
        &self,
        column: &Column,
        limit: ParentLimit<'_>,
        block: BlockNumber,
        out: &mut AstPass<Pg>,
    ) -> QueryResult<()> {
        assert!(column.is_list());

        // Generate
        //      from unnest({parent_ids}) as p(id),
        //           children c
        //     where c.{parent_field} @> array[p.id]
        //       and c.{parent_field} && {parent_ids}
        //       and .. other conditions on c ..
        //     limit {parent_ids.len} + 1
        //
        // The redundant `&&` clause is only added when we have fewer than
        // TYPEA_BATCH_SIZE children and helps Postgres to narrow down the
        // rows it needs to pick from `children` to join with `p(id)`
        out.push_sql("\n/* child_type_a */ from unnest(");
        column.bind_ids(&self.ids, out)?;
        out.push_sql(") as p(id), ");
        out.push_sql(self.table.qualified_name.as_str());
        out.push_sql(" c where ");
        BlockRangeContainsClause::new(&self.table, "c.", block).walk_ast(out.reborrow())?;
        limit.filter(out);
        out.push_sql(" and c.");
        out.push_identifier(column.name.as_str())?;
        out.push_sql(" @> array[p.id]");
        if self.ids.len() < *TYPEA_BATCH_SIZE {
            out.push_sql(" and c.");
            out.push_identifier(column.name.as_str())?;
            out.push_sql(" && ");
            column.bind_ids(&self.ids, out)?;
        }
        self.and_filter(out.reborrow())?;
        limit.single_limit(self.ids.len(), out);
        Ok(())
    }

    fn children_type_b(
        &self,
        column: &Column,
        limit: ParentLimit<'_>,
        block: BlockNumber,
        out: &mut AstPass<Pg>,
    ) -> QueryResult<()> {
        assert!(!column.is_list());

        // Generate
        //      from unnest({parent_ids}) as p(id)
        //           cross join lateral
        //           (select {column names}
        //              from children c
        //             where p.id = c.{parent_field}
        //               and .. other conditions on c ..
        //             order by c.{sort_key}
        //             limit {first} offset {skip}) c
        //     order by c.{sort_key}

        out.push_sql("\n/* children_type_b */  from unnest(");
        column.bind_ids(&self.ids, out)?;
        out.push_sql(") as p(id) cross join lateral (select ");
        write_column_names(&self.column_names, &self.table, out)?;
        out.push_sql(" from ");
        out.push_sql(self.table.qualified_name.as_str());
        out.push_sql(" c where ");
        BlockRangeContainsClause::new(&self.table, "c.", block).walk_ast(out.reborrow())?;
        limit.filter(out);
        out.push_sql(" and p.id = c.");
        out.push_identifier(column.name.as_str())?;
        self.and_filter(out.reborrow())?;
        limit.restrict(out)?;
        out.push_sql(") c");
        Ok(())
    }

    fn child_type_b(
        &self,
        column: &Column,
        limit: ParentLimit<'_>,
        block: BlockNumber,
        out: &mut AstPass<Pg>,
    ) -> QueryResult<()> {
        assert!(!column.is_list());

        // Generate
        //      from unnest({parent_ids}) as p(id), children c
        //     where c.{parent_field} = p.id
        //       and .. other conditions on c ..
        //     limit {parent_ids.len} + 1

        out.push_sql("\n/* child_type_b */  from unnest(");
        column.bind_ids(&self.ids, out)?;
        out.push_sql(") as p(id), ");
        out.push_sql(self.table.qualified_name.as_str());
        out.push_sql(" c where ");
        BlockRangeContainsClause::new(&self.table, "c.", block).walk_ast(out.reborrow())?;
        limit.filter(out);
        out.push_sql(" and p.id = c.");
        out.push_identifier(column.name.as_str())?;
        self.and_filter(out.reborrow())?;
        limit.single_limit(self.ids.len(), out);
        Ok(())
    }

    fn children_type_c(
        &self,
        child_ids: &Vec<Vec<Option<SafeString>>>,
        limit: ParentLimit<'_>,
        block: BlockNumber,
        out: &mut AstPass<Pg>,
    ) -> QueryResult<()> {
        // Generate
        //      from rows from (unnest({parent_ids}), reduce_dim({child_id_matrix}))
        //                  as p(id, child_ids)
        //           cross join lateral
        //           (select {column names}
        //              from children c
        //             where c.id = any(p.child_ids)
        //               and .. other conditions on c ..
        //             order by c.{sort_key}
        //             limit {first} offset {skip}) c
        //     order by c.{sort_key}

        out.push_sql("\n/* children_type_c */  from ");
        out.push_sql("rows from (unnest(");
        out.push_bind_param::<Array<Text>, _>(&self.ids)?;
        out.push_sql("), reduce_dim(");
        self.table.primary_key().push_matrix(&child_ids, out)?;
        out.push_sql(")) as p(id, child_ids)");
        out.push_sql(" cross join lateral (select ");
        write_column_names(&self.column_names, &self.table, out)?;
        out.push_sql(" from ");
        out.push_sql(self.table.qualified_name.as_str());
        out.push_sql(" c where ");
        BlockRangeContainsClause::new(&self.table, "c.", block).walk_ast(out.reborrow())?;
        limit.filter(out);
        out.push_sql(" and c.id = any(p.child_ids)");
        self.and_filter(out.reborrow())?;
        limit.restrict(out)?;
        out.push_sql(") c");
        Ok(())
    }

    fn child_type_d(
        &self,
        child_ids: &Vec<String>,
        limit: ParentLimit<'_>,
        block: BlockNumber,
        out: &mut AstPass<Pg>,
    ) -> QueryResult<()> {
        // Generate
        //      from rows from (unnest({parent_ids}), unnest({child_ids})) as p(id, child_id),
        //           children c
        //     where c.id = p.child_id
        //       and .. other conditions on c ..

        out.push_sql("\n/* child_type_d */ from rows from (unnest(");
        out.push_bind_param::<Array<Text>, _>(&self.ids)?;
        out.push_sql("), unnest(");
        self.table.primary_key().bind_ids(&child_ids, out)?;
        out.push_sql(")) as p(id, child_id), ");
        out.push_sql(self.table.qualified_name.as_str());
        out.push_sql(" c where ");
        BlockRangeContainsClause::new(&self.table, "c.", block).walk_ast(out.reborrow())?;
        limit.filter(out);
        if *TYPED_CHILDREN_SET_SIZE > 0 {
            let mut child_set: Vec<&str> = child_ids.iter().map(|id| id.as_str()).collect();
            child_set.sort();
            child_set.dedup();

            if child_set.len() <= *TYPED_CHILDREN_SET_SIZE {
                out.push_sql(" and c.id = any(");
                self.table.primary_key().bind_ids(&child_set, out)?;
                out.push_sql(")");
            }
        }
        out.push_sql(" and ");
        out.push_sql("c.id = p.child_id");
        self.and_filter(out.reborrow())?;
        limit.single_limit(self.ids.len(), out);
        Ok(())
    }

    fn children(
        &self,
        limit: ParentLimit<'_>,
        block: BlockNumber,
        mut out: AstPass<Pg>,
    ) -> QueryResult<()> {
        match &self.link {
            TableLink::Direct(column, multiplicity) => {
                use ChildMultiplicity::*;
                if column.is_list() {
                    match multiplicity {
                        Many => self.children_type_a(column, limit, block, &mut out),
                        Single => self.child_type_a(column, limit, block, &mut out),
                    }
                } else {
                    match multiplicity {
                        Many => self.children_type_b(column, limit, block, &mut out),
                        Single => self.child_type_b(column, limit, block, &mut out),
                    }
                }
            }
            TableLink::Parent(ParentIds::List(child_ids)) => {
                self.children_type_c(child_ids, limit, block, &mut out)
            }
            TableLink::Parent(ParentIds::Scalar(child_ids)) => {
                self.child_type_d(child_ids, limit, block, &mut out)
            }
        }
    }

    /// Select a basic subset of columns from the child table for use in
    /// the `matches` CTE of queries that need to retrieve entities of
    /// different types or entities that link differently to their parents
    fn children_uniform(
        &self,
        sort_key: &SortKey,
        block: BlockNumber,
        mut out: AstPass<Pg>,
    ) -> QueryResult<()> {
        out.push_sql("select '");
        out.push_sql(self.table.object.as_str());
        out.push_sql("' as entity, c.id, c.vid, p.id::text as g$parent_id");
        sort_key.select(&mut out)?;
        self.children(ParentLimit::Outer, block, out)
    }

    /// Collect all the parent id's from all windows
    fn collect_parents(windows: &Vec<FilterWindow>) -> Vec<String> {
        let parent_ids: HashSet<String> = HashSet::from_iter(
            windows
                .iter()
                .map(|window| window.ids.iter().cloned())
                .flatten(),
        );
        parent_ids.into_iter().collect()
    }
}

/// This is a parallel to `EntityCollection`, but with entity type names
/// and filters translated in a form ready for SQL generation
#[derive(Debug, Clone)]
pub enum FilterCollection<'a> {
    /// Collection made from all entities in a table; each entry is the table
    /// and the filter to apply to it, checked and bound to that table
    All(Vec<(&'a Table, Option<QueryFilter<'a>>, AttributeNames)>),
    /// Collection made from windows of the same or different entity types
    SingleWindow(FilterWindow<'a>),
    MultiWindow(Vec<FilterWindow<'a>>, Vec<String>),
}

impl<'a> FilterCollection<'a> {
    pub fn new(
        layout: &'a Layout,
        collection: EntityCollection,
        filter: Option<&'a EntityFilter>,
    ) -> Result<Self, QueryExecutionError> {
        match collection {
            EntityCollection::All(entities) => {
                // This is a little ugly since we need to propagate errors
                // from the inner closures. We turn each entity type name
                // into the corresponding table, and check and bind the filter
                // to it
                let entities = entities
                    .iter()
                    .map(|(entity, column_names)| {
                        layout
                            .table_for_entity(&entity)
                            .map(|rc| rc.as_ref())
                            .and_then(|table| {
                                filter
                                    .map(|filter| QueryFilter::new(filter, table))
                                    .transpose()
                                    .map(|filter| (table, filter, column_names.clone()))
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(FilterCollection::All(entities))
            }
            EntityCollection::Window(windows) => {
                let windows = windows
                    .into_iter()
                    .map(|window| FilterWindow::new(layout, window, filter))
                    .collect::<Result<Vec<_>, _>>()?;
                let collection = if windows.len() == 1 {
                    let mut windows = windows;
                    FilterCollection::SingleWindow(
                        windows.pop().expect("we just checked there is an element"),
                    )
                } else {
                    let parent_ids = FilterWindow::collect_parents(&windows);
                    FilterCollection::MultiWindow(windows, parent_ids)
                };
                Ok(collection)
            }
        }
    }

    fn first_table(&self) -> Option<&Table> {
        match self {
            FilterCollection::All(entities) => entities.first().map(|pair| pair.0),
            FilterCollection::SingleWindow(window) => Some(window.table),
            FilterCollection::MultiWindow(windows, _) => windows.first().map(|window| window.table),
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            FilterCollection::All(entities) => entities.is_empty(),
            FilterCollection::SingleWindow(_) => false,
            FilterCollection::MultiWindow(windows, _) => windows.is_empty(),
        }
    }
}

/// Convenience to pass the name of the column to order by around. If `name`
/// is `None`, the sort key should be ignored
#[derive(Debug, Clone, Copy)]
pub enum SortKey<'a> {
    None,
    /// Order by `id asc`
    IdAsc,
    /// Order by `id desc`
    IdDesc,
    /// Order by some other column; `column` will never be `id`
    Key {
        column: &'a Column,
        value: Option<&'a str>,
        direction: &'static str,
    },
}

impl<'a> SortKey<'a> {
    fn new(
        order: EntityOrder,
        table: &'a Table,
        filter: Option<&'a EntityFilter>,
    ) -> Result<Self, QueryExecutionError> {
        const ASC: &str = "asc";
        const DESC: &str = "desc";

        fn with_key<'a>(
            table: &'a Table,
            attribute: String,
            filter: Option<&'a EntityFilter>,
            direction: &'static str,
        ) -> Result<SortKey<'a>, QueryExecutionError> {
            let column = table.column_for_field(&attribute)?;
            if column.is_fulltext() {
                match filter {
                    Some(entity_filter) => match entity_filter {
                        EntityFilter::Equal(_, value) => {
                            let sort_value = value.as_str();

                            Ok(SortKey::Key {
                                column,
                                value: sort_value,
                                direction,
                            })
                        }
                        _ => unreachable!(),
                    },
                    None => unreachable!(),
                }
            } else if column.is_primary_key() {
                match direction {
                    ASC => Ok(SortKey::IdAsc),
                    DESC => Ok(SortKey::IdDesc),
                    _ => unreachable!("direction is 'asc' or 'desc'"),
                }
            } else {
                Ok(SortKey::Key {
                    column,
                    value: None,
                    direction,
                })
            }
        }

        match order {
            EntityOrder::Ascending(attr, _) => with_key(table, attr, filter, ASC),
            EntityOrder::Descending(attr, _) => with_key(table, attr, filter, DESC),
            EntityOrder::Default => Ok(SortKey::IdAsc),
            EntityOrder::Unordered => Ok(SortKey::None),
        }
    }

    /// Generate selecting the sort key if it is needed
    fn select(&self, out: &mut AstPass<Pg>) -> QueryResult<()> {
        match self {
            SortKey::None => Ok(()),
            SortKey::IdAsc | SortKey::IdDesc => {
                if *ORDER_BY_BLOCK_RANGE {
                    out.push_sql(", c.");
                    out.push_sql(BLOCK_RANGE_COLUMN);
                }
                Ok(())
            }
            SortKey::Key {
                column,
                value: _,
                direction: _,
            } => {
                if column.is_primary_key() {
                    return Err(constraint_violation!("SortKey::Key never uses 'id'"));
                }
                out.push_sql(", c.");
                out.push_identifier(column.name.as_str())?;
                Ok(())
            }
        }
    }

    /// Generate
    ///   order by [name direction], id
    fn order_by(&self, out: &mut AstPass<Pg>) -> QueryResult<()> {
        match self {
            SortKey::None => Ok(()),
            SortKey::IdAsc => {
                out.push_sql("order by ");
                out.push_identifier(PRIMARY_KEY_COLUMN)?;
                if *ORDER_BY_BLOCK_RANGE {
                    out.push_sql(", ");
                    out.push_sql(BLOCK_RANGE_COLUMN);
                }
                Ok(())
            }
            SortKey::IdDesc => {
                out.push_sql("order by ");
                out.push_identifier(PRIMARY_KEY_COLUMN)?;
                out.push_sql(" desc");
                if *ORDER_BY_BLOCK_RANGE {
                    out.push_sql(", ");
                    out.push_sql(BLOCK_RANGE_COLUMN);
                    out.push_sql(" desc");
                }
                Ok(())
            }
            SortKey::Key {
                column,
                value,
                direction,
            } => {
                out.push_sql("order by ");
                SortKey::sort_expr(column, value, direction, out)
            }
        }
    }

    /// Generate
    ///   order by g$parent_id, [name direction], id
    fn order_by_parent(&self, out: &mut AstPass<Pg>) -> QueryResult<()> {
        match self {
            SortKey::None => Ok(()),
            SortKey::IdAsc => {
                out.push_sql("order by g$parent_id, ");
                out.push_identifier(PRIMARY_KEY_COLUMN)
            }
            SortKey::IdDesc => {
                out.push_sql("order by g$parent_id, ");
                out.push_identifier(PRIMARY_KEY_COLUMN)?;
                out.push_sql(" desc");
                Ok(())
            }
            SortKey::Key {
                column,
                value,
                direction,
            } => {
                out.push_sql("order by g$parent_id, ");
                SortKey::sort_expr(column, value, direction, out)
            }
        }
    }

    /// Generate
    ///   [name direction,] id
    fn sort_expr(
        column: &Column,
        value: &Option<&str>,
        direction: &str,
        out: &mut AstPass<Pg>,
    ) -> QueryResult<()> {
        if column.is_primary_key() {
            // This shouldn't happen since we'd use SortKey::IdAsc/Desc
            return Err(constraint_violation!(
                "sort_expr called with primary key column"
            ));
        }

        match &column.column_type {
            ColumnType::TSVector(config) => {
                let algorithm = match config.algorithm {
                    FulltextAlgorithm::Rank => "ts_rank(",
                    FulltextAlgorithm::ProximityRank => "ts_rank_cd(",
                };
                out.push_sql(algorithm);
                let name = column.name.as_str();
                out.push_identifier(name)?;
                out.push_sql(", to_tsquery(");

                out.push_bind_param::<Text, _>(&value.unwrap())?;
                out.push_sql("))");
            }
            _ => {
                let name = column.name.as_str();
                out.push_identifier(name)?;
            }
        }
        if *REVERSIBLE_ORDER_BY_OFF {
            // Old behavior
            out.push_sql(" ");
            out.push_sql(direction);
            out.push_sql(" nulls last");
            out.push_sql(", ");
            out.push_identifier(PRIMARY_KEY_COLUMN)?;
        } else {
            out.push_sql(" ");
            out.push_sql(direction);
            out.push_sql(", ");
            out.push_identifier(PRIMARY_KEY_COLUMN)?;
            out.push_sql(" ");
            out.push_sql(direction);
        }
        Ok(())
    }
}

/// Generate `[limit {first}] [offset {skip}]
#[derive(Debug, Clone)]
pub struct FilterRange(EntityRange);

impl QueryFragment<Pg> for FilterRange {
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        let range = &self.0;
        if let Some(first) = &range.first {
            out.push_sql("\n limit ");
            out.push_sql(&first.to_string());
        }
        if range.skip > 0 {
            out.push_sql("\noffset ");
            out.push_sql(&range.skip.to_string());
        }
        Ok(())
    }
}

/// The parallel to `EntityQuery`.
///
/// Details of how query generation for `FilterQuery` works can be found
/// `https://github.com/graphprotocol/rfcs/blob/master/engineering-plans/0001-graphql-query-prefetching.md`
#[derive(Debug, Clone)]
pub struct FilterQuery<'a> {
    collection: &'a FilterCollection<'a>,
    sort_key: SortKey<'a>,
    range: FilterRange,
    block: BlockNumber,
    query_id: Option<String>,
}

impl<'a> FilterQuery<'a> {
    pub fn new(
        collection: &'a FilterCollection,
        filter: Option<&'a EntityFilter>,
        order: EntityOrder,
        range: EntityRange,
        block: BlockNumber,
        query_id: Option<String>,
    ) -> Result<Self, QueryExecutionError> {
        // Get the name of the column we order by; if there is more than one
        // table, we are querying an interface, and the order is on an attribute
        // in that interface so that all tables have a column for that. It is
        // therefore enough to just look at the first table to get the name
        let first_table = collection
            .first_table()
            .expect("an entity query always contains at least one entity type/table");
        let sort_key = SortKey::new(order, first_table, filter)?;

        Ok(FilterQuery {
            collection,
            sort_key,
            range: FilterRange(range),
            block,
            query_id,
        })
    }

    /// Generate
    ///     from schema.table c
    ///    where block_range @> $block
    ///      and query_filter
    /// Only used when the query is against a `FilterCollection::All`, i.e.
    /// when we do not need to window
    fn filtered_rows(
        &self,
        table: &Table,
        table_filter: &Option<QueryFilter<'a>>,
        mut out: AstPass<Pg>,
    ) -> QueryResult<()> {
        out.push_sql("\n  from ");
        out.push_sql(table.qualified_name.as_str());
        out.push_sql(" c");
        out.push_sql("\n where ");
        BlockRangeContainsClause::new(&table, "c.", self.block).walk_ast(out.reborrow())?;
        if let Some(filter) = table_filter {
            out.push_sql(" and ");
            filter.walk_ast(out.reborrow())?;
        }
        out.push_sql("\n");
        Ok(())
    }

    fn select_entity_and_data(table: &Table, out: &mut AstPass<Pg>) {
        out.push_sql("select '");
        out.push_sql(table.object.as_str());
        out.push_sql("' as entity, to_jsonb(c.*) as data");
    }

    /// Only one table/filter pair, and no window
    ///
    /// The generated query makes sure we only convert the rows we actually
    /// want to retrieve to JSONB
    ///
    ///   select '..' as entity, to_jsonb(e.*) as data
    ///     from
    ///       (select {column names}
    ///          from table c
    ///         where block_range @> $block
    ///           and filter
    ///         order by .. limit .. skip ..) c
    fn query_no_window_one_entity(
        &self,
        table: &Table,
        filter: &Option<QueryFilter>,
        mut out: AstPass<Pg>,
        column_names: &AttributeNames,
    ) -> QueryResult<()> {
        Self::select_entity_and_data(table, &mut out);
        out.push_sql(" from (select ");
        write_column_names(&column_names, &table, &mut out)?;
        self.filtered_rows(table, filter, out.reborrow())?;
        out.push_sql("\n ");
        self.sort_key.order_by(&mut out)?;
        self.range.walk_ast(out.reborrow())?;
        out.push_sql(") c");
        Ok(())
    }

    /// Only one table/filter pair, and a window
    ///
    /// Generate a query
    ///   select '..' as entity, to_jsonb(e.*) as data
    ///     from (select c.*, p.id as g$parent_id from {window.children(...)}) c
    ///     order by c.g$parent_id, {sort_key}
    ///     limit {first} offset {skip}
    fn query_window_one_entity(
        &self,
        window: &FilterWindow,
        mut out: AstPass<Pg>,
    ) -> QueryResult<()> {
        Self::select_entity_and_data(&window.table, &mut out);
        out.push_sql(" from (\n");
        out.push_sql("select c.*, p.id::text as g$parent_id");
        window.children(
            ParentLimit::Ranked(&self.sort_key, &self.range),
            self.block,
            out.reborrow(),
        )?;
        out.push_sql(") c");
        out.push_sql("\n ");
        self.sort_key.order_by_parent(&mut out)
    }

    /// No windowing, but multiple entity types
    fn query_no_window(
        &self,
        entities: &Vec<(&Table, Option<QueryFilter>, AttributeNames)>,
        mut out: AstPass<Pg>,
    ) -> QueryResult<()> {
        // We have multiple tables which might have different schemas since
        // the entity_types come from implementing the same interface. We
        // need to do the query in two steps: first we build a CTE with the
        // id's of entities matching the filter and order/limit. As a second
        // step, we get matching rows from the underlying tables and convert
        // them to JSONB.
        //
        // Overall, we generate a query
        //
        // with matches as (
        //   select '...' as entity, id, vid, {sort_key}
        //     from {table} c
        //    where {query_filter}
        //    union all
        //    ...
        //    order by {sort_key}
        //    limit n offset m)
        //
        // select m.entity, to_jsonb({column names}) as data, c.id, c.{sort_key}
        //   from {table} c, matches m
        //  where c.vid = m.vid and m.entity = '...'
        //  union all
        //  ...
        //  order by c.{sort_key}

        // Step 1: build matches CTE
        out.push_sql("with matches as (");
        for (i, (table, filter, _column_names)) in entities.iter().enumerate() {
            if i > 0 {
                out.push_sql("\nunion all\n");
            }
            // select '..' as entity,
            //        c.id,
            //        c.vid,
            //        c.${sort_key}
            out.push_sql("select '");
            out.push_sql(&table.object.as_str());
            out.push_sql("' as entity, c.id, c.vid");
            self.sort_key.select(&mut out)?;
            self.filtered_rows(table, filter, out.reborrow())?;
        }
        out.push_sql("\n ");
        self.sort_key.order_by(&mut out)?;
        self.range.walk_ast(out.reborrow())?;

        out.push_sql(")\n");

        // Step 2: convert to JSONB
        for (i, (table, _, column_names)) in entities.iter().enumerate() {
            if i > 0 {
                out.push_sql("\nunion all\n");
            }
            out.push_sql("select m.entity, ");
            jsonb_build_object(column_names, "c", &table, &mut out)?;
            out.push_sql(" as data, c.id");
            self.sort_key.select(&mut out)?;
            out.push_sql("\n  from ");
            out.push_sql(table.qualified_name.as_str());
            out.push_sql(" c,");
            out.push_sql(" matches m");
            out.push_sql("\n where c.vid = m.vid and m.entity = ");
            out.push_bind_param::<Text, _>(&table.object.as_str())?;
        }
        out.push_sql("\n ");
        self.sort_key.order_by(&mut out)?;
        Ok(())
    }

    /// Multiple windows
    fn query_window(
        &self,
        windows: &Vec<FilterWindow>,
        parent_ids: &Vec<String>,
        mut out: AstPass<Pg>,
    ) -> QueryResult<()> {
        // Note that a CTE is an optimization fence, and since we use
        // `matches` multiple times, we actually want to materialize it first
        // before we fill in JSON data in the main query. As a consequence, we
        // restrict the matches results per window in the `matches` CTE to
        // avoid a possibly gigantic materialized `matches` view rather than
        // leave that to the main query
        //
        // Overall, we generate a query
        //
        // with matches as (
        //     select c.*
        //       from (select id from unnest({all_parent_ids}) as q(id)) q
        //            cross join lateral
        //            ({window.children_uniform("q")}
        //             union all
        //             ... range over all windows ...
        //             order by c.{sort_key}
        //             limit $first skip $skip) c)
        //   select m.entity, to_jsonb(c.*) as data, m.parent_id
        //     from matches m, {window.child_table} c
        //    where c.vid = m.vid and m.entity = '{window.child_type}'
        //    union all
        //          ... range over all windows
        //    order by parent_id, c.{sort_key}

        // Step 1: build matches CTE
        out.push_sql("with matches as (");
        out.push_sql("select c.* from ");
        out.push_sql("unnest(");
        out.push_bind_param::<Array<Text>, _>(parent_ids)?;
        out.push_sql("::text[]) as q(id)\n");
        out.push_sql(" cross join lateral (");
        for (i, window) in windows.iter().enumerate() {
            if i > 0 {
                out.push_sql("\nunion all\n");
            }
            window.children_uniform(&self.sort_key, self.block, out.reborrow())?;
        }
        out.push_sql("\n");
        self.sort_key.order_by(&mut out)?;
        self.range.walk_ast(out.reborrow())?;
        out.push_sql(") c)\n");

        // Step 2: convert to JSONB
        // If the parent is an interface, each implementation might store its
        // relationship to the children in different ways, leading to multiple
        // windows that use the same table for the children. We need to make
        // sure each table only appears once in the 'union all' otherwise we'll
        // duplicate entities in the result
        // We only use a table's qualified name and object to save ourselves
        // the hassle of making `Table` hashable
        let unique_child_tables = windows
            .iter()
            .unique_by(|window| {
                (
                    &window.table.qualified_name,
                    &window.table.object,
                    &window.column_names,
                )
            })
            .enumerate()
            .into_iter();

        for (i, window) in unique_child_tables {
            if i > 0 {
                out.push_sql("\nunion all\n");
            }
            out.push_sql("select m.*, ");
            jsonb_build_object(&window.column_names, "c", &window.table, &mut out)?;
            out.push_sql("|| jsonb_build_object('g$parent_id', m.g$parent_id) as data");
            out.push_sql("\n  from ");
            out.push_sql(&window.table.qualified_name.as_str());
            out.push_sql(" c, matches m\n where c.vid = m.vid and m.entity = '");
            out.push_sql(&window.table.object.as_str());
            out.push_sql("'");
        }
        out.push_sql("\n ");
        self.sort_key.order_by_parent(&mut out)
    }
}

impl<'a> QueryFragment<Pg> for FilterQuery<'a> {
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();
        if self.collection.is_empty() {
            return Ok(());
        }

        if let Some(qid) = &self.query_id {
            out.push_sql("/* qid: ");
            out.push_sql(qid);
            out.push_sql(" */\n");
        }
        // We generate four different kinds of queries, depending on whether
        // we need to window and whether we query just one or multiple entity
        // types/windows; the most complex situation is windowing with multiple
        // entity types. The other cases let us simplify the generated SQL
        // considerably and produces faster queries
        //
        // Details of how all this works can be found in
        // `https://github.com/graphprotocol/rfcs/blob/master/engineering-plans/0001-graphql-query-prefetching.md`
        match &self.collection {
            FilterCollection::All(entities) => {
                if entities.len() == 1 {
                    let (table, filter, column_names) = entities
                        .first()
                        .expect("a query always uses at least one table");
                    self.query_no_window_one_entity(table, filter, out, column_names)
                } else {
                    self.query_no_window(entities, out)
                }
            }
            FilterCollection::SingleWindow(window) => self.query_window_one_entity(window, out),
            FilterCollection::MultiWindow(windows, parent_ids) => {
                self.query_window(windows, parent_ids, out)
            }
        }
    }
}

impl<'a> QueryId for FilterQuery<'a> {
    type QueryId = ();

    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<'a> LoadQuery<PgConnection, EntityData> for FilterQuery<'a> {
    fn internal_load(self, conn: &PgConnection) -> QueryResult<Vec<EntityData>> {
        conn.query_by_name(&self)
    }
}

impl<'a, Conn> RunQueryDsl<Conn> for FilterQuery<'a> {}

/// Reduce the upper bound of the current entry's block range to `block` as
/// long as that does not result in an empty block range
#[derive(Debug, Clone, Constructor)]
pub struct ClampRangeQuery<'a, S> {
    table: &'a Table,
    #[allow(dead_code)]
    entity_type: &'a EntityType,
    entity_ids: &'a [S],
    block: BlockNumber,
}

impl<'a, S> QueryFragment<Pg> for ClampRangeQuery<'a, S>
where
    S: AsRef<str> + diesel::serialize::ToSql<Text, Pg>,
{
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        // update table
        //    set block_range = int4range(lower(block_range), $block)
        //  where id in (id1, id2, ..., idN)
        //    and block_range @> INTMAX
        out.unsafe_to_cache_prepared();
        out.push_sql("update ");
        out.push_sql(self.table.qualified_name.as_str());
        out.push_sql("\n   set ");
        out.push_identifier(BLOCK_RANGE_COLUMN)?;
        out.push_sql(" = int4range(lower(");
        out.push_identifier(BLOCK_RANGE_COLUMN)?;
        out.push_sql("), ");
        out.push_bind_param::<Integer, _>(&self.block)?;
        out.push_sql(")\n where ");

        self.table.primary_key().is_in(self.entity_ids, &mut out)?;
        out.push_sql(" and (");
        out.push_sql(BLOCK_RANGE_CURRENT);
        out.push_sql(")");

        Ok(())
    }
}

impl<'a, S> QueryId for ClampRangeQuery<'a, S>
where
    S: AsRef<str> + diesel::serialize::ToSql<Text, Pg>,
{
    type QueryId = ();

    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<'a, S, Conn> RunQueryDsl<Conn> for ClampRangeQuery<'a, S> {}

/// Helper struct for returning the id's touched by the RevertRemove and
/// RevertExtend queries
#[derive(QueryableByName, PartialEq, Eq, Hash)]
pub struct ReturnedEntityData {
    #[sql_type = "Text"]
    pub id: String,
}

impl ReturnedEntityData {
    /// Convert primary key ids from Postgres' internal form to the format we
    /// use by stripping `\\x` off the front of bytes strings
    fn bytes_as_str(table: &Table, mut data: Vec<ReturnedEntityData>) -> Vec<ReturnedEntityData> {
        match table.primary_key().column_type.id_type() {
            IdType::String => data,
            IdType::Bytes => {
                for entry in data.iter_mut() {
                    entry.id = bytes_as_str(&entry.id);
                }
                data
            }
        }
    }
}

/// A query that removes all versions whose block range lies entirely
/// beyond `block`.
#[derive(Debug, Clone, Constructor)]
pub struct RevertRemoveQuery<'a> {
    table: &'a Table,
    block: BlockNumber,
}

impl<'a> QueryFragment<Pg> for RevertRemoveQuery<'a> {
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();

        // Construct a query
        //   delete from table
        //    where lower(block_range) >= $block
        //   returning id
        out.push_sql("delete from ");
        out.push_sql(self.table.qualified_name.as_str());
        out.push_sql("\n where lower(");
        out.push_identifier(BLOCK_RANGE_COLUMN)?;
        out.push_sql(") >= ");
        out.push_bind_param::<Integer, _>(&self.block)?;
        out.push_sql("\nreturning ");
        out.push_sql(PRIMARY_KEY_COLUMN);
        out.push_sql("::text");
        Ok(())
    }
}

impl<'a> QueryId for RevertRemoveQuery<'a> {
    type QueryId = ();

    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<'a> LoadQuery<PgConnection, ReturnedEntityData> for RevertRemoveQuery<'a> {
    fn internal_load(self, conn: &PgConnection) -> QueryResult<Vec<ReturnedEntityData>> {
        conn.query_by_name(&self)
            .map(|data| ReturnedEntityData::bytes_as_str(&self.table, data))
    }
}

impl<'a, Conn> RunQueryDsl<Conn> for RevertRemoveQuery<'a> {}

/// A query that unclamps the block range of all versions that contain
/// `block` by setting the upper bound of the block range to infinity.
#[derive(Debug, Clone, Constructor)]
pub struct RevertClampQuery<'a> {
    table: &'a Table,
    block: BlockNumber,
}

impl<'a> QueryFragment<Pg> for RevertClampQuery<'a> {
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();

        // Construct a query
        //   update table
        //     set block_range = int4range(lower(block_range), null)
        //   where block_range @> $block
        //     and not block_range @> INTMAX
        //     and lower(block_range) <= $block
        //     and coalesce(upper(block_range), INTMAX) > $block
        //     and coalesce(upper(block_range), INTMAX) < INTMAX
        //   returning id
        //
        // The query states the same thing twice, once in terms of ranges
        // and once in terms of the range bounds. That makes it possible
        // for Postgres to use either the exclusion index on the table
        // or the BRIN index
        out.push_sql("update ");
        out.push_sql(self.table.qualified_name.as_str());
        out.push_sql("\n   set ");
        out.push_identifier(BLOCK_RANGE_COLUMN)?;
        out.push_sql(" = int4range(lower(");
        out.push_identifier(BLOCK_RANGE_COLUMN)?;
        out.push_sql("), null)\n where ");
        out.push_identifier(BLOCK_RANGE_COLUMN)?;
        out.push_sql(" @> ");
        out.push_bind_param::<Integer, _>(&self.block)?;
        out.push_sql(" and not ");
        out.push_sql(BLOCK_RANGE_CURRENT);
        out.push_sql(" and lower(");
        out.push_sql(BLOCK_RANGE_COLUMN);
        out.push_sql(") <= ");
        out.push_bind_param::<Integer, _>(&self.block)?;
        out.push_sql(" and coalesce(upper(");
        out.push_sql(BLOCK_RANGE_COLUMN);
        out.push_sql("), 2147483647) > ");
        out.push_bind_param::<Integer, _>(&self.block)?;
        out.push_sql(" and coalesce(upper(");
        out.push_sql(BLOCK_RANGE_COLUMN);
        out.push_sql("), 2147483647) < 2147483647");
        out.push_sql("\nreturning ");
        out.push_sql(PRIMARY_KEY_COLUMN);
        out.push_sql("::text");
        Ok(())
    }
}

impl<'a> QueryId for RevertClampQuery<'a> {
    type QueryId = ();

    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<'a> LoadQuery<PgConnection, ReturnedEntityData> for RevertClampQuery<'a> {
    fn internal_load(self, conn: &PgConnection) -> QueryResult<Vec<ReturnedEntityData>> {
        conn.query_by_name(&self)
            .map(|data| ReturnedEntityData::bytes_as_str(&self.table, data))
    }
}

impl<'a, Conn> RunQueryDsl<Conn> for RevertClampQuery<'a> {}

#[test]
fn block_number_max_is_i32_max() {
    // The code in RevertClampQuery::walk_ast embeds i32::MAX
    // aka BLOCK_NUMBER_MAX in strings for efficiency. This assertion
    // makes sure that BLOCK_NUMBER_MAX still is what we think it is
    assert_eq!(2147483647, graph::prelude::BLOCK_NUMBER_MAX);
}

/// Copy the data of one table to another table. All rows whose `vid` is in
/// the range `[first_vid, last_vid]` will be copied
#[derive(Debug, Clone)]
pub struct CopyEntityBatchQuery<'a> {
    src: &'a Table,
    dst: &'a Table,
    // A list of columns common between src and dst that
    // need to be copied
    columns: Vec<&'a Column>,
    first_vid: i64,
    last_vid: i64,
}

impl<'a> CopyEntityBatchQuery<'a> {
    pub fn new(
        dst: &'a Table,
        src: &'a Table,
        first_vid: i64,
        last_vid: i64,
    ) -> Result<Self, StoreError> {
        let mut columns = Vec::new();
        for dcol in &dst.columns {
            if let Some(scol) = src.column(&dcol.name) {
                if let Some(msg) = dcol.is_assignable_from(scol, &src.object) {
                    return Err(anyhow!("{}", msg).into());
                } else {
                    columns.push(dcol);
                }
            } else if !dcol.is_nullable() {
                return Err(anyhow!(
                    "The attribute {}.{} is non-nullable, \
                     but there is no such attribute in the source",
                    dst.object,
                    dcol.field
                )
                .into());
            }
        }

        Ok(Self {
            src,
            dst,
            columns,
            first_vid,
            last_vid,
        })
    }
}

impl<'a> QueryFragment<Pg> for CopyEntityBatchQuery<'a> {
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();

        // Construct a query
        //   insert into {dst}({columns})
        //   select {columns} from {src}
        out.push_sql("insert into ");
        out.push_sql(self.dst.qualified_name.as_str());
        out.push_sql("(");
        for column in &self.columns {
            out.push_identifier(column.name.as_str())?;
            out.push_sql(", ");
        }
        out.push_sql("block_range)");
        out.push_sql("\nselect ");
        for column in &self.columns {
            out.push_identifier(column.name.as_str())?;
            if let ColumnType::Enum(enum_type) = &column.column_type {
                // Have Postgres convert to the right enum type
                if column.is_list() {
                    out.push_sql("::text[]::");
                    out.push_sql(enum_type.name.as_str());
                    out.push_sql("[]");
                } else {
                    out.push_sql("::text::");
                    out.push_sql(enum_type.name.as_str());
                }
            }
            out.push_sql(", ");
        }
        out.push_sql("block_range from ");
        out.push_sql(self.src.qualified_name.as_str());
        out.push_sql(" where vid >= ");
        out.push_bind_param::<BigInt, _>(&self.first_vid)?;
        out.push_sql(" and vid <= ");
        out.push_bind_param::<BigInt, _>(&self.last_vid)?;
        Ok(())
    }
}

impl<'a> QueryId for CopyEntityBatchQuery<'a> {
    type QueryId = ();

    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<'a, Conn> RunQueryDsl<Conn> for CopyEntityBatchQuery<'a> {}

/// Helper struct for returning the id's touched by the RevertRemove and
/// RevertExtend queries
#[derive(QueryableByName, PartialEq, Eq, Hash)]
pub struct CopyVid {
    #[sql_type = "BigInt"]
    pub vid: i64,
}

fn write_column_names(
    column_names: &AttributeNames,
    table: &Table,
    out: &mut AstPass<Pg>,
) -> QueryResult<()> {
    match column_names {
        AttributeNames::All => out.push_sql(" * "),
        AttributeNames::Select(column_names) => {
            let mut iterator = iter_column_names(column_names, table).peekable();
            while let Some(column_name) = iterator.next() {
                out.push_identifier(&column_name)?;
                if iterator.peek().is_some() {
                    out.push_sql(", ");
                }
            }
        }
    }
    Ok(())
}

fn jsonb_build_object(
    column_names: &AttributeNames,
    table_identifier: &str,
    table: &Table,
    out: &mut AstPass<Pg>,
) -> QueryResult<()> {
    match column_names {
        AttributeNames::All => {
            out.push_sql("to_jsonb(\"");
            out.push_sql(table_identifier);
            out.push_sql("\".*)");
        }
        AttributeNames::Select(column_names) => {
            out.push_sql("jsonb_build_object(");
            let mut iterator = iter_column_names(column_names, table).peekable();
            while let Some(column_name) = iterator.next() {
                // field name as json key
                out.push_sql("'");
                out.push_sql(column_name);
                out.push_sql("', ");
                // column identifier
                out.push_sql(table_identifier);
                out.push_sql(".");
                out.push_identifier(column_name)?;
                if iterator.peek().is_some() {
                    out.push_sql(", ");
                }
            }
            out.push_sql(")");
        }
    }
    Ok(())
}

/// Helper function to iterate over the merged fields of BASE_SQL_COLUMNS and the provided attribute
/// names, yielding valid SQL names for the given table.
fn iter_column_names<'a, 'b>(
    attribute_names: &'a BTreeSet<String>,
    table: &'b Table,
) -> impl Iterator<Item = &'b str> {
    attribute_names
        .iter()
        .map(|attribute_name| {
            // Unwrapping: We have already checked that all attribute names exist in table
            table.column_for_field(attribute_name).unwrap()
        })
        .map(|column| column.name.as_str())
        .chain(BASE_SQL_COLUMNS.iter().copied())
        .sorted()
        .dedup()
}
