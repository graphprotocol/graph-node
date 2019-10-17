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
use diesel::result::QueryResult;
use diesel::sql_types::{Array, Binary, Bool, Integer, Jsonb, Numeric, Range, Text};
use diesel::Connection;
use std::convert::TryFrom;
use std::str::FromStr;

use graph::data::store::scalar;
use graph::prelude::{
    format_err, serde_json, Attribute, Entity, EntityFilter, EntityKey, StoreError, Value,
};

use crate::block_range::{
    BlockNumber, BlockRange, BlockRangeContainsClause, BLOCK_RANGE_COLUMN, BLOCK_RANGE_CURRENT,
};
use crate::entities::STRING_PREFIX_SIZE;
use crate::filter::UnsupportedFilter;
use crate::relational::{Column, ColumnType, Layout, SqlName, Table, PRIMARY_KEY_COLUMN};
use crate::sql_value::SqlValue;

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
    fn value_from_json(
        column_type: &ColumnType,
        json: serde_json::Value,
    ) -> Result<graph::prelude::Value, StoreError> {
        use graph::prelude::Value as g;
        use serde_json::Value as j;
        // Many possible conversion errors are already caught by how
        // we define the schema; for example, we can only get a NULL for
        // a column that is actually nullable
        match (json, column_type) {
            (j::Null, _) => Ok(g::Null),
            (j::Bool(b), _) => Ok(g::Bool(b)),
            (j::Number(number), ColumnType::Int) => match number.as_i64() {
                Some(i) => i32::try_from(i).map(|i| g::Int(i)).map_err(|e| {
                    StoreError::Unknown(format_err!("failed to convert {} to Int: {}", number, e))
                }),
                None => Err(StoreError::Unknown(format_err!(
                    "failed to convert {} to Int",
                    number
                ))),
            },
            (j::Number(number), ColumnType::BigDecimal) => {
                let s = number.to_string();
                scalar::BigDecimal::from_str(s.as_str())
                    .map(|d| g::BigDecimal(d))
                    .map_err(|e| {
                        StoreError::Unknown(format_err!(
                            "failed to convert {} to BigDecimal: {}",
                            number,
                            e
                        ))
                    })
            }
            (j::Number(number), ColumnType::BigInt) => {
                let s = number.to_string();
                scalar::BigInt::from_str(s.as_str())
                    .map(|d| g::BigInt(d))
                    .map_err(|e| {
                        StoreError::Unknown(format_err!(
                            "failed to convert {} to BigInt: {}",
                            number,
                            e
                        ))
                    })
            }
            (j::Number(number), column_type) => Err(StoreError::Unknown(format_err!(
                "can not convert number {} to {:?}",
                number,
                column_type
            ))),
            (j::String(s), ColumnType::String) | (j::String(s), ColumnType::Enum(_)) => {
                Ok(g::String(s))
            }
            (j::String(s), ColumnType::Bytes) => {
                scalar::Bytes::from_str(s.trim_start_matches("\\x"))
                    .map(|b| g::Bytes(b))
                    .map_err(|e| {
                        StoreError::Unknown(format_err!("failed to convert {} to Bytes: {}", s, e))
                    })
            }
            (j::String(s), column_type) => Err(StoreError::Unknown(format_err!(
                "can not convert string {} to {:?}",
                s,
                column_type
            ))),
            (j::Array(values), _) => Ok(g::List(
                values
                    .into_iter()
                    .map(|v| Self::value_from_json(column_type, v))
                    .collect::<Result<Vec<_>, _>>()?,
            )),
            (j::Object(_), _) => {
                unimplemented!("objects as entity attributes are not needed/supported")
            }
        }
    }

    /// Map the `EntityData` to an entity using the schema information
    /// in `Layout`
    pub fn to_entity(self, layout: &Layout) -> Result<Entity, StoreError> {
        let table = layout.table_for_entity(&self.entity)?;

        use serde_json::Value as j;
        match self.data {
            j::Object(map) => {
                let mut entity = Entity::new();
                entity.insert(
                    "__typename".to_owned(),
                    graph::prelude::Value::from(self.entity),
                );
                for (key, json) in map {
                    // Simply ignore keys that do not have an underlying table
                    // column; those will be things like the block_range that
                    // is used internally for versioning
                    if let Some(column) = table.column(&SqlName::from_snake_case(key)).ok() {
                        let value = Self::value_from_json(&column.column_type, json)?;
                        if value != Value::Null {
                            entity.insert(column.field.clone(), value);
                        }
                    }
                }
                Ok(entity)
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
                ColumnType::Enum(name) => {
                    out.push_bind_param::<Text, _>(s)?;
                    out.push_sql("::");
                    out.push_sql(name.as_str());
                    Ok(())
                }
                _ => unreachable!("only string and enum columns have values of type string"),
            },
            Value::Int(i) => out.push_bind_param::<Integer, _>(i),
            Value::BigDecimal(d) => {
                // If a BigDecimal with negative scale gets sent to Diesel (and then Postgres)
                // Postgres will complain with 'invalid scale in external "numeric" value'
                let (_, scale) = d.as_bigint_and_exponent();
                if scale < 0 {
                    // This is safe since we are effectively multiplying the
                    // integer part of d with 10^(-scale), an integer
                    let d = d.with_scale(0);
                    out.push_bind_param::<Numeric, _>(&d)
                } else {
                    out.push_bind_param::<Numeric, _>(d)
                }
            }
            Value::Bool(b) => out.push_bind_param::<Bool, _>(b),
            Value::List(values) => {
                let values = SqlValue::new_array(values.clone());
                match &column_type {
                    ColumnType::BigDecimal | ColumnType::BigInt => {
                        out.push_bind_param::<Array<Numeric>, _>(&values)
                    }
                    ColumnType::Boolean => out.push_bind_param::<Array<Bool>, _>(&values),
                    ColumnType::Bytes => out.push_bind_param::<Array<Binary>, _>(&values),
                    ColumnType::Int => out.push_bind_param::<Array<Integer>, _>(&values),
                    ColumnType::String => out.push_bind_param::<Array<Text>, _>(&values),
                    ColumnType::Enum(name) => {
                        out.push_bind_param::<Array<Text>, _>(&values)?;
                        out.push_sql("::");
                        out.push_sql(name.as_str());
                        out.push_sql("[]");
                        Ok(())
                    }
                }
            }
            Value::Null => {
                out.push_sql("null");
                Ok(())
            }
            Value::Bytes(b) => out.push_bind_param::<Binary, _>(&b.as_slice()),
            Value::BigInt(i) => {
                out.push_bind_param::<Numeric, _>(&i.clone().to_big_decimal(0.into()))
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
                out.push_identifier(column.name.as_str())?;
                out.push_sql(" @> ");
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
    schema: &'a str,
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
        out.push_bind_param::<Text, _>(&self.table.object)?;
        out.push_sql(" as entity, to_jsonb(e.*) as data\n");
        out.push_sql("  from ");
        out.push_identifier(self.schema)?;
        out.push_sql(".");
        out.push_identifier(self.table.name.as_str())?;
        out.push_sql(" e\n where ");
        out.push_identifier(PRIMARY_KEY_COLUMN)?;
        out.push_sql(" = ");
        out.push_bind_param::<Text, _>(&self.id)?;
        out.push_sql(" and ");
        BlockRangeContainsClause::new(self.block).walk_ast(out)
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

#[derive(Debug, Clone)]
pub struct InsertQuery<'a> {
    schema_name: &'a str,
    table: &'a Table,
    key: &'a EntityKey,
    entity: &'a Entity,
    block: BlockNumber,
}

impl<'a> InsertQuery<'a> {
    pub fn new(
        schema_name: &'a str,
        table: &'a Table,
        key: &'a EntityKey,
        entity: &'a Entity,
        block: BlockNumber,
    ) -> Result<InsertQuery<'a>, StoreError> {
        for column in table.columns.iter() {
            if !column.is_nullable() && !entity.contains_key(&column.field) {
                return Err(StoreError::QueryExecutionError(format!(
                    "can not insert entity {}[{}] since value for non-nullable attribute {} is missing. \
                     To fix this, mark the attribute as nullable in the GraphQL schema or change the \
                     mapping code to always set this attribute.",
                    key.entity_type, key.entity_id, column.field
                )));
            }
        }

        Ok(InsertQuery {
            schema_name,
            table,
            key,
            entity,
            block,
        })
    }
}

impl<'a> QueryFragment<Pg> for InsertQuery<'a> {
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();

        // Construct a query
        //   insert into schema.table(column, ...)
        //   values ($1, ...)
        // and convert and bind the entity's values into it
        out.push_sql("insert into ");
        out.push_identifier(self.schema_name)?;
        out.push_sql(".");
        out.push_identifier(self.table.name.as_str())?;

        out.push_sql("(");
        for column in self.table.columns.iter() {
            if self.entity.contains_key(&column.field) {
                out.push_identifier(column.name.as_str())?;
                out.push_sql(", ");
            }
        }
        out.push_identifier(BLOCK_RANGE_COLUMN)?;

        out.push_sql(")\nvalues(");
        for column in self.table.columns.iter() {
            if let Some(value) = self.entity.get(&column.field) {
                QueryValue(value, &column.column_type).walk_ast(out.reborrow())?;
                out.push_sql(", ");
            }
        }
        let block_range: BlockRange = (self.block..).into();
        out.push_bind_param::<Range<Integer>, _>(&block_range)?;
        out.push_sql(")");
        Ok(())
    }
}

impl<'a> QueryId for InsertQuery<'a> {
    type QueryId = ();

    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<'a, Conn> RunQueryDsl<Conn> for InsertQuery<'a> {}

#[derive(Debug, Clone)]
pub struct ConflictingEntityQuery<'a> {
    layout: &'a Layout,
    tables: Vec<&'a Table>,
    entity_id: &'a String,
}
impl<'a> ConflictingEntityQuery<'a> {
    pub fn new(
        layout: &'a Layout,
        entities: Vec<&'a String>,
        entity_id: &'a String,
    ) -> Result<Self, StoreError> {
        let tables = entities
            .iter()
            .map(|entity| layout.table_for_entity(entity).map(|table| table.as_ref()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ConflictingEntityQuery {
            layout,
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
            out.push_bind_param::<Text, _>(&table.object)?;
            out.push_sql(" as entity from ");
            out.push_identifier(&self.layout.schema)?;
            out.push_sql(".");
            out.push_identifier(table.name.as_str())?;
            out.push_sql(" where id = ");
            out.push_bind_param::<Text, _>(self.entity_id)?;
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

#[derive(Debug, Clone, Constructor)]
pub struct FilterQuery<'a> {
    schema: &'a str,
    table_filter_pairs: Vec<(&'a Table, Option<QueryFilter<'a>>)>,
    order: Option<(&'a SqlName, &'a str)>,
    first: Option<String>,
    skip: Option<String>,
    block: BlockNumber,
}

impl<'a> FilterQuery<'a> {
    fn order_by(&self, out: &mut AstPass<Pg>) -> QueryResult<()> {
        out.push_sql("\n order by ");
        if let Some((name, direction)) = &self.order {
            out.push_identifier(name.as_str())?;
            out.push_sql(" ");
            out.push_sql(direction);
            if name.as_str() != PRIMARY_KEY_COLUMN {
                out.push_sql(", ");
                out.push_identifier(PRIMARY_KEY_COLUMN)?;
            }
            Ok(())
        } else {
            out.push_identifier(PRIMARY_KEY_COLUMN)
        }
    }

    fn add_sort_key(&self, out: &mut AstPass<Pg>) -> QueryResult<()> {
        if let Some((name, _)) = self.order {
            if name.as_str() != PRIMARY_KEY_COLUMN {
                out.push_sql(", e.");
                out.push_identifier(name.as_str())?;
            }
        }
        Ok(())
    }

    fn limit(&self, out: &mut AstPass<Pg>) {
        if let Some(first) = &self.first {
            out.push_sql("\n limit ");
            out.push_sql(first);
        }
        if let Some(skip) = &self.skip {
            out.push_sql("\noffset ");
            out.push_sql(skip);
        }
    }

    fn filtered_rows(
        &self,
        table: &Table,
        filter: &Option<QueryFilter<'a>>,
        mut out: AstPass<Pg>,
    ) -> QueryResult<()> {
        // Generate
        //     from schema.table e
        //    where block_range @> $block
        //      and query_filter
        out.push_sql("\n  from ");
        out.push_identifier(&self.schema)?;
        out.push_sql(".");
        out.push_identifier(table.name.as_str())?;
        out.push_sql(" e");
        out.push_sql("\n where ");
        BlockRangeContainsClause::new(self.block).walk_ast(out.reborrow())?;
        if let Some(filter) = filter {
            out.push_sql(" and ");
            filter.walk_ast(out)?;
        }
        Ok(())
    }
}

impl<'a> QueryFragment<Pg> for FilterQuery<'a> {
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();
        if self.table_filter_pairs.is_empty() {
            return Ok(());
        }

        // The queries are a little tricky because we need to defer generating
        // JSONB until we actually know which rows we really need to make these
        // queries fast; otherwise, Postgres spends too much time generating
        // JSONB, most of which will never get used
        if self.table_filter_pairs.len() == 1 {
            // The common case is that we have just one table; for that we
            // can generate a simpler/faster query. We generate
            // select '..' as entity, to_jsonb(e.*) as data
            // from (
            //   select *
            //     from schema.table
            //    where entity_filter
            //      and block_range @> INTMAX
            //    order by ...
            //    limit n offset m
            // ) e
            let (table, filter) = self
                .table_filter_pairs
                .first()
                .expect("we just checked that there is exactly one");
            out.push_sql("select ");
            out.push_bind_param::<Text, _>(&table.object)?;
            out.push_sql(
                " as entity, to_jsonb(e.*) as data\n  from (\n  select *\
                 \n",
            );
            self.filtered_rows(table, filter, out.reborrow())?;
            self.order_by(&mut out)?;
            self.limit(&mut out);
            // close the outer select
            out.push_sql(") e");
        } else {
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
            //   select '...' as entity, id, vid
            //     from table1
            //    where entity_filter
            //    union all
            //    ...
            //    order by ...
            //    limit n offset m)
            // select matches.entity, to_jsonb(e.*) as data, sort_key, id
            //   from table1 e, matches
            //  where e.vid = matches.vid and matches.entity = '...'
            //  union all
            //  ...
            //  order by ...

            // Step 1: build matches CTE
            out.push_sql("with matches as (");
            for (i, (table, filter)) in self.table_filter_pairs.iter().enumerate() {
                if i > 0 {
                    out.push_sql("\nunion all\n");
                }
                out.push_sql("select ");
                out.push_bind_param::<Text, _>(&table.object)?;
                out.push_sql(" as entity, e.id, e.vid");
                self.add_sort_key(&mut out)?;
                self.filtered_rows(table, filter, out.reborrow())?;
            }
            self.order_by(&mut out)?;
            self.limit(&mut out);
            out.push_sql(")\n");

            // Step 2: convert to JSONB
            for (i, (table, _)) in self.table_filter_pairs.iter().enumerate() {
                if i > 0 {
                    out.push_sql("\nunion all\n");
                }
                out.push_sql("select matches.entity, to_jsonb(e.*) as data, e.id");
                self.add_sort_key(&mut out)?;
                out.push_sql("\n  from ");
                out.push_identifier(&self.schema)?;
                out.push_sql(".");
                out.push_identifier(table.name.as_str())?;
                out.push_sql(" e, matches");
                out.push_sql("\n where e.vid = matches.vid and matches.entity = ");
                out.push_bind_param::<Text, _>(&table.object)?;
            }
            self.order_by(&mut out)?;
        }
        Ok(())
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
pub struct ClampRangeQuery<'a> {
    schema: &'a str,
    table: &'a Table,
    key: &'a EntityKey,
    block: BlockNumber,
}

impl<'a> QueryFragment<Pg> for ClampRangeQuery<'a> {
    fn walk_ast(&self, mut out: AstPass<Pg>) -> QueryResult<()> {
        // update table
        //    set block_range = int4range(lower(block_range), $block)
        //  where id = $id
        //    and block_range @> INTMAX
        out.unsafe_to_cache_prepared();
        out.push_sql("update ");
        out.push_identifier(self.schema)?;
        out.push_sql(".");
        out.push_identifier(self.table.name.as_str())?;
        out.push_sql("\n   set ");
        out.push_identifier(BLOCK_RANGE_COLUMN)?;
        out.push_sql(" = int4range(lower(");
        out.push_identifier(BLOCK_RANGE_COLUMN)?;
        out.push_sql("), ");
        out.push_bind_param::<Integer, _>(&self.block)?;
        out.push_sql(")\n where ");
        out.push_identifier(PRIMARY_KEY_COLUMN)?;
        out.push_sql(" = ");
        out.push_bind_param::<Text, _>(&self.key.entity_id)?;
        out.push_sql(" and (");
        out.push_sql(BLOCK_RANGE_CURRENT);
        out.push_sql(")");
        Ok(())
    }
}

impl<'a> QueryId for ClampRangeQuery<'a> {
    type QueryId = ();

    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<'a, Conn> RunQueryDsl<Conn> for ClampRangeQuery<'a> {}

/// Helper struct for returning the id's touched by the RevertRemove and
/// RevertExtend queries
#[derive(QueryableByName, PartialEq, Eq, Hash)]
pub struct RevertEntityData {
    #[sql_type = "Text"]
    pub id: String,
}

/// A query that removes all versions whose block range lies entirely
/// beyond `block`
#[derive(Debug, Clone, Constructor)]
pub struct RevertRemoveQuery<'a> {
    schema: &'a str,
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
        out.push_identifier(&self.schema)?;
        out.push_sql(".");
        out.push_identifier(self.table.name.as_str())?;
        out.push_sql("\n where lower(");
        out.push_identifier(BLOCK_RANGE_COLUMN)?;
        out.push_sql(") >= ");
        out.push_bind_param::<Integer, _>(&self.block)?;
        out.push_sql("\nreturning ");
        out.push_identifier(PRIMARY_KEY_COLUMN)
    }
}

impl<'a> QueryId for RevertRemoveQuery<'a> {
    type QueryId = ();

    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<'a> LoadQuery<PgConnection, RevertEntityData> for RevertRemoveQuery<'a> {
    fn internal_load(self, conn: &PgConnection) -> QueryResult<Vec<RevertEntityData>> {
        conn.query_by_name(&self)
    }
}

impl<'a, Conn> RunQueryDsl<Conn> for RevertRemoveQuery<'a> {}

/// A query that unclamps the block range of all versions that contain
/// `block` by setting the upper bound of the block range to infinity
#[derive(Debug, Clone, Constructor)]
pub struct RevertClampQuery<'a> {
    schema: &'a str,
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
        //   returning id
        out.push_sql("update ");
        out.push_identifier(&self.schema)?;
        out.push_sql(".");
        out.push_identifier(self.table.name.as_str())?;
        out.push_sql("\n   set ");
        out.push_identifier(BLOCK_RANGE_COLUMN)?;
        out.push_sql(" = int4range(lower(");
        out.push_identifier(BLOCK_RANGE_COLUMN)?;
        out.push_sql("), null)\n where");
        out.push_identifier(BLOCK_RANGE_COLUMN)?;
        out.push_sql(" @> ");
        out.push_bind_param::<Integer, _>(&self.block)?;
        out.push_sql(" and not ");
        out.push_sql(BLOCK_RANGE_CURRENT);
        out.push_sql("\nreturning ");
        out.push_identifier(PRIMARY_KEY_COLUMN)
    }
}

impl<'a> QueryId for RevertClampQuery<'a> {
    type QueryId = ();

    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<'a> LoadQuery<PgConnection, RevertEntityData> for RevertClampQuery<'a> {
    fn internal_load(self, conn: &PgConnection) -> QueryResult<Vec<RevertEntityData>> {
        conn.query_by_name(&self)
    }
}

impl<'a, Conn> RunQueryDsl<Conn> for RevertClampQuery<'a> {}
