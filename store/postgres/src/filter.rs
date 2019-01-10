use bigdecimal::BigDecimal;
use diesel::dsl::{self, sql};
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::BoxedSelectStatement;
use diesel::serialize::ToSql;
use diesel::sql_types::{Array, Bool, Float, HasSqlType, Integer, Jsonb, Numeric, Text};
use std::str::FromStr;

use graph::components::store::EntityFilter;
use graph::data::store::*;
use graph::prelude::BigInt;
use graph::serde_json;

use db_schema::entities;
use models::SqlValue;

pub(crate) struct UnsupportedFilter {
    pub filter: String,
    pub value: Value,
}

type FilterExpression = Box<BoxableExpression<entities::table, Pg, SqlType = Bool>>;

trait IntoFilter {
    fn into_filter(self, attribute: String, op: &str) -> FilterExpression;
}

impl IntoFilter for String {
    fn into_filter(self, attribute: String, op: &str) -> FilterExpression {
        Box::new(
            sql("data -> ")
                .bind::<Text, _>(attribute)
                .sql("->> 'data'")
                .sql(op)
                .bind::<Text, _>(self),
        ) as FilterExpression
    }
}

impl IntoFilter for f32 {
    fn into_filter(self, attribute: String, op: &str) -> FilterExpression {
        Box::new(
            sql("(data -> ")
                .bind::<Text, _>(attribute)
                .sql("->> 'data')::float")
                .sql(op)
                .bind::<Float, _>(self),
        ) as FilterExpression
    }
}

impl IntoFilter for i32 {
    fn into_filter(self, attribute: String, op: &str) -> FilterExpression {
        Box::new(
            sql("(data -> ")
                .bind::<Text, _>(attribute)
                .sql("->> 'data')::int")
                .sql(op)
                .bind::<Integer, _>(self),
        ) as FilterExpression
    }
}

impl IntoFilter for bool {
    fn into_filter(self, attribute: String, op: &str) -> FilterExpression {
        Box::new(
            sql("(data -> ")
                .bind::<Text, _>(attribute)
                .sql("->> 'data')::boolean")
                .sql(op)
                .bind::<Bool, _>(self),
        ) as FilterExpression
    }
}

impl IntoFilter for BigInt {
    fn into_filter(self, attribute: String, op: &str) -> FilterExpression {
        Box::new(
            sql("(data -> ")
                .bind::<Text, _>(attribute)
                .sql("->> 'data')::numeric")
                .sql(op)
                // Using `BigDecimal::new(query_value.0, 0)` results in a
                // mismatch of `bignum` versions, go through the string
                // representation to work around that.
                .bind::<Numeric, _>(BigDecimal::from_str(&self.to_string()).unwrap()),
        ) as FilterExpression
    }
}

trait IntoArrayFilter<T>
where
    T: 'static,
{
    fn into_array_filter<U>(self, attribute: String, op: &str, coercion: &str) -> FilterExpression
    where
        T: ToSql<U, Pg>,
        U: 'static,
        Pg: HasSqlType<U>;
}

impl IntoArrayFilter<SqlValue> for Vec<SqlValue> {
    fn into_array_filter<U>(self, attribute: String, op: &str, coercion: &str) -> FilterExpression
    where
        SqlValue: ToSql<U, Pg>,
        U: 'static,
        Pg: HasSqlType<U>,
    {
        Box::new(
            sql("(data -> ")
                .bind::<Text, _>(attribute)
                .sql("->> 'data')")
                .sql(coercion)
                .sql(op)
                .sql("(")
                .bind::<Array<U>, _>(self)
                .sql(")"),
        ) as FilterExpression
    }
}

/// Adds `filter` to a `SELECT data FROM entities` statement.
pub(crate) fn store_filter(
    query: BoxedSelectStatement<Jsonb, entities::table, Pg>,
    filter: EntityFilter,
) -> Result<BoxedSelectStatement<Jsonb, entities::table, Pg>, UnsupportedFilter> {
    Ok(query.filter(build_filter(filter)?))
}

fn build_filter(filter: EntityFilter) -> Result<FilterExpression, UnsupportedFilter> {
    use self::EntityFilter::*;

    let false_expr = Box::new(false.into_sql::<Bool>()) as FilterExpression;
    let true_expr = Box::new(true.into_sql::<Bool>()) as FilterExpression;

    match filter {
        And(filters) => filters.into_iter().try_fold(true_expr, |p, filter| {
            build_filter(filter).map(|filter_expr| Box::new(p.and(filter_expr)) as FilterExpression)
        }),

        Or(filters) => filters.into_iter().try_fold(false_expr, |p, filter| {
            build_filter(filter).map(|filter_expr| Box::new(p.or(filter_expr)) as FilterExpression)
        }),

        Contains(..) | NotContains(..) => {
            let (attribute, contains, op, value) = match filter {
                EntityFilter::Contains(attribute, value) => (attribute, true, " LIKE ", value),
                EntityFilter::NotContains(attribute, value) => {
                    (attribute, false, " NOT LIKE ", value)
                }
                _ => unreachable!(),
            };

            match value {
                Value::String(s) => Ok(s.into_filter(attribute, op)),
                Value::Bytes(b) => Ok(b.to_string().into_filter(attribute, op)),
                Value::List(lst) => {
                    let s = serde_json::to_string(&lst).expect("failed to serialize list value");
                    let predicate = sql("data -> ")
                        .bind::<Text, _>(attribute)
                        .sql("-> 'data' @> ")
                        .bind::<Text, _>(s);
                    if contains {
                        Ok(Box::new(predicate) as FilterExpression)
                    } else {
                        Ok(Box::new(dsl::not(predicate)) as FilterExpression)
                    }
                }
                Value::Null
                | Value::Float(_)
                | Value::Int(_)
                | Value::Bool(_)
                | Value::BigInt(_) => {
                    return Err(UnsupportedFilter {
                        filter: if contains { "contains" } else { "not_contains" }.to_owned(),
                        value,
                    })
                }
            }
        }

        Equal(..) | Not(..) => {
            let (attribute, op, value) = match filter {
                Equal(attribute, value) => (attribute, " = ", value),
                Not(attribute, value) => (attribute, " != ", value),
                _ => unreachable!(),
            };

            match value {
                Value::BigInt(n) => Ok(n.into_filter(attribute, op)),
                Value::Bool(b) => Ok(b.into_filter(attribute, op)),
                Value::Bytes(b) => Ok(b.to_string().into_filter(attribute, op)),
                Value::Float(n) => Ok(n.into_filter(attribute, op)),
                Value::Int(n) => Ok(n.into_filter(attribute, op)),
                Value::List(lst) => {
                    let s = serde_json::to_string(&lst).expect("failed to serialize list value");
                    Ok(s.into_filter(attribute, op))
                }
                Value::Null => Ok(Box::new(
                    sql("data -> ")
                        .bind::<Text, _>(attribute)
                        .sql(" ->> 'type'")
                        .sql(op)
                        .sql("'Null' "),
                ) as FilterExpression),
                Value::String(s) => Ok(s.into_filter(attribute, op)),
            }
        }

        GreaterThan(..) | LessThan(..) | GreaterOrEqual(..) | LessOrEqual(..) => {
            let (attribute, op, value) = match filter {
                GreaterThan(attribute, value) => (attribute, " > ", value),
                LessThan(attribute, value) => (attribute, " < ", value),
                GreaterOrEqual(attribute, value) => (attribute, " >= ", value),
                LessOrEqual(attribute, value) => (attribute, " <= ", value),
                _ => unreachable!(),
            };

            match value {
                Value::BigInt(n) => Ok(n.into_filter(attribute, op)),
                Value::Float(n) => Ok(n.into_filter(attribute, op)),
                Value::Int(n) => Ok(n.into_filter(attribute, op)),
                Value::String(s) => Ok(s.into_filter(attribute, op)),
                Value::Bool(_) | Value::Bytes(_) | Value::List(_) | Value::Null => {
                    return Err(UnsupportedFilter {
                        filter: op.to_owned(),
                        value,
                    })
                }
            }
        }

        In(attribute, values) => {
            if values.is_empty() {
                return Ok(false_expr);
            }
            let op = " = ANY ";

            match values[0] {
                Value::BigInt(_) => Ok(SqlValue::new_array(values).into_array_filter::<Numeric>(
                    attribute,
                    op,
                    "::numeric",
                )),
                Value::Bool(_) => Ok(SqlValue::new_array(values).into_array_filter::<Bool>(
                    attribute,
                    op,
                    "::boolean",
                )),
                Value::Bytes(_) => {
                    Ok(SqlValue::new_array(values).into_array_filter::<Text>(attribute, op, ""))
                }
                Value::Float(_) => Ok(SqlValue::new_array(values)
                    .into_array_filter::<Float>(attribute, op, "::float")),
                Value::Int(_) => Ok(SqlValue::new_array(values)
                    .into_array_filter::<Integer>(attribute, op, "::int")),
                Value::String(_) => {
                    Ok(SqlValue::new_array(values).into_array_filter::<Text>(attribute, op, ""))
                }
                Value::List(_) | Value::Null => {
                    return Err(UnsupportedFilter {
                        filter: "in".to_owned(),
                        value: Value::List(values),
                    })
                }
            }
        }

        NotIn(attribute, values) => {
            if values.is_empty() {
                return Ok(true_expr);
            }

            build_filter(And(values
                .into_iter()
                .map(|value| Not(attribute.clone(), value))
                .collect()))
        }

        StartsWith(..) | NotStartsWith(..) => {
            let (attribute, op, value) = match filter {
                StartsWith(attribute, value) => (attribute, " LIKE ", value),
                NotStartsWith(attribute, value) => (attribute, " NOT LIKE ", value),
                _ => unreachable!(),
            };

            match value {
                Value::String(s) => Ok(format!("{}%", s).into_filter(attribute, op)),
                Value::Bool(_)
                | Value::BigInt(_)
                | Value::Bytes(_)
                | Value::Float(_)
                | Value::Int(_)
                | Value::List(_)
                | Value::Null => {
                    return Err(UnsupportedFilter {
                        filter: if op == " LIKE " {
                            "starts_with"
                        } else {
                            "not_starts_with"
                        }
                        .to_owned(),
                        value,
                    })
                }
            }
        }

        EndsWith(..) | NotEndsWith(..) => {
            let (attribute, op, value) = match filter {
                EndsWith(attribute, value) => (attribute, " LIKE ", value),
                NotEndsWith(attribute, value) => (attribute, " NOT LIKE ", value),
                _ => unreachable!(),
            };

            match value {
                Value::String(s) => Ok(format!("%{}", s).into_filter(attribute, op)),
                Value::Bool(_)
                | Value::BigInt(_)
                | Value::Bytes(_)
                | Value::Float(_)
                | Value::Int(_)
                | Value::List(_)
                | Value::Null => {
                    return Err(UnsupportedFilter {
                        filter: if op == " LIKE " {
                            "ends_with"
                        } else {
                            "not_ends_with"
                        }
                        .to_owned(),
                        value,
                    })
                }
            }
        }
    }
}
