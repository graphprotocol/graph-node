use diesel::dsl::{self, sql};
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::serialize::ToSql;
use diesel::sql_types::{Array, Bool, Double, HasSqlType, Integer, Numeric, Text};
use std::error::Error as StdError;
use std::fmt::{self, Display};

use graph::components::store::EntityFilter;
use graph::data::store::*;
use graph::prelude::serde_json;
use graph::prelude::{BigDecimal, BigInt};

use crate::entities::{EntitySource, STRING_PREFIX_SIZE};
use crate::sql_value::SqlValue;

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

impl StdError for UnsupportedFilter {}

impl From<UnsupportedFilter> for diesel::result::Error {
    fn from(error: UnsupportedFilter) -> Self {
        diesel::result::Error::QueryBuilderError(Box::new(error))
    }
}

type FilterExpression<QS> = Box<dyn BoxableExpression<QS, Pg, SqlType = Bool>>;

trait IntoFilter<QS> {
    fn into_filter(self, attribute: String, op: &str) -> FilterExpression<QS>;
}

impl<QS> IntoFilter<QS> for String {
    fn into_filter(self, attribute: String, op: &str) -> FilterExpression<QS> {
        if &attribute == "id" {
            // Use the `id` column rather than `data->'id'->>'data'` so that
            // Postgres can use the primary key index on the entities table
            Box::new(sql("id").sql(op).bind::<Text, _>(self)) as FilterExpression<QS>
        } else {
            // Generate
            //  (left(attribute, prefix_size) op left(self, prefix_size) and attribute op self)
            // The first condition is there to make the index on attribute usable,
            // the second so that we only return correct results
            Box::new(
                sql("(left(c.data -> ")
                    .bind::<Text, _>(attribute.clone())
                    .sql("->> 'data', ")
                    .sql(&STRING_PREFIX_SIZE.to_string())
                    .sql(") ")
                    .sql(op)
                    .sql(" left(")
                    .bind::<Text, _>(self.clone())
                    .sql(", ")
                    .sql(&STRING_PREFIX_SIZE.to_string())
                    .sql(") and c.data -> ")
                    .bind::<Text, _>(attribute)
                    .sql("->> 'data' ")
                    .sql(op)
                    .bind::<Text, _>(self)
                    .sql(")"),
            ) as FilterExpression<QS>
        }
    }
}

impl<QS> IntoFilter<QS> for f64 {
    fn into_filter(self, attribute: String, op: &str) -> FilterExpression<QS> {
        Box::new(
            sql("(c.data -> ")
                .bind::<Text, _>(attribute)
                .sql("->> 'data')::float")
                .sql(op)
                .bind::<Double, _>(self),
        ) as FilterExpression<QS>
    }
}

impl<QS> IntoFilter<QS> for i32 {
    fn into_filter(self, attribute: String, op: &str) -> FilterExpression<QS> {
        Box::new(
            sql("(c.data -> ")
                .bind::<Text, _>(attribute)
                .sql("->> 'data')::int")
                .sql(op)
                .bind::<Integer, _>(self),
        ) as FilterExpression<QS>
    }
}

impl<QS> IntoFilter<QS> for bool {
    fn into_filter(self, attribute: String, op: &str) -> FilterExpression<QS> {
        Box::new(
            sql("(c.data -> ")
                .bind::<Text, _>(attribute)
                .sql("->> 'data')::boolean")
                .sql(op)
                .bind::<Bool, _>(self),
        ) as FilterExpression<QS>
    }
}

impl<QS> IntoFilter<QS> for BigInt {
    fn into_filter(self, attribute: String, op: &str) -> FilterExpression<QS> {
        Box::new(
            sql("(c.data -> ")
                .bind::<Text, _>(attribute)
                .sql("->> 'data')::numeric")
                .sql(op)
                .bind::<Text, _>(self.to_string())
                .sql("::numeric"),
        ) as FilterExpression<QS>
    }
}

impl<QS> IntoFilter<QS> for BigDecimal {
    fn into_filter(self, attribute: String, op: &str) -> FilterExpression<QS> {
        Box::new(
            sql("(c.data -> ")
                .bind::<Text, _>(attribute)
                .sql("->> 'data')::numeric")
                .sql(op)
                .bind::<Text, _>(self.to_string())
                .sql("::numeric"),
        ) as FilterExpression<QS>
    }
}

trait IntoArrayFilter<QS, T>
where
    T: 'static,
{
    fn into_array_filter<U>(
        self,
        attribute: String,
        op: &str,
        coercion: &str,
    ) -> FilterExpression<QS>
    where
        T: ToSql<U, Pg>,
        U: 'static,
        Pg: HasSqlType<U>;
}

impl<QS> IntoArrayFilter<QS, SqlValue> for Vec<SqlValue> {
    fn into_array_filter<U>(
        self,
        attribute: String,
        op: &str,
        coercion: &str,
    ) -> FilterExpression<QS>
    where
        SqlValue: ToSql<U, Pg>,
        U: 'static,
        Pg: HasSqlType<U>,
    {
        if &attribute == "id" {
            // Use the `id` column rather than `data->'id'->>'data'` so that
            // Postgres can use the primary key index on the entities table
            Box::new(
                sql("c.id")
                    .sql(coercion)
                    .sql(op)
                    .sql("(")
                    .bind::<Array<U>, _>(self)
                    .sql(")"),
            ) as FilterExpression<QS>
        } else {
            Box::new(
                sql("(c.data -> ")
                    .bind::<Text, _>(attribute)
                    .sql("->> 'data')")
                    .sql(coercion)
                    .sql(op)
                    .sql("(")
                    .bind::<Array<U>, _>(self)
                    .sql(")"),
            ) as FilterExpression<QS>
        }
    }
}

pub(crate) fn build_filter<QS>(
    filter: EntityFilter,
) -> Result<FilterExpression<QS>, UnsupportedFilter>
where
    QS: EntitySource + 'static,
{
    use self::EntityFilter::*;

    let false_expr = Box::new(false.into_sql::<Bool>()) as FilterExpression<QS>;
    let true_expr = Box::new(true.into_sql::<Bool>()) as FilterExpression<QS>;

    match filter {
        And(filters) => filters.into_iter().try_fold(true_expr, |p, filter| {
            build_filter(filter)
                .map(|filter_expr| Box::new(p.and(filter_expr)) as FilterExpression<QS>)
        }),

        Or(filters) => filters.into_iter().try_fold(false_expr, |p, filter| {
            build_filter(filter)
                .map(|filter_expr| Box::new(p.or(filter_expr)) as FilterExpression<QS>)
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
                Value::String(s) => {
                    if s.starts_with('%') || s.ends_with('%') {
                        Ok(s.into_filter(attribute, op))
                    } else {
                        Ok(format!("%{}%", s).into_filter(attribute, op))
                    }
                }
                Value::Bytes(b) => Ok(format!("%{}%", b.to_string()).into_filter(attribute, op)),
                Value::List(lst) => {
                    let s = serde_json::to_string(&lst).expect("failed to serialize list value");
                    let predicate = sql("c.data -> ")
                        .bind::<Text, _>(attribute)
                        .sql("-> 'data' @> ")
                        .bind::<Text, _>(s)
                        .sql("::jsonb");
                    if contains {
                        Ok(Box::new(predicate) as FilterExpression<QS>)
                    } else {
                        Ok(Box::new(dsl::not(predicate)) as FilterExpression<QS>)
                    }
                }
                Value::Null
                | Value::BigDecimal(_)
                | Value::Int(_)
                | Value::Bool(_)
                | Value::BigInt(_) => {
                    return Err(UnsupportedFilter {
                        filter: if contains { "contains" } else { "not_contains" }.to_owned(),
                        value,
                    });
                }
            }
        }

        Equal(..) | Not(..) => {
            let (attribute, op, is_negated, value) = match filter {
                Equal(attribute, value) => (attribute, " = ", false, value),
                Not(attribute, value) => (attribute, " != ", true, value),
                _ => unreachable!(),
            };

            match value {
                Value::BigInt(n) => Ok(n.into_filter(attribute, op)),
                Value::Bool(b) => Ok(b.into_filter(attribute, op)),
                Value::Bytes(b) => Ok(b.to_string().into_filter(attribute, op)),
                Value::BigDecimal(n) => Ok(n.into_filter(attribute, op)),
                Value::Int(n) => Ok(n.into_filter(attribute, op)),
                Value::List(lst) => {
                    // In order to compare lists, we have to coerce the database value to jsonb
                    let s = serde_json::to_string(&lst).expect("failed to serialize list value");
                    Ok(Box::new(
                        sql("(")
                            .sql("c.data -> ")
                            .bind::<Text, _>(attribute)
                            .sql("-> 'data'")
                            .sql(")::jsonb")
                            .sql(op)
                            .bind::<Text, _>(s)
                            .sql("::jsonb"),
                    ))
                }
                Value::Null => Ok(if is_negated {
                    // Value is not null if the property is present ("IS NOT NULL") and is not a
                    // value of the 'Null' type.
                    Box::new(
                        sql("c.data -> ")
                            .bind::<Text, _>(attribute.clone())
                            .sql(" IS NOT NULL ")
                            .and(
                                sql("c.data -> ")
                                    .bind::<Text, _>(attribute)
                                    .sql(" ->> 'type' != 'Null' "),
                            ),
                    )
                } else {
                    // Value is null if the property is missing ("IS NULL") or is present but is a
                    // value of the 'Null' type.
                    Box::new(
                        sql("c.data -> ")
                            .bind::<Text, _>(attribute.clone())
                            .sql(" IS NULL ")
                            .or(sql("c.data -> ")
                                .bind::<Text, _>(attribute)
                                .sql(" ->> 'type' = 'Null' ")),
                    )
                }),
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
                Value::BigDecimal(n) => Ok(n.into_filter(attribute, op)),
                Value::Int(n) => Ok(n.into_filter(attribute, op)),
                Value::String(s) => Ok(s.into_filter(attribute, op)),
                Value::Bool(_) | Value::Bytes(_) | Value::List(_) | Value::Null => {
                    return Err(UnsupportedFilter {
                        filter: op.to_owned(),
                        value,
                    });
                }
            }
        }

        In(attribute, values) => {
            if values.is_empty() {
                return Ok(false_expr);
            }
            let op = " = ANY ";

            match values[0] {
                Value::BigInt(_) | Value::BigDecimal(_) => Ok(SqlValue::new_array(values)
                    .into_array_filter::<Numeric>(attribute, op, "::numeric")),
                Value::Bool(_) => Ok(SqlValue::new_array(values).into_array_filter::<Bool>(
                    attribute,
                    op,
                    "::boolean",
                )),
                Value::Bytes(_) => {
                    Ok(SqlValue::new_array(values).into_array_filter::<Text>(attribute, op, ""))
                }
                Value::Int(_) => Ok(SqlValue::new_array(values)
                    .into_array_filter::<Integer>(attribute, op, "::int")),
                Value::String(_) => {
                    Ok(SqlValue::new_array(values).into_array_filter::<Text>(attribute, op, ""))
                }
                Value::List(_) | Value::Null => {
                    return Err(UnsupportedFilter {
                        filter: "in".to_owned(),
                        value: Value::List(values),
                    });
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
                | Value::BigDecimal(_)
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
                    });
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
                | Value::BigDecimal(_)
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
                    });
                }
            }
        }
    }
}
