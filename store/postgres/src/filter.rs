use std::str::FromStr;

use bigdecimal::BigDecimal;
use db_schema::entities;
use diesel::dsl::sql;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::BoxedSelectStatement;
use diesel::sql_types::{Bool, Float, Integer, Jsonb, Numeric, Text};

use thegraph::components::store::StoreFilter;
use thegraph::data::store::*;

use serde_json;

pub(crate) struct UnsupportedFilter {
    pub filter: String,
    pub value: Value,
}

/// Adds `filter` to a `SELECT data FROM entities` statement.
pub(crate) fn store_filter<'a>(
    query: BoxedSelectStatement<'a, Jsonb, entities::table, Pg>,
    filter: StoreFilter,
) -> Result<BoxedSelectStatement<'a, Jsonb, entities::table, Pg>, UnsupportedFilter> {
    Ok(match filter {
        StoreFilter::And(filters) => filters.into_iter().try_fold(query, store_filter)?,
        StoreFilter::Contains(attribute, value) => match value {
            Value::String(query_value) => query.filter(
                sql("data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(" LIKE ")
                    .bind::<Text, _>(query_value),
            ),
            Value::Bytes(query_value) => query.filter(
                sql("data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(" LIKE ")
                    .bind::<Text, _>(query_value.to_string()),
            ),
            Value::List(query_value) => {
                let query_array =
                    serde_json::to_string(&query_value).expect("Failed to serialize Value");
                query.filter(
                    // Is `query_array` contained in array `data ->> attribute`?
                    sql("data ->> ")
                        .bind::<Text, _>(attribute)
                        .sql(" @> ")
                        .bind::<Text, _>(query_array),
                )
            }
            Value::Null | Value::Float(_) | Value::Int(_) | Value::Bool(_) | Value::BigInt(_) => {
                return Err(UnsupportedFilter {
                    filter: "contains".to_owned(),
                    value: value.clone(),
                })
            }
        },
        StoreFilter::Equal(..) | StoreFilter::Not(..) => {
            let (attribute, op, value) = match filter {
                StoreFilter::Equal(attribute, value) => (attribute, "=", value),
                StoreFilter::Not(attribute, value) => (attribute, "!=", value),
                _ => unreachable!(),
            };

            match value {
                Value::String(query_value) => query.filter(
                    sql("(")
                        .sql("data ->> ")
                        .bind::<Text, _>(attribute)
                        .sql(")")
                        .sql(op)
                        .bind::<Text, _>(query_value),
                ),
                Value::Float(query_value) => query.filter(
                    sql("(")
                        .sql("data ->> ")
                        .bind::<Text, _>(attribute)
                        .sql(")")
                        .sql("::float")
                        .sql(op)
                        .bind::<Float, _>(query_value),
                ),
                Value::Int(query_value) => query.filter(
                    sql("(data ->> ")
                        .bind::<Text, _>(attribute)
                        .sql(")")
                        .sql("::int")
                        .sql(op)
                        .bind::<Integer, _>(query_value),
                ),
                Value::Bool(query_value) => query.filter(
                    sql("(data ->> ")
                        .bind::<Text, _>(attribute)
                        .sql(")")
                        .sql("::boolean")
                        .sql(op)
                        .bind::<Bool, _>(query_value),
                ),
                Value::Null => {
                    query.filter(sql("data -> ").bind::<Text, _>(attribute).sql(" = 'null' "))
                }
                Value::List(query_value) => {
                    // Note that lists with the same elements but in different order
                    // are considered not equal.
                    let query_array =
                        serde_json::to_string(&query_value).expect("Failed to serialize Value");
                    query.filter(
                        sql("data ->> ")
                            .bind::<Text, _>(attribute)
                            .sql(op)
                            .bind::<Text, _>(query_array),
                    )
                }
                Value::Bytes(query_value) => {
                    let hex_string =
                        serde_json::to_string(&query_value).expect("Failed to serialize Value");
                    query.filter(
                        sql("(data ->> ")
                            .bind::<Text, _>(attribute)
                            .sql(op)
                            .bind::<Text, _>(hex_string),
                    )
                }
                Value::BigInt(query_value) => query.filter(
                    sql("(data ->> ")
                    .bind::<Text, _>(attribute)
                .sql(")")
                .sql("::numeric")
                .sql(op)
                // Using `BigDecimal::new(query_value.0, 0)` results in a
                // mismatch of `bignum` versions, go through the string
                // representation to work around that.
                .bind::<Numeric, _>(BigDecimal::from_str(&query_value.to_string()).unwrap()),
                ),
            }
        }
        StoreFilter::GreaterThan(attribute, value) => match value {
            Value::String(query_value) => query.filter(
                sql("data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(" > ")
                    .bind::<Text, _>(query_value),
            ),
            Value::Float(query_value) => query.filter(
                sql("(data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(")::float > ")
                    .bind::<Float, _>(query_value as f32),
            ),
            Value::Int(query_value) => query.filter(
                sql("(data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(")::int > ")
                    .bind::<Integer, _>(query_value),
            ),
            _ => unimplemented!(),
        },
        StoreFilter::LessThan(attribute, value) => match value {
            Value::String(query_value) => query.filter(
                sql("data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(" < ")
                    .bind::<Text, _>(query_value),
            ),
            Value::Float(query_value) => query.filter(
                sql("(data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(")::float < ")
                    .bind::<Float, _>(query_value as f32),
            ),
            Value::Int(query_value) => query.filter(
                sql("(data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(")::int < ")
                    .bind::<Integer, _>(query_value),
            ),
            _ => unimplemented!(),
        },
        StoreFilter::GreaterOrEqual(attribute, value) => match value {
            Value::String(query_value) => query.filter(
                sql("data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(" >= ")
                    .bind::<Text, _>(query_value),
            ),
            Value::Float(query_value) => query.filter(
                sql("(data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(")::float >= ")
                    .bind::<Float, _>(query_value as f32),
            ),
            Value::Int(query_value) => query.filter(
                sql("(data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(")::int >= ")
                    .bind::<Integer, _>(query_value),
            ),
            _ => unimplemented!(),
        },
        StoreFilter::LessThanOrEqual(attribute, value) => match value {
            Value::String(query_value) => query.filter(
                sql("data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(" <= ")
                    .bind::<Text, _>(query_value),
            ),
            Value::Float(query_value) => query.filter(
                sql("(data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(")::float <= ")
                    .bind::<Float, _>(query_value as f32),
            ),
            Value::Int(query_value) => query.filter(
                sql("(data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(")::int <= ")
                    .bind::<Integer, _>(query_value),
            ),
            _ => unimplemented!(),
        },
        StoreFilter::NotContains(attribute, value) => match value {
            Value::String(query_value) => query.filter(
                sql("data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(" NOT LIKE ")
                    .bind::<Text, _>(query_value),
            ),
            _ => unimplemented!(),
        },

        // We will add support for more filters later
        _ => unimplemented!(),
    })
}
