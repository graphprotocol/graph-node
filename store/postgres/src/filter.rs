use db_schema::entities;
use diesel::dsl::sql;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::query_builder::BoxedSelectStatement;
use diesel::sql_types::{Bool, Float, Integer, Jsonb, Text};

use thegraph::components::store::StoreFilter;
use thegraph::data::store::*;

/// Adds `filter` to a `SELECT data FROM entities` statement.
pub(crate) fn store_filter<'a>(
    query: BoxedSelectStatement<'a, Jsonb, entities::table, Pg>,
    filter: StoreFilter,
) -> BoxedSelectStatement<'a, Jsonb, entities::table, Pg> {
    match filter {
        StoreFilter::And(filters) => filters.into_iter().fold(query, store_filter),
        StoreFilter::Contains(attribute, value) => match value {
            Value::String(query_value) => query.filter(
                sql("data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(" LIKE ")
                    .bind::<Text, _>(query_value),
            ),
            _ => unimplemented!(),
        },
        StoreFilter::Equal(attribute, value) => match value {
            Value::String(query_value) => query.filter(
                sql("(data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(") = ")
                    .bind::<Text, _>(query_value),
            ),
            Value::Float(query_value) => query.filter(
                sql("(data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(")::float = ")
                    .bind::<Float, _>(query_value),
            ),
            Value::Int(query_value) => query.filter(
                sql("(data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(")::int = ")
                    .bind::<Integer, _>(query_value),
            ),
            Value::Bool(query_value) => query.filter(
                sql("(data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(")::boolean = ")
                    .bind::<Bool, _>(query_value),
            ),
            _ => unimplemented!(),
        },
        StoreFilter::Not(attribute, value) => match value {
            Value::String(query_value) => query.filter(
                sql("data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(" != ")
                    .bind::<Text, _>(query_value),
            ),
            Value::Float(query_value) => query.filter(
                sql("(data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(")::float != ")
                    .bind::<Float, _>(query_value as f32),
            ),
            Value::Int(query_value) => query.filter(
                sql("(data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(")::int != ")
                    .bind::<Integer, _>(query_value),
            ),
            Value::Bool(query_value) => query.filter(
                sql("(data ->> ")
                    .bind::<Text, _>(attribute)
                    .sql(")::boolean != ")
                    .bind::<Bool, _>(query_value),
            ),
            _ => unimplemented!(),
        },
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
    }
}
