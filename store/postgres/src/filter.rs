use diesel::dsl::sql;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::serialize::ToSql;
use diesel::sql_types::{Array, Bool, Double, HasSqlType, Integer, Text};
use std::error::Error as StdError;
use std::fmt::{self, Display};

use graph::data::store::*;
use graph::prelude::{BigDecimal, BigInt};

use crate::entities::STRING_PREFIX_SIZE;
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
