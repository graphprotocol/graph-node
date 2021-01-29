use diesel::pg::Pg;
use diesel::serialize::{self, Output, ToSql};
use diesel::sql_types::{Binary, Bool, Integer, Text};
use graph::prelude::anyhow::anyhow;
use std::io::Write;
use std::str::FromStr;

use graph::data::store::{scalar, Value};

#[derive(Clone, Debug, PartialEq, AsExpression)]
pub struct SqlValue(Value);

impl SqlValue {
    pub fn new_array(values: Vec<Value>) -> Vec<Self> {
        values.into_iter().map(SqlValue).collect()
    }
}

impl ToSql<Bool, Pg> for SqlValue {
    fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> serialize::Result {
        match &self.0 {
            Value::Bool(b) => <bool as ToSql<Bool, Pg>>::to_sql(&b, out),
            v => Err(anyhow!(
                "Failed to convert non-boolean attribute value to boolean in SQL: {}",
                v
            )
            .into()),
        }
    }
}

impl ToSql<Integer, Pg> for SqlValue {
    fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> serialize::Result {
        match &self.0 {
            Value::Int(i) => <i32 as ToSql<Integer, Pg>>::to_sql(&i, out),
            v => Err(anyhow!(
                "Failed to convert non-int attribute value to int in SQL: {}",
                v
            )
            .into()),
        }
    }
}

impl ToSql<Text, Pg> for SqlValue {
    fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> serialize::Result {
        match &self.0 {
            Value::String(s) => <String as ToSql<Text, Pg>>::to_sql(&s, out),
            Value::Bytes(h) => <String as ToSql<Text, Pg>>::to_sql(&h.to_string(), out),
            v => Err(anyhow!(
                "Failed to convert attribute value to String or Bytes in SQL: {}",
                v
            )
            .into()),
        }
    }
}

impl ToSql<Binary, Pg> for SqlValue {
    fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> serialize::Result {
        match &self.0 {
            Value::Bytes(h) => <_ as ToSql<Binary, Pg>>::to_sql(&h.as_slice(), out),
            Value::String(s) => {
                <_ as ToSql<Binary, Pg>>::to_sql(scalar::Bytes::from_str(s)?.as_slice(), out)
            }
            v => Err(anyhow!("Failed to convert attribute value to Bytes in SQL: {}", v).into()),
        }
    }
}
