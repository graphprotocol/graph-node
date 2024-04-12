use anyhow::anyhow;
use diesel::pg::Pg;
use diesel::serialize::{self, Output, ToSql};
use diesel::sql_types::{Binary, Bool, Int8, Integer, Text, Timestamptz};

use std::str::FromStr;

use super::{scalar, Value};

impl ToSql<Bool, Pg> for Value {
    fn to_sql(&self, out: &mut Output<Pg>) -> serialize::Result {
        match self {
            Value::Bool(b) => <bool as ToSql<Bool, Pg>>::to_sql(b, &mut out.reborrow()),
            v => Err(anyhow!(
                "Failed to convert non-boolean attribute value to boolean in SQL: {}",
                v
            )
            .into()),
        }
    }
}

impl ToSql<Integer, Pg> for Value {
    fn to_sql(&self, out: &mut Output<Pg>) -> serialize::Result {
        match self {
            Value::Int(i) => <i32 as ToSql<Integer, Pg>>::to_sql(i, &mut out.reborrow()),
            v => Err(anyhow!(
                "Failed to convert non-int attribute value to int in SQL: {}",
                v
            )
            .into()),
        }
    }
}

impl ToSql<Int8, Pg> for Value {
    fn to_sql(&self, out: &mut Output<Pg>) -> serialize::Result {
        match self {
            Value::Int8(i) => <i64 as ToSql<Int8, Pg>>::to_sql(i, &mut out.reborrow()),
            Value::Int(i) => <i64 as ToSql<Int8, Pg>>::to_sql(&(*i as i64), &mut out.reborrow()),
            v => Err(anyhow!(
                "Failed to convert non-int8 attribute value to int8 in SQL: {}",
                v
            )
            .into()),
        }
    }
}

impl ToSql<Timestamptz, Pg> for Value {
    fn to_sql(&self, out: &mut Output<Pg>) -> serialize::Result {
        match self {
            Value::Timestamp(i) => i.to_sql(&mut out.reborrow()),
            v => Err(anyhow!(
                "Failed to convert non-timestamp attribute value to timestamp in SQL: {}",
                v
            )
            .into()),
        }
    }
}

impl ToSql<Text, Pg> for Value {
    fn to_sql(&self, out: &mut Output<Pg>) -> serialize::Result {
        match self {
            Value::String(s) => <String as ToSql<Text, Pg>>::to_sql(s, &mut out.reborrow()),
            Value::Bytes(h) => {
                <String as ToSql<Text, Pg>>::to_sql(&h.to_string(), &mut out.reborrow())
            }
            v => Err(anyhow!(
                "Failed to convert attribute value to String or Bytes in SQL: {}",
                v
            )
            .into()),
        }
    }
}

impl ToSql<Binary, Pg> for Value {
    fn to_sql(&self, out: &mut Output<Pg>) -> serialize::Result {
        match self {
            Value::Bytes(h) => <_ as ToSql<Binary, Pg>>::to_sql(&h.as_slice(), &mut out.reborrow()),
            Value::String(s) => <_ as ToSql<Binary, Pg>>::to_sql(
                scalar::Bytes::from_str(s)?.as_slice(),
                &mut out.reborrow(),
            ),
            v => Err(anyhow!("Failed to convert attribute value to Bytes in SQL: {}", v).into()),
        }
    }
}
