use diesel::pg::Pg;
use diesel::serialize::{self, Output, ToSql};
use diesel::sql_types::{Binary, Bool, Integer, Numeric, Text};
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
        match self.0 {
            Value::Bool(ref b) => <bool as ToSql<Bool, Pg>>::to_sql(&b, out),
            _ => panic!("Failed to convert non-boolean attribute value to boolean in SQL"),
        }
    }
}

impl ToSql<Integer, Pg> for SqlValue {
    fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> serialize::Result {
        match self.0 {
            Value::Int(ref i) => <i32 as ToSql<Integer, Pg>>::to_sql(&i, out),
            _ => panic!("Failed to convert non-int attribute value to int in SQL"),
        }
    }
}

// Used only for JSONB support
impl ToSql<Numeric, Pg> for SqlValue {
    fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> serialize::Result {
        match &self.0 {
            Value::BigDecimal(d) => <_ as ToSql<Numeric, Pg>>::to_sql(&d, out),
            Value::BigInt(number) => {
                <_ as ToSql<Numeric, Pg>>::to_sql(&scalar::BigDecimal::new(number.clone(), 0), out)
            }
            _ => panic!("Failed to convert attribute value to bigint in SQL"),
        }
    }
}

impl ToSql<Text, Pg> for SqlValue {
    fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> serialize::Result {
        match self.0 {
            Value::String(ref s) => <String as ToSql<Text, Pg>>::to_sql(&s, out),
            Value::Bytes(ref h) => <String as ToSql<Text, Pg>>::to_sql(&h.to_string(), out),
            _ => panic!("Failed to convert attribute value to String or Bytes in SQL"),
        }
    }
}

impl ToSql<Binary, Pg> for SqlValue {
    fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> serialize::Result {
        match self.0 {
            Value::Bytes(ref h) => <_ as ToSql<Binary, Pg>>::to_sql(&h.as_slice(), out),
            Value::String(ref s) => {
                <_ as ToSql<Binary, Pg>>::to_sql(scalar::Bytes::from_str(s)?.as_slice(), out)
            }
            _ => panic!("Failed to convert attribute value to Bytes in SQL"),
        }
    }
}
