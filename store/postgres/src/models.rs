use diesel::pg::Pg;
use diesel::serialize::{self, Output, ToSql};
use diesel::sql_types::{Bool, Integer, Jsonb, Numeric, Text, VarChar};
use graph::serde_json;
use std::io::Write;

use graph::data::store::Value;

pub type EntityJSON = serde_json::Value;

#[derive(Queryable, QueryableByName, Debug)]
pub struct EntityTable {
    #[sql_type = "VarChar"]
    pub id: String,
    #[sql_type = "VarChar"]
    pub subgraph: String,
    #[sql_type = "VarChar"]
    pub entity: String,
    #[sql_type = "Jsonb"]
    pub data: EntityJSON,
    #[sql_type = "VarChar"]
    pub event_source: String,
}

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

impl ToSql<Numeric, Pg> for SqlValue {
    fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> serialize::Result {
        match &self.0 {
            Value::BigDecimal(d) => <_ as ToSql<Numeric, Pg>>::to_sql(&d, out),
            Value::BigInt(number) => {
                <_ as ToSql<Numeric, Pg>>::to_sql(&number.clone().to_big_decimal(0.into()), out)
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
