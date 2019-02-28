use bigdecimal::BigDecimal;
use diesel::pg::Pg;
use diesel::serialize::{self, Output, ToSql};
use diesel::sql_types::{Bool, Double, Integer, Jsonb, Numeric, Text, VarChar};
use graph::serde_json;
use std::io::Write;
use std::str::FromStr;

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

impl ToSql<Double, Pg> for SqlValue {
    fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> serialize::Result {
        match self.0 {
            Value::Float(ref f) => <f64 as ToSql<Double, Pg>>::to_sql(&f, out),
            _ => panic!("Failed to convert non-float attribute value to float in SQL"),
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
        match self.0 {
            Value::BigInt(ref number) => <BigDecimal as ToSql<Numeric, Pg>>::to_sql(
                &BigDecimal::from_str(&number.to_string()).unwrap(),
                out,
            ),
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
