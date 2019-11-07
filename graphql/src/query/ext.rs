//! Extension traits for graphql_parser::query structs

use graphql_parser::query as q;

use std::collections::BTreeMap;

pub trait ValueExt {
    fn as_object(&self) -> &BTreeMap<q::Name, q::Value>;
    fn as_string(&self) -> &str;
}

impl ValueExt for q::Value {
    fn as_object(&self) -> &BTreeMap<q::Name, q::Value> {
        match self {
            q::Value::Object(object) => object,
            _ => panic!("expected a Value::Object"),
        }
    }

    fn as_string(&self) -> &str {
        match self {
            q::Value::String(string) => string,
            _ => panic!("expected a Value::String"),
        }
    }
}
