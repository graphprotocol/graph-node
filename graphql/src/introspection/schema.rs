use std::sync::Arc;

use graphql_parser;

use graph::data::graphql::ext::DocumentExt;
use graph::data::graphql::ext::ObjectTypeExt;
use graph::prelude::s::Document;

use lazy_static::lazy_static;

use crate::schema::ast as sast;

const INTROSPECTION_SCHEMA: &str = include_str!("schema.graphql");

lazy_static! {
    pub static ref INTROSPECTION_DOCUMENT: Document =
        graphql_parser::parse_schema(INTROSPECTION_SCHEMA).unwrap();
    pub static ref INTROSPECTION_QUERY_TYPE: sast::ObjectType = sast::ObjectType::from(Arc::new(
        INTROSPECTION_DOCUMENT
            .get_root_query_type()
            .unwrap()
            .clone()
    ));
}

pub fn is_introspection_field(name: &str) -> bool {
    INTROSPECTION_QUERY_TYPE.field(name).is_some()
}
