use graphql_parser::query as gqlq;
use std::collections::{BTreeMap, HashMap};

use thegraph::prelude::*;

/// Builds a StoreQuery from GraphQL arguments.
pub fn build_query(
    entity: &gqlq::Name,
    arguments: &HashMap<&gqlq::Name, gqlq::Value>,
) -> StoreQuery {
    StoreQuery {
        entity: entity.to_owned(),
        range: build_range(arguments),
        filter: build_filter(arguments),
        order_by: build_order_by(arguments),
        order_direction: build_order_direction(arguments),
    }
}

/// Parses GraphQL arguments into a StoreRange, if present.
fn build_range(arguments: &HashMap<&gqlq::Name, gqlq::Value>) -> Option<StoreRange> {
    let first = arguments
        .get(&"first".to_string())
        .and_then(|value| match value {
            gqlq::Value::Int(n) => n.as_i64(),
            _ => None,
        })
        .and_then(|n| if n > 0 { Some(n as usize) } else { None });

    let skip = arguments
        .get(&"skip".to_string())
        .and_then(|value| match value {
            gqlq::Value::Int(n) => n.as_i64(),
            _ => None,
        })
        .and_then(|n| if n >= 0 { Some(n as usize) } else { None });

    match (first, skip) {
        (None, None) => None,
        (Some(first), None) => Some(StoreRange { first, skip: 0 }),
        (Some(first), Some(skip)) => Some(StoreRange { first, skip }),
        (None, Some(skip)) => Some(StoreRange { first: 100, skip }),
    }
}

/// Parses GraphQL arguments into a StoreFilter, if present.
fn build_filter(arguments: &HashMap<&gqlq::Name, gqlq::Value>) -> Option<StoreFilter> {
    arguments
        .get(&"filter".to_string())
        .and_then(|value| match value {
            gqlq::Value::Object(ref object) => Some(object),
            _ => None,
        })
        .map(|object| build_filter_from_object(object))
}

/// Parses a GraphQL input object into a StoreFilter, if present.
fn build_filter_from_object(object: &BTreeMap<gqlq::Name, gqlq::Value>) -> StoreFilter {
    StoreFilter::And(
        object
            .iter()
            .map(|(k, v)| build_filter_from_key_value_pair(k, v))
            .collect::<Vec<StoreFilter>>(),
    )
}

/// Strips the operator suffix from an input object key such as name_eq.
fn filter_attr(key: &gqlq::Name, suffix: &'static str) -> String {
    key.trim_right_matches(suffix).to_owned()
}

/// Parses a list of GraphQL values (if it is one) into a vector of entity attribute values.
fn list_values(value: &gqlq::Value) -> Vec<Value> {
    match value {
        gqlq::Value::List(values) => values.iter().map(Value::from).collect(),
        _ => vec![],
    }
}

/// Parses a ("name_eq", some_value) style pair into a StoreFilter.
fn build_filter_from_key_value_pair(key: &gqlq::Name, value: &gqlq::Value) -> StoreFilter {
    match key {
        s if s.ends_with("_not") => StoreFilter::Not(filter_attr(s, "_not"), value.into()),
        s if s.ends_with("_gt") => StoreFilter::Not(filter_attr(s, "_gt"), value.into()),
        s if s.ends_with("_lt") => StoreFilter::LessThan(filter_attr(s, "_lt"), value.into()),
        s if s.ends_with("_gte") => {
            StoreFilter::GreaterOrEqual(filter_attr(s, "_gte"), value.into())
        }
        s if s.ends_with("_lte") => {
            StoreFilter::LessThanOrEqual(filter_attr(s, "_lte"), value.into())
        }
        s if s.ends_with("_in") => StoreFilter::In(filter_attr(s, "_in"), list_values(value)),
        s if s.ends_with("_not_in") => {
            StoreFilter::NotIn(filter_attr(s, "_not_in"), list_values(value))
        }
        s if s.ends_with("_contains") => {
            StoreFilter::Contains(filter_attr(s, "_contains"), value.into())
        }
        s if s.ends_with("_not_contains") => {
            StoreFilter::NotContains(filter_attr(s, "_not_contains"), value.into())
        }
        s if s.ends_with("_starts_with") => {
            StoreFilter::StartsWith(filter_attr(s, "_starts_with"), value.into())
        }
        s if s.ends_with("_ends_with") => {
            StoreFilter::EndsWith(filter_attr(s, "_ends_with"), value.into())
        }
        s if s.ends_with("_not_starts_with") => {
            StoreFilter::NotStartsWith(filter_attr(s, "_not_starts_with"), value.into())
        }
        s if s.ends_with("_not_ends_with") => {
            StoreFilter::NotEndsWith(filter_attr(s, "_not_ends_with"), value.into())
        }
        s => StoreFilter::Equal(s.to_owned(), value.into()),
    }
}

/// Parses GraphQL arguments into an attribute name to order by, if present.
fn build_order_by(arguments: &HashMap<&gqlq::Name, gqlq::Value>) -> Option<String> {
    arguments
        .get(&"orderBy".to_string())
        .and_then(|value| match value {
            gqlq::Value::Enum(name) => Some(name.to_owned()),
            _ => None,
        })
}

/// Parses GraphQL arguments into a StoreOrder, if present.
fn build_order_direction(arguments: &HashMap<&gqlq::Name, gqlq::Value>) -> Option<StoreOrder> {
    arguments
        .get(&"orderDirection".to_string())
        .and_then(|value| match value {
            gqlq::Value::Enum(name) if name == "asc" => Some(StoreOrder::Ascending),
            gqlq::Value::Enum(name) if name == "desc" => Some(StoreOrder::Descending),
            _ => None,
        })
}
