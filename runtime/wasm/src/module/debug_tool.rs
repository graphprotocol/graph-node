use std::{collections::HashMap, sync::Arc};

use graph::prelude::{reqwest, Attribute, BigDecimal, BigInt, Entity, Schema, Value};
use graph_graphql::graphql_parser::schema;

use serde::Serialize;

/// Just a wrapper around the GraphQL query string.
#[derive(Serialize)]
pub struct Query {
    query: String,
    variables: Variables,
}

#[derive(Serialize)]
pub struct Variables {
    id: String,
}

fn from_template(entity_type: &str, fields: &Vec<String>) -> String {
    format!(
        "\
query Query ($id: String) {{
    {}(id: $id, subgraphError: allow) {{
        id
        {}
    }}
}}",
        entity_type,
        format!("\n{}\n", fields.join("\n")),
    )
}

pub fn infer_query(schema: Arc<Schema>, entity_type: &str, id: &str) -> (Query, Vec<String>) {
    let fields: Vec<String> = schema
        .document
        .definitions
        .iter()
        .find_map(|def| {
            if let schema::Definition::TypeDefinition(schema::TypeDefinition::Object(o)) = def {
                if o.name == entity_type {
                    Some(o)
                } else {
                    None
                }
            } else {
                None
            }
        })
        .unwrap()
        .fields
        .iter()
        .map(|field| field.name.to_string())
        .collect();

    let query = Query {
        query: from_template(&entity_type.to_lowercase(), &fields),
        variables: Variables { id: id.to_string() },
    };
    return (query, fields);
}

pub fn perform_query(query: Query, endpoint: &str) -> Result<String, anyhow::Error> {
    let client = reqwest::blocking::Client::new();
    let res = client.post(endpoint).json(&query).send()?.text()?;
    Ok(res)
}

pub fn extract_entity(
    raw_json: String,
    entity_type: String,
    fields: Vec<String>,
) -> Result<Entity, anyhow::Error> {
    let json: serde_json::Value = serde_json::from_str(&raw_json).unwrap();
    let entity = &json["data"][&entity_type.to_lowercase()];
    let map: HashMap<Attribute, Value> = {
        let mut map = HashMap::new();
        for f in fields {
            let value = entity.get(&f).unwrap();
            let value = match value {
                serde_json::Value::String(s) => Value::String(s.clone()),
                serde_json::Value::Number(n) => {
                    if n.is_f64() {
                        Value::BigDecimal(BigDecimal::from(n.as_f64().unwrap()))
                    } else if n.is_i64() {
                        Value::BigInt(BigInt::from(n.as_i64().unwrap()))
                    } else {
                        Value::BigInt(BigInt::from(n.as_u64().unwrap()))
                    }
                }
                serde_json::Value::Bool(b) => Value::Bool(*b),
                serde_json::Value::Null => Value::Null,
                _ => return Err(anyhow::anyhow!("store_get: Unsupported value type.")),
            };
            map.insert(f, value);
        }
        map
    };

    let entity = Entity::from(map);
    return Ok(entity);
}
