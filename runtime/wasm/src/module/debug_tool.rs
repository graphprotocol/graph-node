use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};
use serde::Serialize;

use graph::prelude::{
    reqwest,
    s::{Definition, ObjectType, TypeDefinition},
    Attribute, BigDecimal, BigInt, Entity, Schema, Value,
};

#[derive(Serialize)]
struct Query {
    query: String,
    variables: Variables,
}

#[derive(Serialize)]
struct Variables {
    id: String,
}

fn get_query_string(entity_type: &str, fields: &Vec<String>) -> String {
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

fn infer_query(schema: &Arc<Schema>, entity_type: &str, id: &str) -> Result<(Query, Vec<String>)> {
    let entity: Option<&ObjectType> = schema.document.definitions.iter().find_map(|def| {
        if let Definition::TypeDefinition(TypeDefinition::Object(o)) = def {
            if o.name == entity_type {
                Some(o)
            } else {
                None
            }
        } else {
            None
        }
    });

    if let None = entity {
        return Err(anyhow!(
            "infer_query: Unexpected! No object type definition with entity type `{}` found.",
            entity_type
        ));
    }

    let fields: Vec<String> = entity
        .unwrap()
        .fields
        .iter()
        .map(|field| field.name.to_string())
        .collect();

    let query = Query {
        query: get_query_string(&entity_type.to_lowercase(), &fields),
        variables: Variables { id: id.to_string() },
    };
    return Ok((query, fields));
}

fn send(query: &Query, endpoint: &str) -> Result<String> {
    let client = reqwest::blocking::Client::new();
    let res = client.post(endpoint).json(query).send()?.text()?;
    Ok(res)
}

fn extract_entity(raw_json: &str, entity_type: &str, fields: Vec<String>) -> Result<Entity> {
    let json: serde_json::Value = serde_json::from_str(raw_json).unwrap();
    let entity = &json["data"][entity_type.to_lowercase()];
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
                _ => return Err(anyhow!("store_get: Unsupported value type.")),
            };
            map.insert(f, value);
        }
        map
    };

    let entity = Entity::from(map);
    return Ok(entity);
}

pub fn fetch_entity(
    schema: &Arc<Schema>,
    endpoint: &str,
    entity_type: &str,
    id: &str,
) -> Result<Option<Entity>> {
    let (query, fields) = infer_query(schema, entity_type, id)?;
    let raw_json = send(&query, endpoint)?;
    if !raw_json.contains("data") {
        return Ok(None);
    }
    let entity = extract_entity(&raw_json, entity_type, fields)?;
    return Ok(Some(entity));
}
