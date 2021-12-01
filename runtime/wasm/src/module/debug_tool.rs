use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};

use graph::{
    block_on,
    prelude::{
        reqwest,
        s::{Definition, ObjectType, TypeDefinition},
        serde_json, Attribute, BigDecimal, BigInt, Entity, Schema, Serialize, Value,
    },
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

fn get_query_string(entity_type: &str, fields: &[String]) -> String {
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
    let entity: Option<&ObjectType> =
        schema
            .document
            .definitions
            .iter()
            .find_map(|def| match def {
                Definition::TypeDefinition(TypeDefinition::Object(o)) if o.name == entity_type => {
                    Some(o)
                }
                _ => None,
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

async fn send(query: &Query, endpoint: &str) -> Result<String> {
    let client = reqwest::Client::new();
    let res = client
        .post(endpoint)
        .json(query)
        .send()
        .await?
        .text()
        .await?;
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
                _ => return Err(anyhow!("extract_entity: Unsupported value type.")),
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
) -> Result<Entity> {
    let (query, fields) = infer_query(schema, entity_type, id)?;
    let raw_json = block_on(send(&query, endpoint))?;
    if !raw_json.contains("data") {
        return Err(anyhow!("fetch_entity: GraphQL query failed: {}", raw_json));
    }
    let entity = extract_entity(&raw_json, entity_type, fields)?;
    return Ok(entity);
}
