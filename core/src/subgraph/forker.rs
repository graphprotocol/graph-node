use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use graph::{
    block_on,
    components::subgraph::SubgraphFork as SubgraphForkTrait,
    components::subgraph::SubgraphForker as SubgraphForkerTrait,
    prelude::{
        anyhow::{anyhow, Result},
        reqwest,
        s::{Definition, ObjectType, TypeDefinition},
        serde_json, Attribute, BigDecimal, BigInt, DeploymentHash, Entity, Schema, Serialize,
        SubgraphStore, Value,
    },
};

// TODO: Define forking errors.

pub struct SubgraphForker<S> {
    fork_base: String,
    subgraph_store: Arc<S>,
}

impl<S> SubgraphForkerTrait for SubgraphForker<S>
where
    S: SubgraphStore,
{
    type Fork = SubgraphFork;
    fn fork(&self, subgraph_id: &DeploymentHash) -> Result<Self::Fork> {
        let schema = self.
            subgraph_store
            .input_schema(subgraph_id)
            .map_err(|e| {
                anyhow!(
                    "failed to fetch the subgraph input schema for subgraph with id `{}` from the store: {:?}",
                    subgraph_id,
                    e,
                )
            })?;

        Ok(SubgraphFork::new(
            format!("{}/id/{}", self.fork_base, subgraph_id),
            schema,
        ))
    }
}

impl<S> SubgraphForker<S>
where
    S: SubgraphStore,
{
    pub fn new(fork_base: String, subgraph_store: Arc<S>) -> Self {
        Self {
            fork_base,
            subgraph_store,
        }
    }
}

#[derive(Serialize, Debug)]
struct Query {
    query: String,
    variables: Variables,
}

#[derive(Serialize, Debug)]
struct Variables {
    id: String,
}

/// Fork represents a simple subgraph "forking" mechanism which lazily fetches
/// the store associated with the GraphQL endpoint at <base_url>/id/<subgraph_id>.
pub struct SubgraphFork {
    client: reqwest::Client,
    fork_url: String,
    schema: Arc<Schema>,
    fetched_ids: HashSet<String>,
}

impl SubgraphForkTrait for SubgraphFork {
    fn fetch(self, entity_type: String, id: String) -> Result<Option<Entity>> {
        if self.fetched_ids.contains(&id) {
            return Ok(None);
        }

        let (query, fields) = self.infer_query(&entity_type, id)?;
        let raw_json = block_on(self.send(&query))?;
        if !raw_json.contains("data") {
            return Err(anyhow!(
                "the GraphQL query \"{:?}\" failed with {}.",
                query,
                raw_json,
            ));
        }
        let entity = SubgraphFork::extract_entity(&raw_json, &entity_type, fields)?;
        return Ok(Some(entity));
    }
}

impl SubgraphFork {
    fn new(fork_url: String, schema: Arc<Schema>) -> Self {
        Self {
            client: reqwest::Client::new(),
            fork_url,
            schema,
            fetched_ids: HashSet::new(),
        }
    }

    async fn send(&self, query: &Query) -> Result<String> {
        let res = self
            .client
            .post(&self.fork_url)
            .json(query)
            .send()
            .await?
            .text()
            .await?;
        Ok(res)
    }

    fn infer_query(&self, entity_type: &str, id: String) -> Result<(Query, Vec<String>)> {
        let entity: Option<&ObjectType> =
            self.schema
                .document
                .definitions
                .iter()
                .find_map(|def| match def {
                    Definition::TypeDefinition(TypeDefinition::Object(o))
                        if o.name == entity_type =>
                    {
                        Some(o)
                    }
                    _ => None,
                });

        if let None = entity {
            return Err(anyhow!(
                "Fork::infer_query: Unexpected! No object type definition with entity type `{}` found.",
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
            query: SubgraphFork::get_query_string(&entity_type.to_lowercase(), &fields),
            variables: Variables { id },
        };
        return Ok((query, fields));
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
}
