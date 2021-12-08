use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use graph::{
    block_on,
    components::store::SubgraphFork as SubgraphForkTrait,
    prelude::{
        anyhow, info,
        r::Value as RValue,
        reqwest,
        s::{Definition, Field, ObjectType, TypeDefinition},
        serde_json, Attribute, Entity, Logger, Schema, Serialize, StoreError, Value,
    },
    url::Url,
};

// TODO: Define forking errors.

#[derive(Serialize, Debug)]
struct Query {
    query: String,
    variables: Variables,
}

#[derive(Serialize, Debug)]
struct Variables {
    id: String,
}

/// SubgraphFork represents a simple subgraph forking mechanism
/// which lazily fetches entities from a remote subgraph's store
/// associated with a GraphQL endpoint at `fork_url`.
///
/// Since this mechanism is used for debug forks, entities are
/// fetched only once per id in order to avoid fetching an entity
/// that was deleted from the local store and thus causing inconsistencies.
pub struct SubgraphFork {
    client: reqwest::Client,
    fork_url: Url,
    schema: Arc<Schema>,
    fetched_ids: HashSet<String>,
    logger: Logger,
}

impl SubgraphForkTrait for SubgraphFork {
    fn fetch(&self, entity_type: String, id: String) -> Result<Option<Entity>, StoreError> {
        if self.fetched_ids.contains(&id) {
            info!(self.logger, "Already fetched entity! Abort!"; "entity_type" => entity_type, "id" => id);
            return Ok(None);
        }

        info!(self.logger, "Fetching entity from {}", &self.fork_url; "entity_type" => &entity_type, "id" => &id);

        // NOTE: Should compatability check be added?
        // The local entities may not be compatible with the remote ones, resulting in an error.

        let (query, fields) = self.infer_query(&entity_type, id)?;
        let raw_json = block_on(self.send(&query))?;
        if !raw_json.contains("data") {
            return Err(StoreError::Unknown(anyhow!(
                "the GraphQL query \"{:?}\" failed with {}.",
                query,
                raw_json,
            )));
        }
        let entity = SubgraphFork::extract_entity(&raw_json, &entity_type, fields)?;
        return Ok(Some(entity));
    }
}

impl SubgraphFork {
    pub fn new(fork_url: Url, schema: Arc<Schema>, logger: Logger) -> Self {
        Self {
            client: reqwest::Client::new(),
            fork_url,
            schema,
            fetched_ids: HashSet::new(),
            logger,
        }
    }

    async fn send(&self, query: &Query) -> Result<String, StoreError> {
        let res = self
            .client
            .post(self.fork_url.clone())
            .json(query)
            .send()
            .await
            .map_err(|e| StoreError::Unknown(anyhow!(e)))?
            .text()
            .await
            .map_err(|e| StoreError::Unknown(anyhow!(e)))?;
        Ok(res)
    }

    fn infer_query(
        &self,
        entity_type: &str,
        id: String,
    ) -> Result<(Query, &Vec<Field>), StoreError> {
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
            return Err(StoreError::Unknown(anyhow!(
                "Fork::infer_query: Unexpected! No object type definition with entity type `{}` found.",
                entity_type
            )));
        }

        let fields = &entity.unwrap().fields;
        let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();

        let query = Query {
            query: SubgraphFork::query_string(&entity_type.to_lowercase(), &names),
            variables: Variables { id },
        };
        return Ok((query, fields));
    }

    fn query_string(entity_type: &str, fields: &[&str]) -> String {
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

    fn extract_entity(
        raw_json: &str,
        entity_type: &str,
        fields: &Vec<Field>,
    ) -> Result<Entity, StoreError> {
        let json: serde_json::Value = serde_json::from_str(raw_json).unwrap();
        let json = &json["data"][entity_type.to_lowercase()];

        let map: HashMap<Attribute, Value> = {
            let mut map = HashMap::new();
            for f in fields {
                let value = json.get(&f.name).unwrap().clone();
                let value = Value::from_query_value(&RValue::from(value), &f.field_type).map_err(|e| {
                    StoreError::Unknown(anyhow!(
                        "Fork::extract_entity: Unexpected! Failed to convert value to type `{}`: {}",
                        f.field_type,
                        e
                    ))
                })?;
                map.insert(f.name.clone(), value);
            }
            map
        };

        return Ok(Entity::from(map));
    }
}
