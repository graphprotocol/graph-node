use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use graph::{
    block_on,
    components::store::SubgraphFork as SubgraphForkTrait,
    prelude::{
        info,
        r::Value as RValue,
        reqwest,
        s::{Definition, Field, ObjectType, TypeDefinition},
        serde_json, Attribute, DeploymentHash, Entity, Logger, Schema, Serialize, StoreError,
        Value,
    },
    url::Url,
};

#[derive(Serialize, Debug, PartialEq)]
struct Query {
    query: String,
    variables: Variables,
}

#[derive(Serialize, Debug, PartialEq)]
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
pub(crate) struct SubgraphFork {
    client: reqwest::Client,
    endpoint: Url,
    schema: Arc<Schema>,
    fetched_ids: Mutex<HashSet<String>>,
    logger: Logger,
}

impl SubgraphForkTrait for SubgraphFork {
    fn fetch(&self, entity_type: String, id: String) -> Result<Option<Entity>, StoreError> {
        let mut fids = self.fetched_ids.lock().map_err(|e| {
            StoreError::ForkFailure(format!(
                "attempt to acquire lock on `fetched_ids` failed with {}",
                e,
            ))
        })?;
        if fids.contains(&id) {
            info!(self.logger, "Already fetched entity! Abort!"; "entity_type" => entity_type, "id" => id);
            return Ok(None);
        }
        fids.insert(id.clone());

        info!(self.logger, "Fetching entity from {}", &self.endpoint; "entity_type" => &entity_type, "id" => &id);

        // NOTE: Subgraph fork compatability checking (similar to the grafting compatability checks)
        // will be added in the future (in a separate PR).
        // Currently, forking incompatible subgraphs is allowed, but, for example, storing the
        // incompatible fetched entities in the local store results in an error.

        let (query, fields) = self.infer_query(&entity_type, id)?;
        let raw_json = block_on(self.send(&query))?;
        if !raw_json.contains("data") {
            return Err(StoreError::ForkFailure(format!(
                "the GraphQL query \"{:?}\" to `{}` failed with \"{}\"",
                query, self.endpoint, raw_json,
            )));
        }
        let entity = SubgraphFork::extract_entity(&raw_json, &entity_type, fields)?;
        return Ok(Some(entity));
    }
}

impl SubgraphFork {
    pub(crate) fn new(
        base: Url,
        id: DeploymentHash,
        schema: Arc<Schema>,
        logger: Logger,
    ) -> Result<Self, StoreError> {
        Ok(Self {
            client: reqwest::Client::new(),
            endpoint: base
                .join(id.as_str())
                .map_err(|e| StoreError::ForkFailure(format!("failed to join fork base: {}", e)))?,
            schema,
            fetched_ids: Mutex::new(HashSet::new()),
            logger,
        })
    }

    async fn send(&self, query: &Query) -> Result<String, StoreError> {
        let res = self
            .client
            .post(self.endpoint.clone())
            .json(query)
            .send()
            .await
            .map_err(|e| {
                StoreError::ForkFailure(format!(
                    "sending a GraphQL query to `{}` failed with: \"{}\"",
                    self.endpoint, e,
                ))
            })?
            .text()
            .await
            .map_err(|e| {
                StoreError::ForkFailure(format!(
                    "receiving a response from `{}` failed with: \"{}\"",
                    self.endpoint, e,
                ))
            })?;
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
            return Err(StoreError::ForkFailure(format!(
                "Unexpected error during query inference! No object type definition with entity type `{}` found in the GraphQL schema supplied by the user.",
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
        {}
    }}
}}",
            entity_type,
            fields.join("\n        ").trim(),
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
                let value = RValue::from(value);
                let value = Value::from_query_value(&value, &f.field_type).map_err(|e| {
                    StoreError::ForkFailure(format!(
                        "Unexpected error during entity extraction! Failed to convert JSON value `{}` to type `{}`: {}",
                        value,
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

#[cfg(test)]
mod tests {
    use std::{iter::FromIterator, str::FromStr};

    use super::*;

    use graph::{
        data::store::scalar,
        prelude::{s::Type, DeploymentHash},
        slog::{self, o},
    };
    use graphql_parser::parse_schema;

    fn test_base() -> Url {
        Url::parse("https://api.thegraph.com/subgraph/id/").unwrap()
    }

    fn test_id() -> DeploymentHash {
        DeploymentHash::new("test").unwrap()
    }

    fn test_schema() -> Arc<Schema> {
        let schema = Schema::new(
            DeploymentHash::new("test").unwrap(),
            parse_schema::<String>(
                r#"type Gravatar @entity {
  id: ID!
  owner: Bytes!
  displayName: String!
  imageUrl: String!
}"#,
            )
            .unwrap(),
        );
        Arc::new(schema)
    }

    fn test_logger() -> Logger {
        Logger::root(slog::Discard, o!())
    }

    fn test_fields() -> Vec<Field> {
        vec![
            Field {
                position: graphql_parser::Pos { line: 2, column: 3 },
                description: None,
                name: "id".to_string(),
                arguments: vec![],
                field_type: Type::NonNullType(Box::new(Type::NamedType("ID".to_string()))),
                directives: vec![],
            },
            Field {
                position: graphql_parser::Pos { line: 3, column: 3 },
                description: None,
                name: "owner".to_string(),
                arguments: vec![],
                field_type: Type::NonNullType(Box::new(Type::NamedType("Bytes".to_string()))),
                directives: vec![],
            },
            Field {
                position: graphql_parser::Pos { line: 4, column: 3 },
                description: None,
                name: "displayName".to_string(),
                arguments: vec![],
                field_type: Type::NonNullType(Box::new(Type::NamedType("String".to_string()))),
                directives: vec![],
            },
            Field {
                position: graphql_parser::Pos { line: 5, column: 3 },
                description: None,
                name: "imageUrl".to_string(),
                arguments: vec![],
                field_type: Type::NonNullType(Box::new(Type::NamedType("String".to_string()))),
                directives: vec![],
            },
        ]
    }

    #[test]
    fn test_infer_query() {
        let base = test_base();
        let id = test_id();
        let schema = test_schema();
        let logger = test_logger();

        let fork = SubgraphFork::new(base, id, schema, logger).unwrap();

        let (query, fields) = fork.infer_query("Gravatar", "0x00".to_string()).unwrap();
        assert_eq!(
            query,
            Query {
                query: r#"query Query ($id: String) {
    gravatar(id: $id, subgraphError: allow) {
        id
        owner
        displayName
        imageUrl
    }
}"#
                .to_string(),
                variables: Variables {
                    id: "0x00".to_string()
                },
            }
        );

        assert_eq!(fields, &test_fields());
    }

    #[test]
    fn test_extract_entity() {
        let entity = SubgraphFork::extract_entity(
            r#"{
    "data": {
        "gravatar": {
            "id": "0x00",
            "owner": "0x01",
            "displayName": "test",
            "imageUrl": "http://example.com/image.png"
        }
    }
}"#,
            "Gravatar",
            &test_fields(),
        )
        .unwrap();

        assert_eq!(
            entity,
            Entity::from(HashMap::from_iter(
                vec![
                    ("id".to_string(), Value::String("0x00".to_string())),
                    (
                        "owner".to_string(),
                        Value::Bytes(scalar::Bytes::from_str("0x01").unwrap())
                    ),
                    ("displayName".to_string(), Value::String("test".to_string())),
                    (
                        "imageUrl".to_string(),
                        Value::String("http://example.com/image.png".to_string())
                    ),
                ]
                .into_iter()
            ))
        );
    }
}
