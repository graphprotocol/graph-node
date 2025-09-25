mod entity;

use anyhow::{Context, Result};
use arrow::datatypes::Schema;
use itertools::Itertools;

use self::entity::Entity;
use crate::{
    cheap_clone::CheapClone, data::subgraph::DeploymentHash, nozzle::common::Ident,
    schema::InputSchema,
};

/// Generates a Subgraph schema from a list of Arrow schemas.
///
/// # Limitations
///
/// The generated Subgraph entities are immutable and do not contain any relationships to other entities within the schema.
///
/// # Errors
///
/// Returns an error if any of the Arrow schemas cannot be represented as valid Subgraph entities.
///
/// The returned error is deterministic.
pub fn generate_subgraph_schema(
    deployment_hash: &DeploymentHash,
    queries: impl IntoIterator<Item = (Ident, Schema)>,
) -> Result<InputSchema> {
    let mut queries = merge_related_queries(queries)?;
    queries.sort_unstable_by_key(|(name, _)| name.cheap_clone());

    let entities = create_entities(queries)?;
    let mut subgraph_schema = String::new();

    for entity in entities {
        subgraph_schema.extend(std::iter::once(entity.to_string()));
        subgraph_schema.push_str("\n\n");
    }

    let input_schema = InputSchema::parse_latest(&subgraph_schema, deployment_hash.to_owned())
        .context("failed to parse subgraph schema")?;

    Ok(input_schema)
}

fn merge_related_queries(
    queries: impl IntoIterator<Item = (Ident, Schema)>,
) -> Result<Vec<(Ident, Schema)>> {
    queries
        .into_iter()
        .into_group_map_by(|(name, _)| name.cheap_clone())
        .into_iter()
        .map(|(name, related_queries)| {
            let related_schemas = related_queries.into_iter().map(|(_, schema)| schema);

            Schema::try_merge(related_schemas).map(|schema| (name, schema))
        })
        .collect::<Result<Vec<_>, _>>()
        .context("failed to merge schemas of related SQL queries")
}

fn create_entities(queries: Vec<(Ident, Schema)>) -> Result<Vec<Entity>> {
    queries
        .into_iter()
        .map(|(name, schema)| {
            Entity::new(name.cheap_clone(), schema)
                .with_context(|| format!("failed to create entity '{}'", name))
        })
        .collect::<Result<Vec<_>, _>>()
}
