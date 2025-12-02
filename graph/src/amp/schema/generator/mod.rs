mod entity;

use anyhow::{Context, Result};
use itertools::Itertools;

use self::entity::SchemaEntity;
use crate::{data::subgraph::DeploymentHash, schema::InputSchema};

/// Generates a subgraph schema from a list of Arrow schemas.
///
/// # Limitations
///
/// The generated subgraph entities are immutable and do not contain any relationships to other entities within the schema.
///
/// # Errors
///
/// Returns an error if any of the Arrow schemas cannot be represented as valid subgraph entities.
///
/// The returned error is deterministic.
pub fn generate_subgraph_schema(
    deployment_hash: &DeploymentHash,
    named_schemas: impl IntoIterator<Item = (String, arrow::datatypes::Schema)>,
) -> Result<InputSchema> {
    let mut named_schemas = merge_related_schemas(named_schemas)?;
    named_schemas.sort_unstable_by_key(|(name, _)| name.clone());

    let entities = create_entities(named_schemas)?;
    let mut subgraph_schema = String::new();

    for entity in entities {
        subgraph_schema.extend(std::iter::once(entity.to_string()));
        subgraph_schema.push_str("\n\n");
    }

    let input_schema = InputSchema::parse_latest(&subgraph_schema, deployment_hash.to_owned())
        .context("failed to parse subgraph schema")?;

    Ok(input_schema)
}

fn merge_related_schemas(
    named_schemas: impl IntoIterator<Item = (String, arrow::datatypes::Schema)>,
) -> Result<Vec<(String, arrow::datatypes::Schema)>> {
    named_schemas
        .into_iter()
        .into_group_map_by(|(name, _)| name.clone())
        .into_iter()
        .map(|(name, related_schemas)| {
            let related_schemas = related_schemas.into_iter().map(|(_, schema)| schema);

            arrow::datatypes::Schema::try_merge(related_schemas).map(|schema| (name, schema))
        })
        .collect::<Result<Vec<_>, _>>()
        .context("failed to merge schemas of related SQL queries")
}

fn create_entities(queries: Vec<(String, arrow::datatypes::Schema)>) -> Result<Vec<SchemaEntity>> {
    queries
        .into_iter()
        .map(|(name, schema)| {
            SchemaEntity::new(name.clone(), schema)
                .with_context(|| format!("failed to create entity '{}'", name))
        })
        .collect::<Result<Vec<_>, _>>()
}
