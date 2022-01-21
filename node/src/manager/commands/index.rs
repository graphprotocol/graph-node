use graph::{
    components::store::EntityType,
    prelude::{anyhow, DeploymentHash, StoreError},
};
use graph_store_postgres::SubgraphStore;
use std::{collections::HashSet, sync::Arc};

fn validate_fields<T: AsRef<str>>(fields: &[T]) -> Result<(), anyhow::Error> {
    // Must be non-empty. Double checking, since [`StructOpt`] already checks this.
    if fields.is_empty() {
        anyhow::bail!("at least one field must be informed")
    }
    // All values must be unique
    let unique: HashSet<_> = fields.iter().map(AsRef::as_ref).collect();
    if fields.len() != unique.len() {
        anyhow::bail!("entity fields must be unique")
    }
    Ok(())
}
pub async fn create(
    store: Arc<SubgraphStore>,
    id: String,
    entity_name: String,
    field_names: Vec<String>,
    index_method: String,
) -> Result<(), anyhow::Error> {
    validate_fields(&field_names)?;
    let deployment_hash = DeploymentHash::new(id)
        .map_err(|e| anyhow::anyhow!("Subgraph hash must be a valid IPFS hash: {}", e))?;
    let entity_type = EntityType::new(entity_name);
    println!("Index creation started. Please wait.");
    match store
        .create_manual_index(&deployment_hash, entity_type, field_names, index_method)
        .await
    {
        Ok(()) => Ok(()),
        Err(StoreError::Canceled) => {
            eprintln!("Index creation attempt faield. Please retry.");
            ::std::process::exit(1);
        }
        Err(other) => Err(anyhow::anyhow!(other)),
    }
}
