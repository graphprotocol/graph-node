use crate::manager::deployment::find_single_deployment_locator;
use graph::prelude::{anyhow, StoreError};
use graph_store_postgres::{connection_pool::ConnectionPool, SubgraphStore};
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
    pool: ConnectionPool,
    id: &str,
    entity_name: &str,
    field_names: Vec<String>,
    index_method: String,
) -> Result<(), anyhow::Error> {
    validate_fields(&field_names)?;
    let deployment_locator = find_single_deployment_locator(&pool, &id)?;
    println!("Index creation started. Please wait.");
    match store
        .create_manual_index(&deployment_locator, entity_name, field_names, index_method)
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

pub async fn list(
    store: Arc<SubgraphStore>,
    pool: ConnectionPool,
    id: String,
    entity_name: &str,
) -> Result<(), anyhow::Error> {
    let deployment_locator = find_single_deployment_locator(&pool, &id)?;
    let indexes: Vec<String> = store
        .indexes_for_entity(&deployment_locator, entity_name)
        .await?;
    for index in &indexes {
        println!("{index}")
    }
    Ok(())
}

pub async fn drop(
    store: Arc<SubgraphStore>,
    pool: ConnectionPool,
    id: &str,
    index_name: &str,
) -> Result<(), anyhow::Error> {
    let deployment_locator = find_single_deployment_locator(&pool, &id)?;
    store
        .drop_index_for_deployment(&deployment_locator, &index_name)
        .await?;
    println!("Dropped index {index_name}");
    Ok(())
}
