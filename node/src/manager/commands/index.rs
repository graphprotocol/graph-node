use crate::manager::deployment::DeploymentSearch;
use graph::{
    components::store::DeploymentLocator,
    itertools::Itertools,
    prelude::{anyhow, StoreError},
};
use graph_store_postgres::{
    command_support::index::CreateIndex, connection_pool::ConnectionPool, SubgraphStore,
};
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
    search: DeploymentSearch,
    entity_name: &str,
    field_names: Vec<String>,
    index_method: String,
) -> Result<(), anyhow::Error> {
    validate_fields(&field_names)?;
    let deployment_locator = search.locate_unique(&pool)?;
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
    search: DeploymentSearch,
    entity_name: &str,
) -> Result<(), anyhow::Error> {
    fn header(indexes: &[CreateIndex], loc: &DeploymentLocator, entity: &str) {
        use CreateIndex::*;

        let index = indexes.iter().find(|index| matches!(index, Parsed { .. }));
        match index {
            Some(Parsed { nsp, table, .. }) => {
                println!("{:^76}", format!("Indexes for {nsp}.{table}"))
            }
            _ => println!("{:^76}", format!("Indexes for sgd{}.{entity}", loc.id)),
        }
        println!("{: ^12} IPFS hash: {}", "", loc.hash);
        println!("{:-^76}", "");
    }

    fn footer() {
        println!("  (a): account-like flag set");
    }

    fn print_index(index: &CreateIndex) {
        use CreateIndex::*;

        match index {
            Unknown { defn } => {
                println!("*unknown*");
                println!("  {defn}");
            }
            Parsed {
                unique,
                name,
                nsp: _,
                table: _,
                method,
                columns,
                cond,
                with,
            } => {
                let unique = if *unique { " unique" } else { "" };
                let start = format!("{name}{unique} using {method}");
                let columns = columns.into_iter().map(|c| c.to_string()).join(", ");
                if start.len() + columns.len() <= 76 {
                    println!("{start}({columns})");
                } else {
                    println!("{start}");
                    println!("  on ({})", columns);
                }
                if let Some(cond) = cond {
                    println!("  where {cond}");
                }
                if let Some(with) = with {
                    println!("  with {with}");
                }
            }
        }
    }

    let deployment_locator = search.locate_unique(&pool)?;
    let indexes: Vec<_> = store
        .indexes_for_entity(&deployment_locator, entity_name)
        .await?;

    let mut first = true;
    header(&indexes, &deployment_locator, entity_name);
    for index in &indexes {
        if !first {
            println!("{:-^76}", "");
            first = false;
        }
        print_index(index);
    }
    Ok(())
}

pub async fn drop(
    store: Arc<SubgraphStore>,
    pool: ConnectionPool,
    search: DeploymentSearch,
    index_name: &str,
) -> Result<(), anyhow::Error> {
    let deployment_locator = search.locate_unique(&pool)?;
    store
        .drop_index_for_deployment(&deployment_locator, &index_name)
        .await?;
    println!("Dropped index {index_name}");
    Ok(())
}
