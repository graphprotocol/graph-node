use std::sync::Arc;

use graph::prelude::{anyhow, Error, SubgraphName, SubgraphStore as _};
use graph_store_postgres::SubgraphStore;

use crate::manager::deployment::DeploymentSearch;

pub async fn run(store: Arc<SubgraphStore>, name: &DeploymentSearch) -> Result<(), Error> {
    match name {
        DeploymentSearch::Name { name } => {
            let subgraph_name = SubgraphName::new(name)
                .map_err(|name| anyhow!("illegal subgraph name `{name}`"))?;
            println!("Removing subgraph {}", name);
            store.remove_subgraph(subgraph_name).await?;
            println!("Subgraph {} removed", name);
        }
        _ => {
            return Err(anyhow!(
                "Remove command expects a subgraph name, but got either hash or namespace: {}",
                name
            ))
        }
    }

    Ok(())
}
