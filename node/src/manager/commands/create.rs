use std::sync::Arc;

use graph::prelude::{Error, SubgraphName, SubgraphStore as _, anyhow};
use graph_store_postgres::SubgraphStore;

pub async fn run(store: Arc<SubgraphStore>, name: String) -> Result<(), Error> {
    let name = SubgraphName::new(name.clone())
        .map_err(|name| anyhow!("illegal subgraph name `{name}`"))?;

    println!("creating subgraph {}", name);
    store.create_subgraph(name).await?;

    Ok(())
}
