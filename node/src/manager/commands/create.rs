use std::sync::Arc;

use graph::prelude::{anyhow::Context, Error, SubgraphName, SubgraphStore as _};
use graph_store_postgres::SubgraphStore;

pub fn run(store: Arc<SubgraphStore>, name: String) -> Result<(), Error> {
    let name = SubgraphName::new(name.clone())
        .with_context(|| format!("illegal subgraph name `{:#}`", name))?;

    println!("creating subgraph {}", name);
    store.create_subgraph(name)?;

    Ok(())
}
