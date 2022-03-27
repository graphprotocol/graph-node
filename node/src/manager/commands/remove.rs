use std::sync::Arc;

use graph::prelude::{anyhow, Error, SubgraphName, SubgraphStore as _};
use graph_store_postgres::SubgraphStore;

pub fn run(store: Arc<SubgraphStore>, name: String) -> Result<(), Error> {
    let name = SubgraphName::new(name.clone())
        .map_err(|err| anyhow!("illegal subgraph name `{}` with error `{}`", name, err))?;

    println!("Removing subgraph {}", name);
    store.remove_subgraph(name)?;

    Ok(())
}
