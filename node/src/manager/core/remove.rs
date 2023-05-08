use std::sync::Arc;

use graph::prelude::{anyhow, Error, SubgraphName, SubgraphStore as _};
use graph_store_postgres::SubgraphStore;

pub fn run(store: Arc<SubgraphStore>, name: &str, enable_logging: bool) -> Result<(), Error> {
    let name = SubgraphName::new(name).map_err(|()| anyhow!("illegal subgraph name `{}`", name))?;

    if enable_logging {
        println!("Removing subgraph {}", name);
    }

    store.remove_subgraph(name)?;

    Ok(())
}
