use std::sync::Arc;

use graph::prelude::{anyhow, Error, SubgraphName, SubgraphStore as _};
use graph_store_postgres::SubgraphStore;

pub fn run(
    store: Arc<SubgraphStore>,
    name: String,
    out: &mut dyn std::io::Write,
) -> Result<SubgraphName, Error> {
    let name = SubgraphName::new(name.clone())
        .map_err(|()| anyhow!("illegal subgraph name `{}`", name))?;

    writeln!("creating subgraph {}", name);

    store.create_subgraph(name.clone())?;

    Ok(name)
}
