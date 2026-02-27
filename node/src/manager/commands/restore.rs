use std::sync::Arc;

use graph::{bail, prelude::anyhow::Result};

use graph::prelude::SubgraphName;
use graph_store_postgres::{RestoreMode, Shard, SubgraphStore, PRIMARY_SHARD};

pub async fn run(
    subgraph_store: Arc<SubgraphStore>,
    directory: String,
    shard: Option<String>,
    name: Option<String>,
    replace: bool,
    add: bool,
    force: bool,
) -> Result<()> {
    if add && shard.is_none() {
        bail!("--add requires --shard");
    }

    let directory = std::path::Path::new(&directory).canonicalize()?;
    let stat = std::fs::metadata(&directory)?;

    if !stat.is_dir() {
        bail!(
            "The path `{}` is not a directory",
            directory.to_string_lossy()
        );
    }

    let shard = match shard {
        Some(s) => Shard::new(s)?,
        None => PRIMARY_SHARD.clone(),
    };

    let name = name
        .map(|n| SubgraphName::new(n.clone()).map_err(|_| anyhow::anyhow!("invalid name `{n}`")))
        .transpose()?;

    let mode = if replace {
        RestoreMode::Replace
    } else if add {
        RestoreMode::Add
    } else if force {
        RestoreMode::Force
    } else {
        RestoreMode::Default
    };

    subgraph_store
        .restore(&directory, shard, name, mode)
        .await?;
    Ok(())
}
