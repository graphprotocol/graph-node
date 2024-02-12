use std::sync::Arc;
use std::{collections::HashSet, convert::TryFrom};

use graph::anyhow::bail;
use graph::components::store::{BlockStore as _, ChainStore as _};
use graph::prelude::{anyhow, BlockNumber, BlockPtr, StoreEvent};
use graph_store_postgres::{
    command_support::catalog, connection_pool::ConnectionPool, BlockStore, NotificationSender,
};

use crate::manager::deployment::{Deployment, DeploymentSearch};

async fn block_ptr(
    store: Arc<BlockStore>,
    searches: &[DeploymentSearch],
    deployments: &[Deployment],
    hash: &str,
    number: BlockNumber,
    force: bool,
) -> Result<BlockPtr, anyhow::Error> {
    let block_ptr_to = BlockPtr::try_from((hash, number as i64))
        .map_err(|e| anyhow!("error converting to block pointer: {}", e))?;

    let chains = deployments.iter().map(|d| &d.chain).collect::<HashSet<_>>();
    if chains.len() > 1 {
        let names = searches
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        bail!("the deployments matching `{names}` are on different chains");
    }
    let chain = chains.iter().next().unwrap();
    let chain_store = match store.chain_store(chain) {
        None => bail!("can not find chain store for {}", chain),
        Some(store) => store,
    };
    if let Some((_, number, _)) = chain_store.block_number(&block_ptr_to.hash).await? {
        if number != block_ptr_to.number {
            bail!(
                "the given hash is for block number {} but the command specified block number {}",
                number,
                block_ptr_to.number
            );
        }
    } else if !force {
        bail!(
            "the chain {} does not have a block with hash {} \
               (run with --force to avoid this error)",
            chain,
            block_ptr_to.hash
        );
    }
    Ok(block_ptr_to)
}

pub async fn run(
    primary: ConnectionPool,
    sender: &NotificationSender,
    search: &DeploymentSearch,
    should_pause: bool,
) -> Result<(), anyhow::Error> {
    let locator = search.locate_unique(&primary)?;

    let conn = primary.get()?;
    let conn = catalog::Connection::new(conn);

    let site = conn
        .locate_site(locator.clone())?
        .ok_or_else(|| anyhow!("failed to locate site for {locator}"))?;

    let change = match conn.assignment_status(&site)? {
        Some((_, is_paused)) => {
            if should_pause {
                if is_paused {
                    println!("deployment {locator} is already paused");
                    return Ok(());
                }
                println!("pausing {locator}");
                conn.pause_subgraph(&site)?
            } else {
                println!("resuming {locator}");
                conn.resume_subgraph(&site)?
            }
        }
        None => {
            println!("deployment {locator} not found");
            return Ok(());
        }
    };
    println!("Operation completed");
    conn.send_store_event(sender, &StoreEvent::new(change))?;

    Ok(())
}
