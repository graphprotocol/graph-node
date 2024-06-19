use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::{collections::HashSet, convert::TryFrom};

use crate::manager::commands::assign::pause_or_resume;
use crate::manager::deployment::DeploymentSearch;
use graph::anyhow::bail;
use graph::components::store::{BlockStore as _, ChainStore as _, DeploymentLocator};
use graph::env::ENV_VARS;
use graph::prelude::{anyhow, BlockNumber, BlockPtr};
use graph_store_postgres::command_support::catalog::{self as store_catalog};
use graph_store_postgres::{connection_pool::ConnectionPool, Store};
use graph_store_postgres::{BlockStore, NotificationSender};

async fn block_ptr(
    store: Arc<BlockStore>,
    locators: &HashSet<(String, DeploymentLocator)>,
    searches: &Vec<DeploymentSearch>,
    hash: &str,
    number: BlockNumber,
    force: bool,
) -> Result<BlockPtr, anyhow::Error> {
    let block_ptr_to = BlockPtr::try_from((hash, number as i64))
        .map_err(|e| anyhow!("error converting to block pointer: {}", e))?;

    let chains = locators
        .iter()
        .map(|(chain, _)| chain)
        .collect::<HashSet<_>>();

    if chains.len() > 1 {
        let names = searches
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        bail!("the deployments matching `{names}` are on different chains");
    }

    let chain = chains.iter().next().unwrap().to_string();

    let chain_store = match store.chain_store(&chain) {
        None => bail!("can not find chain store for {}", chain),
        Some(store) => store,
    };
    if let Some((_, number, _, _)) = chain_store.block_number(&block_ptr_to.hash).await? {
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
    store: Arc<Store>,
    searches: Vec<DeploymentSearch>,
    block_hash: Option<String>,
    block_number: Option<BlockNumber>,
    sender: &NotificationSender,
    force: bool,
    sleep: Duration,
    start_block: bool,
) -> Result<(), anyhow::Error> {
    // Sanity check
    if !start_block && (block_hash.is_none() || block_number.is_none()) {
        bail!("--block-hash and --block-number must be specified when --start-block is not set");
    }
    let pconn = primary.get()?;
    let mut conn = store_catalog::Connection::new(pconn);

    let subgraph_store = store.subgraph_store();
    let block_store = store.block_store();

    let mut locators = HashSet::new();

    for search in &searches {
        let results = search.lookup(&primary)?;

        let deployment_locators: HashSet<(String, DeploymentLocator)> = results
            .iter()
            .map(|deployment| (deployment.chain.clone(), deployment.locator()))
            .collect();

        if deployment_locators.len() > 1 {
            bail!(
                "Multiple deployments found for the search : {}. Try using the id of the deployment (eg: sgd143) to uniquely identify the deployment.",
                search
            );
        }
        locators.extend(deployment_locators);
    }

    if locators.is_empty() {
        println!("No deployments found");
        return Ok(());
    }

    let block_ptr_to = if start_block {
        None
    } else {
        Some(
            block_ptr(
                block_store,
                &locators,
                &searches,
                block_hash.as_deref().unwrap_or_default(),
                block_number.unwrap_or_default(),
                force,
            )
            .await?,
        )
    };

    println!("Checking if its safe to rewind deployments");
    for (_, locator) in &locators {
        let site = conn
            .locate_site(locator.clone())?
            .ok_or_else(|| anyhow!("failed to locate site for {locator}"))?;
        let deployment_store = subgraph_store.for_site(&site)?;
        let deployment_details = deployment_store.deployment_details_for_id(locator)?;
        let block_number_to = block_ptr_to.as_ref().map(|b| b.number).unwrap_or(0);

        if block_number_to < deployment_details.earliest_block_number + ENV_VARS.reorg_threshold {
            bail!(
                "The block number {} is not safe to rewind to for deployment {}. The earliest block number of this deployment is {}. You can only safely rewind to block number {}",
                block_ptr_to.as_ref().map(|b| b.number).unwrap_or(0),
                locator,
                deployment_details.earliest_block_number,
                deployment_details.earliest_block_number + ENV_VARS.reorg_threshold
            );
        }
    }

    println!("Pausing deployments");
    for (_, locator) in &locators {
        pause_or_resume(primary.clone(), &sender, &locator, true)?;
    }

    // There's no good way to tell that a subgraph has in fact stopped
    // indexing. We sleep and hope for the best.
    println!(
        "\nWaiting {}s to make sure pausing was processed",
        sleep.as_secs()
    );
    thread::sleep(sleep);

    println!("\nRewinding deployments");
    for (chain, loc) in &locators {
        let block_store = store.block_store();
        let deployment_details = subgraph_store.load_deployment_by_id(loc.clone().into())?;
        let block_ptr_to = block_ptr_to.clone();

        let start_block = deployment_details.start_block.or_else(|| {
            block_store
                .chain_store(chain)
                .and_then(|chain_store| chain_store.genesis_block_ptr().ok())
        });

        match (block_ptr_to, start_block) {
            (Some(block_ptr), _) => {
                subgraph_store.rewind(loc.hash.clone(), block_ptr)?;
                println!("  ... rewound {}", loc);
            }
            (None, Some(start_block_ptr)) => {
                subgraph_store.truncate(loc.hash.clone(), start_block_ptr)?;
                println!("  ... truncated {}", loc);
            }
            (None, None) => {
                println!("  ... Failed to find start block for {}", loc);
            }
        }
    }

    println!("Resuming deployments");
    for (_, locator) in &locators {
        pause_or_resume(primary.clone(), &sender, locator, false)?;
    }
    Ok(())
}
