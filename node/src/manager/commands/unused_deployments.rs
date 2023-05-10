use std::usize;
use std::{sync::Arc, time::Instant};

use graph::anyhow::anyhow;
use graph::prelude::{anyhow::Error, chrono};
use graph_core::graphman::core;
use graph_store_postgres::{SubgraphStore, UnusedDeployment};

pub fn list(store: Arc<SubgraphStore>, existing: bool) -> Result<(), Error> {
    let list = core::unused_deployments::list(store, existing)?;

    if list.is_empty() {
        println!("no unused deployments");
    } else {
        list.render();
    }

    Ok(())
}

pub fn record(store: Arc<SubgraphStore>) -> Result<(), Error> {
    let recorded = core::unused_deployments::record(store, true)?;

    println!("Recorded {} unused deployments", recorded.len());

    Ok(())
}

pub fn remove(
    store: Arc<SubgraphStore>,
    count: usize,
    deployment: Option<&str>,
    older: Option<chrono::Duration>,
) -> Result<(), Error> {
    let on_unused_empty = || {
        match &deployment {
            Some(s) => println!("No unused subgraph matches `{}`", s),
            None => println!("Nothing to remove."),
        }
        return Ok(());
    };

    let depl_store = store.clone();
    let on_remove_deployment = |(i, deployment): (usize, &UnusedDeployment)| {
        println!("{:=<36} {:4} {:=<36}", "", i + 1, "");
        println!(
            "removing {} from {}",
            deployment.namespace, deployment.shard
        );
        println!("  {:>14}: {}", "deployment id", deployment.deployment);
        println!("  {:>14}: {}", "entities", deployment.entity_count);
        if let Some(subgraphs) = &deployment.subgraphs {
            let mut first = true;
            for name in subgraphs {
                if first {
                    println!("  {:>14}: {}", "subgraphs", name);
                } else {
                    println!("  {:>14}  {}", "", name);
                }
                first = false;
            }
        }

        let start = Instant::now();
        match depl_store.remove_deployment(deployment.id) {
            Ok(()) => {
                println!(
                    "done removing {} from {} in {:.1}s\n",
                    deployment.namespace,
                    deployment.shard,
                    start.elapsed().as_millis() as f64 / 1000.0
                );

                Ok(())
            }
            Err(e) => {
                println!("removal failed: {}", e);

                Err(anyhow!(e))
            }
        }
    };

    core::unused_deployments::remove(
        store,
        count,
        deployment,
        older,
        on_unused_empty,
        on_remove_deployment,
    )
}
