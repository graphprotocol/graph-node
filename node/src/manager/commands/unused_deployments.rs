use std::{sync::Arc, time::Instant};

use graph::prelude::{anyhow::anyhow, anyhow::Error, SubgraphDeploymentId};
use graph_store_postgres::{unused, SubgraphStore, UnusedDeployment};

use crate::manager::display::List;

fn make_list() -> List {
    List::new(vec!["id", "shard", "namespace", "subgraphs", "entities"])
}

fn add_row(list: &mut List, deployment: UnusedDeployment) {
    let UnusedDeployment {
        id,
        shard,
        namespace,
        subgraphs,
        entity_count,
        ..
    } = deployment;
    let subgraphs = subgraphs.unwrap_or(vec![]).join(", ");

    list.append(vec![
        id,
        shard,
        namespace,
        subgraphs,
        entity_count.to_string(),
    ])
}

pub fn list(store: Arc<SubgraphStore>, existing: bool) -> Result<(), Error> {
    let mut list = make_list();

    let filter = if existing {
        unused::Filter::New
    } else {
        unused::Filter::All
    };

    for deployment in store.list_unused_deployments(filter)? {
        add_row(&mut list, deployment);
    }

    if list.is_empty() {
        println!("no unused deployments");
    } else {
        list.render();
    }

    Ok(())
}

pub fn record(store: Arc<SubgraphStore>) -> Result<(), Error> {
    let mut list = make_list();

    println!("Recording unused deployments. This might take a while.");
    let recorded = store.record_unused_deployments()?;

    for deployment in store.list_unused_deployments(unused::Filter::New)? {
        if recorded.iter().find(|r| r.id == deployment.id).is_some() {
            add_row(&mut list, deployment);
        }
    }

    list.render();
    println!("Recorded {} unused deployments", recorded.len());

    Ok(())
}

pub fn remove(
    store: Arc<SubgraphStore>,
    count: usize,
    deployment: Option<String>,
) -> Result<(), Error> {
    let unused = store.list_unused_deployments(unused::Filter::New)?;
    let unused = match &deployment {
        None => unused,
        Some(deployment) => unused
            .into_iter()
            .filter(|u| u.id.as_str() == deployment)
            .collect::<Vec<_>>(),
    };

    if unused.is_empty() {
        match &deployment {
            Some(s) => println!("No unused subgraph matches `{}`", s),
            None => println!("Nothing to remove."),
        }
        return Ok(());
    }

    for (i, deployment) in unused.iter().take(count).enumerate() {
        let id = SubgraphDeploymentId::new(&deployment.id)
            .map_err(|s| anyhow!("illegal subgraph deployment id: {}", s))?;

        println!("{:=<36} {:4} {:=<36}", "", i + 1, "");
        println!(
            "removing {} from {}",
            deployment.namespace, deployment.shard
        );
        println!("  {:>14}: {}", "deployment id", deployment.id);
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
        match store.remove_deployment(&id) {
            Ok(()) => {
                println!(
                    "done removing {} from {} in {:.1}s\n",
                    deployment.namespace,
                    deployment.shard,
                    start.elapsed().as_millis() as f64 / 1000.0
                );
            }
            Err(e) => {
                println!("removal failed: {}", e)
            }
        }
    }
    Ok(())
}
