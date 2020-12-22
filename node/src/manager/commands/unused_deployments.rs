use std::sync::Arc;

use graph::prelude::anyhow::Error;
use graph_store_postgres::{unused, ShardedStore, UnusedDeployment};

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

pub fn list(store: Arc<ShardedStore>, existing: bool) -> Result<(), Error> {
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

pub fn record(store: Arc<ShardedStore>) -> Result<(), Error> {
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
