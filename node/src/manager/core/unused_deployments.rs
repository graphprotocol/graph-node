use std::sync::Arc;

use graph::prelude::{anyhow::Error, chrono};
use graph_store_postgres::{unused, DeploymentDetail, SubgraphStore, UnusedDeployment};

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
    let subgraphs = subgraphs.unwrap_or_default().join(", ");

    list.append(vec![
        id.to_string(),
        shard,
        namespace,
        subgraphs,
        entity_count.to_string(),
    ])
}

pub fn list(store: Arc<SubgraphStore>, existing: bool) -> Result<List, Error> {
    let mut list = make_list();

    let filter = if existing {
        unused::Filter::New
    } else {
        unused::Filter::All
    };

    for deployment in store.list_unused_deployments(filter)? {
        add_row(&mut list, deployment);
    }

    Ok(list)
}

pub fn record(
    store: Arc<SubgraphStore>,
    enable_logging: bool,
) -> Result<Vec<DeploymentDetail>, Error> {
    let mut list = make_list();

    if enable_logging {
        println!("Recording unused deployments. This might take a while.");
    }

    let recorded = store.record_unused_deployments()?;

    for unused in store.list_unused_deployments(unused::Filter::New)? {
        if recorded.iter().any(|r| r.deployment == unused.deployment) {
            add_row(&mut list, unused);
        }
    }

    list.render();

    Ok(recorded)
}

pub fn remove<T, U>(
    store: Arc<SubgraphStore>,
    count: usize,
    deployment: Option<&str>,
    older: Option<chrono::Duration>,
    on_unused_empty: T,
    on_remove_deployment: U,
) -> Result<(), Error>
where
    T: FnOnce() -> Result<(), Error>,
    U: Fn((usize, &UnusedDeployment)) -> Result<(), Error>,
{
    let filter = match older {
        Some(duration) => unused::Filter::UnusedLongerThan(duration),
        None => unused::Filter::New,
    };
    let unused = store.list_unused_deployments(filter)?;
    let unused = match &deployment {
        None => unused,
        Some(deployment) => unused
            .into_iter()
            .filter(|u| &u.deployment == deployment)
            .collect::<Vec<_>>(),
    };

    if unused.is_empty() {
        return on_unused_empty();
    }

    for unused_deployment in unused.iter().take(count).enumerate() {
        on_remove_deployment(unused_deployment)?;
    }

    Ok(())
}
