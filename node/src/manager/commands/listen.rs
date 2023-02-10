use std::iter::FromIterator;
use std::sync::Arc;
use std::{collections::BTreeSet, io::Write};

use futures::compat::Future01CompatExt;
//use futures::future;
use graph::{
    components::store::{EntityType, SubscriptionManager as _},
    prelude::{serde_json, Error, Stream, SubscriptionFilter},
};
use graph_store_postgres::connection_pool::ConnectionPool;
use graph_store_postgres::SubscriptionManager;

use crate::manager::deployment::DeploymentSearch;

async fn listen(
    mgr: Arc<SubscriptionManager>,
    filter: BTreeSet<SubscriptionFilter>,
) -> Result<(), Error> {
    let events = mgr.subscribe(filter);
    println!("press ctrl-c to stop");
    let res = events
        .inspect(move |event| {
            serde_json::to_writer_pretty(std::io::stdout(), event)
                .expect("event can be serialized to JSON");
            writeln!(std::io::stdout()).unwrap();
            std::io::stdout().flush().unwrap();
        })
        .collect()
        .compat()
        .await;

    match res {
        Ok(_) => {
            println!("stream finished")
        }
        Err(()) => {
            eprintln!("stream failed")
        }
    }
    Ok(())
}

pub async fn assignments(mgr: Arc<SubscriptionManager>) -> Result<(), Error> {
    println!("waiting for assignment events");
    listen(
        mgr,
        FromIterator::from_iter([SubscriptionFilter::Assignment]),
    )
    .await?;

    Ok(())
}

pub async fn entities(
    primary_pool: ConnectionPool,
    mgr: Arc<SubscriptionManager>,
    search: &DeploymentSearch,
    entity_types: Vec<String>,
) -> Result<(), Error> {
    let locator = search.locate_unique(&primary_pool)?;
    let filter = entity_types
        .into_iter()
        .map(|et| SubscriptionFilter::Entities(locator.hash.clone(), EntityType::new(et)))
        .collect();

    println!("waiting for store events from {}", locator);
    listen(mgr, filter).await?;

    Ok(())
}
