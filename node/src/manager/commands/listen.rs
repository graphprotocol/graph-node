use std::io::Write;
use std::sync::Arc;

use futures::compat::Future01CompatExt;
//use futures::future;
use graph::{
    components::store::{EntityType, SubscriptionManager as _},
    prelude::{serde_json, Error, Stream, SubscriptionFilter},
};
use graph_store_postgres::SubscriptionManager;

use crate::manager::deployment;

async fn listen(
    mgr: Arc<SubscriptionManager>,
    filter: Vec<SubscriptionFilter>,
) -> Result<(), Error> {
    let events = mgr.subscribe(filter);
    println!("press ctrl-c to stop");
    let res = events
        .inspect(move |event| {
            serde_json::to_writer_pretty(std::io::stdout(), event)
                .expect("event can be serialized to JSON");
            writeln!(std::io::stdout(), "").unwrap();
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
    listen(mgr, vec![SubscriptionFilter::Assignment]).await?;

    Ok(())
}

pub async fn entities(
    mgr: Arc<SubscriptionManager>,
    deployment: String,
    entity_types: Vec<String>,
) -> Result<(), Error> {
    let deployment = deployment::as_hash(deployment)?;
    let filter: Vec<_> = entity_types
        .into_iter()
        .map(|et| SubscriptionFilter::Entities(deployment.clone(), EntityType::new(et)))
        .collect();

    println!("waiting for store events from {}", deployment);
    listen(mgr, filter).await?;

    Ok(())
}
