use std::io::Write;
use std::sync::Arc;

use futures::compat::Future01CompatExt;
//use futures::future;
use graph::{
    components::store::{EntityType, SubscriptionManager as _},
    prelude::{anyhow, serde_json, Error, Stream, SubgraphDeploymentId, SubscriptionFilter},
};
use graph_store_postgres::SubscriptionManager;

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
    let deployment = SubgraphDeploymentId::new(deployment)
        .map_err(|s| anyhow!("illegal deployment hash `{}`", s))?;
    let filter: Vec<_> = entity_types
        .into_iter()
        .map(|et| SubscriptionFilter::Entities(deployment.clone(), EntityType::new(et)))
        .collect();

    println!("waiting for store events from {}", deployment);
    listen(mgr, filter).await?;

    Ok(())
}
