use std::io::Write;
use std::sync::Arc;

use graph::futures03::{future, StreamExt};

use graph::{
    components::store::SubscriptionManager as _,
    prelude::{serde_json, Error},
};
use graph_store_postgres::SubscriptionManager;

async fn listen(mgr: Arc<SubscriptionManager>) -> Result<(), Error> {
    let events = mgr.subscribe();
    println!("press ctrl-c to stop");
    events
        .for_each(move |event| {
            serde_json::to_writer_pretty(std::io::stdout(), &event)
                .expect("event can be serialized to JSON");
            println!();
            std::io::stdout().flush().unwrap();
            future::ready(())
        })
        .await;

    Ok(())
}

pub async fn assignments(mgr: Arc<SubscriptionManager>) -> Result<(), Error> {
    println!("waiting for assignment events");
    listen(mgr).await?;

    Ok(())
}
