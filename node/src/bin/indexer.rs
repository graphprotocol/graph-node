use std::sync::Arc;

use graph::{
    anyhow::Result,
    blockchain::{block_stream::BlockStreamEvent, mock::MockBlockchain},
    indexer::{
        block_stream::IndexerBlockStream,
        store::{SledIndexerStore, DB_NAME},
    },
    prelude::{prost::Message, DeploymentHash, MetricsRegistry},
    slog::info,
    tokio::{self, time::Instant},
    tokio_stream::StreamExt,
};
use uniswap::proto::edgeandnode::uniswap::v1::{Event, EventType, Events};

#[tokio::main]
pub async fn main() -> Result<()> {
    let deployment: &str = "QmagGaBm7FL9uQWg1bk52Eb3LTN4owkvxEKkirtyXNLQc9";
    let hash = DeploymentHash::new(deployment.clone()).unwrap();
    let db = Arc::new(sled::open(DB_NAME).unwrap());
    let store = Arc::new(
        SledIndexerStore::new(
            db,
            deployment,
            graph::indexer::store::StateSnapshotFrequency::Never,
        )
        .unwrap(),
    );
    let logger = graph::log::logger(true);
    let metrics = Arc::new(MetricsRegistry::mock());

    let mut stream = IndexerBlockStream::<MockBlockchain>::new(
        hash,
        store,
        None,
        vec![12369730],
        vec![12369750],
        logger.clone(),
        "handleBlock".into(),
        metrics,
    );

    let earlier = Instant::now();
    let mut prev_ptr = 0;
    loop {
        let x = stream.next().await;
        if x.is_none() {
            break;
        }

        let (ptr, data) = match x.unwrap().unwrap() {
            BlockStreamEvent::ProcessWasmBlock(ptr, _, data, _, _) => (ptr, data),
            _ => unreachable!(),
        };
        // 12369739
        if ptr.number < 12369739 {
            continue;
        }

        if ptr.number > 12369739 {
            return Ok(());
        }
        if prev_ptr > ptr.number {
            break;
        } else {
            prev_ptr = ptr.number;
        }
        let evts: Events = Message::decode(data.as_ref()).unwrap();
        let x = evts.events.first();
        if x.is_none() {
            continue;
        }
        let x = x.unwrap();
        let pool_created: Vec<&Event> = evts
            .events
            .iter()
            .filter(|e| e.r#type() == EventType::PoolCreated)
            .collect();
        // info!(&logger, "====== {}:{} ======", x.address, x.block_number);
        pool_created.iter().for_each(|e| {
            info!(&logger, "poolCreated: owner:{} addr:{}", e.owner, e.address);
        });
    }
    let diff = Instant::now().duration_since(earlier).as_secs();
    println!("### Total streaming time: {}s", diff);

    Ok(())
}
