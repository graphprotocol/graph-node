use std::env;
use std::io::Read;
use std::collections::BTreeMap;

use graph::prelude::{*};
use graph::log::logger;
use structopt::StructOpt;
use fluvio::{Fluvio, Offset, consumer::{SmartModuleInvocation,SmartModuleInvocationWasm,SmartModuleKind, ConsumerConfig, PartitionSelectionStrategy}, dataplane::smartmodule::{SmartModuleExtraParams}};
use flate2::bufread::GzEncoder;
use flate2::Compression;

#[derive(Debug, StructOpt)]
struct Opt {
    pub deployment: String,
    pub start_block: u64,
    pub end_block: u64,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Set up logger
    let logger = match env::var_os("GRAPH_LOG") {
        Some(_) => logger(false),
        None => Logger::root(slog::Discard, o!()),
    };

    let opt = Opt::from_args();

    let raw_buffer = std::fs::read("/Users/jannis/work/graphprotocol/graph-node/target/wasm32-unknown-unknown/debug/block_range_filter.wasm")?;
    let mut encoder = GzEncoder::new(raw_buffer.as_slice(), Compression::default());
    let mut buffer = Vec::with_capacity(raw_buffer.len());
    encoder.read_to_end(&mut buffer)?;

    let topic = opt.deployment.to_lowercase();
    let fluvio = Fluvio::connect().await?;
    let consumer = fluvio.consumer(PartitionSelectionStrategy::All(topic.clone())).await?;
    let mut stream = consumer.stream_with_config(
        Offset::beginning(),
        ConsumerConfig::builder()
            .smartmodule(Some(SmartModuleInvocation {
                wasm: SmartModuleInvocationWasm::AdHoc(buffer),
                kind: SmartModuleKind::Filter,
                params: SmartModuleExtraParams::from(
                    vec![
                        (String::from("start_block"), format!("{}", opt.start_block)),
                        (String::from("end_block"), format!("{}", opt.end_block))
                    ].into_iter().collect::<BTreeMap<_,_>>()
                ),
            }))
            .build()?
    ).await?;

    while let Some(Ok(record)) = stream.next().await {
        let value = String::from_utf8_lossy(record.value()).to_string();
        println!("{}", value);
    }

    Ok(())
}
