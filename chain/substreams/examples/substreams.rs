use anyhow::{format_err, Context, Error};
use graph::blockchain::block_stream::BlockStreamEvent;
use graph::blockchain::substreams_block_stream::SubstreamsBlockStream;
use graph::prelude::{info, tokio, DeploymentHash, Registry};
use graph::tokio_stream::StreamExt;
use graph::{
    env::env_var,
    firehose::FirehoseEndpoint,
    log::logger,
    substreams::{self},
};
use graph_chain_substreams::mapper::Mapper;
use graph_core::MetricsRegistry;
use prost::Message;
use std::env;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let module_name = env::args().nth(1).unwrap();

    let token_env = env_var("SUBSTREAMS_API_TOKEN", "".to_string());
    let mut token: Option<String> = None;
    if token_env.len() > 0 {
        token = Some(token_env);
    }

    let endpoint = env_var(
        "SUBSTREAMS_ENDPOINT",
        "https://api-dev.streamingfast.io".to_string(),
    );

    let package_file = env_var("SUBSTREAMS_PACKAGE", "".to_string());
    if package_file == "" {
        panic!("Environment variable SUBSTREAMS_PACKAGE must be set");
    }

    let package = read_package(&package_file)?;

    let logger = logger(true);
    // Set up Prometheus registry
    let prometheus_registry = Arc::new(Registry::new());
    let metrics_registry = Arc::new(MetricsRegistry::new(
        logger.clone(),
        prometheus_registry.clone(),
    ));

    let firehose = Arc::new(FirehoseEndpoint::new(
        "substreams",
        &endpoint,
        token,
        false,
        1,
    ));

    let mut stream: SubstreamsBlockStream<graph_chain_substreams::Chain> =
        SubstreamsBlockStream::new(
            DeploymentHash::new("substreams".to_string()).unwrap(),
            firehose.clone(),
            None,
            None,
            Arc::new(Mapper {}),
            package.modules.clone(),
            module_name.to_string(),
            vec![12369621],
            vec![12370000],
            logger.clone(),
            metrics_registry,
        );

    loop {
        match stream.next().await {
            None => {
                break;
            }
            Some(event) => match event {
                Err(_) => {}
                Ok(block_stream_event) => match block_stream_event {
                    BlockStreamEvent::Revert(_, _) => {}
                    BlockStreamEvent::ProcessBlock(block_with_trigger, _) => {
                        let changes = block_with_trigger.block.entities_changes;
                        for change in changes.entity_changes {
                            info!(&logger, "----- Entity -----");
                            info!(
                                &logger,
                                "name: {} operation: {}", change.entity, change.operation
                            );
                            for field in change.fields {
                                info!(&logger, "field: {}, type: {}", field.name, field.value_type);
                                info!(&logger, "new value: {}", hex::encode(field.new_value));
                                info!(&logger, "old value: {}", hex::encode(field.old_value));
                            }
                        }
                    }
                },
            },
        }
    }

    Ok(())
}

fn read_package(file: &str) -> Result<substreams::Package, anyhow::Error> {
    let content = std::fs::read(file).context(format_err!("read package {}", file))?;
    substreams::Package::decode(content.as_ref()).context("decode command")
}
