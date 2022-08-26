#![allow(unused_variables)]
#![allow(unused_mut)]

use graph::anyhow::Error;
use graph::{
    env::env_var,
    prelude::{prost, tokio, tonic},
    {firehose, firehose::FirehoseEndpoint, firehose::ForkStep},
};
use graph_chain_near::codec;
use prost::Message;
use std::sync::Arc;
use std::time::Duration;
use tonic::Streaming;
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let fmt_layer = tracing_subscriber::fmt::layer();
    let filter_layer = tracing_subscriber::filter::Targets::new()
        .with_target("tonic", tracing::Level::TRACE)
        .with_target("hyper", tracing::Level::TRACE)
        // Set to `DEBUG` or even `TRACE` for deeper http2 tracing.
        .with_target("h2", tracing::Level::INFO);

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();

    let cursor: Option<String> = None;
    let token_env = env_var("SF_API_TOKEN", "".to_string());

    // With n = 100 it reliably works in my testing.
    // With n = 101, a connection would often hang in a debug build, but not in a release build.
    // With n = 102 it would reliably hang one connection.
    // With n = 103 it would reliably hang two connections, and so forth.
    let n: usize = env_var("NEAR_EXAMPLE_N_CONNS", 102);
    let mut token: Option<String> = None;
    if token_env.len() > 0 {
        token = Some(token_env);
    }

    let firehose = Arc::new(FirehoseEndpoint::new(
        "firehose",
        "https://mainnet.near.streamingfast.io",
        token.clone(),
        false,
        // Adding more conns here does not help, it seems only one is ever used.
        1,
    ));

    let barrier = Arc::new(tokio::sync::Barrier::new(n));
    for i in 0..n {
        let mut cursor = cursor.clone();
        let mut firehose = firehose.clone();
        let token = token.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            loop {
                println!("Connecting to the stream!");
                let mut stream: Streaming<firehose::Response> = match tokio::time::timeout(
                    Duration::from_secs(10),
                    firehose.clone().stream_blocks(firehose::Request {
                        start_block_num: 12369739,
                        start_cursor: match &cursor {
                            Some(c) => c.clone(),
                            None => String::from(""),
                        },
                        fork_steps: vec![ForkStep::StepNew as i32, ForkStep::StepUndo as i32],
                        ..Default::default()
                    }),
                )
                .await
                .map_err(|e| Error::from(e))
                .and_then(|x| x)
                {
                    Ok(s) => s,
                    Err(e) => {
                        println!("Could not connect to stream! {}", e);
                        // Creating a new `tonic::Channel` is a workaround.
                        // firehose = Arc::new(
                        //     FirehoseEndpoint::new(
                        //         "firehose",
                        //         "https://mainnet.near.streamingfast.io",
                        //         token.clone(),
                        //         false,
                        //         // Adding more conns here does not help.
                        //         1,
                        //     )
                        //     .await,
                        // );
                        continue;
                    }
                };

                loop {
                    let resp = match stream.message().await {
                        Ok(Some(t)) => t,
                        Ok(None) => {
                            println!("Stream completed");
                            return;
                        }
                        Err(e) => {
                            println!("Error getting message {}", e);
                            break;
                        }
                    };

                    let b = codec::Block::decode(resp.block.unwrap().value.as_ref());
                    match b {
                        Ok(b) => {
                            barrier.wait().await;
                            println!(
                                "Block #{} ({}), firehose {}",
                                b.header().height,
                                resp.step,
                                i
                            );

                            cursor = Some(resp.cursor)
                        }
                        Err(e) => panic!("Unable to decode {:?}", e),
                    }
                }
            }
        });
    }

    std::future::pending().await
}
