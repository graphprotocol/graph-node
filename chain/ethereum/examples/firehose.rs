use anyhow::Error;
use graph::{
    firehose::{
        bstream::BlockResponseV2, bstream::BlocksRequestV2, bstream::ForkStep,
        endpoints::FirehoseEndpoint,
    },
    log::logger,
    prelude::{prost, tokio, tonic},
};
use graph_chain_ethereum::codec;
use prost::Message;
use std::sync::Arc;
use tonic::Streaming;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut cursor: Option<String> = None;

    let logger = logger(true);
    let firehose = Arc::new(
        FirehoseEndpoint::new(logger, "firehose", "https://bsc.streamingfast.io:443", None).await?,
    );

    loop {
        println!("connecting to the stream!");
        let mut stream: Streaming<BlockResponseV2> = match firehose
            .clone()
            .stream_blocks(BlocksRequestV2 {
                start_block_num: 7000000,
                start_cursor: match &cursor {
                    Some(c) => c.clone(),
                    None => String::from(""),
                },
                fork_steps: vec![ForkStep::StepNew as i32, ForkStep::StepUndo as i32],
                ..Default::default()
            })
            .await
        {
            Ok(s) => s,
            Err(e) => {
                println!("could not connect to stream! {}", e);
                continue;
            }
        };

        loop {
            let resp = match stream.message().await {
                Ok(Some(t)) => t,
                Ok(None) => {
                    println!("stream completed");
                    break;
                }
                Err(e) => {
                    println!("error getting message {}", e);
                    break;
                }
            };

            let b = codec::Block::decode(resp.block.unwrap().value.as_ref());
            match b {
                Ok(b) => {
                    println!(
                        "Block #{} ({}) ({})",
                        b.number,
                        hex::encode(b.hash),
                        resp.step
                    );
                    cursor = Some(resp.cursor)
                }
                Err(e) => panic!("Unable to decode {:?}", e),
            }
        }
    }
}
