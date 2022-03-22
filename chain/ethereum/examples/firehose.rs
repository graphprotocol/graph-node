use anyhow::Error;
use graph::{
    env::env_var,
    log::logger,
    prelude::{prost, tokio, tonic},
    {firehose, firehose::FirehoseEndpoint, firehose::ForkStep},
};
use graph_chain_ethereum::codec;
use hex::ToHex;
use prost::Message;
use std::sync::Arc;
use tonic::Streaming;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let mut cursor: Option<String> = None;
    let token_env = env_var("SF_API_TOKEN", "".to_string());
    let mut token: Option<String> = None;
    if token_env.len() > 0 {
        token = Some(token_env);
    }

    let logger = logger(true);
    let firehose = Arc::new(
        FirehoseEndpoint::new(
            logger,
            "firehose",
            "https://api.streamingfast.io:443",
            token,
        )
        .await?,
    );

    loop {
        println!("Connecting to the stream!");
        let mut stream: Streaming<firehose::Response> = match firehose
            .clone()
            .stream_blocks(firehose::Request {
                start_block_num: 12369739,
                stop_block_num: 12369739,
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
                println!("Could not connect to stream! {}", e);
                continue;
            }
        };

        loop {
            let resp = match stream.message().await {
                Ok(Some(t)) => t,
                Ok(None) => {
                    println!("Stream completed");
                    return Ok(());
                }
                Err(e) => {
                    println!("Error getting message {}", e);
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
                    b.transaction_traces.iter().for_each(|trx| {
                        let mut logs: Vec<String> = vec![];
                        trx.calls.iter().for_each(|call| {
                            call.logs.iter().for_each(|log| {
                                logs.push(format!(
                                    "Log {} Topics, Address {}, Trx Index {}, Block Index {}",
                                    log.topics.len(),
                                    log.address.encode_hex::<String>(),
                                    log.index,
                                    log.block_index
                                ));
                            })
                        });

                        if logs.len() > 0 {
                            println!("Transaction {}", trx.hash.encode_hex::<String>());
                            logs.iter().for_each(|log| println!("{}", log));
                        }
                    });

                    cursor = Some(resp.cursor)
                }
                Err(e) => panic!("Unable to decode {:?}", e),
            }
        }
    }
}
