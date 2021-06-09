use postgres::{Client as PostgreClient, NoTls};
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};
use serde::Deserialize;
use serde_json::json;
use std::error::Error;
use std::hash::Hash;
use reqwest::Client;
use tokio_compat_02::FutureExt;

#[derive(Deserialize, Debug)]
struct BlockResult {
    id: u64,
    jsonrpc: String,
    result: String,
}

#[derive(Clone)]
pub struct IndexerSubstrate {
}

impl IndexerSubstrate {
    pub async fn start_index() {
        let mut client =
            PostgreClient::connect("postgresql://graph-node:let-me-in@localhost:5432/sgtest", NoTls).unwrap();

        // TODO: Add new Block Ingestor with Block Pointer to replace this
        for i in 0..1000 {
            let gist_body = json!({
                "jsonrpc": "2.0",
                "method": "chain_getBlockHash",
                "params": [i],
                "id": 1
            });
            let request_url = "http://localhost:9933";
            let response = Client::new()
                .post(request_url)
                .json(&gist_body)
                .send().compat().await.unwrap();

            // TODO: This should be written by user
            let block_result: BlockResult = response.json().await.unwrap();
            let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
            println!("Timestamp {:?}", timestamp);
            println!("Hash {:?}", block_result.result);

            // TODO: Add new postgres store
            // TODO: Add deployment logic to create new database + store based on IPFS subgraph
            let result = client.execute(
                "INSERT INTO graph.index (hash, timestamp) VALUES ($1, $2)",
                &[&block_result.result.as_bytes(), &timestamp.to_string()],
            );

            match result {
                Ok(_) => {
                    println!("Data insert success");
                },
                Err(e) => {
                    println!("Insert error: {}", e);
                }
            };
        }
    }
}