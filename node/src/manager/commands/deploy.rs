use graph::{
    anyhow::Ok,
    prelude::{
        anyhow::{bail, Result},
        reqwest,
        serde_json::{json, Value},
    },
};

use crate::manager::deployment::DeploymentSearch;

// Function to send an RPC request and handle errors
async fn send_rpc_request(url: &str, payload: Value) -> Result<()> {
    let client = reqwest::Client::new();
    let response = client.post(url).json(&payload).send().await?;

    if response.status().is_success() {
        Ok(())
    } else {
        Err(response
            .error_for_status()
            .expect_err("Failed to parse error response")
            .into())
    }
}

// Function to send subgraph_create request
async fn send_create_request(name: &str, url: &str) -> Result<()> {
    // Construct the JSON payload for subgraph_create
    let create_payload = json!({
        "jsonrpc": "2.0",
        "method": "subgraph_create",
        "params": {
            "name": name,
        },
        "id": "1"
    });

    // Send the subgraph_create request
    send_rpc_request(url, create_payload)
        .await
        .map_err(|e| e.context(format!("Failed to create subgraph with name `{}`", name)))
}

// Function to send subgraph_deploy request
async fn send_deploy_request(name: &str, deployment: &str, url: &str) -> Result<()> {
    // Construct the JSON payload for subgraph_deploy
    let deploy_payload = json!({
        "jsonrpc": "2.0",
        "method": "subgraph_deploy",
        "params": {
            "name": name,
            "ipfs_hash": deployment,
        },
        "id": "1"
    });

    // Send the subgraph_deploy request
    send_rpc_request(url, deploy_payload).await.map_err(|e| {
        e.context(format!(
            "Failed to deploy subgraph `{}` to `{}`",
            deployment, name
        ))
    })
}

pub async fn run(deployment: DeploymentSearch, name: String, url: String) -> Result<()> {
    println!("Deploying subgraph `{}` to `{}`", name, url);
    if let DeploymentSearch::Hash { hash, shard: _ } = deployment {
        // Send the subgraph_create request
        send_create_request(&name, &url).await?;

        // Send the subgraph_deploy request
        send_deploy_request(&name, &hash, &url).await?;
        println!("Subgraph `{}` deployed to `{}`", name, url);
        Ok(())
    } else {
        bail!("Deployment not found");
    }
}
