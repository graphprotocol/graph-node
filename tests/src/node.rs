use crate::setup::{process_stdio, IntegrationTestSetup, StdIO};
use anyhow::Context;
use tokio::process::{Child, Command};

pub async fn run_graph_node(test_setup: &IntegrationTestSetup) -> anyhow::Result<Child> {
    use std::process::Stdio;

    let mut command = Command::new(test_setup.graph_node_bin.as_os_str());
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // postgres
        .arg("--postgres-url")
        .arg(&test_setup.postgres_uri)
        // ethereum
        .arg("--ethereum-rpc")
        .arg(&test_setup.ganache_uri)
        // ipfs
        .arg("--ipfs")
        .arg(&test_setup.ipfs_uri)
        // http port
        .arg("--http-port")
        .arg(test_setup.graph_node_ports.http.to_string())
        // index node port
        .arg("--index-node-port")
        .arg(test_setup.graph_node_ports.index.to_string())
        // ws  port
        .arg("--ws-port")
        .arg(test_setup.graph_node_ports.ws.to_string())
        // admin  port
        .arg("--admin-port")
        .arg(test_setup.graph_node_ports.admin.to_string())
        // metrics  port
        .arg("--metrics-port")
        .arg(test_setup.graph_node_ports.metrics.to_string());

    command
        .spawn()
        .context("failed to start graph-node command.")
}

pub async fn stop_graph_node(child: &mut Child) -> anyhow::Result<StdIO> {
    child.kill().await.context("Failed to kill graph-node")?;

    // capture stdio
    let stdout = match child.stdout.take() {
        Some(mut data) => Some(process_stdio(&mut data, "[graph-node:stdout] ").await?),
        None => None,
    };
    let stderr = match child.stderr.take() {
        Some(mut data) => Some(process_stdio(&mut data, "[graph-node:stderr] ").await?),
        None => None,
    };

    Ok(StdIO { stdout, stderr })
}
