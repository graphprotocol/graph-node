use anyhow::Context;
use std::{path::PathBuf, sync::Arc};
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};

use crate::{basename, pretty_output, GraphNodePorts};

/// Contains all information a test command needs
#[derive(Debug)]
pub struct IntegrationTestSetup {
    pub postgres_uri: String,
    pub ipfs_uri: String,
    pub ganache_port: u16,
    pub ganache_uri: String,
    pub graph_node_ports: GraphNodePorts,
    pub graph_node_bin: Arc<PathBuf>,
    pub test_directory: Option<PathBuf>,
}

impl IntegrationTestSetup {
    pub fn test_name(&self) -> String {
        match self.test_directory {
            Some(ref test_directory) => basename(test_directory),
            None => "".to_string(),
        }
    }

    pub fn graph_node_admin_uri(&self) -> String {
        let ws_port = self.graph_node_ports.admin;
        format!("http://localhost:{}/", ws_port)
    }
}

pub async fn run_graph_node(test_setup: &IntegrationTestSetup) -> anyhow::Result<Child> {
    use std::process::Stdio;

    let mut command = Command::new(test_setup.graph_node_bin.as_os_str());
    command
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
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

    // add test specific environment variables
    // TODO: it might be interesting to refactor this conditional into a new datatype that ties
    // the test name and its environment variables together.
    if test_setup.test_name().as_str() == "data-source-revert" {
        command.env(
            "FAILPOINTS",
            "test_reorg=return(2);error_on_duplicate_ds=return",
        );
    }

    command
        .spawn()
        .context("failed to start graph-node command.")
}

#[derive(Debug)]
pub struct StdIO {
    stdout: Option<String>,
    stderr: Option<String>,
}

impl std::fmt::Display for StdIO {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref stdout) = self.stdout {
            write!(f, "{}", stdout)?;
        }
        if let Some(ref stderr) = self.stderr {
            write!(f, "{}", stderr)?
        }
        Ok(())
    }
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

pub async fn process_stdio<T: AsyncReadExt + Unpin>(
    stdio: &mut T,
    prefix: &str,
) -> anyhow::Result<String> {
    let mut buffer: Vec<u8> = Vec::new();
    stdio
        .read_to_end(&mut buffer)
        .await
        .context("failed to read stdio")?;
    Ok(pretty_output(&buffer, prefix))
}
