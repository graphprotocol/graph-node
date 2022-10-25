use anyhow::Context;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncReadExt;

use crate::helpers::{basename, pretty_output, GraphNodePorts};

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

#[derive(Debug)]
pub struct StdIO {
    pub stdout: Option<String>,
    pub stderr: Option<String>,
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
