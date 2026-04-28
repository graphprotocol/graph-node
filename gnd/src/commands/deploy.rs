//! Deploy command for deploying subgraphs to a Graph Node.
//!
//! This command builds a subgraph (unless an IPFS hash is provided),
//! uploads it to IPFS, and deploys it to a Graph Node.

use std::{
    io::{self, IsTerminal},
    path::PathBuf,
};

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use url::Url;

use crate::commands::auth::get_deploy_key;
use crate::commands::build::{BuildOpt, run_build};
use crate::output::{Step, step};
use crate::prompt::{normalize_version_label, prompt_version_label};
use crate::services::GraphNodeClient;

/// Default IPFS URL used by The Graph
const DEFAULT_IPFS_URL: &str = "https://api.thegraph.com/ipfs/api/v0";

/// Default deploy URL for Subgraph Studio
const SUBGRAPH_STUDIO_URL: &str = "https://api.studio.thegraph.com/deploy/";

#[derive(Clone, Debug, Parser)]
#[clap(about = "Deploy a subgraph to a Graph Node")]
pub struct DeployOpt {
    /// Name to deploy the subgraph as (e.g., "user/subgraph")
    #[clap()]
    pub subgraph_name: String,

    /// Path to the subgraph manifest
    #[clap(default_value = "subgraph.yaml")]
    pub manifest: PathBuf,

    /// Graph Node admin URL. Defaults to Subgraph Studio if not provided.
    #[clap(short = 'g', long)]
    pub node: Option<String>,

    /// IPFS node URL to upload build results to
    #[clap(short = 'i', long, default_value = DEFAULT_IPFS_URL)]
    pub ipfs: String,

    /// Deploy key for authentication
    #[clap(long)]
    pub deploy_key: Option<String>,

    /// Version label for the deployment (e.g., "v0.0.1")
    #[clap(short = 'l', long)]
    pub version_label: Option<String>,

    /// IPFS hash of an already-uploaded subgraph manifest to deploy
    #[clap(long)]
    pub ipfs_hash: Option<String>,

    /// Output directory for build results
    #[clap(short = 'o', long, default_value = "build/")]
    pub output_dir: PathBuf,

    /// Skip subgraph migrations
    #[clap(long)]
    pub skip_migrations: bool,

    /// Network configuration to use from the networks config file
    #[clap(long)]
    pub network: Option<String>,

    /// Networks config file path
    #[clap(long, default_value = "networks.json")]
    pub network_file: PathBuf,

    /// Fork subgraph ID for debugging
    #[clap(long)]
    pub debug_fork: Option<String>,

    /// Skip the asc version check (use with caution)
    #[clap(long, env = "GND_SKIP_ASC_VERSION_CHECK")]
    pub skip_asc_version_check: bool,
}

/// Run the deploy command.
pub async fn run_deploy(opt: DeployOpt) -> Result<()> {
    // Use Subgraph Studio URL if no node is provided
    let node = opt.node.as_deref().unwrap_or(SUBGRAPH_STUDIO_URL);

    // Validate URLs
    validate_url(node, "node")?;
    validate_url(&opt.ipfs, "IPFS")?;

    // Get deploy key (from flag or stored auth)
    let deploy_key = match &opt.deploy_key {
        Some(key) => Some(key.clone()),
        None => get_deploy_key(node)?,
    };

    let version_label =
        match resolve_version_label(opt.version_label.as_deref(), io::stdin().is_terminal())? {
            Some(label) => label,
            None => prompt_version_label()?,
        };

    // Get or build the IPFS hash
    let ipfs_hash = match &opt.ipfs_hash {
        Some(hash) => {
            step(Step::Skip, "Build (using provided IPFS hash)");
            hash.clone()
        }
        None => {
            // Build the subgraph and upload to IPFS
            build_and_upload(&opt).await?
        }
    };

    // Deploy to Graph Node
    deploy_to_node(
        &opt,
        node,
        &ipfs_hash,
        &version_label,
        deploy_key.as_deref(),
    )
    .await
}

/// Validate that a URL is well-formed.
fn validate_url(url: &str, name: &str) -> Result<()> {
    Url::parse(url)
        .map_err(|e| anyhow!("Invalid {} URL '{}': {}", name, url, e))
        .map(|parsed| {
            match parsed.scheme() {
                "http" | "https" => {}
                scheme => {
                    return Err(anyhow!(
                        "Unsupported protocol '{}' for {} URL. Must be http:// or https://",
                        scheme,
                        name
                    ));
                }
            }
            Ok(())
        })??;
    Ok(())
}

/// Parse and validate a normalized version label.
fn parse_version_label(label: &str) -> Result<String> {
    let normalized = normalize_version_label(label);
    if normalized.is_empty() {
        return Err(anyhow!("Version label cannot be empty"));
    }
    Ok(normalized)
}

/// Resolve version label from flag or interactive prompt mode.
fn resolve_version_label(
    version_label: Option<&str>,
    stdin_is_terminal: bool,
) -> Result<Option<String>> {
    match version_label {
        Some(label) => parse_version_label(label)
            .context("Invalid --version-label value")
            .map(Some),
        None if stdin_is_terminal => Ok(None),
        None => Err(anyhow!(
            "--version-label is required in non-interactive mode. \
             Pass --version-label <LABEL>."
        )),
    }
}

/// Build the subgraph and upload to IPFS.
async fn build_and_upload(opt: &DeployOpt) -> Result<String> {
    // Run the build command with IPFS upload enabled
    let build_opt = BuildOpt {
        manifest: opt.manifest.clone(),
        output_dir: opt.output_dir.clone(),
        output_format: "wasm".to_string(),
        skip_migrations: opt.skip_migrations,
        watch: false,
        ipfs: Some(opt.ipfs.clone()),
        network: opt.network.clone(),
        network_file: opt.network_file.clone(),
        skip_asc_version_check: opt.skip_asc_version_check,
    };

    match run_build(build_opt).await? {
        Some(ipfs_hash) => Ok(ipfs_hash),
        None => Err(anyhow!(
            "Build succeeded but no IPFS hash was returned. This is unexpected."
        )),
    }
}

/// Deploy the subgraph to the Graph Node.
async fn deploy_to_node(
    opt: &DeployOpt,
    node: &str,
    ipfs_hash: &str,
    version_label: &str,
    deploy_key: Option<&str>,
) -> Result<()> {
    step(Step::Deploy, &format!("Deploying to {}", node));

    let client = GraphNodeClient::new(node, deploy_key)?;

    let result = client
        .deploy_subgraph(
            &opt.subgraph_name,
            ipfs_hash,
            Some(version_label),
            opt.debug_fork.as_deref(),
        )
        .await
        .with_context(|| {
            format!(
                "Failed to deploy subgraph '{}' to {}",
                opt.subgraph_name, node
            )
        })?;

    // Normalize URLs if they're relative (start with ':')
    let base = Url::parse(node)?;
    let base_str = format!(
        "{}://{}",
        base.scheme(),
        base.host_str().unwrap_or("localhost")
    );

    let playground = if result.playground.starts_with(':') {
        format!("{}{}", base_str, result.playground)
    } else {
        result.playground
    };

    let queries = if result.queries.starts_with(':') {
        format!("{}{}", base_str, result.queries)
    } else {
        result.queries
    };

    step(Step::Done, &format!("Deployed to {}", playground));
    println!();
    println!("Subgraph endpoints:");
    println!("Queries (HTTP):     {}", queries);
    println!();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_url_valid_http() {
        assert!(validate_url("http://localhost:8020", "node").is_ok());
    }

    #[test]
    fn test_validate_url_valid_https() {
        assert!(validate_url("https://api.thegraph.com/ipfs", "IPFS").is_ok());
    }

    #[test]
    fn test_validate_url_invalid() {
        assert!(validate_url("not-a-url", "test").is_err());
    }

    #[test]
    fn test_validate_url_unsupported_protocol() {
        assert!(validate_url("ftp://example.com", "test").is_err());
    }

    #[test]
    fn test_subgraph_studio_url() {
        // Verify the default URL is valid
        assert!(validate_url(SUBGRAPH_STUDIO_URL, "node").is_ok());
    }

    #[test]
    fn test_default_ipfs_url() {
        // Verify the default IPFS URL is valid
        assert!(validate_url(DEFAULT_IPFS_URL, "IPFS").is_ok());
    }

    #[test]
    fn test_parse_version_label_rejects_empty_after_normalize() {
        assert!(parse_version_label("").is_err());
        assert!(parse_version_label("   ").is_err());
        assert!(parse_version_label("\"\"").is_err());
        assert!(parse_version_label(" \"\" ").is_err());
        assert!(parse_version_label("\"   \"").is_err());
    }

    #[test]
    fn test_resolve_version_label_from_flag() {
        assert_eq!(
            resolve_version_label(Some(" \"v1.0.0\" "), false).unwrap(),
            Some("v1.0.0".to_string())
        );
    }

    #[test]
    fn test_resolve_version_label_interactive_without_flag() {
        assert_eq!(resolve_version_label(None, true).unwrap(), None);
    }

    #[test]
    fn test_resolve_version_label_non_interactive_requires_flag() {
        let err = resolve_version_label(None, false).unwrap_err();
        assert!(
            err.to_string()
                .contains("--version-label is required in non-interactive mode")
        );
    }
}
