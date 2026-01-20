//! Publish command for publishing subgraphs to The Graph's decentralized network.
//!
//! This command builds a subgraph, uploads it to IPFS, and opens a browser
//! to complete the publishing process on The Graph Network.

use std::path::PathBuf;

use anyhow::{anyhow, Result};
use clap::Parser;
use inquire::Confirm;

use crate::commands::build::{run_build, BuildOpt};
use crate::output::{step, Step};

/// Default IPFS URL for The Graph's hosted IPFS node.
const DEFAULT_IPFS_URL: &str = "https://api.thegraph.com/ipfs/api/v0";

/// Default webapp URL for publishing.
const DEFAULT_WEBAPP_URL: &str = "https://cli.thegraph.com/publish";

#[derive(Clone, Debug, Parser)]
#[clap(about = "Publish a subgraph to The Graph's decentralized network")]
pub struct PublishOpt {
    /// Path to the subgraph manifest
    #[clap(default_value = "subgraph.yaml")]
    pub manifest: PathBuf,

    /// IPFS node URL to upload build results to
    #[clap(short = 'i', long, default_value = DEFAULT_IPFS_URL)]
    pub ipfs: String,

    /// IPFS hash of the subgraph manifest to deploy (skips build)
    #[clap(long)]
    pub ipfs_hash: Option<String>,

    /// Subgraph ID to publish to (for updating existing subgraphs)
    #[clap(long)]
    pub subgraph_id: Option<String>,

    /// The network to use for the subgraph deployment
    #[clap(long, default_value = "arbitrum-one")]
    pub protocol_network: String,

    /// The API key for Subgraph queries (required when updating existing subgraph)
    #[clap(long)]
    pub api_key: Option<String>,

    /// URL of the web UI to use for publishing
    #[clap(long, default_value = DEFAULT_WEBAPP_URL)]
    pub webapp_url: String,

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

    /// Skip the asc version check (use with caution)
    #[clap(long, env = "GND_SKIP_ASC_VERSION_CHECK")]
    pub skip_asc_version_check: bool,
}

/// Run the publish command.
pub async fn run_publish(opt: PublishOpt) -> Result<()> {
    // Validate: if subgraph_id is provided, api_key is required
    if opt.subgraph_id.is_some() && opt.api_key.is_none() {
        return Err(anyhow!(
            "API key is required to publish to an existing subgraph (--api-key).\n\
             See https://thegraph.com/docs/en/deploying/subgraph-studio-faqs/#2-how-do-i-create-an-api-key"
        ));
    }

    // Validate protocol network
    if opt.protocol_network != "arbitrum-one" && opt.protocol_network != "arbitrum-sepolia" {
        return Err(anyhow!(
            "Invalid protocol network '{}'. Must be 'arbitrum-one' or 'arbitrum-sepolia'",
            opt.protocol_network
        ));
    }

    // Extract URL-related fields before potentially moving opt fields into BuildOpt
    let webapp_url = opt.webapp_url.clone();
    let subgraph_id = opt.subgraph_id.clone();
    let protocol_network = opt.protocol_network.clone();
    let api_key = opt.api_key.clone();

    // Get the IPFS hash - either from flag or by building
    let ipfs_hash = if let Some(hash) = opt.ipfs_hash {
        step(Step::Skip, "Using provided IPFS hash");
        hash
    } else {
        // Build the subgraph and upload to IPFS
        step(Step::Load, "Building subgraph for publishing");

        // Validate manifest exists
        if !opt.manifest.exists() {
            return Err(anyhow!(
                "Manifest '{}' not found.\n\
                 Please provide either:\n\
                 - A valid manifest path: gnd publish <manifest>\n\
                 - An IPFS hash: gnd publish --ipfs-hash <hash>",
                opt.manifest.display()
            ));
        }

        let build_opt = BuildOpt {
            manifest: opt.manifest,
            output_dir: opt.output_dir,
            output_format: "wasm".to_string(),
            skip_migrations: opt.skip_migrations,
            watch: false,
            ipfs: Some(opt.ipfs),
            network: opt.network,
            network_file: opt.network_file,
            skip_asc_version_check: opt.skip_asc_version_check,
        };

        match run_build(build_opt).await? {
            Some(hash) => hash,
            None => {
                return Err(anyhow!(
                    "Build completed but IPFS upload failed. Cannot proceed with publish."
                ));
            }
        }
    };

    // Prompt user to open browser
    step(Step::Generate, "Ready to publish to The Graph Network");

    let open_browser = Confirm::new("Open browser to continue publishing?")
        .with_default(true)
        .prompt()?;

    if !open_browser {
        println!("\nTo publish manually, visit:");
        println!(
            "  {}",
            build_publish_url(
                &webapp_url,
                &ipfs_hash,
                &subgraph_id,
                &protocol_network,
                &api_key
            )
        );
        return Ok(());
    }

    // Build the URL with query parameters
    let url = build_publish_url(
        &webapp_url,
        &ipfs_hash,
        &subgraph_id,
        &protocol_network,
        &api_key,
    );

    step(Step::Done, "Opening browser to complete publishing");
    println!("\n  {}\n", url);

    // Open the browser
    open::that(&url).map_err(|e| anyhow!("Failed to open browser: {}", e))?;

    Ok(())
}

/// Build the publish URL with query parameters.
fn build_publish_url(
    base_url: &str,
    ipfs_hash: &str,
    subgraph_id: &Option<String>,
    protocol_network: &str,
    api_key: &Option<String>,
) -> String {
    let mut params = vec![format!("id={}", ipfs_hash)];

    if let Some(id) = subgraph_id {
        params.push(format!("subgraphId={}", id));
    }

    params.push(format!("network={}", protocol_network));

    if let Some(key) = api_key {
        params.push(format!("apiKey={}", key));
    }

    format!("{}?{}", base_url, params.join("&"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_publish_url_basic() {
        let url = build_publish_url(
            DEFAULT_WEBAPP_URL,
            "QmTest123",
            &None,
            "arbitrum-one",
            &None,
        );
        assert_eq!(
            url,
            "https://cli.thegraph.com/publish?id=QmTest123&network=arbitrum-one"
        );
    }

    #[test]
    fn test_build_publish_url_with_subgraph_id() {
        let url = build_publish_url(
            DEFAULT_WEBAPP_URL,
            "QmTest456",
            &Some("my-subgraph".to_string()),
            "arbitrum-sepolia",
            &Some("abc123".to_string()),
        );
        assert_eq!(
            url,
            "https://cli.thegraph.com/publish?id=QmTest456&subgraphId=my-subgraph&network=arbitrum-sepolia&apiKey=abc123"
        );
    }

    #[test]
    fn test_build_publish_url_custom_webapp() {
        let url = build_publish_url(
            "https://custom.example.com/publish",
            "QmCustom",
            &None,
            "arbitrum-one",
            &None,
        );
        assert_eq!(
            url,
            "https://custom.example.com/publish?id=QmCustom&network=arbitrum-one"
        );
    }
}
