use anyhow::{Context, Result};
use clap::Parser;

use crate::commands::auth::get_deploy_key;
use crate::services::GraphNodeClient;

#[derive(Clone, Debug, Parser)]
#[clap(about = "Register a subgraph name with a Graph Node")]
pub struct CreateOpt {
    /// The subgraph name to register (e.g., "user/subgraph")
    #[clap(value_name = "SUBGRAPH_NAME")]
    pub subgraph_name: String,

    /// Graph Node admin URL
    #[clap(long, short = 'g', value_name = "URL", help = "Graph Node URL")]
    pub node: String,

    /// Access token for authentication
    #[clap(long, value_name = "TOKEN", help = "Graph access token")]
    pub access_token: Option<String>,
}

/// Run the create command
pub async fn run_create(opt: CreateOpt) -> Result<()> {
    println!("Creating subgraph in Graph node: {}", opt.node);

    // Get access token (from flag or from config)
    let access_token = match &opt.access_token {
        Some(token) => Some(token.clone()),
        None => get_deploy_key(&opt.node)
            .ok()
            .flatten()
            .map(|key| key.to_string()),
    };

    let client = GraphNodeClient::new(&opt.node, access_token.as_deref())
        .context("Failed to create Graph Node client")?;

    client
        .create_subgraph(&opt.subgraph_name)
        .await
        .context("Failed to create subgraph")?;

    println!("✔ Created subgraph: {}", opt.subgraph_name);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_opt_parsing() {
        // Test that required args are enforced
        let result = CreateOpt::try_parse_from(["create"]);
        assert!(result.is_err());

        // Test with just subgraph name (missing --node)
        let result = CreateOpt::try_parse_from(["create", "user/subgraph"]);
        assert!(result.is_err());

        // Test with all required args
        let result = CreateOpt::try_parse_from([
            "create",
            "user/subgraph",
            "--node",
            "http://localhost:8020",
        ]);
        assert!(result.is_ok());
        let opt = result.unwrap();
        assert_eq!(opt.subgraph_name, "user/subgraph");
        assert_eq!(opt.node, "http://localhost:8020");
        assert!(opt.access_token.is_none());

        // Test with access token
        let result = CreateOpt::try_parse_from([
            "create",
            "user/subgraph",
            "--node",
            "http://localhost:8020",
            "--access-token",
            "my-token",
        ]);
        assert!(result.is_ok());
        let opt = result.unwrap();
        assert_eq!(opt.access_token, Some("my-token".to_string()));

        // Test short flag for node
        let result =
            CreateOpt::try_parse_from(["create", "user/subgraph", "-g", "http://localhost:8020"]);
        assert!(result.is_ok());
        let opt = result.unwrap();
        assert_eq!(opt.node, "http://localhost:8020");
    }
}
