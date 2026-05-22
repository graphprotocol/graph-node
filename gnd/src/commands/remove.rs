use anyhow::{Context, Result};
use clap::Parser;

use crate::commands::auth::get_deploy_key;
use crate::services::GraphNodeClient;

#[derive(Clone, Debug, Parser)]
#[clap(about = "Unregister a subgraph name from a Graph Node")]
pub struct RemoveOpt {
    /// The subgraph name to unregister
    #[clap(value_name = "SUBGRAPH_NAME")]
    pub subgraph_name: String,

    /// Graph Node admin URL
    #[clap(long, short = 'g', value_name = "URL", help = "Graph Node URL")]
    pub node: String,

    /// Access token for authentication
    #[clap(long, value_name = "TOKEN", help = "Graph access token")]
    pub access_token: Option<String>,
}

/// Run the remove command
pub async fn run_remove(opt: RemoveOpt) -> Result<()> {
    println!("Removing subgraph from Graph node: {}", opt.node);

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
        .remove_subgraph(&opt.subgraph_name)
        .await
        .context("Failed to remove subgraph")?;

    println!("✔ Removed subgraph: {}", opt.subgraph_name);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remove_opt_parsing() {
        // Test that required args are enforced
        let result = RemoveOpt::try_parse_from(["remove"]);
        assert!(result.is_err());

        // Test with just subgraph name (missing --node)
        let result = RemoveOpt::try_parse_from(["remove", "user/subgraph"]);
        assert!(result.is_err());

        // Test with all required args
        let result = RemoveOpt::try_parse_from([
            "remove",
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
        let result = RemoveOpt::try_parse_from([
            "remove",
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
            RemoveOpt::try_parse_from(["remove", "user/subgraph", "-g", "http://localhost:8020"]);
        assert!(result.is_ok());
        let opt = result.unwrap();
        assert_eq!(opt.node, "http://localhost:8020");
    }
}
