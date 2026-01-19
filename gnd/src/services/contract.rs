//! Contract ABI fetching service.
//!
//! This module provides functionality to fetch verified contract ABIs and metadata
//! from block explorer APIs (Etherscan, Blockscout) and Sourcify.

use std::env;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value as JsonValue;

/// Timeout for HTTP requests to external APIs.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Contract service for fetching ABIs and metadata.
pub struct ContractService {
    client: Client,
    registry: NetworksRegistry,
}

/// A network definition from the registry.
#[derive(Debug, Clone, Deserialize)]
pub struct Network {
    /// Network identifier (e.g., "mainnet", "goerli")
    pub id: String,
    /// Short display name
    #[serde(rename = "shortName")]
    pub short_name: Option<String>,
    /// Full display name
    #[serde(rename = "fullName")]
    pub full_name: Option<String>,
    /// Alternative names for the network
    #[serde(default)]
    pub aliases: Vec<String>,
    /// CAIP-2 chain identifier (e.g., "eip155:1")
    #[serde(rename = "caip2Id")]
    pub caip2_id: String,
    /// Block explorer API URLs
    #[serde(rename = "apiUrls", default)]
    pub api_urls: Vec<ApiUrl>,
    /// JSON-RPC URLs
    #[serde(rename = "rpcUrls", default)]
    pub rpc_urls: Vec<String>,
}

/// An API URL entry from the registry.
#[derive(Debug, Clone, Deserialize)]
pub struct ApiUrl {
    /// The API URL (may contain {ENVVAR} placeholders)
    pub url: String,
    /// The type of API (etherscan, blockscout, etc.)
    pub kind: String,
}

/// The networks registry.
#[derive(Debug, Clone, Deserialize)]
pub struct NetworksRegistry {
    /// List of networks
    pub networks: Vec<Network>,
}

impl NetworksRegistry {
    /// Load the registry from the default URL.
    pub async fn load() -> Result<Self> {
        let client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("Failed to create HTTP client")?;

        let url = "https://networks-registry.thegraph.com/TheGraphNetworksRegistry.json";
        let response = client
            .get(url)
            .send()
            .await
            .context("Failed to fetch networks registry")?;

        if !response.status().is_success() {
            return Err(anyhow!(
                "Failed to fetch networks registry: {}",
                response.status()
            ));
        }

        response
            .json::<NetworksRegistry>()
            .await
            .context("Failed to parse networks registry")
    }

    /// Load from a JSON string (for testing).
    pub fn from_json(json: &str) -> Result<Self> {
        serde_json::from_str(json).context("Failed to parse networks registry JSON")
    }

    /// Get a network by ID or alias.
    pub fn get_network(&self, id: &str) -> Option<&Network> {
        let id_lower = id.to_lowercase();
        self.networks.iter().find(|n| {
            n.id.to_lowercase() == id_lower
                || n.aliases.iter().any(|a| a.to_lowercase() == id_lower)
        })
    }
}

/// Result of fetching contract info from Sourcify.
#[derive(Debug)]
pub struct SourcifyResult {
    /// The contract ABI as JSON
    pub abi: JsonValue,
    /// The contract name
    pub name: String,
    /// The deployment block number
    pub start_block: u64,
}

/// Response from Etherscan getabi API.
#[derive(Debug, Deserialize)]
struct EtherscanResponse {
    status: String,
    message: String,
    result: Option<JsonValue>,
}

/// Response from Etherscan getsourcecode API.
#[derive(Debug, Deserialize)]
struct SourceCodeResponse {
    status: String,
    #[allow(dead_code)]
    message: String,
    result: Option<Vec<SourceCodeResult>>,
}

#[derive(Debug, Deserialize)]
struct SourceCodeResult {
    #[serde(rename = "ContractName")]
    contract_name: Option<String>,
}

/// Response from Etherscan getcontractcreation API.
#[derive(Debug, Deserialize)]
struct ContractCreationResponse {
    status: String,
    #[allow(dead_code)]
    message: String,
    result: Option<Vec<ContractCreationResult>>,
}

#[derive(Debug, Deserialize)]
struct ContractCreationResult {
    #[serde(rename = "blockNumber")]
    block_number: Option<String>,
    #[serde(rename = "txHash")]
    tx_hash: Option<String>,
}

/// Response from Sourcify API.
#[derive(Debug, Deserialize)]
struct SourcifyResponse {
    abi: Option<JsonValue>,
    compilation: Option<SourcifyCompilation>,
    deployment: Option<SourcifyDeployment>,
}

#[derive(Debug, Deserialize)]
struct SourcifyCompilation {
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SourcifyDeployment {
    #[serde(rename = "blockNumber")]
    block_number: Option<String>,
}

/// Response from eth_getTransactionByHash RPC.
#[derive(Debug, Deserialize)]
struct RpcResponse {
    result: Option<TransactionResult>,
}

#[derive(Debug, Deserialize)]
struct TransactionResult {
    #[serde(rename = "blockNumber")]
    block_number: Option<String>,
}

impl ContractService {
    /// Create a new contract service with the given registry.
    pub fn new(registry: NetworksRegistry) -> Self {
        let client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .expect("Failed to create HTTP client");

        Self { client, registry }
    }

    /// Create a new contract service, loading the registry from the default URL.
    pub async fn load() -> Result<Self> {
        let registry = NetworksRegistry::load().await?;
        Ok(Self::new(registry))
    }

    /// Get the contract ABI for a given network and address.
    pub async fn get_abi(&self, network_id: &str, address: &str) -> Result<JsonValue> {
        let urls = self.get_etherscan_urls(network_id)?;
        if urls.is_empty() {
            return Err(anyhow!(
                "No contract API available for {} in the registry",
                network_id
            ));
        }

        let mut last_error = None;

        for url in urls {
            let api_url = format!("{}?module=contract&action=getabi&address={}", url, address);

            match self.fetch_from_etherscan(&api_url).await {
                Ok(response) => {
                    if response.status == "1" {
                        if let Some(result) = response.result {
                            // The result is a JSON string containing the ABI
                            if let Some(abi_str) = result.as_str() {
                                return serde_json::from_str(abi_str)
                                    .context("Failed to parse ABI JSON");
                            }
                            return Ok(result);
                        }
                    }
                    last_error = Some(anyhow!(
                        "{} - {}",
                        response.message,
                        response.result.unwrap_or(JsonValue::Null)
                    ));
                }
                Err(e) => {
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("No API URLs available")))
    }

    /// Get the contract name for a given network and address.
    pub async fn get_contract_name(&self, network_id: &str, address: &str) -> Result<String> {
        let urls = self.get_etherscan_urls(network_id)?;
        if urls.is_empty() {
            return Err(anyhow!(
                "No contract API available for {} in the registry",
                network_id
            ));
        }

        let mut last_error = None;

        for url in urls {
            let api_url = format!(
                "{}?module=contract&action=getsourcecode&address={}",
                url, address
            );

            let response = self
                .client
                .get(&api_url)
                .send()
                .await
                .context("Contract API is unreachable")?;

            if response.status().is_success() {
                match response.json::<SourceCodeResponse>().await {
                    Ok(data) => {
                        if data.status == "1" {
                            if let Some(results) = data.result {
                                if let Some(first) = results.first() {
                                    if let Some(name) = &first.contract_name {
                                        if !name.is_empty() {
                                            return Ok(name.clone());
                                        }
                                    }
                                }
                            }
                        }
                        last_error = Some(anyhow!("Contract name is empty"));
                    }
                    Err(e) => {
                        last_error = Some(anyhow!("Invalid JSON: {}", e));
                    }
                }
            } else {
                last_error = Some(anyhow!(
                    "{} {}",
                    response.status(),
                    response.status().canonical_reason().unwrap_or("")
                ));
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("Name not found")))
    }

    /// Get the deployment block number for a contract.
    pub async fn get_start_block(&self, network_id: &str, address: &str) -> Result<u64> {
        let urls = self.get_etherscan_urls(network_id)?;
        if urls.is_empty() {
            return Err(anyhow!(
                "No contract API available for {} in the registry",
                network_id
            ));
        }

        let mut last_error = None;

        for url in urls {
            let api_url = format!(
                "{}?module=contract&action=getcontractcreation&contractaddresses={}",
                url, address
            );

            let response = self
                .client
                .get(&api_url)
                .send()
                .await
                .context("Contract API is unreachable")?;

            if response.status().is_success() {
                match response.json::<ContractCreationResponse>().await {
                    Ok(data) => {
                        if data.status == "1" {
                            if let Some(results) = data.result {
                                if let Some(first) = results.first() {
                                    // Try direct block number first
                                    if let Some(block) = &first.block_number {
                                        if let Ok(num) = block.parse::<u64>() {
                                            return Ok(num);
                                        }
                                    }
                                    // Fall back to fetching transaction
                                    if let Some(tx_hash) = &first.tx_hash {
                                        if let Ok(block) =
                                            self.get_block_from_tx(network_id, tx_hash).await
                                        {
                                            return Ok(block);
                                        }
                                    }
                                }
                            }
                        }
                        last_error = Some(anyhow!("No contract creation info found"));
                    }
                    Err(e) => {
                        last_error = Some(anyhow!("Invalid JSON: {}", e));
                    }
                }
            } else {
                last_error = Some(anyhow!(
                    "{} {}",
                    response.status(),
                    response.status().canonical_reason().unwrap_or("")
                ));
            }
        }

        Err(last_error
            .unwrap_or_else(|| anyhow!("Failed to fetch contract deployment transaction")))
    }

    /// Fetch contract info from Sourcify.
    pub async fn get_from_sourcify(
        &self,
        network_id: &str,
        address: &str,
    ) -> Result<SourcifyResult> {
        let network = self
            .registry
            .get_network(network_id)
            .ok_or_else(|| anyhow!("Invalid network {}", network_id))?;

        // Sourcify only supports EVM chains
        if !network.caip2_id.starts_with("eip155") {
            return Err(anyhow!(
                "Invalid chainId, Sourcify API only supports EVM chains"
            ));
        }

        // Validate address
        if !address.starts_with("0x") || address.len() != 42 {
            return Err(anyhow!(
                "Invalid address, must start with 0x prefix and be 20 bytes long"
            ));
        }

        let chain_id = network
            .caip2_id
            .split(':')
            .nth(1)
            .ok_or_else(|| anyhow!("Invalid CAIP-2 ID"))?;

        let url = format!(
            "https://sourcify.dev/server/v2/contract/{}/{}?fields=abi,compilation,deployment",
            chain_id, address
        );

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Sourcify API is unreachable")?;

        if response.status().as_u16() == 404 {
            return Err(anyhow!("Contract is not verified on Sourcify"));
        }

        if !response.status().is_success() {
            return Err(anyhow!(
                "Sourcify API returned status {}",
                response.status()
            ));
        }

        let data: SourcifyResponse = response.json().await.context("Invalid Sourcify response")?;

        let abi = data.abi.ok_or_else(|| anyhow!("Contract ABI is missing"))?;

        let name = data
            .compilation
            .and_then(|c| c.name)
            .ok_or_else(|| anyhow!("Contract name is missing"))?;

        let start_block = data
            .deployment
            .and_then(|d| d.block_number)
            .and_then(|b| b.parse::<u64>().ok())
            .ok_or_else(|| anyhow!("Contract deployment block is missing"))?;

        Ok(SourcifyResult {
            abi,
            name,
            start_block,
        })
    }

    /// Get all contract info (ABI, name, start block), trying Sourcify first.
    pub async fn get_contract_info(&self, network_id: &str, address: &str) -> Result<ContractInfo> {
        // Try Sourcify first (more reliable for verified contracts)
        if let Ok(sourcify) = self.get_from_sourcify(network_id, address).await {
            return Ok(ContractInfo {
                abi: sourcify.abi,
                name: sourcify.name,
                start_block: Some(sourcify.start_block),
            });
        }

        // Fall back to Etherscan-compatible APIs
        let abi = self.get_abi(network_id, address).await?;
        let name = self.get_contract_name(network_id, address).await.ok();
        let start_block = self.get_start_block(network_id, address).await.ok();

        Ok(ContractInfo {
            abi,
            name: name.unwrap_or_else(|| "Contract".to_string()),
            start_block,
        })
    }

    /// Get Etherscan-compatible API URLs for a network.
    fn get_etherscan_urls(&self, network_id: &str) -> Result<Vec<String>> {
        let network = self
            .registry
            .get_network(network_id)
            .ok_or_else(|| anyhow!("Invalid network {}", network_id))?;

        let urls: Vec<String> = network
            .api_urls
            .iter()
            .filter(|url| url.kind == "etherscan" || url.kind == "blockscout")
            .map(|url| self.apply_env_vars(&url.url))
            .filter(|url| !url.is_empty())
            .collect();

        Ok(urls)
    }

    /// Get JSON-RPC URLs for a network.
    fn get_rpc_urls(&self, network_id: &str) -> Result<Vec<String>> {
        let network = self
            .registry
            .get_network(network_id)
            .ok_or_else(|| anyhow!("Invalid network {}", network_id))?;

        let urls: Vec<String> = network
            .rpc_urls
            .iter()
            .map(|url| self.apply_env_vars(url))
            .filter(|url| !url.is_empty())
            .collect();

        Ok(urls)
    }

    /// Replace {ENVVAR} placeholders with environment variable values.
    fn apply_env_vars(&self, url: &str) -> String {
        // Find {ENVVAR} pattern
        let re = regex::Regex::new(r"\{([^}]+)\}").unwrap();
        if let Some(caps) = re.captures(url) {
            let var_name = &caps[1];
            if let Ok(value) = env::var(var_name) {
                return url.replace(&format!("{{{}}}", var_name), &value);
            }
            // If env var not found, return empty string (skip this URL)
            return String::new();
        }
        url.to_string()
    }

    /// Fetch from an Etherscan-compatible API.
    async fn fetch_from_etherscan(&self, url: &str) -> Result<EtherscanResponse> {
        let response = self
            .client
            .get(url)
            .send()
            .await
            .context("Contract API is unreachable")?;

        if !response.status().is_success() {
            return Err(anyhow!(
                "{} {}",
                response.status(),
                response.status().canonical_reason().unwrap_or("")
            ));
        }

        response
            .json::<EtherscanResponse>()
            .await
            .context("Invalid JSON response")
    }

    /// Get block number from transaction hash via JSON-RPC.
    async fn get_block_from_tx(&self, network_id: &str, tx_hash: &str) -> Result<u64> {
        let urls = self.get_rpc_urls(network_id)?;
        if urls.is_empty() {
            return Err(anyhow!("No JSON-RPC available for {}", network_id));
        }

        let mut last_error = None;

        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_getTransactionByHash",
            "params": [tx_hash],
            "id": 1
        });

        for url in urls {
            match self
                .client
                .post(&url)
                .header("Content-Type", "application/json")
                .json(&body)
                .send()
                .await
            {
                Ok(response) => {
                    if response.status().is_success() {
                        match response.json::<RpcResponse>().await {
                            Ok(data) => {
                                if let Some(result) = data.result {
                                    if let Some(block_hex) = result.block_number {
                                        // Parse hex block number
                                        let block_str = block_hex.trim_start_matches("0x");
                                        if let Ok(block) = u64::from_str_radix(block_str, 16) {
                                            return Ok(block);
                                        }
                                    }
                                }
                                last_error = Some(anyhow!("No block number in transaction"));
                            }
                            Err(e) => {
                                last_error = Some(anyhow!("Invalid JSON: {}", e));
                            }
                        }
                    } else {
                        last_error = Some(anyhow!("RPC returned {}", response.status()));
                    }
                }
                Err(e) => {
                    last_error = Some(anyhow!("RPC request failed: {}", e));
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("Failed to fetch transaction {}", tx_hash)))
    }
}

/// Information about a verified contract.
#[derive(Debug)]
pub struct ContractInfo {
    /// The contract ABI as JSON
    pub abi: JsonValue,
    /// The contract name
    pub name: String,
    /// The deployment block number (if available)
    pub start_block: Option<u64>,
}

/// Well-known Etherscan API URLs for common networks (fallback if registry unavailable).
pub fn get_fallback_etherscan_url(network: &str) -> Option<&'static str> {
    // Note: These require API keys for high-volume usage
    static ETHERSCAN_URLS: &[(&str, &str)] = &[
        ("mainnet", "https://api.etherscan.io/api"),
        ("goerli", "https://api-goerli.etherscan.io/api"),
        ("sepolia", "https://api-sepolia.etherscan.io/api"),
        ("polygon", "https://api.polygonscan.com/api"),
        ("arbitrum-one", "https://api.arbiscan.io/api"),
        ("optimism", "https://api-optimistic.etherscan.io/api"),
        ("base", "https://api.basescan.org/api"),
        ("bnb", "https://api.bscscan.com/api"),
        ("avalanche", "https://api.snowtrace.io/api"),
        ("gnosis", "https://api.gnosisscan.io/api"),
    ];

    ETHERSCAN_URLS
        .iter()
        .find(|(n, _)| *n == network)
        .map(|(_, url)| *url)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_REGISTRY_JSON: &str = r#"{
        "networks": [
            {
                "id": "mainnet",
                "shortName": "Ethereum",
                "fullName": "Ethereum Mainnet",
                "aliases": ["ethereum", "eth"],
                "caip2Id": "eip155:1",
                "apiUrls": [
                    {"url": "https://api.etherscan.io/api", "kind": "etherscan"},
                    {"url": "https://eth.blockscout.com/api", "kind": "blockscout"}
                ],
                "rpcUrls": ["https://ethereum-rpc.publicnode.com"]
            },
            {
                "id": "goerli",
                "aliases": [],
                "caip2Id": "eip155:5",
                "apiUrls": [],
                "rpcUrls": []
            }
        ]
    }"#;

    #[test]
    fn test_parse_registry() {
        let registry = NetworksRegistry::from_json(TEST_REGISTRY_JSON).unwrap();
        assert_eq!(registry.networks.len(), 2);

        let mainnet = registry.get_network("mainnet").unwrap();
        assert_eq!(mainnet.id, "mainnet");
        assert_eq!(mainnet.caip2_id, "eip155:1");
        assert_eq!(mainnet.api_urls.len(), 2);
        assert_eq!(mainnet.rpc_urls.len(), 1);
    }

    #[test]
    fn test_get_network_by_alias() {
        let registry = NetworksRegistry::from_json(TEST_REGISTRY_JSON).unwrap();

        assert!(registry.get_network("eth").is_some());
        assert!(registry.get_network("ethereum").is_some());
        assert!(registry.get_network("MAINNET").is_some());
        assert!(registry.get_network("invalid").is_none());
    }

    #[test]
    fn test_apply_env_vars() {
        let registry = NetworksRegistry::from_json(TEST_REGISTRY_JSON).unwrap();
        let service = ContractService::new(registry);

        // URL without env var
        assert_eq!(
            service.apply_env_vars("https://api.etherscan.io/api"),
            "https://api.etherscan.io/api"
        );

        // URL with non-existent env var should return empty string
        let url_with_var = "https://api.etherscan.io/api?apikey={NONEXISTENT_VAR}";
        assert_eq!(service.apply_env_vars(url_with_var), "");
    }

    #[test]
    fn test_get_etherscan_urls() {
        let registry = NetworksRegistry::from_json(TEST_REGISTRY_JSON).unwrap();
        let service = ContractService::new(registry);

        let urls = service.get_etherscan_urls("mainnet").unwrap();
        assert_eq!(urls.len(), 2);
        assert!(urls.contains(&"https://api.etherscan.io/api".to_string()));

        // Network without API URLs
        let urls = service.get_etherscan_urls("goerli").unwrap();
        assert!(urls.is_empty());

        // Invalid network
        assert!(service.get_etherscan_urls("invalid").is_err());
    }

    #[test]
    fn test_fallback_etherscan_urls() {
        assert_eq!(
            get_fallback_etherscan_url("mainnet"),
            Some("https://api.etherscan.io/api")
        );
        assert_eq!(
            get_fallback_etherscan_url("polygon"),
            Some("https://api.polygonscan.com/api")
        );
        assert_eq!(get_fallback_etherscan_url("unknown"), None);
    }
}
