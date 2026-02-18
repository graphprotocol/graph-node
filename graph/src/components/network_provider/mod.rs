mod chain_identifier_validator;
mod extended_blocks_check;
mod genesis_hash_check;
mod network_details;
mod provider_check;
mod provider_manager;

pub use self::chain_identifier_validator::chain_id_validator;
pub use self::chain_identifier_validator::ChainIdentifierValidationError;
pub use self::chain_identifier_validator::ChainIdentifierValidator;
pub use self::extended_blocks_check::ExtendedBlocksCheck;
pub use self::genesis_hash_check::GenesisHashCheck;
pub use self::network_details::NetworkDetails;
pub use self::provider_check::ProviderCheck;
pub use self::provider_check::ProviderCheckStatus;
pub use self::provider_manager::ProviderCheckStrategy;
pub use self::provider_manager::ProviderManager;

use crate::http::Uri;
use std::collections::HashMap;
use std::sync::Arc;

// Used to increase memory efficiency.
// Currently, there is no need to create a separate type for this.
pub type ChainName = crate::data::value::Word;

// Used to increase memory efficiency.
// Currently, there is no need to create a separate type for this.
pub type ProviderName = crate::data::value::Word;

/// Resolved per-chain Amp configuration, with the address parsed as a `Uri`.
///
/// This struct is the *runtime* counterpart of the TOML-level `AmpConfig`
/// (which stores the address as a plain `String`). The `Config::amp_chain_configs()`
/// method bridges the two by parsing each address string into a `Uri`.
#[derive(Clone, Debug)]
pub struct AmpChainConfig {
    pub address: Uri,
    pub token: Option<String>,
    pub context_dataset: String,
    pub context_table: String,
    pub network: Option<String>,
}

/// Holds per-chain Amp Flight clients, keyed by chain name.
///
/// This wrapper is used to pass per-chain Amp clients through the system
/// instead of a single global `Option<Arc<AC>>`. Use `get(chain_name)` to
/// retrieve the client for a specific chain.
pub struct AmpClients<AC> {
    clients: HashMap<String, Arc<AC>>,
}

impl<AC> AmpClients<AC> {
    /// Creates a new `AmpClients` from a map of chain names to clients.
    pub fn new(clients: HashMap<String, Arc<AC>>) -> Self {
        Self { clients }
    }

    /// Returns the Amp client for the given chain, or `None` if no client
    /// is configured for that chain.
    pub fn get(&self, chain_name: &str) -> Option<Arc<AC>> {
        self.clients.get(chain_name).cloned()
    }

    /// Returns `true` if no Amp clients are configured.
    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }
}

// Manual Clone impl: only requires `Arc<AC>: Clone` (always true), not `AC: Clone`.
impl<AC> Clone for AmpClients<AC> {
    fn clone(&self) -> Self {
        Self {
            clients: self.clients.clone(),
        }
    }
}

impl<AC> std::fmt::Debug for AmpClients<AC> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmpClients")
            .field("chains", &self.clients.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl<AC> Default for AmpClients<AC> {
    fn default() -> Self {
        Self {
            clients: HashMap::new(),
        }
    }
}

/// Maps Amp network names to internal graph-node chain names.
///
/// Amp-powered subgraphs may use different network names than graph-node
/// (e.g., Amp uses `"ethereum-mainnet"` while graph-node uses `"mainnet"`).
/// This type provides a config-driven translation layer.
#[derive(Clone, Debug, Default)]
pub struct AmpChainNames(HashMap<ChainName, ChainName>);

impl AmpChainNames {
    pub fn new(mapping: HashMap<ChainName, ChainName>) -> Self {
        AmpChainNames(mapping)
    }

    /// Returns the internal chain name for an Amp alias, or the input
    /// unchanged if no alias matches.
    pub fn resolve(&self, name: &ChainName) -> ChainName {
        self.0.get(name).cloned().unwrap_or_else(|| name.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn amp_chain_names_resolve_known_alias() {
        let mut map = HashMap::new();
        map.insert(
            ChainName::from("ethereum-mainnet"),
            ChainName::from("mainnet"),
        );
        let names = AmpChainNames::new(map);
        assert_eq!(
            names.resolve(&ChainName::from("ethereum-mainnet")),
            ChainName::from("mainnet")
        );
    }

    #[test]
    fn amp_chain_names_resolve_unknown_passthrough() {
        let names = AmpChainNames::default();
        assert_eq!(
            names.resolve(&ChainName::from("mainnet")),
            ChainName::from("mainnet")
        );
    }

    #[test]
    fn amp_clients_returns_client_for_configured_chain() {
        let mut map = HashMap::new();
        map.insert("mainnet".to_string(), Arc::new(42u32));
        let clients = AmpClients::new(map);
        let client = clients.get("mainnet");
        assert!(client.is_some());
        assert_eq!(*client.unwrap(), 42);
    }

    #[test]
    fn amp_clients_returns_none_for_unconfigured_chain() {
        let map: HashMap<String, Arc<u32>> = HashMap::new();
        let clients = AmpClients::new(map);
        assert!(clients.get("mainnet").is_none());
    }

    /// Verifies the condition that causes Amp manager registration:
    /// `!amp_clients.is_empty()` is true when at least one chain has config.
    #[test]
    fn amp_manager_registered_when_chain_has_config() {
        let mut map = HashMap::new();
        map.insert("mainnet".to_string(), Arc::new(42u32));
        let clients = AmpClients::new(map);
        assert!(
            !clients.is_empty(),
            "Amp manager should be registered when at least one chain has config"
        );
    }

    /// Verifies the condition that skips Amp manager registration:
    /// `amp_clients.is_empty()` is true when no chains have config.
    #[test]
    fn amp_manager_not_registered_without_config() {
        let clients: AmpClients<u32> = AmpClients::new(HashMap::new());
        assert!(
            clients.is_empty(),
            "Amp manager should not be registered when no chains have config"
        );
    }

    /// Simulates the error path in downstream consumers: when a subgraph
    /// references a chain with no Amp client, the consumer should treat
    /// `get()` returning `None` as an error.
    #[test]
    fn amp_clients_error_for_unconfigured_amp_chain() {
        let mut map = HashMap::new();
        map.insert("mainnet".to_string(), Arc::new(1u32));
        let clients = AmpClients::new(map);

        // "matic" is not configured.
        let result = clients
            .get("matic")
            .ok_or_else(|| "Amp is not configured for chain 'matic'".to_string());
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Amp is not configured for chain 'matic'"
        );
    }
}
