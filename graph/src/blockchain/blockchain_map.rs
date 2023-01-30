use anyhow::{anyhow, Context, Error};
use std::{
    any::{Any, TypeId},
    borrow::Cow,
    collections::HashMap,
    sync::Arc,
};

use super::Blockchain;
use crate::prelude::CheapClone;

type NetworkId = u32;

/// A collection of blockchains, keyed by [`BlockchainKind`] and chain name.
#[derive(Default, Debug, Clone)]
pub struct BlockchainMap {
    // Using `Cow` instead of `String` allows for retrievals using `&str`.
    chains: HashMap<(TypeId, Cow<'static, str>), Arc<dyn Any + Send + Sync>>,
    aliases: NetworkAliases,
}

impl BlockchainMap {
    pub fn new(aliases: NetworkAliases) -> Self {
        Self {
            chains: HashMap::new(),
            aliases,
        }
    }

    pub fn insert<C>(&mut self, chain_name: impl ToString, chain: Arc<C>)
    where
        C: Send + Sync + 'static,
    {
        self.chains.insert(
            (TypeId::of::<C>(), Cow::Owned(chain_name.to_string())),
            chain,
        );
    }

    pub fn get<C: Blockchain>(&self, chain_name: impl AsRef<str>) -> Result<Arc<C>, Error> {
        let unaliased = self.aliases.original_alias_of(chain_name.as_ref())?;

        self.chains
            .get(&(TypeId::of::<C>(), Cow::Borrowed(unaliased)))
            .with_context(|| {
                format!(
                    "no network {} found on chain {}",
                    chain_name.as_ref(),
                    C::KIND
                )
            })?
            .cheap_clone()
            .downcast()
            .map_err(|_| anyhow!("unable to downcast, wrong type for blockchain {}", C::KIND))
    }
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum AliasingError {
    #[error("Chain name {0} does not exist")]
    AliasDoesNotExist(String),
}

/// Defines classes of equivalence across network names.
#[derive(Debug, Clone, Default)]
pub struct NetworkAliases {
    aliases: HashMap<NetworkId, Vec<String>>,
    network_ids: HashMap<String, NetworkId>,
    next_network_id: NetworkId,
}

impl NetworkAliases {
    const BUILT_IN: &'static [&'static [&'static str]] = &[
        &["mainnet", "ethereum", "eip155:1"],
        &["xdai", "gnosis", "eip155:100"],
    ];

    pub fn built_ins() -> Self {
        let mut aliases = Self::default();
        for equivalence_group in Self::BUILT_IN.iter() {
            let alias_of = equivalence_group[0];
            aliases.insert(alias_of, None).unwrap();
            for alias in equivalence_group[1..].iter() {
                aliases.insert(alias, Some(alias_of)).unwrap();
            }
        }
        aliases
    }

    pub fn insert(
        &mut self,
        network: impl ToString,
        alias_of: Option<&str>,
    ) -> Result<(), AliasingError> {
        if let Some(alias_of) = alias_of {
            let chain_id = self
                .network_ids
                .get(alias_of)
                .ok_or_else(|| AliasingError::AliasDoesNotExist(alias_of.to_string()))?;
            self.aliases
                .entry(*chain_id)
                .or_default()
                .push(network.to_string());
        } else {
            self.network_ids
                .insert(network.to_string(), self.next_network_id);
            self.aliases.insert(self.next_network_id, vec![]);
            self.next_network_id += 1;
        }

        Ok(())
    }

    /// Returns the first ever registered alias of `chain`.
    pub fn original_alias_of(&self, chain: &str) -> Result<&str, AliasingError> {
        let chain_id = self
            .network_ids
            .get(chain)
            .ok_or_else(|| AliasingError::AliasDoesNotExist(chain.to_string()))?;
        Ok(self.aliases[chain_id][0].as_str())
    }

    /// Checks whether `network1` and `network2` refer to the same chain.
    pub fn is_alias_of(&self, network1: &str, network2: &str) -> Result<bool, AliasingError> {
        let network1_id = *self
            .network_ids
            .get(network1)
            .ok_or_else(|| AliasingError::AliasDoesNotExist(network1.to_owned()))?;
        let network2_id = *self
            .network_ids
            .get(network2)
            .ok_or_else(|| AliasingError::AliasDoesNotExist(network2.to_owned()))?;
        Ok(network1_id == network2_id)
    }

    /// Idempotent removal.
    pub fn remove(&mut self, network: &str) {
        let network_id = *self.network_ids.get(network).unwrap_or(&0);
        self.aliases.remove(&network_id);
        self.network_ids.remove(network);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn built_ins_aliases() {
        let aliases = NetworkAliases::built_ins();
        assert!(aliases.is_alias_of("xdai", "gnosis").unwrap());
        assert!(aliases.is_alias_of("gnosis", "xdai").unwrap());
        assert!(aliases.is_alias_of("xdai", "eip155:100").unwrap());
        assert!(aliases.is_alias_of("gnosis", "eip155:100").unwrap());
        assert!(aliases.is_alias_of("eip155:100", "eip155:100").unwrap());
        assert!(!aliases.is_alias_of("eip155:100", "eip155:1").unwrap());
    }

    #[test]
    fn original_alias() {
        let aliases = NetworkAliases::built_ins();
        assert_eq!(aliases.original_alias_of("mainnet").unwrap(), "mainnet");
        assert_eq!(aliases.original_alias_of("ethereum").unwrap(), "mainnet");
        assert_eq!(aliases.original_alias_of("eip155:1").unwrap(), "mainnet");
    }
}
