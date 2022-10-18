use std::collections::HashMap;

type NetworkId = u32;

const BUILT_IN_ALIASES: &[&[&str]] = &[
    &["mainnet", "ethereum", "eip155:1"],
    &["xdai", "gnosis", "eip155:100"],
];

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
    pub fn built_ins() -> Self {
        let mut aliases = Self::default();
        for equivalence_group in BUILT_IN_ALIASES {
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
            let network_id = *self
                .network_ids
                .get(alias_of)
                .ok_or_else(|| AliasingError::AliasDoesNotExist(alias_of.to_string()))?;
            self.network_ids.insert(network.to_string(), network_id);
            self.aliases
                .entry(network_id)
                .or_default()
                .push(network.to_string());
        } else {
            self.network_ids
                .insert(network.to_string(), self.next_network_id);
            self.aliases
                .insert(self.next_network_id, vec![network.to_string()]);
            self.next_network_id += 1;
        }

        Ok(())
    }

    /// Returns the first ever registered alias of `chain`.
    pub fn original_alias_of(&self, chain: &str) -> Result<&str, AliasingError> {
        let network_id = self
            .network_ids
            .get(chain)
            .ok_or_else(|| AliasingError::AliasDoesNotExist(chain.to_string()))?;
        Ok(self.aliases[network_id][0].as_str())
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn built_ins() {
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
