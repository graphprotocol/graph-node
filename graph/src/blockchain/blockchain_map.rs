use anyhow::{anyhow, Context, Error};
use std::{
    any::{Any, TypeId},
    borrow::Cow,
    collections::HashMap,
    sync::Arc,
};

use super::{aliases::NetworkAliases, Blockchain};
use crate::prelude::CheapClone;

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
