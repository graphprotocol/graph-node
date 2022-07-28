use crate::{
    blockchain::{Blockchain, DataSource as _, UnresolvedDataSource as _},
    components::{link_resolver::LinkResolver, store::BlockNumber},
    offchain::{self, OFFCHAIN_KINDS},
    prelude::{CheapClone as _, DataSourceContext},
};
use anyhow::Error;
use semver::Version;
use serde::{de::IntoDeserializer as _, Deserialize, Deserializer};
use slog::Logger;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug)]
pub enum DataSource<C: Blockchain> {
    Onchain(C::DataSource),
    Offchain(offchain::DataSource),
}

impl<C: Blockchain> DataSource<C> {
    pub fn as_onchain(&self) -> Option<&C::DataSource> {
        match self {
            Self::Onchain(ds) => Some(&ds),
            Self::Offchain(_) => None,
        }
    }

    pub fn address(&self) -> Option<&[u8]> {
        match self {
            Self::Onchain(ds) => ds.address(),
            Self::Offchain(_) => None,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Onchain(ds) => ds.name(),
            Self::Offchain(ds) => &ds.name,
        }
    }

    pub fn kind(&self) -> &str {
        match self {
            Self::Onchain(ds) => ds.kind(),
            Self::Offchain(ds) => &ds.kind,
        }
    }

    pub fn creation_block(&self) -> Option<BlockNumber> {
        match self {
            Self::Onchain(ds) => ds.creation_block(),
            Self::Offchain(ds) => ds.creation_block,
        }
    }

    pub fn context(&self) -> Arc<Option<DataSourceContext>> {
        match self {
            Self::Onchain(ds) => ds.context(),
            Self::Offchain(ds) => ds.context.clone(),
        }
    }

    pub fn api_version(&self) -> Version {
        match self {
            Self::Onchain(ds) => ds.api_version(),
            Self::Offchain(ds) => ds.mapping.api_version.clone(),
        }
    }

    pub fn runtime(&self) -> Option<Arc<Vec<u8>>> {
        match self {
            Self::Onchain(ds) => ds.runtime(),
            Self::Offchain(ds) => Some(ds.mapping.runtime.cheap_clone()),
        }
    }

    pub fn is_duplicate_of(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Onchain(a), Self::Onchain(b)) => a.is_duplicate_of(b),
            (Self::Offchain(a), Self::Offchain(b)) => {
                a.kind == b.kind && a.name == b.name && a.source == b.source
            }
            _ => false,
        }
    }

    pub fn validate(&self) -> Vec<Error> {
        match self {
            Self::Onchain(ds) => ds.validate(),
            Self::Offchain(_) => vec![],
        }
    }
}

#[derive(Debug)]
pub enum UnresolvedDataSource<C: Blockchain> {
    Onchain(C::UnresolvedDataSource),
    Offchain(offchain::UnresolvedDataSource),
}

impl<'de, C: Blockchain> Deserialize<'de> for UnresolvedDataSource<C> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let map: BTreeMap<String, serde_json::Value> = BTreeMap::deserialize(deserializer)?;
        let kind = map
            .get("kind")
            .ok_or(serde::de::Error::missing_field("kind"))?
            .as_str()
            .unwrap_or("?");
        if OFFCHAIN_KINDS.contains(&kind) {
            offchain::UnresolvedDataSource::deserialize(map.into_deserializer())
                .map_err(serde::de::Error::custom)
                .map(UnresolvedDataSource::Offchain)
        } else if (&C::KIND.to_string() == kind) || C::ALIASES.contains(&kind) {
            C::UnresolvedDataSource::deserialize(map.into_deserializer())
                .map_err(serde::de::Error::custom)
                .map(UnresolvedDataSource::Onchain)
        } else {
            Err(serde::de::Error::custom(format!(
                "data source has invalid `kind`; expected {}, file/ipfs",
                C::KIND,
            )))
        }
    }
}

impl<C: Blockchain> UnresolvedDataSource<C> {
    pub async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
    ) -> Result<DataSource<C>, anyhow::Error> {
        match self {
            Self::Onchain(unresolved) => unresolved
                .resolve(resolver, logger)
                .await
                .map(DataSource::Onchain),
            Self::Offchain(unresolved) => unresolved
                .resolve(resolver, logger)
                .await
                .map(DataSource::Offchain),
        }
    }
}
