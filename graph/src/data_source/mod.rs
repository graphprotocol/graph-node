pub mod offchain;

use crate::{
    blockchain::{
        BlockPtr, Blockchain, DataSource as _, DataSourceTemplate as _, TriggerData as _,
        UnresolvedDataSource as _, UnresolvedDataSourceTemplate as _,
    },
    components::{
        link_resolver::LinkResolver,
        store::{BlockNumber, StoredDynamicDataSource},
        subgraph::DataSourceTemplateInfo,
    },
    data_source::offchain::OFFCHAIN_KINDS,
    prelude::{CheapClone as _, DataSourceContext},
};
use anyhow::Error;
use semver::Version;
use serde::{de::IntoDeserializer as _, Deserialize, Deserializer};
use slog::{Logger, SendSyncRefUnwindSafeKV};
use std::{collections::BTreeMap, fmt, sync::Arc};

#[derive(Debug)]
pub enum DataSource<C: Blockchain> {
    Onchain(C::DataSource),
    Offchain(offchain::DataSource),
}

impl<C: Blockchain> TryFrom<DataSourceTemplateInfo<C>> for DataSource<C> {
    type Error = Error;

    fn try_from(info: DataSourceTemplateInfo<C>) -> Result<Self, Self::Error> {
        match &info.template {
            DataSourceTemplate::Onchain(_) => {
                C::DataSource::try_from(info).map(DataSource::Onchain)
            }
            DataSourceTemplate::Offchain(_) => {
                offchain::DataSource::try_from(info).map(DataSource::Offchain)
            }
        }
    }
}

impl<C: Blockchain> DataSource<C> {
    pub fn as_onchain(&self) -> Option<&C::DataSource> {
        match self {
            Self::Onchain(ds) => Some(&ds),
            Self::Offchain(_) => None,
        }
    }

    pub fn as_offchain(&self) -> Option<&offchain::DataSource> {
        match self {
            Self::Onchain(_) => None,
            Self::Offchain(ds) => Some(&ds),
        }
    }

    pub fn address(&self) -> Option<Vec<u8>> {
        match self {
            Self::Onchain(ds) => ds.address().map(ToOwned::to_owned),
            Self::Offchain(ds) => ds.address(),
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

    pub fn match_and_decode(
        &self,
        trigger: &TriggerData<C>,
        block: &Arc<C::Block>,
        logger: &Logger,
    ) -> Result<Option<TriggerWithHandler<MappingTrigger<C>>>, Error> {
        match (self, trigger) {
            (Self::Onchain(ds), TriggerData::Onchain(trigger)) => ds
                .match_and_decode(trigger, block, logger)
                .map(|t| t.map(|t| t.map(MappingTrigger::Onchain))),
            (Self::Offchain(ds), TriggerData::Offchain(trigger)) => {
                Ok(ds.match_and_decode(trigger))
            }
            (Self::Onchain(_), TriggerData::Offchain(_))
            | (Self::Offchain(_), TriggerData::Onchain(_)) => Ok(None),
        }
    }

    pub fn is_duplicate_of(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Onchain(a), Self::Onchain(b)) => a.is_duplicate_of(b),
            (Self::Offchain(a), Self::Offchain(b)) => {
                // See also: data-source-is-duplicate-of
                a.manifest_idx == b.manifest_idx && a.source == b.source && a.context == b.context
            }
            _ => false,
        }
    }

    pub fn as_stored_dynamic_data_source(&self) -> StoredDynamicDataSource {
        match self {
            Self::Onchain(ds) => ds.as_stored_dynamic_data_source(),
            Self::Offchain(ds) => ds.as_stored_dynamic_data_source(),
        }
    }

    pub fn from_stored_dynamic_data_source(
        template: &DataSourceTemplate<C>,
        stored: StoredDynamicDataSource,
    ) -> Result<Self, Error> {
        match template {
            DataSourceTemplate::Onchain(template) => {
                C::DataSource::from_stored_dynamic_data_source(template, stored)
                    .map(DataSource::Onchain)
            }
            DataSourceTemplate::Offchain(template) => {
                offchain::DataSource::from_stored_dynamic_data_source(template, stored)
                    .map(DataSource::Offchain)
            }
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

impl<C: Blockchain> UnresolvedDataSource<C> {
    pub async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
        manifest_idx: u32,
    ) -> Result<DataSource<C>, anyhow::Error> {
        match self {
            Self::Onchain(unresolved) => unresolved
                .resolve(resolver, logger, manifest_idx)
                .await
                .map(DataSource::Onchain),
            Self::Offchain(unresolved) => unresolved
                .resolve(resolver, logger, manifest_idx)
                .await
                .map(DataSource::Offchain),
        }
    }
}

#[derive(Debug)]
pub enum DataSourceTemplate<C: Blockchain> {
    Onchain(C::DataSourceTemplate),
    Offchain(offchain::DataSourceTemplate),
}

impl<C: Blockchain> DataSourceTemplate<C> {
    pub fn as_onchain(&self) -> Option<&C::DataSourceTemplate> {
        match self {
            Self::Onchain(ds) => Some(ds),
            Self::Offchain(_) => None,
        }
    }

    pub fn into_onchain(self) -> Option<C::DataSourceTemplate> {
        match self {
            Self::Onchain(ds) => Some(ds),
            Self::Offchain(_) => None,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Onchain(ds) => ds.name(),
            Self::Offchain(ds) => &ds.name,
        }
    }

    pub fn api_version(&self) -> semver::Version {
        match self {
            Self::Onchain(ds) => ds.api_version(),
            Self::Offchain(ds) => ds.mapping.api_version.clone(),
        }
    }

    pub fn runtime(&self) -> Option<Arc<Vec<u8>>> {
        match self {
            Self::Onchain(ds) => ds.runtime(),
            Self::Offchain(ds) => Some(ds.mapping.runtime.clone()),
        }
    }

    pub fn manifest_idx(&self) -> u32 {
        match self {
            Self::Onchain(ds) => ds.manifest_idx(),
            Self::Offchain(ds) => ds.manifest_idx,
        }
    }
}

#[derive(Clone, Debug)]
pub enum UnresolvedDataSourceTemplate<C: Blockchain> {
    Onchain(C::UnresolvedDataSourceTemplate),
    Offchain(offchain::UnresolvedDataSourceTemplate),
}

impl<C: Blockchain> Default for UnresolvedDataSourceTemplate<C> {
    fn default() -> Self {
        Self::Onchain(C::UnresolvedDataSourceTemplate::default())
    }
}

impl<C: Blockchain> UnresolvedDataSourceTemplate<C> {
    pub async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
        manifest_idx: u32,
    ) -> Result<DataSourceTemplate<C>, Error> {
        match self {
            Self::Onchain(ds) => ds
                .resolve(resolver, logger, manifest_idx)
                .await
                .map(DataSourceTemplate::Onchain),
            Self::Offchain(ds) => ds
                .resolve(resolver, logger, manifest_idx)
                .await
                .map(DataSourceTemplate::Offchain),
        }
    }
}

pub struct TriggerWithHandler<T> {
    pub trigger: T,
    handler: String,
    block_ptr: BlockPtr,
    logging_extras: Arc<dyn SendSyncRefUnwindSafeKV>,
}

impl<T: fmt::Debug> fmt::Debug for TriggerWithHandler<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("TriggerWithHandler");
        builder.field("trigger", &self.trigger);
        builder.field("handler", &self.handler);
        builder.finish()
    }
}

impl<T> TriggerWithHandler<T> {
    pub fn new(trigger: T, handler: String, block_ptr: BlockPtr) -> Self {
        Self {
            trigger,
            handler,
            block_ptr,
            logging_extras: Arc::new(slog::o! {}),
        }
    }

    pub fn new_with_logging_extras(
        trigger: T,
        handler: String,
        block_ptr: BlockPtr,
        logging_extras: Arc<dyn SendSyncRefUnwindSafeKV>,
    ) -> Self {
        TriggerWithHandler {
            trigger,
            handler,
            block_ptr,
            logging_extras,
        }
    }

    /// Additional key-value pairs to be logged with the "Done processing trigger" message.
    pub fn logging_extras(&self) -> Arc<dyn SendSyncRefUnwindSafeKV> {
        self.logging_extras.cheap_clone()
    }

    pub fn handler_name(&self) -> &str {
        &self.handler
    }

    fn map<T_>(self, f: impl FnOnce(T) -> T_) -> TriggerWithHandler<T_> {
        TriggerWithHandler {
            trigger: f(self.trigger),
            handler: self.handler,
            block_ptr: self.block_ptr,
            logging_extras: self.logging_extras,
        }
    }

    pub fn block_ptr(&self) -> BlockPtr {
        self.block_ptr.clone()
    }
}

pub enum TriggerData<C: Blockchain> {
    Onchain(C::TriggerData),
    Offchain(offchain::TriggerData),
}

impl<C: Blockchain> TriggerData<C> {
    pub fn error_context(&self) -> String {
        match self {
            Self::Onchain(trigger) => trigger.error_context(),
            Self::Offchain(trigger) => format!("{:?}", trigger.source),
        }
    }
}

#[derive(Debug)]
pub enum MappingTrigger<C: Blockchain> {
    Onchain(C::MappingTrigger),
    Offchain(offchain::TriggerData),
}

macro_rules! clone_data_source {
    ($t:ident) => {
        impl<C: Blockchain> Clone for $t<C> {
            fn clone(&self) -> Self {
                match self {
                    Self::Onchain(ds) => Self::Onchain(ds.clone()),
                    Self::Offchain(ds) => Self::Offchain(ds.clone()),
                }
            }
        }
    };
}

clone_data_source!(DataSource);
clone_data_source!(DataSourceTemplate);

macro_rules! deserialize_data_source {
    ($t:ident) => {
        impl<'de, C: Blockchain> Deserialize<'de> for $t<C> {
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
                    offchain::$t::deserialize(map.into_deserializer())
                        .map_err(serde::de::Error::custom)
                        .map($t::Offchain)
                } else if (&C::KIND.to_string() == kind) || C::ALIASES.contains(&kind) {
                    C::$t::deserialize(map.into_deserializer())
                        .map_err(serde::de::Error::custom)
                        .map($t::Onchain)
                } else {
                    Err(serde::de::Error::custom(format!(
                        "data source has invalid `kind`; expected {}, file/ipfs",
                        C::KIND,
                    )))
                }
            }
        }
    };
}

deserialize_data_source!(UnresolvedDataSource);
deserialize_data_source!(UnresolvedDataSourceTemplate);
