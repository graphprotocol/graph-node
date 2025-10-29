pub mod causality_region;
pub mod common;
pub mod offchain;
pub mod subgraph;

use crate::data::subgraph::DeploymentHash;

pub use self::DataSource as DataSourceEnum;
pub use causality_region::CausalityRegion;

#[cfg(test)]
mod tests;

use crate::{
    blockchain::{
        Block, BlockPtr, BlockTime, Blockchain, DataSource as _, DataSourceTemplate as _,
        MappingTriggerTrait, TriggerData as _, UnresolvedDataSource as _,
        UnresolvedDataSourceTemplate as _,
    },
    components::{
        link_resolver::LinkResolver,
        store::{BlockNumber, StoredDynamicDataSource},
    },
    data_source::{offchain::OFFCHAIN_KINDS, subgraph::SUBGRAPH_DS_KIND},
    prelude::{CheapClone as _, DataSourceContext},
    schema::{EntityType, InputSchema},
};
use anyhow::{anyhow, Context, Error};
use semver::Version;
use serde::{de::IntoDeserializer as _, Deserialize, Deserializer};
use slog::{Logger, SendSyncRefUnwindSafeKV};
use std::{
    collections::{BTreeMap, HashSet},
    fmt,
    sync::Arc,
};
use thiserror::Error;

use crate::amp;

#[derive(Debug)]
pub enum DataSource<C: Blockchain> {
    Onchain(C::DataSource),
    Offchain(offchain::DataSource),
    Subgraph(subgraph::DataSource),
    Amp(amp::manifest::DataSource),
}

#[derive(Error, Debug)]
pub enum DataSourceCreationError {
    /// The creation of the data source should be ignored.
    #[error("ignoring data source creation due to invalid parameter: '{0}', error: {1:#}")]
    Ignore(String, Error),

    /// Other errors.
    #[error("error creating data source: {0:#}")]
    Unknown(#[from] Error),
}

/// Which entity types a data source can read and write to.
///
/// Currently this is only enforced on offchain data sources and templates, based on the `entities`
/// key in the manifest. This informs which entity tables need an explicit `causality_region` column
/// and which will always have `causality_region == 0`.
///
/// Note that this is just an optimization and not sufficient for causality region isolation, since
/// generally the causality region is a property of the entity, not of the entity type.
///
/// See also: entity-type-access
pub enum EntityTypeAccess {
    Any,
    Restriced(Vec<EntityType>),
}

impl fmt::Display for EntityTypeAccess {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match self {
            Self::Any => write!(f, "Any"),
            Self::Restriced(entities) => {
                let strings = entities.iter().map(|e| e.typename()).collect::<Vec<_>>();
                write!(f, "{}", strings.join(", "))
            }
        }
    }
}

impl EntityTypeAccess {
    pub fn allows(&self, entity_type: &EntityType) -> bool {
        match self {
            Self::Any => true,
            Self::Restriced(types) => types.contains(entity_type),
        }
    }
}

impl<C: Blockchain> DataSource<C> {
    pub fn as_onchain(&self) -> Option<&C::DataSource> {
        match self {
            Self::Onchain(ds) => Some(ds),
            Self::Offchain(_) => None,
            Self::Subgraph(_) => None,
            Self::Amp(_) => None,
        }
    }

    pub fn as_subgraph(&self) -> Option<&subgraph::DataSource> {
        match self {
            Self::Onchain(_) => None,
            Self::Offchain(_) => None,
            Self::Subgraph(ds) => Some(ds),
            Self::Amp(_) => None,
        }
    }

    pub fn is_chain_based(&self) -> bool {
        match self {
            Self::Onchain(_) => true,
            Self::Offchain(_) => false,
            Self::Subgraph(_) => true,
            Self::Amp(_) => true,
        }
    }

    pub fn as_offchain(&self) -> Option<&offchain::DataSource> {
        match self {
            Self::Onchain(_) => None,
            Self::Offchain(ds) => Some(ds),
            Self::Subgraph(_) => None,
            Self::Amp(_) => None,
        }
    }

    pub fn network(&self) -> Option<&str> {
        match self {
            DataSourceEnum::Onchain(ds) => ds.network(),
            DataSourceEnum::Offchain(_) => None,
            DataSourceEnum::Subgraph(ds) => ds.network(),
            Self::Amp(ds) => Some(&ds.network),
        }
    }

    pub fn start_block(&self) -> Option<BlockNumber> {
        match self {
            DataSourceEnum::Onchain(ds) => Some(ds.start_block()),
            DataSourceEnum::Offchain(_) => None,
            DataSourceEnum::Subgraph(ds) => Some(ds.source.start_block),
            Self::Amp(ds) => Some(ds.source.start_block as i32),
        }
    }

    pub fn is_onchain(&self) -> bool {
        self.as_onchain().is_some()
    }

    pub fn is_offchain(&self) -> bool {
        self.as_offchain().is_some()
    }

    pub fn address(&self) -> Option<Vec<u8>> {
        match self {
            Self::Onchain(ds) => ds.address().map(ToOwned::to_owned),
            Self::Offchain(ds) => ds.address(),
            Self::Subgraph(ds) => ds.address(),
            Self::Amp(ds) => Some(ds.source.address.to_vec()),
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Onchain(ds) => ds.name(),
            Self::Offchain(ds) => &ds.name,
            Self::Subgraph(ds) => &ds.name,
            Self::Amp(ds) => ds.name.as_str(),
        }
    }

    pub fn kind(&self) -> String {
        match self {
            Self::Onchain(ds) => ds.kind().to_owned(),
            Self::Offchain(ds) => ds.kind.to_string(),
            Self::Subgraph(ds) => ds.kind.clone(),
            Self::Amp(_) => amp::manifest::DataSource::KIND.to_string(),
        }
    }

    pub fn min_spec_version(&self) -> Version {
        match self {
            Self::Onchain(ds) => ds.min_spec_version(),
            Self::Offchain(ds) => ds.min_spec_version(),
            Self::Subgraph(ds) => ds.min_spec_version(),
            Self::Amp(_) => amp::manifest::DataSource::MIN_SPEC_VERSION,
        }
    }

    pub fn end_block(&self) -> Option<BlockNumber> {
        match self {
            Self::Onchain(ds) => ds.end_block(),
            Self::Offchain(_) => None,
            Self::Subgraph(_) => None,
            Self::Amp(ds) => Some(ds.source.end_block as i32),
        }
    }

    pub fn creation_block(&self) -> Option<BlockNumber> {
        match self {
            Self::Onchain(ds) => ds.creation_block(),
            Self::Offchain(ds) => ds.creation_block,
            Self::Subgraph(ds) => ds.creation_block,
            Self::Amp(_) => None,
        }
    }

    pub fn context(&self) -> Arc<Option<DataSourceContext>> {
        match self {
            Self::Onchain(ds) => ds.context(),
            Self::Offchain(ds) => ds.context.clone(),
            Self::Subgraph(ds) => ds.context.clone(),
            Self::Amp(_) => Arc::new(None),
        }
    }

    pub fn api_version(&self) -> Version {
        match self {
            Self::Onchain(ds) => ds.api_version(),
            Self::Offchain(ds) => ds.mapping.api_version.clone(),
            Self::Subgraph(ds) => ds.mapping.api_version.clone(),
            Self::Amp(ds) => ds.transformer.api_version.clone(),
        }
    }

    pub fn runtime(&self) -> Option<Arc<Vec<u8>>> {
        match self {
            Self::Onchain(ds) => ds.runtime(),
            Self::Offchain(ds) => Some(ds.mapping.runtime.cheap_clone()),
            Self::Subgraph(ds) => Some(ds.mapping.runtime.cheap_clone()),
            Self::Amp(_) => None,
        }
    }

    pub fn entities(&self) -> EntityTypeAccess {
        match self {
            // Note: Onchain data sources have an `entities` field in the manifest, but it has never
            // been enforced.
            Self::Onchain(_) => EntityTypeAccess::Any,
            Self::Offchain(ds) => EntityTypeAccess::Restriced(ds.mapping.entities.clone()),
            Self::Subgraph(_) => EntityTypeAccess::Any,
            Self::Amp(_) => EntityTypeAccess::Any,
        }
    }

    pub fn handler_kinds(&self) -> HashSet<&str> {
        match self {
            Self::Onchain(ds) => ds.handler_kinds(),
            Self::Offchain(ds) => vec![ds.handler_kind()].into_iter().collect(),
            Self::Subgraph(ds) => vec![ds.handler_kind()].into_iter().collect(),
            Self::Amp(_) => HashSet::new(),
        }
    }

    pub fn has_declared_calls(&self) -> bool {
        match self {
            Self::Onchain(ds) => ds.has_declared_calls(),
            Self::Offchain(_) => false,
            Self::Subgraph(_) => false,
            Self::Amp(_) => false,
        }
    }

    pub fn match_and_decode(
        &self,
        trigger: &TriggerData<C>,
        block: &Arc<C::Block>,
        logger: &Logger,
    ) -> Result<Option<TriggerWithHandler<MappingTrigger<C>>>, Error> {
        match (self, trigger) {
            (Self::Onchain(ds), _) if ds.has_expired(block.number()) => Ok(None),
            (Self::Onchain(ds), TriggerData::Onchain(trigger)) => ds
                .match_and_decode(trigger, block, logger)
                .map(|t| t.map(|t| t.map(MappingTrigger::Onchain))),
            (Self::Offchain(ds), TriggerData::Offchain(trigger)) => {
                Ok(ds.match_and_decode(trigger))
            }
            (Self::Subgraph(ds), TriggerData::Subgraph(trigger)) => {
                ds.match_and_decode(block, trigger)
            }
            (Self::Onchain(_), TriggerData::Offchain(_))
            | (Self::Offchain(_), TriggerData::Onchain(_))
            | (Self::Onchain(_), TriggerData::Subgraph(_))
            | (Self::Offchain(_), TriggerData::Subgraph(_))
            | (Self::Subgraph(_), TriggerData::Onchain(_))
            | (Self::Subgraph(_), TriggerData::Offchain(_)) => Ok(None),
            (Self::Amp(_), _) => Ok(None),
        }
    }

    pub fn is_duplicate_of(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Onchain(a), Self::Onchain(b)) => a.is_duplicate_of(b),
            (Self::Offchain(a), Self::Offchain(b)) => a.is_duplicate_of(b),
            _ => false,
        }
    }

    pub fn as_stored_dynamic_data_source(&self) -> StoredDynamicDataSource {
        match self {
            Self::Onchain(ds) => ds.as_stored_dynamic_data_source(),
            Self::Offchain(ds) => ds.as_stored_dynamic_data_source(),
            Self::Subgraph(_) => todo!(), // TODO(krishna)
            Self::Amp(_) => unreachable!(),
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
            DataSourceTemplate::Subgraph(_) => todo!(), // TODO(krishna)
        }
    }

    pub fn validate(&self, spec_version: &semver::Version) -> Vec<Error> {
        match self {
            Self::Onchain(ds) => ds.validate(spec_version),
            Self::Offchain(_) => vec![],
            Self::Subgraph(_) => vec![], // TODO(krishna)
            Self::Amp(_) => Vec::new(),
        }
    }

    pub fn causality_region(&self) -> CausalityRegion {
        match self {
            Self::Onchain(_) => CausalityRegion::ONCHAIN,
            Self::Offchain(ds) => ds.causality_region,
            Self::Subgraph(_) => CausalityRegion::ONCHAIN,
            Self::Amp(_) => CausalityRegion::ONCHAIN,
        }
    }
}

#[derive(Debug)]
pub enum UnresolvedDataSource<C: Blockchain> {
    Onchain(C::UnresolvedDataSource),
    Offchain(offchain::UnresolvedDataSource),
    Subgraph(subgraph::UnresolvedDataSource),
    Amp(amp::manifest::data_source::RawDataSource),
}

impl<C: Blockchain> UnresolvedDataSource<C> {
    pub async fn resolve<AC: amp::Client>(
        self,
        deployment_hash: &DeploymentHash,
        resolver: &Arc<dyn LinkResolver>,
        amp_client: Option<Arc<AC>>,
        logger: &Logger,
        manifest_idx: u32,
        spec_version: &semver::Version,
    ) -> Result<DataSource<C>, anyhow::Error> {
        match self {
            Self::Onchain(unresolved) => unresolved
                .resolve(
                    deployment_hash,
                    resolver,
                    logger,
                    manifest_idx,
                    spec_version,
                )
                .await
                .map(DataSource::Onchain),
            Self::Subgraph(unresolved) => unresolved
                .resolve::<C, AC>(
                    deployment_hash,
                    resolver,
                    amp_client,
                    logger,
                    manifest_idx,
                    spec_version,
                )
                .await
                .map(DataSource::Subgraph),
            Self::Offchain(_unresolved) => {
                anyhow::bail!(
                    "static file data sources are not yet supported, \\
                     for details see https://github.com/graphprotocol/graph-node/issues/3864"
                );
            }
            Self::Amp(raw_data_source) => match amp_client {
                Some(amp_client) => raw_data_source
                    .resolve(logger, resolver.as_ref(), amp_client.as_ref())
                    .await
                    .map(DataSource::Amp)
                    .map_err(Error::from),
                None => Err(anyhow!("support for Amp data sources is not enabled")),
            },
        }
        .with_context(|| format!("failed to resolve data source at index {manifest_idx}"))
    }
}

#[derive(Debug, Clone)]
pub struct DataSourceTemplateInfo {
    pub api_version: semver::Version,
    pub runtime: Option<Arc<Vec<u8>>>,
    pub name: String,
    pub manifest_idx: Option<u32>,
    pub kind: String,
}

#[derive(Debug)]
pub enum DataSourceTemplate<C: Blockchain> {
    Onchain(C::DataSourceTemplate),
    Offchain(offchain::DataSourceTemplate),
    Subgraph(subgraph::DataSourceTemplate),
}

impl<C: Blockchain> DataSourceTemplate<C> {
    pub fn info(&self) -> DataSourceTemplateInfo {
        match self {
            DataSourceTemplate::Onchain(template) => template.info(),
            DataSourceTemplate::Offchain(template) => template.clone().into(),
            DataSourceTemplate::Subgraph(template) => template.clone().into(),
        }
    }

    pub fn as_onchain(&self) -> Option<&C::DataSourceTemplate> {
        match self {
            Self::Onchain(ds) => Some(ds),
            Self::Offchain(_) => None,
            Self::Subgraph(_) => todo!(), // TODO(krishna)
        }
    }

    pub fn as_offchain(&self) -> Option<&offchain::DataSourceTemplate> {
        match self {
            Self::Onchain(_) => None,
            Self::Offchain(t) => Some(t),
            Self::Subgraph(_) => todo!(), // TODO(krishna)
        }
    }

    pub fn into_onchain(self) -> Option<C::DataSourceTemplate> {
        match self {
            Self::Onchain(ds) => Some(ds),
            Self::Offchain(_) => None,
            Self::Subgraph(_) => todo!(), // TODO(krishna)
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Onchain(ds) => &ds.name(),
            Self::Offchain(ds) => &ds.name,
            Self::Subgraph(ds) => &ds.name,
        }
    }

    pub fn api_version(&self) -> semver::Version {
        match self {
            Self::Onchain(ds) => ds.api_version(),
            Self::Offchain(ds) => ds.mapping.api_version.clone(),
            Self::Subgraph(ds) => ds.mapping.api_version.clone(),
        }
    }

    pub fn runtime(&self) -> Option<Arc<Vec<u8>>> {
        match self {
            Self::Onchain(ds) => ds.runtime(),
            Self::Offchain(ds) => Some(ds.mapping.runtime.clone()),
            Self::Subgraph(ds) => Some(ds.mapping.runtime.clone()),
        }
    }

    pub fn manifest_idx(&self) -> u32 {
        match self {
            Self::Onchain(ds) => ds.manifest_idx(),
            Self::Offchain(ds) => ds.manifest_idx,
            Self::Subgraph(ds) => ds.manifest_idx,
        }
    }

    pub fn kind(&self) -> String {
        match self {
            Self::Onchain(ds) => ds.kind().to_string(),
            Self::Offchain(ds) => ds.kind.to_string(),
            Self::Subgraph(ds) => ds.kind.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum UnresolvedDataSourceTemplate<C: Blockchain> {
    Onchain(C::UnresolvedDataSourceTemplate),
    Offchain(offchain::UnresolvedDataSourceTemplate),
    Subgraph(subgraph::UnresolvedDataSourceTemplate),
}

impl<C: Blockchain> Default for UnresolvedDataSourceTemplate<C> {
    fn default() -> Self {
        Self::Onchain(C::UnresolvedDataSourceTemplate::default())
    }
}

impl<C: Blockchain> UnresolvedDataSourceTemplate<C> {
    pub async fn resolve(
        self,
        deployment_hash: &DeploymentHash,
        resolver: &Arc<dyn LinkResolver>,
        schema: &InputSchema,
        logger: &Logger,
        manifest_idx: u32,
        spec_version: &semver::Version,
    ) -> Result<DataSourceTemplate<C>, Error> {
        match self {
            Self::Onchain(ds) => ds
                .resolve(
                    deployment_hash,
                    resolver,
                    logger,
                    manifest_idx,
                    spec_version,
                )
                .await
                .map(|ti| DataSourceTemplate::Onchain(ti)),
            Self::Offchain(ds) => ds
                .resolve(deployment_hash, resolver, logger, manifest_idx, schema)
                .await
                .map(DataSourceTemplate::Offchain),
            Self::Subgraph(ds) => ds
                .resolve(
                    deployment_hash,
                    resolver,
                    logger,
                    manifest_idx,
                    spec_version,
                )
                .await
                .map(DataSourceTemplate::Subgraph),
        }
    }
}

pub struct TriggerWithHandler<T> {
    pub trigger: T,
    handler: String,
    block_ptr: BlockPtr,
    timestamp: BlockTime,
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
    pub fn new(trigger: T, handler: String, block_ptr: BlockPtr, timestamp: BlockTime) -> Self {
        Self::new_with_logging_extras(
            trigger,
            handler,
            block_ptr,
            timestamp,
            Arc::new(slog::o! {}),
        )
    }

    pub fn new_with_logging_extras(
        trigger: T,
        handler: String,
        block_ptr: BlockPtr,
        timestamp: BlockTime,
        logging_extras: Arc<dyn SendSyncRefUnwindSafeKV>,
    ) -> Self {
        TriggerWithHandler {
            trigger,
            handler,
            block_ptr,
            timestamp,
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
            timestamp: self.timestamp,
            logging_extras: self.logging_extras,
        }
    }

    pub fn block_ptr(&self) -> BlockPtr {
        self.block_ptr.clone()
    }

    pub fn timestamp(&self) -> BlockTime {
        self.timestamp
    }
}

#[derive(Debug)]
pub enum TriggerData<C: Blockchain> {
    Onchain(C::TriggerData),
    Offchain(offchain::TriggerData),
    Subgraph(subgraph::TriggerData),
}

impl<C: Blockchain> TriggerData<C> {
    pub fn error_context(&self) -> String {
        match self {
            Self::Onchain(trigger) => trigger.error_context(),
            Self::Offchain(trigger) => format!("{:?}", trigger.source),
            Self::Subgraph(trigger) => format!("{:?}", trigger.source),
        }
    }
}

#[derive(Debug)]
pub enum MappingTrigger<C: Blockchain> {
    Onchain(C::MappingTrigger),
    Offchain(offchain::TriggerData),
    Subgraph(subgraph::MappingEntityTrigger),
}

impl<C: Blockchain> MappingTrigger<C> {
    pub fn error_context(&self) -> Option<String> {
        match self {
            Self::Onchain(trigger) => Some(trigger.error_context()),
            Self::Offchain(_) => None, // TODO: Add error context for offchain triggers
            Self::Subgraph(_) => None, // TODO(krishna)
        }
    }

    pub fn as_onchain(&self) -> Option<&C::MappingTrigger> {
        match self {
            Self::Onchain(trigger) => Some(trigger),
            Self::Offchain(_) => None,
            Self::Subgraph(_) => None, // TODO(krishna)
        }
    }
}

impl<C: Blockchain> Clone for DataSource<C> {
    fn clone(&self) -> Self {
        match self {
            Self::Onchain(ds) => Self::Onchain(ds.clone()),
            Self::Offchain(ds) => Self::Offchain(ds.clone()),
            Self::Subgraph(ds) => Self::Subgraph(ds.clone()),
            Self::Amp(ds) => Self::Amp(ds.clone()),
        }
    }
}

impl<C: Blockchain> Clone for DataSourceTemplate<C> {
    fn clone(&self) -> Self {
        match self {
            Self::Onchain(ds) => Self::Onchain(ds.clone()),
            Self::Offchain(ds) => Self::Offchain(ds.clone()),
            Self::Subgraph(ds) => Self::Subgraph(ds.clone()),
        }
    }
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

        if OFFCHAIN_KINDS.contains_key(&kind) {
            offchain::UnresolvedDataSource::deserialize(map.into_deserializer())
                .map_err(serde::de::Error::custom)
                .map(UnresolvedDataSource::Offchain)
        } else if SUBGRAPH_DS_KIND == kind {
            subgraph::UnresolvedDataSource::deserialize(map.into_deserializer())
                .map_err(serde::de::Error::custom)
                .map(UnresolvedDataSource::Subgraph)
        } else if amp::manifest::DataSource::KIND == kind {
            amp::manifest::data_source::RawDataSource::deserialize(map.into_deserializer())
                .map(UnresolvedDataSource::Amp)
                .map_err(serde::de::Error::custom)
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

impl<'de, C: Blockchain> Deserialize<'de> for UnresolvedDataSourceTemplate<C> {
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

        if OFFCHAIN_KINDS.contains_key(&kind) {
            offchain::UnresolvedDataSourceTemplate::deserialize(map.into_deserializer())
                .map_err(serde::de::Error::custom)
                .map(UnresolvedDataSourceTemplate::Offchain)
        } else if SUBGRAPH_DS_KIND == kind {
            subgraph::UnresolvedDataSourceTemplate::deserialize(map.into_deserializer())
                .map_err(serde::de::Error::custom)
                .map(UnresolvedDataSourceTemplate::Subgraph)
        } else if (&C::KIND.to_string() == kind) || C::ALIASES.contains(&kind) {
            C::UnresolvedDataSourceTemplate::deserialize(map.into_deserializer())
                .map_err(serde::de::Error::custom)
                .map(UnresolvedDataSourceTemplate::Onchain)
        } else {
            Err(serde::de::Error::custom(format!(
                "data source has invalid `kind`; expected {}, file/ipfs",
                C::KIND,
            )))
        }
    }
}
