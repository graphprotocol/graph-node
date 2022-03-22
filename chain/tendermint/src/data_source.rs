use std::collections::{BTreeMap, HashMap, HashSet};
use std::{convert::TryFrom, sync::Arc};

use anyhow::{Error, Result};

use graph::{
    blockchain::{self, Block, Blockchain, TriggerWithHandler},
    components::store::StoredDynamicDataSource,
    data::subgraph::DataSourceContext,
    prelude::{
        anyhow, async_trait, info, BlockNumber, CheapClone, DataSourceTemplateInfo, Deserialize,
        Link, LinkResolver, Logger,
    },
};

use crate::chain::Chain;
use crate::codec;
use crate::trigger::TendermintTrigger;

pub const TENDERMINT_KIND: &str = "tendermint";

const DYNAMIC_DATA_SOURCE_ERROR: &str = "Tendermint subgraphs do not support dynamic data sources";
const TEMPLATE_ERROR: &str = "Tendermint subgraphs do not support templates";

/// Runtime representation of a data source.
// Note: Not great for memory usage that this needs to be `Clone`, considering how there may be tens
// of thousands of data sources in memory at once.
#[derive(Clone, Debug)]
pub struct DataSource {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub source: Source,
    pub mapping: Mapping,
    pub context: Arc<Option<DataSourceContext>>,
    pub creation_block: Option<BlockNumber>,
}

impl blockchain::DataSource<Chain> for DataSource {
    fn address(&self) -> Option<&[u8]> {
        None
    }

    fn start_block(&self) -> BlockNumber {
        self.source.start_block
    }

    fn match_and_decode(
        &self,
        trigger: &<Chain as Blockchain>::TriggerData,
        block: &Arc<<Chain as Blockchain>::Block>,
        _logger: &Logger,
    ) -> Result<Option<TriggerWithHandler<Chain>>> {
        if self.source.start_block > block.number() {
            return Ok(None);
        }

        let handler = match trigger {
            TendermintTrigger::Block(_) => match self.handler_for_block() {
                Some(handler) => handler.handler,
                None => return Ok(None),
            },

            TendermintTrigger::Event { event_data, origin } => {
                match self.handler_for_event(event_data.event(), *origin) {
                    Some(handler) => handler.handler,
                    None => return Ok(None),
                }
            }
        };

        Ok(Some(TriggerWithHandler::new(
            trigger.cheap_clone(),
            handler,
        )))
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> &str {
        &self.kind
    }

    fn network(&self) -> Option<&str> {
        self.network.as_deref()
    }

    fn context(&self) -> Arc<Option<DataSourceContext>> {
        self.context.cheap_clone()
    }

    fn creation_block(&self) -> Option<BlockNumber> {
        self.creation_block
    }

    fn is_duplicate_of(&self, other: &Self) -> bool {
        let DataSource {
            kind,
            network,
            name,
            source,
            mapping,
            context,

            // The creation block is ignored for detection duplicate data sources.
            // Contract ABI equality is implicit in `source` and `mapping.abis` equality.
            creation_block: _,
        } = self;

        // mapping_request_sender, host_metrics, and (most of) host_exports are operational structs
        // used at runtime but not needed to define uniqueness; each runtime host should be for a
        // unique data source.
        kind == &other.kind
            && network == &other.network
            && name == &other.name
            && source == &other.source
            && mapping.block_handlers == other.mapping.block_handlers
            && mapping.event_handlers == other.mapping.event_handlers
            && context == &other.context
    }

    fn as_stored_dynamic_data_source(&self) -> StoredDynamicDataSource {
        unimplemented!("{}", DYNAMIC_DATA_SOURCE_ERROR);
    }

    fn from_stored_dynamic_data_source(
        _templates: &BTreeMap<&str, &DataSourceTemplate>,
        _stored: StoredDynamicDataSource,
    ) -> Result<Self> {
        Err(anyhow!(DYNAMIC_DATA_SOURCE_ERROR))
    }

    fn validate(&self) -> Vec<Error> {
        let mut errors = Vec::new();

        if self.kind != TENDERMINT_KIND {
            errors.push(anyhow!(
                "data source has invalid `kind`, expected {} but found {}",
                TENDERMINT_KIND,
                self.kind
            ))
        }

        // Ensure there is only one block handler
        if self.mapping.block_handlers.len() > 1 {
            errors.push(anyhow!("data source has duplicated block handlers"));
        }

        // Ensure that each event type + origin filter combination has only one handler

        // group handler origin filters by event type
        let mut event_types = HashMap::with_capacity(self.mapping.event_handlers.len());
        for event_handler in self.mapping.event_handlers.iter() {
            let origins = event_types
                .entry(&event_handler.event)
                // 3 is the maximum number of valid handlers for an event type (1 for each origin)
                .or_insert(HashSet::with_capacity(3));

            // insert returns false if value was already in the set
            if !origins.insert(event_handler.origin) {
                errors.push(multiple_origin_err(
                    &event_handler.event,
                    event_handler.origin,
                ))
            }
        }

        // Ensure each event type either has:
        // 1 handler with no origin filter
        // OR
        // 1 or more handlers with origin filter
        for (event_type, origins) in event_types.iter() {
            if origins.len() > 1 {
                if !origins.iter().all(Option::is_some) {
                    errors.push(combined_origins_err(event_type))
                }
            }
        }

        errors
    }

    fn api_version(&self) -> semver::Version {
        self.mapping.api_version.clone()
    }

    fn runtime(&self) -> &[u8] {
        self.mapping.runtime.as_ref()
    }
}

impl DataSource {
    fn from_manifest(
        kind: String,
        network: Option<String>,
        name: String,
        source: Source,
        mapping: Mapping,
        context: Option<DataSourceContext>,
    ) -> Result<Self> {
        // Data sources in the manifest are created "before genesis" so they have no creation block.
        let creation_block = None;

        Ok(DataSource {
            kind,
            network,
            name,
            source,
            mapping,
            context: Arc::new(context),
            creation_block,
        })
    }

    fn handler_for_block(&self) -> Option<MappingBlockHandler> {
        self.mapping.block_handlers.first().cloned()
    }

    fn handler_for_event(
        &self,
        event: &codec::Event,
        event_origin: EventOrigin,
    ) -> Option<MappingEventHandler> {
        self.mapping
            .event_handlers
            .iter()
            .find(|handler| {
                let event_type_matches = event.event_type == handler.event;

                if let Some(handler_origin) = handler.origin {
                    event_type_matches && event_origin == handler_origin
                } else {
                    event_type_matches
                }
            })
            .cloned()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
pub struct UnresolvedDataSource {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub source: Source,
    pub mapping: UnresolvedMapping,
    pub context: Option<DataSourceContext>,
}

#[async_trait]
impl blockchain::UnresolvedDataSource<Chain> for UnresolvedDataSource {
    async fn resolve(self, resolver: &impl LinkResolver, logger: &Logger) -> Result<DataSource> {
        let UnresolvedDataSource {
            kind,
            network,
            name,
            source,
            mapping,
            context,
        } = self;

        info!(logger, "Resolve data source"; "name" => &name, "source" => &source.start_block);

        let mapping = mapping.resolve(&*resolver, logger).await?;

        DataSource::from_manifest(kind, network, name, source, mapping, context)
    }
}

impl TryFrom<DataSourceTemplateInfo<Chain>> for DataSource {
    type Error = Error;

    fn try_from(_info: DataSourceTemplateInfo<Chain>) -> Result<Self> {
        Err(anyhow!(TEMPLATE_ERROR))
    }
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
pub struct BaseDataSourceTemplate<M> {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub mapping: M,
}

pub type UnresolvedDataSourceTemplate = BaseDataSourceTemplate<UnresolvedMapping>;
pub type DataSourceTemplate = BaseDataSourceTemplate<Mapping>;

#[async_trait]
impl blockchain::UnresolvedDataSourceTemplate<Chain> for UnresolvedDataSourceTemplate {
    async fn resolve(
        self,
        _resolver: &impl LinkResolver,
        _logger: &Logger,
    ) -> Result<DataSourceTemplate> {
        Err(anyhow!(TEMPLATE_ERROR))
    }
}

impl blockchain::DataSourceTemplate<Chain> for DataSourceTemplate {
    fn name(&self) -> &str {
        unimplemented!("{}", TEMPLATE_ERROR);
    }

    fn api_version(&self) -> semver::Version {
        unimplemented!("{}", TEMPLATE_ERROR);
    }

    fn runtime(&self) -> &[u8] {
        unimplemented!("{}", TEMPLATE_ERROR);
    }
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnresolvedMapping {
    pub api_version: String,
    pub language: String,
    pub entities: Vec<String>,
    #[serde(default)]
    pub block_handlers: Vec<MappingBlockHandler>,
    #[serde(default)]
    pub event_handlers: Vec<MappingEventHandler>,
    pub file: Link,
}

impl UnresolvedMapping {
    pub async fn resolve(self, resolver: &impl LinkResolver, logger: &Logger) -> Result<Mapping> {
        let UnresolvedMapping {
            api_version,
            language,
            entities,
            block_handlers,
            event_handlers,
            file: link,
        } = self;

        let api_version = semver::Version::parse(&api_version)?;

        info!(logger, "Resolve mapping"; "link" => &link.link);
        let module_bytes = resolver.cat(logger, &link).await?;

        Ok(Mapping {
            api_version,
            language,
            entities,
            block_handlers: block_handlers.clone(),
            event_handlers: event_handlers.clone(),
            runtime: Arc::new(module_bytes),
            link,
        })
    }
}

#[derive(Clone, Debug)]
pub struct Mapping {
    pub api_version: semver::Version,
    pub language: String,
    pub entities: Vec<String>,
    pub block_handlers: Vec<MappingBlockHandler>,
    pub event_handlers: Vec<MappingEventHandler>,
    pub runtime: Arc<Vec<u8>>,
    pub link: Link,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct MappingBlockHandler {
    pub handler: String,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct MappingEventHandler {
    pub event: String,
    pub origin: Option<EventOrigin>,
    pub handler: String,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq, Deserialize)]
pub struct Source {
    #[serde(rename = "startBlock", default)]
    pub start_block: BlockNumber,
}

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Deserialize)]
pub enum EventOrigin {
    BeginBlock,
    DeliverTx,
    EndBlock,
}

fn multiple_origin_err(event_type: &str, origin: Option<EventOrigin>) -> Error {
    let origin_err_name = match origin {
        Some(origin) => format!("{:?}", origin),
        None => "no".to_string(),
    };

    anyhow!(
        "data source has multiple {} event handlers with {} origin",
        event_type,
        origin_err_name,
    )
}

fn combined_origins_err(event_type: &str) -> Error {
    anyhow!(
        "data source has combined origin and no-origin {} event handlers",
        event_type
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use graph::blockchain::DataSource as _;

    #[test]
    fn test_event_handlers_origin_validation() {
        let cases = [
            (
                DataSource::with_event_handlers(vec![
                    MappingEventHandler::with_origin("event_1", None),
                    MappingEventHandler::with_origin("event_2", None),
                    MappingEventHandler::with_origin("event_3", None),
                ]),
                vec![],
            ),
            (
                DataSource::with_event_handlers(vec![
                    MappingEventHandler::with_origin("event_1", Some(EventOrigin::BeginBlock)),
                    MappingEventHandler::with_origin("event_2", Some(EventOrigin::BeginBlock)),
                    MappingEventHandler::with_origin("event_1", Some(EventOrigin::DeliverTx)),
                    MappingEventHandler::with_origin("event_1", Some(EventOrigin::EndBlock)),
                    MappingEventHandler::with_origin("event_2", Some(EventOrigin::DeliverTx)),
                    MappingEventHandler::with_origin("event_2", Some(EventOrigin::EndBlock)),
                ]),
                vec![],
            ),
            (
                DataSource::with_event_handlers(vec![
                    MappingEventHandler::with_origin("event_1", None),
                    MappingEventHandler::with_origin("event_1", None),
                    MappingEventHandler::with_origin("event_2", None),
                    MappingEventHandler::with_origin("event_2", Some(EventOrigin::BeginBlock)),
                    MappingEventHandler::with_origin("event_3", Some(EventOrigin::EndBlock)),
                    MappingEventHandler::with_origin("event_3", Some(EventOrigin::EndBlock)),
                ]),
                vec![
                    multiple_origin_err("event_1", None),
                    combined_origins_err("event_2"),
                    multiple_origin_err("event_3", Some(EventOrigin::EndBlock)),
                ],
            ),
        ];

        for (data_source, errors) in &cases {
            let validation_errors = data_source.validate();

            assert_eq!(errors.len(), validation_errors.len());

            for error in errors.iter() {
                assert!(
                    validation_errors
                        .iter()
                        .any(|validation_error| validation_error.to_string() == error.to_string()),
                    r#"expected "{}" to be in validation errors, but it wasn't"#,
                    error
                );
            }
        }
    }

    impl DataSource {
        fn with_event_handlers(event_handlers: Vec<MappingEventHandler>) -> DataSource {
            DataSource {
                kind: "tendermint".to_string(),
                network: None,
                name: "Test".to_string(),
                source: Source { start_block: 1 },
                mapping: Mapping {
                    api_version: semver::Version::new(0, 0, 0),
                    language: "".to_string(),
                    entities: vec![],
                    block_handlers: vec![],
                    event_handlers,
                    runtime: Arc::new(vec![]),
                    link: "test".to_string().into(),
                },
                context: Arc::new(None),
                creation_block: None,
            }
        }
    }

    impl MappingEventHandler {
        fn with_origin(event_type: &str, origin: Option<EventOrigin>) -> MappingEventHandler {
            MappingEventHandler {
                event: event_type.to_string(),
                origin,
                handler: "handler".to_string(),
            }
        }
    }
}
