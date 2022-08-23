use std::sync::Arc;

use anyhow::{anyhow, Error};
use graph::{
    blockchain,
    cheap_clone::CheapClone,
    components::link_resolver::LinkResolver,
    prelude::{async_trait, BlockNumber, DataSourceTemplateInfo, Link},
    slog::Logger,
};

use prost::Message;
use serde::Deserialize;

use crate::{chain::Chain, Block, TriggerData};

pub const SUBSTREAMS_KIND: &str = "substreams";

const DYNAMIC_DATA_SOURCE_ERROR: &str = "Substreams do not support dynamic data sources";
const TEMPLATE_ERROR: &str = "Substreams do not support templates";

const ALLOWED_MAPPING_KIND: [&'static str; 1] = ["substreams/graph-entities"];

#[derive(Clone, Debug, PartialEq)]
/// Represents the DataSource portion of the manifest once it has been parsed
/// and the substream spkg has been downloaded + parsed.
pub struct DataSource {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub(crate) source: Source,
    pub mapping: Mapping,
    pub context: Arc<Option<graph::prelude::DataSourceContext>>,
    pub initial_block: Option<BlockNumber>,
}

impl TryFrom<DataSourceTemplateInfo<Chain>> for DataSource {
    type Error = anyhow::Error;

    fn try_from(_value: DataSourceTemplateInfo<Chain>) -> Result<Self, Self::Error> {
        Err(anyhow!("Substreams does not support templates"))
    }
}

impl blockchain::DataSource<Chain> for DataSource {
    fn address(&self) -> Option<&[u8]> {
        None
    }

    fn start_block(&self) -> BlockNumber {
        self.initial_block.unwrap_or(0)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn kind(&self) -> &str {
        &self.kind
    }

    fn network(&self) -> Option<&str> {
        self.network.as_ref().map(|s| s.as_str())
    }

    fn context(&self) -> Arc<Option<graph::prelude::DataSourceContext>> {
        self.context.cheap_clone()
    }

    fn creation_block(&self) -> Option<BlockNumber> {
        None
    }

    fn api_version(&self) -> semver::Version {
        self.mapping.api_version.clone()
    }

    // runtime is not needed for substreams, it will cause the host creation to be skipped.
    fn runtime(&self) -> Option<Arc<Vec<u8>>> {
        None
    }

    // match_and_decode only seems to be used on the default trigger processor which substreams
    // bypasses so it should be fine to leave it unimplemented.
    fn match_and_decode(
        &self,
        _trigger: &TriggerData,
        _block: &Arc<Block>,
        _logger: &Logger,
    ) -> Result<Option<blockchain::TriggerWithHandler<Chain>>, Error> {
        unimplemented!()
    }

    fn is_duplicate_of(&self, _other: &Self) -> bool {
        todo!()
    }

    fn as_stored_dynamic_data_source(&self) -> graph::components::store::StoredDynamicDataSource {
        unimplemented!("{}", DYNAMIC_DATA_SOURCE_ERROR)
    }

    fn validate(&self) -> Vec<Error> {
        let mut errs = vec![];

        if &self.kind != SUBSTREAMS_KIND {
            errs.push(anyhow!(
                "data source has invalid `kind`, expected {} but found {}",
                SUBSTREAMS_KIND,
                self.kind
            ))
        }

        if self.name.is_empty() {
            errs.push(anyhow!("name cannot be empty"));
        }

        if !ALLOWED_MAPPING_KIND.contains(&self.mapping.kind.as_str()) {
            errs.push(anyhow!(
                "mapping kind has to be one of {:?}, found {}",
                ALLOWED_MAPPING_KIND,
                self.mapping.kind
            ))
        }

        errs
    }

    fn from_stored_dynamic_data_source(
        _template: &<Chain as blockchain::Blockchain>::DataSourceTemplate,
        _stored: graph::components::store::StoredDynamicDataSource,
    ) -> Result<Self, Error> {
        Err(anyhow!(DYNAMIC_DATA_SOURCE_ERROR))
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
/// Module name comes from the manifest, package is the parsed spkg file.
pub struct Source {
    pub module_name: String,
    pub package: graph::substreams::Package,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mapping {
    pub api_version: semver::Version,
    pub kind: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
/// Raw representation of the data source for deserialization purposes.
pub struct UnresolvedDataSource {
    pub kind: String,
    pub network: Option<String>,
    pub name: String,
    pub(crate) source: UnresolvedSource,
    pub mapping: UnresolvedMapping,
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
/// Text api_version, before parsing and validation.
pub struct UnresolvedMapping {
    pub api_version: String,
    pub kind: String,
}

#[async_trait]
impl blockchain::UnresolvedDataSource<Chain> for UnresolvedDataSource {
    async fn resolve(
        self,
        resolver: &Arc<dyn LinkResolver>,
        logger: &Logger,
        _manifest_idx: u32,
    ) -> Result<DataSource, Error> {
        let content = resolver.cat(logger, &self.source.package.file).await?;

        let package = graph::substreams::Package::decode(content.as_ref())?;

        let initial_block: Option<u64> = match package.modules {
            Some(ref modules) => modules.modules.iter().map(|x| x.initial_block).min(),
            None => None,
        };

        let initial_block: Option<i32> = initial_block
            .map_or(Ok(None), |x: u64| TryInto::<i32>::try_into(x).map(Some))
            .map_err(anyhow::Error::from)?;

        Ok(DataSource {
            kind: SUBSTREAMS_KIND.into(),
            network: self.network,
            name: self.name,
            source: Source {
                module_name: self.source.package.module_name,
                package,
            },
            mapping: Mapping {
                api_version: semver::Version::parse(&self.mapping.api_version)?,
                kind: self.mapping.kind,
            },
            context: Arc::new(None),
            initial_block,
        })
    }
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
/// Source is a part of the manifest and this is needed for parsing.
pub struct UnresolvedSource {
    package: UnresolvedPackage,
}

#[derive(Clone, Debug, Default, Hash, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
/// The unresolved Package section of the manifest.
pub struct UnresolvedPackage {
    pub module_name: String,
    pub file: Link,
}

#[derive(Debug, Clone, Default, Deserialize)]
/// This is necessary for the Blockchain trait associated types, substreams do not support
/// data source templates so this is a noop and is not expected to be called.
pub struct NoopDataSourceTemplate {}

impl blockchain::DataSourceTemplate<Chain> for NoopDataSourceTemplate {
    fn name(&self) -> &str {
        unimplemented!("{}", TEMPLATE_ERROR);
    }

    fn api_version(&self) -> semver::Version {
        unimplemented!("{}", TEMPLATE_ERROR);
    }

    fn runtime(&self) -> Option<Arc<Vec<u8>>> {
        unimplemented!("{}", TEMPLATE_ERROR);
    }

    fn manifest_idx(&self) -> u32 {
        todo!()
    }
}

#[async_trait]
impl blockchain::UnresolvedDataSourceTemplate<Chain> for NoopDataSourceTemplate {
    async fn resolve(
        self,
        _resolver: &Arc<dyn LinkResolver>,
        _logger: &Logger,
        _manifest_idx: u32,
    ) -> Result<NoopDataSourceTemplate, anyhow::Error> {
        unimplemented!("{}", TEMPLATE_ERROR)
    }
}

#[cfg(test)]
mod test {
    use std::{str::FromStr, sync::Arc};

    use anyhow::Error;
    use graph::{
        blockchain::{DataSource as _, UnresolvedDataSource as _},
        components::link_resolver::LinkResolver,
        prelude::{async_trait, serde_yaml, JsonValueStream, Link},
        slog::{o, Discard, Logger},
    };

    use crate::{DataSource, Mapping, UnresolvedDataSource, UnresolvedMapping, SUBSTREAMS_KIND};

    const EMPTY_PACKAGE: graph::substreams::Package = graph::substreams::Package {
        proto_files: vec![],
        version: 0,
        modules: None,
        module_meta: vec![],
        package_meta: vec![],
    };

    #[test]
    fn parse_data_source() {
        let ds: UnresolvedDataSource = serde_yaml::from_str(TEMPLATE_DATA_SOURCE).unwrap();
        let expected = UnresolvedDataSource {
            kind: SUBSTREAMS_KIND.into(),
            network: Some("mainnet".into()),
            name: "Uniswap".into(),
            source: crate::UnresolvedSource {
                package: crate::UnresolvedPackage {
                    module_name: "output".into(),
                    file: Link {
                        link: "/ipfs/QmbHnhUFZa6qqqRyubUYhXntox1TCBxqryaBM1iNGqVJzT".into(),
                    },
                },
            },
            mapping: UnresolvedMapping {
                api_version: "0.0.7".into(),
                kind: "substreams/graph-entities".into(),
            },
        };
        assert_eq!(ds, expected);
    }

    #[tokio::test]
    async fn data_source_conversion() {
        let ds: UnresolvedDataSource = serde_yaml::from_str(TEMPLATE_DATA_SOURCE).unwrap();
        let link_resolver: Arc<dyn LinkResolver> = Arc::new(NoopLinkResolver {});
        let logger = Logger::root(Discard, o!());
        let ds: DataSource = ds.resolve(&link_resolver, &logger, 0).await.unwrap();
        let expected = DataSource {
            kind: SUBSTREAMS_KIND.into(),
            network: Some("mainnet".into()),
            name: "Uniswap".into(),
            source: crate::Source {
                module_name: "output".into(),
                package: EMPTY_PACKAGE,
            },
            mapping: Mapping {
                api_version: semver::Version::from_str("0.0.7").unwrap(),
                kind: "substreams/graph-entities".into(),
            },
            context: Arc::new(None),
            initial_block: None,
        };
        assert_eq!(ds, expected);
    }

    #[test]
    fn data_source_validation() {
        let mut ds = gen_data_source();
        assert_eq!(true, ds.validate().is_empty());

        ds.network = None;
        assert_eq!(true, ds.validate().is_empty());

        ds.kind = "asdasd".into();
        ds.name = "".into();
        ds.mapping.kind = "asdasd".into();
        let errs: Vec<String> = ds.validate().into_iter().map(|e| e.to_string()).collect();
        assert_eq!(
            errs,
            vec![
                "data source has invalid `kind`, expected substreams but found asdasd",
                "name cannot be empty",
                "mapping kind has to be one of [\"substreams/graph-entities\"], found asdasd"
            ]
        );
    }

    fn gen_data_source() -> DataSource {
        DataSource {
            kind: SUBSTREAMS_KIND.into(),
            network: Some("mainnet".into()),
            name: "Uniswap".into(),
            source: crate::Source {
                module_name: "".to_string(),
                package: EMPTY_PACKAGE,
            },
            mapping: Mapping {
                api_version: semver::Version::from_str("0.0.7").unwrap(),
                kind: "substreams/graph-entities".into(),
            },
            context: Arc::new(None),
            initial_block: None,
        }
    }

    const TEMPLATE_DATA_SOURCE: &str = r#"
        kind: substreams
        name: Uniswap
        network: mainnet
        source:
          package:
            moduleName: output
            file:
              /: /ipfs/QmbHnhUFZa6qqqRyubUYhXntox1TCBxqryaBM1iNGqVJzT
              # This IPFs path would be generated from a local path at deploy time
        mapping:
          kind: substreams/graph-entities
          apiVersion: 0.0.7
    "#;

    #[derive(Debug)]
    struct NoopLinkResolver {}

    #[async_trait]
    impl LinkResolver for NoopLinkResolver {
        fn with_timeout(&self, _timeout: std::time::Duration) -> Box<dyn LinkResolver> {
            unimplemented!()
        }

        fn with_retries(&self) -> Box<dyn LinkResolver> {
            unimplemented!()
        }

        async fn cat(&self, _logger: &Logger, _link: &Link) -> Result<Vec<u8>, Error> {
            Ok(vec![])
        }

        async fn get_block(&self, _logger: &Logger, _link: &Link) -> Result<Vec<u8>, Error> {
            unimplemented!()
        }

        async fn json_stream(
            &self,
            _logger: &Logger,
            _link: &Link,
        ) -> Result<JsonValueStream, Error> {
            unimplemented!()
        }
    }
}
