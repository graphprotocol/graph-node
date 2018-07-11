use futures::prelude::*;
use serde_yaml;
use thegraph::components::data_sources::{DataSourceDefinitionLoader as LoaderTrait,
                                         DataSourceDefinitionLoaderError};
use thegraph::components::link_resolver::LinkResolver;
use thegraph::data::data_sources::*;

#[derive(Default)]
pub struct DataSourceDefinitionLoader;

impl LoaderTrait for DataSourceDefinitionLoader {
    /// Right now the only supported links are of the form:
    /// `/ipfs/QmUmg7BZC1YP1ca66rRtWKxpXp77WgVHrnv263JtDuvs2k`
    fn load_from_ipfs<'a>(
        &self,
        ipfs_link: &str,
        ipfs_client: &'a impl LinkResolver,
    ) -> Box<Future<Item = DataSourceDefinition, Error = DataSourceDefinitionLoaderError> + 'a>
    {
        let ipfs_link = ipfs_link.to_owned();
        Box::new(
            ipfs_client
                .cat(&Link { link: ipfs_link.clone() })
                .map_err(|e| DataSourceDefinitionLoaderError::ResolveError(e))
                .and_then(move |file_bytes| {
                    let file = String::from_utf8(file_bytes.to_vec())
                        .map_err(|_| DataSourceDefinitionLoaderError::NonUtf8)?;
                    let mut raw: serde_yaml::Value = serde_yaml::from_str(&file)?;
                    {
                        let raw_mapping = raw.as_mapping_mut()
                            .ok_or(DataSourceDefinitionLoaderError::InvalidFormat)?;

                        // Inject the IPFS hash as the ID of the data source
                        // into the definition.
                        raw_mapping.insert(
                            serde_yaml::Value::from("id"),
                            serde_yaml::Value::from(ipfs_link.trim_left_matches("/ipfs/")),
                        );

                        // Inject the IPFS link as the location of the data
                        // source into the definition
                        raw_mapping.insert(
                            serde_yaml::Value::from("location"),
                            serde_yaml::Value::from(ipfs_link),
                        );
                    }
                    // Parse the YAML data into an UnresolvedDataSourceDefinition
                    let unresolved: UnresolvedDataSourceDefinition = serde_yaml::from_value(raw)?;
                    Ok(unresolved)
                })
                .and_then(move |unresolved| {
                    unresolved
                        .resolve(ipfs_client)
                        .map_err(|e| DataSourceDefinitionLoaderError::ResolveError(e))
                }),
        )
    }
}
