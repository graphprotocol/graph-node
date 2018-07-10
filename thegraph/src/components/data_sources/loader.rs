use components::link_resolver::LinkResolver;
use data::data_sources::DataSourceDefinition;
use futures::Future;
use serde_yaml;
use std::error::Error;
use std::io;

#[derive(Debug)]
pub enum DataSourceDefinitionLoaderError {
    ParseError(serde_yaml::Error),
    NonUtf8,
    InvalidFormat,
    SchemaMissing,
    SchemaIOError(io::Error),
    ResolveError(Box<Error>),
}

impl From<serde_yaml::Error> for DataSourceDefinitionLoaderError {
    fn from(e: serde_yaml::Error) -> Self {
        DataSourceDefinitionLoaderError::ParseError(e)
    }
}

/// Common trait for components that are able to load `DataSourceDefinition`s.
pub trait DataSourceDefinitionLoader {
    /// Loads a `DataSourceDefinition` from IPFS.
    fn load_from_ipfs<'a>(
        &self,
        ipfs_link: &str,
        ipfs_client: &'a impl LinkResolver,
    ) -> Box<Future<Item = DataSourceDefinition, Error = DataSourceDefinitionLoaderError> + 'a>;
}
