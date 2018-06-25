use serde_yaml;
use std::io;
use std::path::PathBuf;

use data::data_sources::DataSourceDefinition;

#[derive(Debug)]
pub enum DataSourceDefinitionLoaderError {
    ParseError(serde_yaml::Error),
    InvalidFormat,
    SchemaMissing,
    SchemaIOError(io::Error),
}

impl From<serde_yaml::Error> for DataSourceDefinitionLoaderError {
    fn from(e: serde_yaml::Error) -> Self {
        DataSourceDefinitionLoaderError::ParseError(e)
    }
}

/// Common trait for components that are able to load `DataSourceDefinition`s.
pub trait DataSourceDefinitionLoader {
    /// Loads a `DataSourceDefinition` from a local path.
    fn load_from_path(
        &self,
        path: PathBuf,
    ) -> Result<DataSourceDefinition, DataSourceDefinitionLoaderError>;
}
