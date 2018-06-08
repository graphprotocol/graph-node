use serde_yaml;
use std::path::Path;

use data::data_sources::DataSourceDefinition;

#[derive(Debug)]
pub enum DataSourceDefinitionLoaderError {
    ParseError(serde_yaml::Error),
    SchemaMissing,
}

impl From<serde_yaml::Error> for DataSourceDefinitionLoaderError {
    fn from(e: serde_yaml::Error) -> Self {
        DataSourceDefinitionLoaderError::ParseError(e)
    }
}

/// Common trait for components that are able to `DataSourceDefinition`s.
pub trait DataSourceDefinitionLoader {
    /// Loads a `DataSourceDefinition` from a local path.
    fn load_from_path(
        &self,
        path: Path,
    ) -> Result<DataSourceDefinition, DataSourceDefinitionLoaderError>;
}
