use serde_yaml;
use std::fs::File;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use thegraph::components::data_sources::{
    DataSourceDefinitionLoader as LoaderTrait, DataSourceDefinitionLoaderError,
};
use thegraph::data::data_sources::*;

#[derive(Default)]
pub struct DataSourceDefinitionLoader;

impl DataSourceDefinitionLoader {
    fn resolve_path(&self, parent: Option<&Path>, path: &Path) -> PathBuf {
        let is_relative = path.is_relative() || path.starts_with("./");
        match (is_relative, parent) {
            (true, Some(parent_path)) if parent_path.is_dir() => parent_path.join(path),
            _ => path.clone().to_path_buf(),
        }
    }

    fn load_schema_from_path(
        &self,
        path: &Path,
    ) -> Result<String, DataSourceDefinitionLoaderError> {
        let mut file =
            File::open(path).map_err(|e| DataSourceDefinitionLoaderError::SchemaIOError(e))?;

        let mut sdl = String::new();
        file.read_to_string(&mut sdl)
            .map_err(|e| DataSourceDefinitionLoaderError::SchemaIOError(e))?;

        Ok(sdl)
    }
}

impl LoaderTrait for DataSourceDefinitionLoader {
    fn load_from_path(
        &self,
        path: PathBuf,
    ) -> Result<DataSourceDefinition, DataSourceDefinitionLoaderError> {
        // Read the YAML data from the definition file
        let file = File::open(&path).expect("Failed to open data source definition file");
        let mut raw: serde_yaml::Value =
            serde_yaml::from_reader(file).map_err(DataSourceDefinitionLoaderError::from)?;

        {
            let raw_mapping = raw.as_mapping_mut()
                .ok_or(DataSourceDefinitionLoaderError::InvalidFormat)?;

            // Resolve the schema path into a GraphQL SDL string
            let schema = raw_mapping
                .get(&serde_yaml::Value::from("schema"))
                .and_then(|schema| schema.get(&serde_yaml::Value::from("source")))
                .and_then(|source| source.get(&serde_yaml::Value::from("path")))
                .and_then(|path| path.as_str())
                .ok_or(DataSourceDefinitionLoaderError::SchemaMissing)
                .map(|schema_path| self.resolve_path(path.parent(), Path::new(schema_path)))
                .and_then(|path| self.load_schema_from_path(&path))?;

            // Replace the schema path with the SDL
            raw_mapping.insert(
                serde_yaml::Value::String(String::from("schema")),
                serde_yaml::Value::String(schema),
            );

            // Inject the location of the data source into the definition
            raw_mapping.insert(
                serde_yaml::Value::from("location"),
                serde_yaml::Value::from(path.clone()
                    .to_str()
                    .ok_or(DataSourceDefinitionLoaderError::InvalidPath(path))?),
            );
        }

        // Parse the YAML data into a DataSourceDefinition
        serde_yaml::from_value(raw).map_err(DataSourceDefinitionLoaderError::from)
    }
}
