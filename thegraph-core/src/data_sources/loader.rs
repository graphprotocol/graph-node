use graphql_parser;
use serde_yaml;
use std::error::Error;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use thegraph::components::data_sources::{DataSourceDefinitionLoader as LoaderTrait,
                                         DataSourceDefinitionLoaderError};
use thegraph::data::data_sources::*;
use thegraph::data::schema::Schema;

#[derive(Default)]
pub struct DataSourceDefinitionLoader;

impl DataSourceDefinitionLoader {
    fn resolve_path(&self, parent: Option<Path>, path: &Path) -> Path {
        let is_relative = path.is_relative() || path.starts_with("./");
        match (is_relative, parent) {
            (true, Some(parent_path)) if parent_path.is_dir() => dir.join(path),
            _ => path.clone(),
        }
    }

    fn load_schema_from_path(
        &self,
        path: &Path,
    ) -> Result<Schema, DataSourceDefinitionLoaderError> {
        let mut file = File::open(&schema_filename)
            .expect(format!("Failed to open schema file: {}", schema_filename.display()).as_str());
        let mut schema_str = String::new();
        file.read_to_string(&mut schema_str)
            .expect("Failed to read schema file");

        // Parse and remember the schema
        let document = graphql_parser::parse_schema(&schema_str).expect("Failed to parse schema");
        Schema {
            id: String::from("local-data-source-schema"),
            document: document,
        }
    }
}

impl LoaderTrait for DataSourceDefinitionLoader {
    fn load_from_path(
        &self,
        path: Path,
    ) -> Result<DataSourceDefinition, DataSourceDefinitionLoaderError> {
        let file = File::open(&path).expect("Failed to open data source definition file");
        let raw: serde_yaml::Value =
            serde_yaml::from_reader(reader).map_err(DataSourceDefinitionLoaderError::from);

        let schema_path = raw.as_mapping()
            .and_then(|m| m.get("schema"))
            .and_then(|location| location.get("path"))
            .and_then(|path| path.as_string())
            .ok_or(DataSourceDefinitionLoaderError::SchemaMissing)?;

        if let Some(schema_path) = schema_path {
            let path = self.resolve_path(path.parent(), &schema_path);
            let schema = self.load_schema_from_path(&path)?;

            let m = raw.as_mapping_mut().unwrap();
            m.insert("schema", schema);
        }

        serde_yaml::from_value(raw).map_err(DataSourceDefinitionLoaderError::from)
    }
}
