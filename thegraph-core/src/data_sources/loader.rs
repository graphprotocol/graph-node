use graphql_parser;
use thegraph::components::data_sources::{ DataSourceDefinition as DataSourceDefinitionTrait };
use thegraph::data::data_sources::{ SourcePath, SourceDataSet };
use serde_yaml;
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::path::Path;

#[derive(Debug, PartialEq, Deserialize)]
pub struct SourceDefinition {
    pub spec_version: String,
    pub schema: SourcePath,
    pub datasets: SourceDataSet,
}

impl DataSourceDefinitionTrait for SourceDefinition {
    // Deserialize yaml into SourceDefinition struct
    fn build_definition(definition: serde_yaml::Value) -> Self {
        // Use serde_yaml and serde_derive to deserialize the data source definition
        let schema_definition: SourceDefinition = serde_yaml::from_value(definition).unwrap();
        schema_definition
    }

    // Extract graphql schema using the DataSourceDefinition
    fn extract_schema(&self) -> Schema {

        // Get definition file from filesystem and return string
        let schema_path = Path::new(self.schema.path);
        let display_schema_path = schema_path.display();

        let mut schema_file = match File::open(&schema_path) {
            Err(why) => panic!(
                "couldn't open schema at {}: {}",
                display_schema_path,
                why.description()
            ),
            Ok(file) => file,
        };
        let mut schema_string = String::new();
        schema_file.read_to_string(&mut schema_string).unwrap();

        // Parse graphql schema string into graphql Document
        // and place into Schema struct
        let graphql_schema = Schema {
            id: "test".to_string(),
            document: graphql_parser::parse_schema(&schema_string).unwrap(),
        };

        graphql_schema
    }
}
