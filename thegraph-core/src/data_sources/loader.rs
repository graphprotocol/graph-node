use thegraph::data::schema::Schema;
use graphql_parser;
use thegraph::components::data_sources::{ DataSourceDefinition as DataSourceDefinitionTrait };
use thegraph::data::data_sources::{ SourcePath, SourceDataSet };
use serde_yaml;
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::path::Path;

#[derive(Debug, PartialEq, Deserialize)]
pub struct DataSourceDefinition {
    #[serde(rename = "specVersion")]
    pub spec_version: String,
    pub schema: SourcePath,
    pub datasets: SourceDataSet,
}

impl DataSourceDefinitionTrait for DataSourceDefinition {
    // Deserialize .yaml into DataSourceDefinition struct
    fn build_definition(definition: serde_yaml::Value) -> Self {
        // Use serde_yaml and serde_derive to deserialize the data source definition
        let schema_definition: DataSourceDefinition = serde_yaml::from_value(definition)
            .expect("Failed to deserialize yaml value");
        schema_definition
    }

    // Extract graphql schema from path
    fn extract_schema(&self) -> Schema {

        // Get definition file from filesystem and return string
        let schema_path = Path::new(&self.schema.path);
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
        schema_file.read_to_string(&mut schema_string)
            .expect("Failed to read schema file out to string");

        // Parse graphql schema string into graphql Document
        // and place into Schema struct
        let graphql_schema = Schema {
            id: "test".to_string(),
            document: graphql_parser::parse_schema(&schema_string)
                .expect("Failed to parse schema from schema string"),
        };

        graphql_schema
    }
}


#[cfg(test)]
mod tests {
    use thegraph::components::data_sources::{ DataSourceDefinition as DataSourceDefinitionTrait };
    use serde_yaml;
    use super::DataSourceDefinition;

    #[test]
    fn data_source_definition_builds_from_yaml() {
        // Test yaml
        let test_yaml =
            r#"
            specVersion: 0.0.1
            schema:
              path: ./schema.graphql
            datasets:
                data:
                  kind: ethereum/contract
                  name: Registry
                  address: '0x22843e74c59580b3eaf6c233fa67d8b7c561a835'
                  structure:
                    abi:
                      path: ./abis/Registry.abi
                mapping:
                  kind: ethereum/events
                  apiVersion: 0.0.1
                  language: TypeScript
                  entities:
                    - Meme
                  config:
                    name: memeABI
                    value:
                      path: ./abis/Meme.abi
                      contentType: text/json
                  source:
                    path: ./mapping.js}"#;

        let test_yaml: serde_yaml::Value = serde_yaml::from_str(&test_yaml.to_string())
            .expect("Failed to process test yaml string into serde_yaml::Value");

        let test_definition = DataSourceDefinition::build_definition(test_yaml);

        assert_eq!("./schema.graphql".to_string(), test_definition.schema.path);
        assert_eq!("0.0.1".to_string(), test_definition.datasets.mapping.api_version);
        assert_eq!("0.0.1".to_string(), test_definition.spec_version);
        assert_eq!("./abis/Meme.abi".to_string(), test_definition.datasets.mapping.config.value.path);
        assert_eq!("Registry".to_string(), test_definition.datasets.data.name);
    }

    #[test]
    #[should_panic]
    fn data_source_definition_fails_invalid_yaml() {
        // Test yaml
        let test_yaml =
            r#"
            specVersion: 0.0.1
            schema:
              path: ./schema.graphql
            datasets:
                data:
                  name: Registry
                  address: '0x22843e74c59580b3eaf6c233fa67d8b7c561a835'
                  structure:
                    abi:
                      path: ./abis/Registry.abi
                mapping:
                  kind: ethereum/events
                  apiVersion: 0.0.1
                  language: TypeScript
                  entities:
                    - Meme
                  config:
                    name: memeABI
                    value:
                      path: ./abis/Meme.abi
                      contentType: text/json
                  source:
                    path: ./mapping.js}"#;

        let test_yaml: serde_yaml::Value = serde_yaml::from_str(&test_yaml.to_string())
            .expect("Failed to process test yaml string into serde_yaml::Value");

        let _test_definition = DataSourceDefinition::build_definition(test_yaml);
    }

    #[test]
    fn extract_graphql_schema_using_yaml_data_source_definition() {
        unimplemented!();
    }
}
