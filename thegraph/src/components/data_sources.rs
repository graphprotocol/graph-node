use futures::sync::mpsc::Receiver;

use data::data_sources::SourceDefinition;
use data::schema::Schema;
use util::stream::StreamError;

use graphql_parser;
use serde_yaml;

use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::path::Path;

/// Events emitted by [DataSourceProvider](trait.DataSourceProvider.html) implementations.
#[derive(Clone, Debug)]
pub enum DataSourceProviderEvent {
    /// A data source was added to the provider.
    DataSourceAdded(&'static str),
    /// A data source was removed from the provider.
    DataSourceRemoved(&'static str),
}

/// Schema-only events emitted by a [DataSourceProvider](trait.DataSourceProvider.html).
#[derive(Clone, Debug)]
pub enum SchemaEvent {
    /// A data source with a new schema was added.
    SchemaAdded(Schema),
    /// A data source with an existing schema was removed.
    SchemaRemoved(Schema),
}

/// Common trait for data source providers.
pub trait DataSourceProvider {
    /// Receiver from which others can read events emitted by the data source provider.
    /// Can only be called once. Any consecutive call will result in a StreamError.
    fn event_stream(&mut self) -> Result<Receiver<DataSourceProviderEvent>, StreamError>;

    /// Receiver from whith others can read schema-only events emitted by the data source provider.
    /// Can only be called once. Any consecutive call will result in a StreamError.
    fn schema_event_stream(&mut self) -> Result<Receiver<SchemaEvent>, StreamError>;
}

/// Common trait for data source definition schema.
pub trait DataSourceDefinition {
    fn new(definition: serde_yaml::Value) -> Schema {
        // Deserialize yaml file into SourceDefinition struct
        let schema_definition: SourceDefinition = serde_yaml::from_value(definition).unwrap();

        // Get definition file from filesystem and return string
        let schema_path = Path::new(&schema_definition.schema.path);
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
