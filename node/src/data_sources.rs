use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver};
use graphql_parser::{Pos, schema};
use slog;
use tokio_core::reactor::Handle;

use graph::components::data_sources::{DataSourceProviderEvent, SchemaEvent};
use graph::data::data_sources::DataSourceDefinitionResolveError;
use graph::prelude::{DataSourceProvider as DataSourceProviderTrait, *};

pub struct DataSourceProvider {
    _logger: slog::Logger,
    event_stream: Option<Receiver<DataSourceProviderEvent>>,
    schema_event_stream: Option<Receiver<SchemaEvent>>,
}

impl DataSourceProvider {
    pub fn new<'a>(
        logger: slog::Logger,
        runtime: Handle,
        link: &str,
        resolver: &'a impl LinkResolver,
    ) -> impl Future<Item = Self, Error = DataSourceDefinitionResolveError> + 'a {
        // Load the data source definition
        DataSourceDefinition::resolve(
            Link {
                link: link.to_owned(),
            },
            resolver,
        ).map(move |data_source| {
            let schema = DataSourceProvider::add_package_id_directives(
                &mut data_source.schema.clone(),
                data_source.id.clone(),
            );
            let (event_sink, event_stream) = channel(100);

            // Push the data source into the stream
            runtime.spawn(
                event_sink
                    .send(DataSourceProviderEvent::DataSourceAdded(data_source))
                    .map_err(|e| panic!("Failed to forward data source: {}", e))
                    .map(|_| ()),
            );

            let (schema_event_sink, schema_event_stream) = channel(100);

            // Push the schema into the stream
            runtime.spawn(
                schema_event_sink
                    .send(SchemaEvent::SchemaAdded(schema))
                    .map_err(|e| panic!("Failed to forward data source schema: {}", e))
                    .map(|_| ()),
            );

            // Create the data source provider
            DataSourceProvider {
                _logger: logger.new(o!("component" => "DataSourceProvider")),
                event_stream: Some(event_stream),
                schema_event_stream: Some(schema_event_stream),
            }
        })
    }

    // Add directive containing the data source id (package id) for:
    // Object type definitions, interface type definitions, and enum type definitions
    pub fn add_package_id_directives(schema: &mut Schema, package: String) -> Schema {
        for definition in schema.document.definitions.iter_mut() {
            let package_id_argument: Vec<(schema::Name, schema::Value)> =
                vec![("id".to_string(), schema::Value::String(package.clone()))];
            let package_id_directive = schema::Directive {
                name: "packageId".to_string(),
                position: Pos::default(),
                arguments: package_id_argument,
            };
            if let schema::Definition::TypeDefinition(ref mut type_definition) = definition {
                match type_definition {
                    schema::TypeDefinition::Object(ref mut object_type) => {
                        object_type.directives.push(package_id_directive);
                    }
                    schema::TypeDefinition::Interface(ref mut interface_type) => {
                        interface_type.directives.push(package_id_directive);
                    }
                    schema::TypeDefinition::Enum(ref mut enum_type) => {
                        enum_type.directives.push(package_id_directive);
                    }
                    schema::TypeDefinition::Scalar(_scalar_type) => (),
                    schema::TypeDefinition::InputObject(_input_object_type) => (),
                    schema::TypeDefinition::Union(_union_type) => (),
                }
            }
        };
        schema.clone()
    }
}

// iterate over all object type, interface type, and enum type definitions and add packageId directive
// with an `id: "the data source has here" argument to each type
impl DataSourceProviderTrait for DataSourceProvider {}

impl EventProducer<DataSourceProviderEvent> for DataSourceProvider {
    fn take_event_stream(
        &mut self,
    ) -> Option<Box<Stream<Item = DataSourceProviderEvent, Error = ()>>> {
        self.event_stream
            .take()
            .map(|s| Box::new(s) as Box<Stream<Item = DataSourceProviderEvent, Error = ()>>)
    }
}

impl EventProducer<SchemaEvent> for DataSourceProvider {
    fn take_event_stream(&mut self) -> Option<Box<Stream<Item = SchemaEvent, Error = ()>>> {
        self.schema_event_stream
            .take()
            .map(|s| Box::new(s) as Box<Stream<Item = SchemaEvent, Error = ()>>)
    }
}