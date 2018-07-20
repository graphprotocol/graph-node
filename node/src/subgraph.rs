use futures::prelude::*;
use futures::sync::mpsc::{channel, Receiver};
use graphql_parser::{schema, Pos};
use slog;
use tokio_core::reactor::Handle;

use graph::components::subgraph::{SchemaEvent, SubgraphProviderEvent};
use graph::data::subgraph::SubgraphManifestResolveError;
use graph::prelude::{SubgraphProvider as SubgraphProviderTrait, *};

pub struct SubgraphProvider {
    _logger: slog::Logger,
    event_stream: Option<Receiver<SubgraphProviderEvent>>,
    schema_event_stream: Option<Receiver<SchemaEvent>>,
}

impl SubgraphProvider {
    pub fn new<'a>(
        logger: slog::Logger,
        runtime: Handle,
        link: &str,
        resolver: &'a impl LinkResolver,
    ) -> impl Future<Item = Self, Error = SubgraphManifestResolveError> + 'a {
        // Load the subgraph definition
        SubgraphManifest::resolve(
            Link {
                link: link.to_owned(),
            },
            resolver,
        ).map(move |subgraph| {
            let schema =
                Self::add_subgraph_id_directives(&mut subgraph.schema.clone(), subgraph.id.clone());
            let (event_sink, event_stream) = channel(100);

            // Push the subgraph into the stream
            runtime.spawn(
                event_sink
                    .send(SubgraphProviderEvent::SubgraphAdded(subgraph))
                    .map_err(|e| panic!("Failed to forward subgraph: {}", e))
                    .map(|_| ()),
            );

            let (schema_event_sink, schema_event_stream) = channel(100);

            // Push the schema into the stream
            runtime.spawn(
                schema_event_sink
                    .send(SchemaEvent::SchemaAdded(schema))
                    .map_err(|e| panic!("Failed to forward subgraph schema: {}", e))
                    .map(|_| ()),
            );
            // Create the subgraph provider
            SubgraphProvider {
                _logger: logger.new(o!("component" => "SubgraphProvider")),
                event_stream: Some(event_stream),
                schema_event_stream: Some(schema_event_stream),
            }
        })
    }

    // Adds a @subgraphId(id: ...) directive to object/interface/enum types in the schema.
    fn add_subgraph_id_directives(schema: &mut Schema, id: String) -> Schema {
        for definition in schema.document.definitions.iter_mut() {
            let subgraph_id_argument = (
                schema::Name::from("id"),
                schema::Value::String(subgraph.clone()),
            );

            let subgraph_id_directive = schema::Directive {
                name: "subgraphId".to_string(),
                position: Pos::default(),
                arguments: vec![subgraph_id_argument],
            };

            match definition {
                schema::Definition::TypeDefinition(ref mut type_definition) => {
                    match type_definition {
                        schema::TypeDefinition::Object(ref mut object_type) => {
                            object_type.directives.push(subgraph_id_directive);
                        }
                        schema::TypeDefinition::Interface(ref mut interface_type) => {
                            interface_type.directives.push(subgraph_id_directive);
                        }
                        schema::TypeDefinition::Enum(ref mut enum_type) => {
                            enum_type.directives.push(subgraph_id_directive);
                        }
                        schema::TypeDefinition::Scalar(_scalar_type) => (),
                        schema::TypeDefinition::InputObject(_input_object_type) => (),
                        schema::TypeDefinition::Union(_union_type) => (),
                    }
                }
                _ => (),
            };
        }
        schema.clone()
    }
}

impl SubgraphProviderTrait for SubgraphProvider {}

impl EventProducer<SubgraphProviderEvent> for SubgraphProvider {
    fn take_event_stream(
        &mut self,
    ) -> Option<Box<Stream<Item = SubgraphProviderEvent, Error = ()>>> {
        self.event_stream
            .take()
            .map(|s| Box::new(s) as Box<Stream<Item = SubgraphProviderEvent, Error = ()>>)
    }
}

impl EventProducer<SchemaEvent> for SubgraphProvider {
    fn take_event_stream(&mut self) -> Option<Box<Stream<Item = SchemaEvent, Error = ()>>> {
        self.schema_event_stream
            .take()
            .map(|s| Box::new(s) as Box<Stream<Item = SchemaEvent, Error = ()>>)
    }
}
