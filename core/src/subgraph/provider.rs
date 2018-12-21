use futures::sync::mpsc::{channel, Receiver, Sender};
use std::collections::HashSet;
use std::sync::Mutex;

use graph::prelude::{SubgraphAssignmentProvider as SubgraphAssignmentProviderTrait, *};
use graph_graphql::schema::ast as sast;

pub struct SubgraphAssignmentProvider<L, S> {
    logger: Logger,
    event_stream: Option<Receiver<SubgraphAssignmentProviderEvent>>,
    event_sink: Sender<SubgraphAssignmentProviderEvent>,
    resolver: Arc<L>,
    subgraphs_running: Arc<Mutex<HashSet<SubgraphDeploymentId>>>,
    store: Arc<S>,
}

impl<L, S> SubgraphAssignmentProvider<L, S>
where
    L: LinkResolver,
    S: Store,
{
    pub fn new(logger: Logger, resolver: Arc<L>, store: Arc<S>) -> Self {
        let (event_sink, event_stream) = channel(100);

        // Create the subgraph provider
        SubgraphAssignmentProvider {
            logger: logger.new(o!("component" => "SubgraphAssignmentProvider")),
            event_stream: Some(event_stream),
            event_sink,
            resolver,
            subgraphs_running: Arc::new(Mutex::new(HashSet::new())),
            store,
        }
    }

    /// Clones but forcing receivers to `None`.
    fn clone(&self) -> Self {
        SubgraphAssignmentProvider {
            logger: self.logger.clone(),
            event_stream: None,
            event_sink: self.event_sink.clone(),
            resolver: self.resolver.clone(),
            subgraphs_running: self.subgraphs_running.clone(),
            store: self.store.clone(),
        }
    }
}

impl<L, S> SubgraphAssignmentProviderTrait for SubgraphAssignmentProvider<L, S>
where
    L: LinkResolver,
    S: Store,
{
    fn start(
        &self,
        id: SubgraphDeploymentId,
    ) -> Box<Future<Item = (), Error = SubgraphAssignmentProviderError> + Send + 'static> {
        let self_clone = self.clone();

        let link = format!("/ipfs/{}", id);

        Box::new(
            SubgraphManifest::resolve(Link { link }, self.resolver.clone())
                .map_err(SubgraphAssignmentProviderError::ResolveError)
                .and_then(move |subgraph| -> Box<Future<Item = _, Error = _> + Send> {
                    // If subgraph ID already in set
                    if !self_clone
                        .subgraphs_running
                        .lock()
                        .unwrap()
                        .insert(subgraph.id.clone())
                    {
                        return Box::new(future::err(
                            SubgraphAssignmentProviderError::AlreadyRunning(subgraph.id),
                        ));
                    }

                    // Build indexes for each entity attribute pair in the Subgraph
                    let mut indexing_ops = vec![];
                    for (entity_number, schema_type) in subgraph
                        .schema
                        .document
                        .definitions
                        .clone()
                        .into_iter()
                        .enumerate()
                    {
                        match schema_type {
                            schema::Definition::TypeDefinition(definition) => match definition {
                                schema::TypeDefinition::Object(schema_object) => {
                                    for (attribute_number, entity_field) in
                                        schema_object.fields.into_iter().enumerate()
                                    {
                                        let subgraph_id = subgraph.id.clone();
                                        let index_name: String = format!(
                                            "{}_{}_{}_idx",
                                            subgraph_id, entity_number, attribute_number,
                                        );
                                        let field_value_type = match sast::get_valid_value_type(
                                            &entity_field.field_type,
                                        ) {
                                            Ok(value_type) => value_type,
                                            Err(_) => continue,
                                        };

                                        let (index_type, index_operator) = match field_value_type {
                                            ValueType::Boolean
                                            | ValueType::BigInt
                                            | ValueType::Bytes
                                            | ValueType::Float
                                            | ValueType::ID
                                            | ValueType::Int => {
                                                (String::from("btree"), String::from(""))
                                            }
                                            ValueType::String => {
                                                (String::from("gin"), String::from("gin_trgm_ops"))
                                            }
                                        };
                                        let attribute_name = entity_field.name;
                                        let entity_name = schema_object.name.clone();

                                        indexing_ops.push(AttributeIndexOperation {
                                            subgraph_id,
                                            index_name,
                                            index_type,
                                            index_operator,
                                            attribute_name,
                                            entity_name,
                                        });
                                    }
                                }
                                _ => (),
                            },
                            _ => (),
                        }
                    }
                    self_clone
                        .store
                        .clone()
                        .build_entity_attribute_indexes(indexing_ops)
                        .map_err(|err| {
                            error!(
                                self_clone.logger,
                                "Failed to create indexes on subgraph entities: {}", err
                            )
                        })
                        .ok();

                    // Send events to trigger subgraph processing
                    Box::new(
                        self_clone
                            .event_sink
                            .clone()
                            .send(SubgraphAssignmentProviderEvent::SubgraphStart(subgraph))
                            .map_err(|e| panic!("failed to forward subgraph: {}", e))
                            .map(|_| ()),
                    )
                }),
        )
    }

    fn stop(
        &self,
        id: SubgraphDeploymentId,
    ) -> Box<Future<Item = (), Error = SubgraphAssignmentProviderError> + Send + 'static> {
        // If subgraph ID was in set
        if self.subgraphs_running.lock().unwrap().remove(&id) {
            // Shut down subgraph processing
            Box::new(
                self.event_sink
                    .clone()
                    .send(SubgraphAssignmentProviderEvent::SubgraphStop(id))
                    .map_err(|e| panic!("failed to forward subgraph shut down event: {}", e))
                    .map(|_| ()),
            )
        } else {
            Box::new(future::err(SubgraphAssignmentProviderError::NotRunning(id)))
        }
    }
}

impl<L, S> EventProducer<SubgraphAssignmentProviderEvent> for SubgraphAssignmentProvider<L, S> {
    fn take_event_stream(
        &mut self,
    ) -> Option<Box<Stream<Item = SubgraphAssignmentProviderEvent, Error = ()> + Send>> {
        self.event_stream.take().map(|s| {
            Box::new(s) as Box<Stream<Item = SubgraphAssignmentProviderEvent, Error = ()> + Send>
        })
    }
}
