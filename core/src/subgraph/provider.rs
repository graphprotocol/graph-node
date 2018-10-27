use futures::sync::mpsc::{channel, Receiver, Sender};
use std::collections::HashSet;

use graph::prelude::{SubgraphProvider as SubgraphProviderTrait, *};
use graph_graphql::prelude::validate_schema;

pub struct SubgraphProvider<L, S> {
    logger: slog::Logger,
    event_stream: Option<Receiver<SubgraphProviderEvent>>,
    event_sink: Sender<SubgraphProviderEvent>,
    schema_event_stream: Option<Receiver<SchemaEvent>>,
    schema_event_sink: Sender<SchemaEvent>,
    resolver: Arc<L>,
    store: Arc<S>,
}

impl<L, S> SubgraphProvider<L, S>
where
    L: LinkResolver,
    S: Store,
{
    pub fn init(
        logger: slog::Logger,
        resolver: Arc<L>,
        store: Arc<S>,
    ) -> impl Future<Item = Self, Error = Error> {
        let (schema_event_sink, schema_event_stream) = channel(100);
        let (event_sink, event_stream) = channel(100);

        // Create the subgraph provider
        let provider = SubgraphProvider {
            logger: logger.new(o!("component" => "SubgraphProvider")),
            event_stream: Some(event_stream),
            event_sink,
            schema_event_stream: Some(schema_event_stream),
            schema_event_sink,
            resolver,
            store,
        };

        // Deploy named subgraph found in store
        provider.deploy_saved_subgraphs().map(move |()| provider)
    }

    fn deploy_saved_subgraphs(&self) -> impl Future<Item = (), Error = Error> {
        let self_clone = self.clone();

        future::result(self.store.read_all_subgraph_names()).and_then(
            move |subgraph_names_and_ids| {
                let self_clone = self_clone.clone();

                let subgraph_ids = subgraph_names_and_ids
                    .into_iter()
                    .filter_map(|(_name, id_opt)| id_opt)
                    .collect::<HashSet<SubgraphId>>();

                stream::iter_ok(subgraph_ids).for_each(move |id| {
                    let self_clone = self_clone.clone();

                    SubgraphManifest::resolve(
                        Link {
                            link: format!("/ipfs/{}", id),
                        },
                        self_clone.resolver.clone(),
                    ).map_err(|e| format_err!("subgraph manifest resolve error: {}", e))
                    .and_then(
                        // Validate the subgraph schema before deploying the subgraph
                        |subgraph| match validate_schema(&subgraph.schema.document) {
                            Err(e) => Err(e),
                            _ => Ok(subgraph),
                        },
                    ).and_then(move |mut subgraph| {
                        let self_clone = self_clone.clone();

                        subgraph
                            .schema
                            .add_subgraph_id_directives(subgraph.id.clone());

                        self_clone
                            .send_add_events(subgraph)
                            .map_err(|e| format_err!("failed to deploy saved subgraph: {}", e))
                    })
                })
            },
        )
    }

    fn send_add_events(&self, subgraph: SubgraphManifest) -> impl Future<Item = (), Error = Error> {
        let schema_addition = self
            .schema_event_sink
            .clone()
            .send(SchemaEvent::SchemaAdded(subgraph.schema.clone()))
            .map_err(|e| panic!("failed to forward subgraph schema: {}", e))
            .map(|_| ());

        let subgraph_start = self
            .event_sink
            .clone()
            .send(SubgraphProviderEvent::SubgraphStart(subgraph))
            .map_err(|e| panic!("failed to forward subgraph: {}", e))
            .map(|_| ());

        schema_addition.join(subgraph_start).map(|_| ())
    }

    fn send_remove_events(
        &self,
        id: String,
    ) -> impl Future<Item = (), Error = SubgraphProviderError> + Send + 'static {
        let schema_removal = self
            .schema_event_sink
            .clone()
            .send(SchemaEvent::SchemaRemoved(id.clone()))
            .map_err(|e| panic!("failed to forward schema removal: {}", e))
            .map(|_| ());

        let subgraph_stop = self
            .event_sink
            .clone()
            .send(SubgraphProviderEvent::SubgraphStop(id))
            .map_err(|e| panic!("failed to forward subgraph removal: {}", e))
            .map(|_| ());

        schema_removal.join(subgraph_stop).map(|_| ())
    }

    /// Clones but forcing receivers to `None`.
    fn clone(&self) -> Self {
        SubgraphProvider {
            logger: self.logger.clone(),
            event_stream: None,
            event_sink: self.event_sink.clone(),
            schema_event_stream: None,
            schema_event_sink: self.schema_event_sink.clone(),
            resolver: self.resolver.clone(),
            store: self.store.clone(),
        }
    }
}

impl<L, S> SubgraphProviderTrait for SubgraphProvider<L, S>
where
    L: LinkResolver,
    S: Store,
{
    fn deploy(
        &self,
        name: String,
        link: String,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static> {
        // Check that the name contains only allowed characters.
        if !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Box::new(future::err(SubgraphProviderError::InvalidName(name)));
        }

        let self_clone = self.clone();

        let subgraph_resolution = SubgraphManifest::resolve(Link { link }, self.resolver.clone())
            .map_err(SubgraphProviderError::ResolveError)
            .and_then(
                // Validate the subgraph schema before deploying the subgraph
                |subgraph| match validate_schema(&subgraph.schema.document) {
                    Err(e) => Err(SubgraphProviderError::SchemaValidationError(e)),
                    _ => Ok(subgraph),
                },
            );

        Box::new(
            // Cleanly undeploy this name if it exists (removes old mapping)
            self.remove(name.clone())
                .then(|result| {
                    match result {
                        // Name not found is fine. No old mapping to remove.
                        Err(SubgraphProviderError::NameNotFound(_)) => Ok(()),
                        other => other,
                    }
                }).join(subgraph_resolution)
                .and_then(move |((), mut subgraph)| {
                    subgraph
                        .schema
                        .add_subgraph_id_directives(subgraph.id.clone());

                    future::result(
                        self_clone
                            .store
                            .find_subgraph_names_by_id(subgraph.id.clone()),
                    ).map_err(SubgraphProviderError::Unknown)
                    .and_then(move |existing_names| {
                        // Add new mapping
                        future::result(
                            self_clone
                                .store
                                .write_subgraph_name(name.clone(), Some(subgraph.id.clone())),
                        ).map_err(SubgraphProviderError::Unknown)
                        .and_then(
                            move |()| -> Box<Future<Item = _, Error = _> + Send> {
                                if existing_names.is_empty() {
                                    // Start subgraph processing
                                    Box::new(
                                        self_clone
                                            .send_add_events(subgraph)
                                            .map_err(SubgraphProviderError::Unknown),
                                    )
                                } else {
                                    // Subgraph is already started
                                    Box::new(future::ok(()))
                                }
                            },
                        )
                    })
                }),
        )
    }

    /// Remove the subgraph and signal the removal to the graphql server and the
    /// runtime manager. Error if the subgraph is not present.
    fn remove(
        &self,
        name: String,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static> {
        let self_clone = self.clone();
        let store = self.store.clone();
        let name_clone = name.clone();

        // Look up name mapping
        Box::new(
            future::result(store.read_subgraph_name(name.clone()))
                .map_err(SubgraphProviderError::Unknown)
                .and_then(move |id_opt_opt: Option<Option<SubgraphId>>| {
                    id_opt_opt
                        .ok_or_else(|| SubgraphProviderError::NameNotFound(name_clone.clone()))
                }).and_then(move |id_opt: Option<SubgraphId>| {
                    let store = store.clone();

                    // Delete this name->ID mapping
                    future::result(store.delete_subgraph_name(name.clone()))
                        .map_err(SubgraphProviderError::Unknown)
                        .and_then(move |()| -> Box<Future<Item = _, Error = _> + Send> {
                            let store = store.clone();

                            // If name mapping pointed to a non-null ID
                            if let Some(id) = id_opt {
                                Box::new(
                            future::result(store.find_subgraph_names_by_id(id.clone()))
                                .map_err(SubgraphProviderError::Unknown)
                                .and_then(move |names| -> Box<Future<Item=_, Error=_> + Send> {
                                    // If this subgraph ID is no longer pointed to by any subgraph names
                                    if names.is_empty() {
                                        // Shut down subgraph processing
                                        // Note: there's a possible race condition if two names
                                        // pointing to the same ID are removed at the same time.
                                        // That's fine for now and will be fixed when this is
                                        // redone for the hosted service.
                                        Box::new(self_clone.send_remove_events(id))
                                    } else {
                                        // Leave subgraph running
                                        Box::new(future::ok(()))
                                    }
                                })
                            )
                            } else {
                                // Nothing to shut down
                                Box::new(future::ok(()))
                            }
                        })
                }),
        )
    }

    fn list(&self) -> Result<Vec<(String, Option<SubgraphId>)>, Error> {
        self.store.read_all_subgraph_names()
    }
}

impl<L, S> EventProducer<SubgraphProviderEvent> for SubgraphProvider<L, S> {
    fn take_event_stream(
        &mut self,
    ) -> Option<Box<Stream<Item = SubgraphProviderEvent, Error = ()> + Send>> {
        self.event_stream
            .take()
            .map(|s| Box::new(s) as Box<Stream<Item = SubgraphProviderEvent, Error = ()> + Send>)
    }
}

impl<L, S> EventProducer<SchemaEvent> for SubgraphProvider<L, S> {
    fn take_event_stream(&mut self) -> Option<Box<Stream<Item = SchemaEvent, Error = ()> + Send>> {
        self.schema_event_stream
            .take()
            .map(|s| Box::new(s) as Box<Stream<Item = SchemaEvent, Error = ()> + Send>)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use graph_mock::MockStore;

    #[test]
    fn rejects_name_bad_for_urls() {
        extern crate failure;

        struct FakeLinkResolver;

        impl LinkResolver for FakeLinkResolver {
            fn cat(&self, _: &Link) -> Box<Future<Item = Vec<u8>, Error = failure::Error> + Send> {
                unimplemented!()
            }
        }
        let logger = slog::Logger::root(slog::Discard, o!());
        let store = Arc::new(MockStore::new());
        let provider = Arc::new(
            SubgraphProvider::init(logger, Arc::new(FakeLinkResolver), store)
                .wait()
                .unwrap(),
        );
        let bad = "/../funky%2F:9001".to_owned();
        let result = provider.deploy(bad.clone(), "".to_owned());
        match result.wait() {
            Err(SubgraphProviderError::InvalidName(name)) => assert_eq!(name, bad),
            x => panic!("unexpected test result {:?}", x),
        }
    }
}
