use futures::sync::mpsc::{channel, Receiver, Sender};
use std::collections::HashMap;

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

                let subgraph_names_by_id = subgraph_names_and_ids
                    .into_iter()
                    .filter_map(|(name, id_opt)| id_opt.map(move |id| (id, name)))
                    .collect::<HashMap<String, String>>();

                stream::iter_ok(subgraph_names_by_id.into_iter()).for_each(move |(id, name)| {
                    let self_clone = self_clone.clone();

                    SubgraphManifest::resolve(Link { link: format!("/ipfs/{}", id) }, self_clone.resolver.clone())
                        .map_err(|e| format_err!("subgraph manifest resolve error: {}", e))
                        .and_then(
                            // Validate the subgraph schema before deploying the subgraph
                            |subgraph| match validate_schema(&subgraph.schema.document) {
                                Err(e) => Err(e),
                                _ => Ok(subgraph),
                            }
                        ).and_then(move |mut subgraph| {
                            let self_clone = self_clone.clone();

                            subgraph.schema.add_subgraph_id_directives(subgraph.id.clone());

                            self_clone
                                .send_add_events(subgraph, name.clone())
                                .map_err(|e| format_err!("failed to deploy saved subgraph: {}", e))
                        })
                })
            },
        )
    }

    fn send_add_events(&self, subgraph: SubgraphManifest, name: String) -> impl Future<Item=(), Error=Error> {
        // Push the subgraph and the schema into their streams
        self
            .schema_event_sink
            .clone()
            .send(SchemaEvent::SchemaAdded(subgraph.schema.clone()))
            .map_err(|e| panic!("failed to forward subgraph schema: {}", e))
            .join(
                self
                    .event_sink
                    .clone()
                    .send(SubgraphProviderEvent::SubgraphStart(
                        name,
                        subgraph,
                    )).map_err(|e| panic!("failed to forward subgraph: {}", e)),
            ).map(|_| ())
    }

    fn send_remove_events(
        &self,
        names: Vec<String>,
        id: String,
    ) -> impl Future<Item = (), Error = SubgraphProviderError> + Send + 'static {
        let id_for_schema_removals = id.clone();
        let schema_removals = names
            .into_iter()
            .map(move |name| {
                self.schema_event_sink
                    .clone()
                    .send(SchemaEvent::SchemaRemoved(
                        name,
                        id_for_schema_removals.clone(),
                    )).map_err(|e| panic!("failed to forward schema removal: {}", e))
                    .map(|_| ())
            }).collect::<Vec<_>>();

        let subgraph_stop = self
            .event_sink
            .clone()
            .send(SubgraphProviderEvent::SubgraphStop(id))
            .map_err(|e| panic!("failed to forward subgraph removal: {}", e))
            .map(|_| ());

        future::collect(schema_removals)
            .join(subgraph_stop)
            .map(|_| ())
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
            return Box::new(Err(SubgraphProviderError::InvalidName(name)).into_future());
        }

        let subgraph_added_name = name.clone();

        let self_clone1 = self.clone();
        let self_clone2 = self.clone();
        let self_clone3 = self.clone();
        Box::new(
            SubgraphManifest::resolve(Link { link }, self.resolver.clone())
                .map_err(SubgraphProviderError::ResolveError)
                .and_then(
                    // Validate the subgraph schema before deploying the subgraph
                    |subgraph| match validate_schema(&subgraph.schema.document) {
                        Err(e) => Err(SubgraphProviderError::SchemaValidationError(e)),
                        _ => Ok(subgraph),
                    },
                ).and_then(move |mut subgraph| {
                    let new_id = subgraph.id.clone();
                    let name_clone = name.clone();

                    subgraph.schema.add_subgraph_id_directives(new_id.clone());

                    future::result(
                        // Read the old ID
                        self_clone1.store.read_subgraph_name(name.clone()),
                    ).map_err(SubgraphProviderError::Unknown)
                    .and_then(move |old_id_opt| {
                        // Write the new ID
                        self_clone1
                            .store
                            .write_subgraph_name(name.clone(), Some(new_id.clone()))
                            .map(move |()| old_id_opt)
                            .map_err(SubgraphProviderError::Unknown)
                    }).and_then(
                        move |old_id_opt_opt| -> Box<Future<Item = _, Error = _> + Send> {
                            // If a subgraph is being updated, remove the old subgraph.
                            match old_id_opt_opt {
                                Some(Some(old_id)) => {
                                    Box::new(future::result(self_clone2.store.find_subgraph_names_by_id(old_id.clone()))
                                        .map_err(SubgraphProviderError::Unknown)
                                        .and_then(move |names_with_old_id| -> Box<Future<Item=_, Error=_> + Send> {
                                            if names_with_old_id.is_empty() {
                                                // No more subgraph names map to this ID
                                                Box::new(
                                                    self_clone2.send_remove_events(vec![name_clone], old_id),
                                                )
                                            } else {
                                                // Other subgraph names use this ID
                                                Box::new(future::ok(()))
                                            }
                                        }))
                                }

                                // Subgraph name exists but has a null ID
                                Some(None) => Box::new(future::ok(())),

                                // Subgraph name does not exist
                                None => Box::new(future::ok(())),
                            }
                        },
                    ).and_then(move |_| {
                        self_clone3.send_add_events(subgraph, subgraph_added_name)
                            .map_err(SubgraphProviderError::Unknown)
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
        let names_and_id_opt_result = self
            .store
            .read_subgraph_name(name.clone())
            .map_err(SubgraphProviderError::Unknown)
            .and_then(move |subgraph_id_opt_from_name_opt| {
                match subgraph_id_opt_from_name_opt {
                    Some(subgraph_id_opt_from_name) => {
                        // name_or_id is a valid name, so use it to determine ID
                        let id_opt = subgraph_id_opt_from_name;

                        Ok((vec![name], id_opt))
                    }
                    None => {
                        Err(SubgraphProviderError::NotFound(name))
                    }
                }
            }).and_then(|(names, id_opt): (Vec<String>, Option<String>)| {
                for name in names.iter() {
                    self.store
                        .delete_subgraph_name(name.clone())
                        .map_err(SubgraphProviderError::Unknown)?;
                }

                Ok((names, id_opt))
            });

        match names_and_id_opt_result {
            Err(e) => Box::new(future::err(e)),
            Ok((names, Some(id))) => {
                // Shut down subgraph
                Box::new(self.send_remove_events(names, id))
            }
            Ok((_, None)) => {
                // No associated subgraph, so nothing to shut down
                Box::new(future::ok(()))
            }
        }
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
    let provider = Arc::new(SubgraphProvider::new(logger, Arc::new(FakeLinkResolver)));
    let bad = "/../funky%2F:9001".to_owned();
    let result = provider.deploy(bad.clone(), "".to_owned());
    match result.wait() {
        Err(SubgraphProviderError::InvalidName(name)) => assert_eq!(name, bad),
        x => panic!("unexpected test result {:?}", x),
    }
}
