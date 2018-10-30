use std::collections::HashSet;

use graph::prelude::{
    SubgraphProvider as SubgraphProviderTrait,
    SubgraphProviderWithNames as SubgraphProviderWithNamesTrait, *,
};

pub struct SubgraphProviderWithNames<P, S> {
    logger: slog::Logger,
    provider: Arc<P>,
    store: Arc<S>,
}

impl<P, S> Clone for SubgraphProviderWithNames<P, S> {
    fn clone(&self) -> Self {
        Self {
            logger: self.logger.clone(),
            provider: self.provider.clone(),
            store: self.store.clone(),
        }
    }
}

impl<P, S> SubgraphProviderWithNames<P, S>
where
    P: SubgraphProviderTrait,
    S: Store,
{
    pub fn init(
        logger: slog::Logger,
        provider: Arc<P>,
        store: Arc<S>,
    ) -> impl Future<Item = Self, Error = Error> {
        // Create the named subgraph provider
        let provider = SubgraphProviderWithNames {
            logger: logger.new(o!("component" => "SubgraphProviderWithNames")),
            provider,
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

                stream::iter_ok(subgraph_ids)
                    .for_each(move |id| self_clone.provider.start(id).from_err())
            },
        )
    }
}

impl<P, S> SubgraphProviderWithNamesTrait for SubgraphProviderWithNames<P, S>
where
    P: SubgraphProviderTrait,
    S: Store,
{
    fn deploy(
        &self,
        name: String,
        id: SubgraphId,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static> {
        let self_clone = self.clone();

        // Check that the name contains only allowed characters.
        if !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Box::new(future::err(SubgraphProviderError::InvalidName(name)));
        }

        Box::new(
            // Cleanly undeploy this name if it exists (removes old mapping)
            self.remove(name.clone())
                .then(|result| {
                    match result {
                        // Name not found is fine. No old mapping to remove.
                        Err(SubgraphProviderError::NameNotFound(_)) => Ok(()),
                        other => other,
                    }
                }).and_then(move |()| {
                    future::result(self_clone.store.find_subgraph_names_by_id(id.clone()))
                        .from_err()
                        .and_then(move |existing_names| {
                            // Add new mapping
                            future::result(
                                self_clone
                                    .store
                                    .write_subgraph_name(name.clone(), Some(id.clone())),
                            ).from_err()
                            .and_then(
                                move |()| -> Box<Future<Item = _, Error = _> + Send> {
                                    if existing_names.is_empty() {
                                        // Start subgraph processing
                                        Box::new(self_clone.provider.start(id))
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
                .from_err()
                .and_then(move |id_opt_opt: Option<Option<SubgraphId>>| {
                    id_opt_opt
                        .ok_or_else(|| SubgraphProviderError::NameNotFound(name_clone.clone()))
                }).and_then(move |id_opt: Option<SubgraphId>| {
                    let store = store.clone();

                    // Delete this name->ID mapping
                    future::result(store.delete_subgraph_name(name.clone()))
                        .from_err()
                        .and_then(move |()| -> Box<Future<Item = _, Error = _> + Send> {
                            let store = store.clone();

                            // If name mapping pointed to a non-null ID
                            if let Some(id) = id_opt {
                                Box::new(
                                    future::result(store.find_subgraph_names_by_id(id.clone()))
                                        .from_err()
                                        .and_then(move |names| -> Box<Future<Item=_, Error=_> + Send> {
                                            // If this subgraph ID is no longer pointed to by any
                                            // subgraph names
                                            if names.is_empty() {
                                                // Shut down subgraph processing
                                                // Note: there's a possible race condition if two
                                                // names pointing to the same ID are removed at the
                                                // same time.
                                                // That's fine for now and will be fixed when this
                                                // is redone for the hosted service.
                                                Box::new(self_clone.provider.stop(id))
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

#[cfg(test)]
mod tests {
    use super::super::SubgraphProvider;
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
        let provider = SubgraphProvider::new(logger.clone(), Arc::new(FakeLinkResolver));
        let name_provider = Arc::new(
            SubgraphProviderWithNames::init(logger, Arc::new(provider), store)
                .wait()
                .unwrap(),
        );
        let bad = "/../funky%2F:9001".to_owned();
        let result = name_provider.deploy(bad.clone(), "".to_owned());
        match result.wait() {
            Err(SubgraphProviderError::InvalidName(name)) => assert_eq!(name, bad),
            x => panic!("unexpected test result {:?}", x),
        }
    }
}
