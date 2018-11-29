use std::collections::HashSet;

use graph::prelude::{
    SubgraphProvider as SubgraphProviderTrait,
    SubgraphProviderWithNames as SubgraphProviderWithNamesTrait, *,
};

pub struct SubgraphProviderWithNames<P, S> {
    logger: slog::Logger,
    provider: Arc<P>,
    store: Arc<S>,
    node_id: NodeId,
    deployment_event_stream_cancel_guard: CancelGuard, // cancels on drop
}

impl<P, S> SubgraphProviderWithNames<P, S>
where
    P: SubgraphProviderTrait,
    S: SubgraphDeploymentStore,
{
    pub fn new(logger: slog::Logger, provider: Arc<P>, store: Arc<S>, node_id: NodeId) -> Self {
        let logger = logger.new(o!("component" => "SubgraphProviderWithNames"));

        SubgraphProviderWithNames {
            logger,
            provider,
            store,
            node_id,
            deployment_event_stream_cancel_guard: CancelGuard::new(),
        }
    }

    pub fn start(&self) -> impl Future<Item = (), Error = Error> {
        let logger_clone1 = self.logger.clone();
        let logger_clone2 = self.logger.clone();
        let provider = self.provider.clone();
        let node_id = self.node_id.clone();
        let deployment_event_stream_cancel_handle =
            self.deployment_event_stream_cancel_guard.handle();

        // The order of the following three steps is important:
        // - Start deployment event stream
        // - Read deployments table and start deployed subgraphs
        // - Start processing deployment event stream
        //
        // Starting the event stream before reading the deployments table ensures that no
        // deployments are missed in the period of time between the table read and starting event
        // processing.
        // Delaying the start of event processing until after the table has been read and processed
        // ensures that Remove events happen after the deployed subgraphs have been started, not
        // before (otherwise a subgraph could be left running due to a race condition).
        //
        // The discrepancy between the start time of the event stream and the table read can result
        // in some extraneous events on start up. Examples:
        // - The event stream sees an Add event for subgraph A, but the table query finds that
        //   subgraph A is already in the table.
        // - The event stream sees a Remove event for subgraph B, but the table query finds that
        //   subgraph B has already been removed.
        // The `handle_deployment_events` function handles these cases by ignoring AlreadyRunning
        // (on subgraph start) or NotRunning (on subgraph stop) error types, which makes the
        // operations idempotent.

        // Start event stream
        let deployment_event_stream = self.store.deployment_events(node_id.clone());

        // Deploy named subgraphs found in store
        self.start_deployed_subgraphs().and_then(move |()| {
            // Spawn a task to handle deployment events
            tokio::spawn(future::lazy(move || {
                deployment_event_stream
                    .map_err(SubgraphProviderError::Unknown)
                    .map_err(CancelableError::Error)
                    .cancelable(&deployment_event_stream_cancel_handle, || {
                        CancelableError::Cancel
                    }).for_each(move |deployment_event| {
                        assert_eq!(deployment_event.node_id(), &node_id);
                        handle_deployment_event(deployment_event, provider.clone(), &logger_clone1)
                    }).map_err(move |e| match e {
                        CancelableError::Cancel => {}
                        CancelableError::Error(e) => {
                            error!(logger_clone2, "deployment event stream failed: {}", e);
                            panic!("deployment event stream error: {}", e);
                        }
                    })
            }));

            Ok(())
        })
    }

    fn start_deployed_subgraphs(&self) -> impl Future<Item = (), Error = Error> {
        let provider = self.provider.clone();

        future::result(self.store.read_by_node_id(self.node_id.clone())).and_then(
            move |names_and_subgraph_ids| {
                let provider = provider.clone();

                let subgraph_ids = names_and_subgraph_ids
                    .into_iter()
                    .map(|(_name, id)| id)
                    .collect::<HashSet<SubgraphId>>();

                stream::iter_ok(subgraph_ids).for_each(move |id| provider.start(id).from_err())
            },
        )
    }
}

impl<P, S> SubgraphProviderWithNamesTrait for SubgraphProviderWithNames<P, S>
where
    P: SubgraphProviderTrait,
    S: SubgraphDeploymentStore,
{
    fn deploy(
        &self,
        name: SubgraphDeploymentName,
        id: SubgraphId,
        node_id: NodeId,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static> {
        debug!(
            self.logger,
            "Writing deployment entry to store: name = {:?}, subgraph ID = {:?}",
            name.to_string(),
            id.to_string()
        );

        Box::new(future::result(self.store.write(name, id, node_id)).from_err())
    }

    fn remove(
        &self,
        name: SubgraphDeploymentName,
    ) -> Box<Future<Item = (), Error = SubgraphProviderError> + Send + 'static> {
        debug!(
            self.logger,
            "Removing deployment entry from store: {:?}",
            name.to_string()
        );

        Box::new(
            future::result(self.store.remove(name.clone()))
                .from_err()
                .and_then(move |did_remove| {
                    if did_remove {
                        Ok(())
                    } else {
                        Err(SubgraphProviderError::NameNotFound(name.to_string()))
                    }
                }),
        )
    }

    fn list(&self) -> Result<Vec<(SubgraphDeploymentName, SubgraphId)>, Error> {
        self.store.read_by_node_id(self.node_id.clone())
    }
}

fn handle_deployment_event<P>(
    event: DeploymentEvent,
    provider: Arc<P>,
    logger: &Logger,
) -> Box<Future<Item = (), Error = CancelableError<SubgraphProviderError>> + Send>
where
    P: SubgraphProviderTrait,
{
    let logger = logger.to_owned();

    debug!(logger, "Received deployment event: {:?}", event);

    match event {
        DeploymentEvent::Add {
            deployment_name: _,
            subgraph_id,
            node_id: _,
        } => {
            Box::new(
                provider
                    .start(subgraph_id.clone())
                    .then(move |result| -> Result<(), _> {
                        match result {
                            Ok(()) => Ok(()),
                            Err(SubgraphProviderError::AlreadyRunning(_)) => Ok(()),
                            Err(e) => {
                                // Errors here are likely an issue with the subgraph.
                                // These will be recorded eventually so that they can be displayed
                                // in a UI.
                                error!(
                                    logger,
                                    "Subgraph instance failed to start: {}", e;
                                    "subgraph_id" => subgraph_id.to_string()
                                );
                                Ok(())
                            }
                        }
                    }),
            )
        }
        DeploymentEvent::Remove {
            deployment_name: _,
            subgraph_id,
            node_id: _,
        } => Box::new(
            provider
                .stop(subgraph_id)
                .then(|result| match result {
                    Ok(()) => Ok(()),
                    Err(SubgraphProviderError::NotRunning(_)) => Ok(()),
                    Err(e) => Err(e),
                }).map_err(CancelableError::Error),
        ),
    }
}
