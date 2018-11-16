use failure::*;
use futures::sync::mpsc::{channel, Sender};
use std::sync::Mutex;

use graph::components::store::*;
use graph::prelude::*;
use graph::web3::types::H256;

/// A mock `ChainHeadUpdateListener`
pub struct MockChainHeadUpdateListener {}

impl ChainHeadUpdateListener for MockChainHeadUpdateListener {
    fn start(&mut self) {}
}

impl EventProducer<ChainHeadUpdate> for MockChainHeadUpdateListener {
    fn take_event_stream(
        &mut self,
    ) -> Option<Box<Stream<Item = ChainHeadUpdate, Error = ()> + Send>> {
        unimplemented!();
    }
}

pub struct MockStore {
    entities: Vec<Entity>,
    subgraph_deployments: Mutex<Vec<(SubgraphDeploymentName, SubgraphId, NodeId)>>,
    subgraph_deployment_event_senders: Mutex<Vec<Sender<DeploymentEvent>>>,
}

impl MockStore {
    /// Creates a new mock `Store`.
    pub fn new() -> Self {
        // Create a few test entities
        let mut entities = vec![];
        for (i, name) in ["Joe", "Jeff", "Linda"].iter().enumerate() {
            let mut entity = Entity::new();
            entity.insert("id".to_string(), Value::String(i.to_string()));
            entity.insert("name".to_string(), Value::String(name.to_string()));
            entities.push(entity);
        }

        MockStore {
            entities,
            subgraph_deployments: Default::default(),
            subgraph_deployment_event_senders: Default::default(),
        }
    }

    fn emit_deployment_event(&self, event: DeploymentEvent) {
        for sender in self
            .subgraph_deployment_event_senders
            .lock()
            .unwrap()
            .iter()
        {
            let sender = sender.clone();
            let event = event.clone();
            tokio::spawn(future::lazy(move || {
                sender
                    .send(event)
                    .map(|_| ())
                    .map_err(|e| panic!("sender error: {}", e))
            }));
        }
    }
}

impl Store for MockStore {
    fn get(&self, key: EntityKey) -> Result<Option<Entity>, QueryExecutionError> {
        if key.entity_type == "User" {
            self.entities
                .iter()
                .find(|entity| {
                    let id = entity.get("id").unwrap();
                    match *id {
                        Value::String(ref s) => s == &key.entity_id,
                        _ => false,
                    }
                }).map(|entity| Some(entity.clone()))
                .ok_or_else(|| unimplemented!())
        } else {
            unimplemented!()
        }
    }

    fn find(&self, _query: EntityQuery) -> Result<Vec<Entity>, QueryExecutionError> {
        Ok(self.entities.clone())
    }

    fn add_subgraph_if_missing(&self, _: SubgraphId, _: EthereumBlockPointer) -> Result<(), Error> {
        unimplemented!();
    }

    fn block_ptr(&self, _: SubgraphId) -> Result<EthereumBlockPointer, Error> {
        unimplemented!();
    }

    fn set_block_ptr_with_no_changes(
        &self,
        _: SubgraphId,
        _: EthereumBlockPointer,
        _: EthereumBlockPointer,
    ) -> Result<(), Error> {
        unimplemented!();
    }

    fn transact_block_operations(
        &self,
        _: SubgraphId,
        _: EthereumBlockPointer,
        _: EthereumBlockPointer,
        _: Vec<EntityOperation>,
    ) -> Result<(), Error> {
        unimplemented!();
    }

    fn apply_set_operation(&self, _: EntityOperation, _: String) -> Result<(), Error> {
        Ok(())
    }

    fn revert_block_operations(
        &self,
        _: SubgraphId,
        _: EthereumBlockPointer,
        _: EthereumBlockPointer,
    ) -> Result<(), Error> {
        unimplemented!();
    }

    fn subscribe(&self, _: Vec<SubgraphEntityPair>) -> EntityChangeStream {
        unimplemented!();
    }

    fn count_entities(&self, _: SubgraphId) -> Result<u64, Error> {
        unimplemented!();
    }
}

impl SubgraphDeploymentStore for MockStore {
    fn read_by_node_id(
        &self,
        by_node_id: NodeId,
    ) -> Result<Vec<(SubgraphDeploymentName, SubgraphId)>, Error> {
        Ok(self
            .subgraph_deployments
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(name, subgraph_id, node_id)| {
                if *node_id == by_node_id {
                    Some((name.to_owned(), subgraph_id.to_owned()))
                } else {
                    None
                }
            }).collect())
    }

    fn write(
        &self,
        name: SubgraphDeploymentName,
        new_subgraph_id: SubgraphId,
        new_node_id: NodeId,
    ) -> Result<(), Error> {
        let mut deployments = self.subgraph_deployments.lock().unwrap();

        // Find first deployment with matching name
        for deployment in deployments.iter_mut() {
            if deployment.0 == name {
                // Keep old values for event
                let old_subgraph_id = deployment.1.to_owned();
                let old_node_id = deployment.2.to_owned();

                // Update deployment
                deployment.1 = new_subgraph_id.clone();
                deployment.2 = new_node_id.clone();

                // Send events
                self.emit_deployment_event(DeploymentEvent::Remove {
                    deployment_name: name.clone(),
                    subgraph_id: old_subgraph_id.clone(),
                    node_id: old_node_id.clone(),
                });
                self.emit_deployment_event(DeploymentEvent::Add {
                    deployment_name: name.clone(),
                    subgraph_id: new_subgraph_id.clone(),
                    node_id: new_node_id.clone(),
                });

                return Ok(());
            }
        }

        // Add deployment
        deployments.push((name.clone(), new_subgraph_id.clone(), new_node_id.clone()));

        // Send event
        self.emit_deployment_event(DeploymentEvent::Add {
            deployment_name: name.clone(),
            subgraph_id: new_subgraph_id.clone(),
            node_id: new_node_id.clone(),
        });
        Ok(())
    }

    fn read(&self, by_name: SubgraphDeploymentName) -> Result<Option<(SubgraphId, NodeId)>, Error> {
        Ok(self
            .subgraph_deployments
            .lock()
            .unwrap()
            .iter()
            .find(|(name, _, _)| *name == by_name)
            .map(|(_, subgraph_id, node_id)| (subgraph_id.to_owned(), node_id.to_owned())))
    }

    fn remove(&self, by_name: SubgraphDeploymentName) -> Result<bool, Error> {
        let mut deployments = self.subgraph_deployments.lock().unwrap();

        // Find deployment by name
        let pos_opt = deployments.iter().position(|(name, _, _)| *name == by_name);

        // If found
        if let Some(pos) = pos_opt {
            // Remove the deployment
            let (deployment_name, subgraph_id, node_id) = deployments.swap_remove(pos);

            // Send event
            self.emit_deployment_event(DeploymentEvent::Remove {
                deployment_name,
                subgraph_id,
                node_id,
            });

            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn deployment_events(
        &self,
        by_node_id: NodeId,
    ) -> Box<Stream<Item = DeploymentEvent, Error = Error> + Send> {
        let (sender, receiver) = channel(100);
        self.subgraph_deployment_event_senders
            .lock()
            .unwrap()
            .push(sender);
        Box::new(
            receiver
                .filter(move |event| match event {
                    DeploymentEvent::Add { node_id, .. } => *node_id == by_node_id,
                    DeploymentEvent::Remove { node_id, .. } => *node_id == by_node_id,
                }).map_err(|()| format_err!("receiver error")),
        )
    }
}

impl ChainStore for MockStore {
    type ChainHeadUpdateListener = MockChainHeadUpdateListener;

    fn genesis_block_ptr(&self) -> Result<EthereumBlockPointer, Error> {
        unimplemented!();
    }

    fn upsert_blocks<'a, B, E>(&self, _: B) -> Box<Future<Item = (), Error = E> + Send + 'a>
    where
        B: Stream<Item = EthereumBlock, Error = E> + Send + 'a,
        E: From<Error> + Send + 'a,
    {
        unimplemented!();
    }

    fn attempt_chain_head_update(&self, _: u64) -> Result<Vec<H256>, Error> {
        unimplemented!();
    }

    fn chain_head_updates(&self) -> Self::ChainHeadUpdateListener {
        unimplemented!();
    }

    fn chain_head_ptr(&self) -> Result<Option<EthereumBlockPointer>, Error> {
        unimplemented!();
    }

    fn block(&self, _: H256) -> Result<Option<EthereumBlock>, Error> {
        unimplemented!();
    }

    fn ancestor_block(
        &self,
        _: EthereumBlockPointer,
        _: u64,
    ) -> Result<Option<EthereumBlock>, Error> {
        unimplemented!();
    }
}

pub struct FakeStore;

impl Store for FakeStore {
    fn get(&self, _: EntityKey) -> Result<Option<Entity>, QueryExecutionError> {
        unimplemented!();
    }

    fn find(&self, _: EntityQuery) -> Result<Vec<Entity>, QueryExecutionError> {
        unimplemented!();
    }

    fn add_subgraph_if_missing(&self, _: SubgraphId, _: EthereumBlockPointer) -> Result<(), Error> {
        unimplemented!();
    }

    fn block_ptr(&self, _: SubgraphId) -> Result<EthereumBlockPointer, Error> {
        unimplemented!();
    }

    fn set_block_ptr_with_no_changes(
        &self,
        _: SubgraphId,
        _: EthereumBlockPointer,
        _: EthereumBlockPointer,
    ) -> Result<(), Error> {
        unimplemented!();
    }

    fn transact_block_operations(
        &self,
        _: SubgraphId,
        _: EthereumBlockPointer,
        _: EthereumBlockPointer,
        _: Vec<EntityOperation>,
    ) -> Result<(), Error> {
        unimplemented!();
    }

    fn apply_set_operation(&self, _: EntityOperation, _: String) -> Result<(), Error> {
        unimplemented!()
    }

    fn revert_block_operations(
        &self,
        _: SubgraphId,
        _: EthereumBlockPointer,
        _: EthereumBlockPointer,
    ) -> Result<(), Error> {
        unimplemented!();
    }

    fn subscribe(&self, _: Vec<SubgraphEntityPair>) -> EntityChangeStream {
        unimplemented!();
    }

    fn count_entities(&self, _: SubgraphId) -> Result<u64, Error> {
        unimplemented!();
    }
}

impl ChainStore for FakeStore {
    type ChainHeadUpdateListener = MockChainHeadUpdateListener;

    fn genesis_block_ptr(&self) -> Result<EthereumBlockPointer, Error> {
        unimplemented!();
    }

    fn upsert_blocks<'a, B, E>(&self, _: B) -> Box<Future<Item = (), Error = E> + Send + 'a>
    where
        B: Stream<Item = EthereumBlock, Error = E> + Send + 'a,
        E: From<Error> + Send + 'a,
    {
        unimplemented!();
    }

    fn attempt_chain_head_update(&self, _: u64) -> Result<Vec<H256>, Error> {
        unimplemented!();
    }

    fn chain_head_updates(&self) -> Self::ChainHeadUpdateListener {
        unimplemented!();
    }

    fn chain_head_ptr(&self) -> Result<Option<EthereumBlockPointer>, Error> {
        unimplemented!();
    }

    fn block(&self, _: H256) -> Result<Option<EthereumBlock>, Error> {
        unimplemented!();
    }

    fn ancestor_block(
        &self,
        _: EthereumBlockPointer,
        _: u64,
    ) -> Result<Option<EthereumBlock>, Error> {
        unimplemented!();
    }
}
